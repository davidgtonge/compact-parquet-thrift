# compact-parquet-thrift

A small **allocation-free** Thrift Compact Protocol reader/writer in Rust,
**Parquet footer and page-header visitors**, incremental **Parquet value decoders**, and a **streaming page assembler**. Designed for a column-oriented,
range-readable R2/S3 Parquet reader.

- `#![no_std]`, `#![forbid(unsafe_code)]`, zero required dependencies.
- **Reader:** borrowed bytes; no `Vec`, `String`, or generated object graph.
- **Writer:** arbitrary `Sink`, including a caller-owned fixed-capacity buffer.
- **Struct scope:** field ID delta state is stack-local, without a heap stack.
- **Unknown fields:** recursively skip, or pass through raw encoded values.
- **Parquet:** inspect row groups, flat column names, page offsets, compressed
  sizes and codecs without constructing the full Thrift metadata tree.
- **Defensive parsing:** checked offsets and integer conversions, bounded
  recursion, strict varints, and unrecognized-tag errors.
- **Value decoding:** resumable PLAIN/RLE/dictionary/delta streams and borrowed byte arrays.
- **Page assembly:** bounded Thrift headers, V1/V2 levels, nullable rows, raw Snappy streaming, CRC32.

## Quick start

```rust
use compact_parquet_thrift::{Reader, Writer, SliceSink, WireType};

// Thrift Compact: { 1: i32 = 123, 2: bool = true }
let mut output = [0u8; 32];
let mut writer = Writer::new(SliceSink::new(&mut output));
writer.structure(|w| {
    w.field_i32(1, 123)?;
    w.field_bool(2, true)
})?;

let written = writer.into_inner();
let mut reader = Reader::new(written.as_written());
let mut fields = reader.structure()?;
let first = fields.next_field()?.unwrap();
assert_eq!((first.id, first.kind), (1, WireType::I32));
assert_eq!(fields.i32()?, 123);
assert_eq!(fields.next_field()?.unwrap().bool_value()?, true);
assert_eq!(fields.next_field()?, None);
# Ok::<(), compact_parquet_thrift::Error>(())
```

Nested structs use `writer.field_struct(id, |w| ...)` and
`reader.structure()?`. Lists and maps are written with
`writer.field_header(id, WireType::List)?` followed by
`writer.list_header(element_type, len)?` and the element values. A list's
length is declared by its caller; this low-level codec does not count elements.

### Borrowing raw field values

```rust
let mut input = Reader::new(footer_bytes);
let mut struct_reader = input.structure()?;
writer.structure(|w| {
    while let Some(field) = struct_reader.next_field()? {
        let original = struct_reader.raw_value(field.kind)?;
        // Keep field payload unchanged, but regenerate its delta-encoded header.
        w.raw_field(field.id, field.kind, original)?;
    }
    Ok(())
})?;
```

This enables **forward-compatible metadata rewriting** without deserializing
unknown nested fields. It is valid because each nested struct carries its own
field-ID delta context. Do not copy a raw *field header* into a different field
order; only the encoded *value* is safe to reuse this way.

### Inspect a real Parquet file

```bash
cargo run --example inspect -- my-data.parquet
cargo run --example stream_values
cargo run --example stream_page
cargo test
```

`examples/inspect.rs` reads **only the tail and footer**, then visits columns.
`parquet::footer_range(file_size, &trailer)` works equally with an R2 range
request. `parquet::scan_footer(&footer, visitor)` borrows the returned bytes.

## Architecture

```text
R2 head + trailing 8 bytes
       |
       v
parquet::footer_range()          -- locate footer
       |
       v
R2 range fetch for footer       -- transport owns input buffer
       |
       v
Reader<'a>                      -- borrow metadata, parse field-by-field
       |
       +--> parquet::scan_footer()   -- visitor receives ColumnChunkInfo<'a>
       |
       +--> Writer<Sink>             -- optionally regenerate metadata
```

`Writer` delegates all output to a `Sink`, so it can write directly into a
preallocated WASM memory region. The `Sink` implementation can also flush an
internal buffer to a file or transport, and `Writer` maintains no output buffer.


## Streaming Parquet pages

The `pages` module connects the existing Thrift and value decoders. A
`PageFramer` parses headers from arbitrary R2 input slices and emits borrowed
body fragments. A `PageAssembler` handles a **single** page's compressed
payload, with a pluggable `Inflate` implementation and a `FlatPage` value
assembler. The caller creates a new assembler for each page and can reuse all
scratch memory.

```text
R2/S3 Range Stream -> PageFramer -> PageAssembler -> PageEvent sink
                         |               |        (no row Vec)
                    Thrift header        +--> optional CRC32
                                         +--> Snappy / codec adapter
                                         +--> V1/V2 definition levels
                                         +--> PLAIN / RLE / DELTA decoder
```

A flat nullable data page uses **one bit per row** of caller-owned `present`
workspace. Required fields and dictionary pages don't need the bitmap.
Numeric events contain a row number and physical value; strings produce
`BytesStart`, borrowed `BytesChunk`, and `BytesEnd` with a stable row number.
Dictionary pages use `Location::Dictionary(index)`; dictionary-encoded data
pages produce `Value::DictionaryId(index)` rather than allocating strings.

For a compact runnable usage example see `examples/stream_page.rs`. For
V1/V2 + Snappy + dictionary + delta integration tests see `tests/pages.rs`.
See `PAGE-CHANGELOG.md` for exact format support and limitations.

**Memory, excluding the R2 transport:**

| Resource | Ownership / cost |
| --- | --- |
| Page-header buffer | caller-owned, typically 256-1024 B |
| Nullable presence bits | `ceil(num_values / 8)` bytes |
| Snappy history / output buffer | caller-owned, configurable; e.g. 64 KiB / 32 KiB |
| Delta int mini-block widths | typically 4 B |
| Delta-length string lengths | `4 * non_null_values` bytes |
| Delta-prefix string parts | `8 * non_null_values` bytes + previous-value buffer |

`RawSnappy` supports backreferences only up to the capacity of its history
buffer. The codec trait accommodates externally supplied streaming ZSTD or
other codecs, but only raw Snappy and uncompressed bytes are implemented here.
Nested/repeated fields, logical-type conversions, BYTE_STREAM_SPLIT, and
ALP remain outside this initial page assembly layer.

## Streaming Parquet value decoders

This second layer is in `src/values/`. It is **not** the Thrift Compact protocol:
these decoders handle binary Parquet *page contents*, after any required
decompression, with a separate state machine per column/page.

| Decoder | Parquet physical / encoding | Retained memory |
| --- | --- | --- |
| `PlainValues::<i32/i64/f32/f64/Int96>` | PLAIN numeric / INT96 | 12-byte carry |
| `PlainBool` | PLAIN BOOLEAN, LSB first | counters only |
| `ByteArrays` | PLAIN BYTE_ARRAY | 4-byte length prefix |
| `FixedByteArrays` | PLAIN FIXED_LEN_BYTE_ARRAY | counters only |
| `HybridRle` | RLE / bit-packed indices or levels | 32-byte partial group |
| `DictionaryIds` | RLE_DICTIONARY / legacy PLAIN_DICTIONARY | `HybridRle` |
| `Levels::v1` / `Levels::v2` | repetition or definition levels | `HybridRle` |
| `RleBool` | RLE BOOLEAN (4-byte length prefix in either page version) | `Levels` |
| `DeltaBinary::<i32 / i64>` | DELTA_BINARY_PACKED (5) | caller's mini-block widths, normally 4 bytes |
| `DeltaLengthByteArrays` | DELTA_LENGTH_BYTE_ARRAY (6) | `4 * count` bytes of caller-owned lengths |
| `DeltaByteArrays` | DELTA_BYTE_ARRAY (7) | `8 * count` bytes for prefix/suffix lengths + previous-value buffer |

Every `feed(&bytes, callback)` returns the *number of input bytes consumed*.
It may return less than `bytes.len()` on reaching the declared logical count
or the end of a V1/V2 framed level stream. It may hold a tiny incomplete
number, prefix or bitpacked group until the next call. Always check
`finish()` at the end of the stream to detect truncation. For a nullable
column, the count passed to the *value* decoder is the number of present
(non-null) values, not the number of row/level positions. A consumer may
retain numeric values, update an aggregate, or fill a caller-owned buffer.

`ByteArrays` and `FixedByteArrays` use `Start(length)`, `Chunk(&[u8])`,
`End` events because an individual value could be much bigger than a
network chunk. `Chunk` *borrows the caller's input*; it is not copied. The
`max_value_bytes` limit on `ByteArrays` prevents untrusted lengths from
requesting unbounded memory. `DictionaryIds` returns numeric indices, not
materialized strings.

```rust
use compact_parquet_thrift::values::PlainValues;

let mut decoder = PlainValues::<f64>::new(3);
let mut total = 0.0;
for bytes in payload.chunks(7) { // e.g. arbitrary R2 / decompressor chunks
    let used = decoder.feed(bytes, |v| total += v)?;
    assert_eq!(used, bytes.len());
}
decoder.finish()?;
```

### Important boundaries

- These are **encoding decoders**, not full Parquet readers. They expect
  *uncompressed page value bytes*, and do not fetch R2 objects or parse
  compressed page data if used directly. The `pages` layer now handles
  page headers, nullable row alignment and Snappy decompression; row-group
  coordination remains external. V1 compresses the full body; V2 may compress just
  the values section.
- Level framing differs: V1 uses a 4-byte encoded-length prefix and V2
  passes length from `DataPageHeaderV2`. Dictionary IDs have a one-byte
  **bit-width** prefix, not a length prefix. RLE BOOLEAN has a length prefix
  even in page V2.
- Reconstructing nullable rows from a stream of levels followed by a stream
  of values needs coordination. A bounded bitmap, replay, or independent
  cursors can handle this; a no-buffer single-pass merger is **not** generally
  possible when levels precede all values. `FlatPage` now uses a caller-owned
  1-bit-per-row presence bitmap.
- `BYTE_STREAM_SPLIT` physically separates individual byte lanes: a strictly
  sequential reader cannot reconstruct the first value until later lanes
  arrive without buffering or seeking. Delta encodings are now supported,
  with explicit caller-owned scratch space. BYTE_STREAM_SPLIT is not yet supported.
- The codec does not automatically check unused bytes at a page boundary;
  the page layer must compare consumed bytes against the section length.
- Callbacks are synchronous and infallible. For backpressure, consume data
  in small chunks and suspend the *next* read. A failed callback cannot
  recover the partially consumed page.

See `examples/stream_values.rs`, `examples/stream_delta.rs`,
`tests/values.rs`, and `tests/delta.rs`.

### Delta encodings: allocation-free streaming

`DELTA_BINARY_PACKED` (5) retains only one mini-block's **width bytes**. In
Parquet's common 128-value/4-mini-block layout, pass a four-byte scratch:

```rust
use compact_parquet_thrift::values::DeltaBinary;

let mut widths = [0u8; 4];
let mut reader = DeltaBinary::<i64>::new(physical_values, &mut widths);
for chunk in incoming_bytes {
    let used = reader.feed(chunk, |value| consume(value))?;
    // `used` can be less than `chunk.len()`: the stream ended mid-chunk.
}
reader.finish()?;
```

The decoder validates the encoded value count, the block and mini-block
geometry, width limits, and signed/unsigned LEB128. It accepts nonzero
padding bits and arbitrary **unused** mini-block widths, consuming the *whole*
last mini-block before stopping. Signed arithmetic wraps at `i32`/`i64`
physical width, as Parquet requires. The caller can re-use its width scratch
for the next stream via `reset(expected_count)`.

**Length-prefixed delta strings cannot be fully stateless in one forward
pass.** The format stores all lengths *before* the concatenated bytes. Rather
than internally allocating, the APIs require the caller to provide exactly
bounded workspaces:

```rust
use compact_parquet_thrift::values::{DeltaLengthByteArrays, DeltaByteArrays, DeltaByteParts};

let mut widths = [0u8; 4];
let mut lengths = [0u32; 1024];
let mut decoder = DeltaLengthByteArrays::new(1024, &mut lengths, &mut widths, 65536)?;
// decoder.feed(input, |event| { ... })?;

let mut parts = [DeltaByteParts::default(); 1024];
let mut widths = [0u8; 4];
let mut previous = [0u8; 65536];
let mut decoder = DeltaByteArrays::new(1024, &mut parts, &mut widths, &mut previous, 65536)?;
// decoder.feed(input, |event| { ... })?;
```

`DeltaLengthByteArrays` emits `ByteArrayEvent::{Start, Chunk, End}` with
zero-copy `Chunk` fragments borrowed from the *input*. `DeltaByteArrays`
reconstructs front-compressed values with **in-place suffix replacement**;
it emits the prefix borrowed from `previous`, then suffix chunks borrowed
from input. Its callback uses a higher-ranked lifetime so these fragments
cannot escape. For FIXED_LEN_BYTE_ARRAY, use `.with_fixed_length(n)?`.

Both can resume at any byte boundary, including middle of a delta header,
bit-packed value, length stream, or suffix. Their `feed()` return value is the
exact number of bytes consumed. They are **one-pass** readers; reducing string
metadata storage further would require a seek/replay-based two-pass mode.

Memory required (not counting transport/decompression):

| Decoder | Additional caller-owned memory |
|---|---|
| DELTA_BINARY_PACKED | `miniblocks_per_block` bytes |
| DELTA_LENGTH_BYTE_ARRAY | `4 * physical_value_count` bytes + mini widths |
| DELTA_BYTE_ARRAY | `8 * physical_value_count` bytes + mini widths + maximum reconstructed string length |

`max_length` limits individual values. The caller must bound physical value
counts and page sizes before allocating workspace; constructors reject
undersized arrays. All decoders stop after the declared stream, not at the
end of an R2 chunk. A pluggable decompressor and Parquet page assembler are implemented in
`src/pages/`, reusing these value decoders.

For DELTA_BYTE_ARRAY, the first value in each *standard* page is reconstructed
from an empty previous value. A historical parquet-mr bug once allowed
cross-page prefix references; that non-standard recovery mode is not
implemented here.

The source and tests have been statically checked, but no `cargo` or `rustc`
was available in the current environment. **Tests have not been executed.**
Run `cargo test`, `cargo fmt --check`, and `cargo clippy --all-targets`
locally before using this crate. The tests include reference encoding bytes,
an independent test-only encoder, all chunk sizes, multi-block streams,
32/64-bit wrapping, arbitrary padding, negative lengths, invalid prefixes,
fixed-length values, and truncated input.


## Protocol coverage

| Construct | Read | Write |
| --- | --- | --- |
| Field delta headers + STOP | Yes | Yes |
| Signed byte, i16/i32/i64 zigzag varints | Yes | Yes |
| Double (little-endian) | Yes | Yes |
| Binary/string (borrowed UTF-8 only when requested) | Yes | Yes |
| Boolean field header and collection values | Yes | Yes |
| Struct, list, set, map | Yes | Yes |
| UUID | Yes | Yes |
| Unknown-field skip / raw pass-through | Yes | Yes |
| Thrift RPC message envelope | No | No |

This targets **Thrift Compact serialization of Parquet metadata**, not Thrift
RPC messages. The page layer implements flat-column V1/V2 assembly with raw
Snappy decompression, an uncompressed path and an adapter for other codecs.
Generated schema models, encrypted metadata, complete nested schema validation,
other built-in compression formats, and BYTE_STREAM_SPLIT are not implemented.

## Streaming boundaries and memory model

The Thrift metadata codec itself never allocates. `Reader<'a>` reads from one **already available
contiguous byte slice**, because zero-copy borrows require that storage to remain
valid. In particular, this initial implementation does **not yet** parse a
footer split across asynchronously arriving network chunks; the transport must
buffer the footer. The `Writer` does support streaming sinks directly.

For Parquet, buffering just the footer (not the full object) is often reasonable:
its size is known from the trailing eight bytes. A true push-based incremental
Thrift parser would need a resumable state machine, a caller-owned bounded
container stack, and chunk callbacks for binary fields spanning buffers.
That is deliberately **not** claimed here.

The nesting limit defaults to 64; `Reader::with_max_depth` and
`Writer::with_max_depth` allow a tighter limit. The codec does not enforce
global byte or collection-element limits; set those at the transport/application
boundary before accepting untrusted input. `raw_value` traverses the encoded
structure to find its end rather than copying it. A writer error can leave
partial bytes in its sink: discard/reset the output after errors.

## Tests and reference vectors

`tests/compact.rs` includes independent wire-format vectors generated with
Apache Thrift's Python `TCompactProtocol`, including a nested struct, list,
map, negative integers, inline booleans, malformed inputs, and page header V2.
`tests/parquet.rs` exercises two genuine Parquet files with nested schema and
list columns (small upstream pyogrio test fixtures). `tests/values.rs` tests
chunk-size invariance (including single-byte feeds), integer/floating-point
PLAIN, byte-array fragments, dictionary indices, RLE bit widths 0–32, level
framing, and malformed inputs.

`tests/pages.rs` adds V1/V2 nullable page assembly, CRC, raw Snappy with
backreferences, dictionary pages and dictionary IDs, all three delta formats,
and an end-to-end test against a genuine Parquet INT64 dictionary column.

**Validation status:** The Rust toolchain was unavailable in this authoring
environment, so the Cargo suite has **not** been executed. Static delimiter
checks passed; the CRC32 routine was independently checked against zlib, and
real fixture dictionary bytes were independently decoded. Before relying on
this implementation, run `cargo test`, `cargo clippy --all-targets`, and
`cargo fmt --check`.

## Format references

- [Apache Thrift Compact Protocol](https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md)
- [Official Parquet Thrift IDL](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift)
- [Parquet encodings, including all three DELTA formats](https://parquet.apache.org/docs/file-format/data-pages/encodings/)
- [Parquet data page layout](https://parquet.apache.org/docs/file-format/data-pages/)
