# compact-parquet-thrift

An allocation-free Thrift Compact Protocol codec and bounded-memory Parquet
reader and writer for Rust. Built for range-reading Parquet from object
storage inside WebAssembly, where memory is fixed and input arrives in
arbitrary chunks.

- `#![no_std]`, no `unsafe`, no dependencies, no allocation in the library.
- **Thrift:** zero-copy `Reader` over a borrowed slice, `Writer` into any
  `Sink` (including a fixed caller-owned buffer), unknown-field skip and raw
  pass-through.
- **Parquet metadata:** locate the footer from the last eight bytes, visit
  every column chunk's offsets, sizes, codec and statistics, parse page
  headers.
- **Pages:** incremental page framing, raw Snappy, CRC-32, V1 and V2 data
  pages, dictionary pages, nullable flat columns.
- **Values:** resumable PLAIN, RLE / bit-packed, dictionary ids and all three
  DELTA encodings; input may be split at any byte.
- **Encoder:** Parquet files, row groups and footers for the same subset,
  with min/max and null-count statistics.
- **Untrusted input:** checked lengths and integer conversions, bounded
  nesting, caller-set size limits; malformed data returns an error.

## Usage

```toml
[dependencies]
compact-parquet-thrift = "0.2"
```

Thrift Compact round trip:

```rust
use compact_parquet_thrift::{Reader, SliceSink, WireType, Writer};

// { 1: i32 = 123, 2: bool = true }
let mut output = [0u8; 32];
let mut writer = Writer::new(SliceSink::new(&mut output));
writer.structure(|w| {
    w.field_i32(1, 123)?;
    w.field_bool(2, true)
})?;
let written = writer.into_inner();

let mut reader = Reader::new(written.as_written());
let mut fields = reader.structure()?;
let first = fields.next_field()?.ok_or(compact_parquet_thrift::Error::EndOfInput)?;
assert_eq!((first.id, first.kind), (1, WireType::I32));
assert_eq!(fields.i32()?, 123);
# Ok::<(), compact_parquet_thrift::Error>(())
```

Write a Parquet file into a caller-owned buffer:

```rust
use compact_parquet_thrift::encode::{
    scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY,
};

let values = [3i64, 1, 4, 1, 5];
let columns = [Column {
    name: "n",
    rows: values.len(),
    codec: SNAPPY,
    present: None,
    values: ColumnValues::I64(&values),
}];
let mut places = [ChunkPlace::default()];
let mut scratch = vec![0u8; scratch_len(values.len(), 64).unwrap()];
let mut snappy_table = vec![0u32; 1024];
let mut file = vec![0u8; 4096];
let len = write_file(&mut file, 5, &columns, &mut places, &mut scratch, &mut snappy_table)?;
assert_eq!(&file[len - 4..len], b"PAR1");
# Ok::<(), compact_parquet_thrift::encode::EncodeError>(())
```

Decode values from chunks as they arrive:

```rust
use compact_parquet_thrift::values::PlainValues;

let page = [10u8, 0, 0, 0, 20, 0, 0, 0, 30, 0, 0, 0];
let mut decoder = PlainValues::<i32>::new(3);
let mut sum = 0;
for chunk in page.chunks(5) {
    decoder.feed(chunk, |v| sum += v)?;
}
decoder.finish()?;
assert_eq!(sum, 60);
# Ok::<(), compact_parquet_thrift::values::Error>(())
```

More in [`examples/`](examples): `inspect` prints a file's column chunks
from its footer alone; `stream_page`, `stream_values` and `stream_delta`
decode pages and value streams in small fragments.

## Reading a file from object storage

```text
HEAD + last 8 bytes  -> parquet::footer_range   -> footer byte range
footer range request -> parquet::scan_footer     -> ColumnChunkInfo per chunk
chunk range stream   -> pages::PageFramer        -> page header + body fragments
page body fragments  -> pages::PageAssembler     -> PageEvent per value or null
```

The caller owns every buffer. A nullable page needs `ceil(rows / 8)` bytes
for its presence bitmap, since Parquet stores all definition levels before
any value.

| Workspace | Size |
| --- | --- |
| Page header buffer | a few hundred bytes |
| Presence bitmap (optional columns) | `ceil(rows / 8)` bytes |
| Snappy history / output | caller's choice, e.g. 64 KiB / 32 KiB |
| Delta mini-block widths | 4 bytes for the common layout |
| `DELTA_LENGTH_BYTE_ARRAY` lengths | 4 bytes per value |
| `DELTA_BYTE_ARRAY` prefix/suffix | 8 bytes per value, plus the longest value |

`ColumnSpec` sets the largest page, value count and byte-array value the
decoder accepts, so untrusted headers cannot request unbounded work.
Callbacks run synchronously; a page that fails part-way may already have
emitted some values, so commit per-page results only after `finish()`.

## Scope

Supported: flat columns (no repetition, definition level 0 or 1); PLAIN,
RLE boolean, `RLE_DICTIONARY` / `PLAIN_DICTIONARY`, `DELTA_BINARY_PACKED`,
`DELTA_LENGTH_BYTE_ARRAY` and `DELTA_BYTE_ARRAY`; uncompressed and raw
Snappy, with other codecs pluggable through the `Inflate` trait.

Not supported: nested or repeated columns, `BYTE_STREAM_SPLIT`, legacy
`BIT_PACKED` levels, built-in ZSTD / GZIP / LZ4, encrypted footers, page
indexes, logical type conversion, Thrift RPC messages. Each fails with an
explicit error rather than a guess.

The Thrift `Reader` needs the whole footer as one contiguous slice; page
headers can be retried as more bytes arrive (`Error::EndOfInput`).

## Minimum supported Rust version

Rust 1.83.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets && cargo test --doc
cargo build --target wasm32-unknown-unknown --release
```

Two optional checks use local resources: set `DUCKDB` to a DuckDB CLI to
cross-check encoded files, and `PARQUET_DATA_DIR` to a directory of Parquet
files to decode them against their own metadata (`tests/wcol_files.rs`).

Test fixtures: `tests/fixtures/*.bin` were generated with Apache Thrift's
Python `TCompactProtocol`; the two `.parquet` files come from the MIT-licensed
[pyogrio](https://github.com/geopandas/pyogrio) test suite.

## References

- [Thrift Compact Protocol](https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md)
- [Parquet Thrift IDL](https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift)
- [Parquet encodings](https://parquet.apache.org/docs/file-format/data-pages/encodings/)

## License

Licensed under either of Apache License 2.0 or MIT license, at your option.
