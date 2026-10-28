# Streaming page assembly

Added `src/pages/` with a bounded-memory, no_std pipeline for *flat* Parquet columns:

- `PageFramer`: incremental Thrift Compact page-header recognition, payload
  byte-length framing, and borrowed body slices. Header storage is caller-owned.
- `FlatPage`: V1/V2 definition levels, null-aware scalar and string events,
  dictionary values/IDs, and all previously implemented primitive/delta encodings.
- `PageAssembler`: joins decompression, V1/V2 level placement, value decoding,
  length verification, and optional compressed-payload CRC32 checks.
- `Inflate`: codec adapter trait. `Identity` emits borrowed input bytes;
  `RawSnappy` is an incremental raw-Snappy block decoder using caller-provided
  history and output buffers. No full-page buffer is required.
- End-to-end page fixtures in `tests/pages.rs` and `examples/stream_page.rs`.

## Important limitations

- Flat scalar columns only: max repetition level 0, max definition level 0 or 1.
  Nested repetition and arbitrary definition-level depths are rejected.
- Dictionary data pages emit checked IDs, **not expanded values**. A caller must
  persist dictionary entries from the dictionary page and resolve IDs as needed.
- PLAIN, RLE boolean, RLE_DICTIONARY, all three DELTA encodings are supported;
  BYTE_STREAM_SPLIT, BIT_PACKED levels and newer ALP encodings are not.
- Snappy and UNCOMPRESSED are built in. Other codecs can be supplied by
  implementing `Inflate` with a reusable decoder context. Do not claim general
  ZSTD support from this crate alone.
- Chunk-compressed Snappy backreferences longer than the supplied history
  window return `SnappyHistoryTooSmall`. Increase the buffer or reject them.
- A nullable page needs `ceil(num_values / 8)` bytes of definition bitmap,
  even though decoded values themselves stream without allocation.
- Delta strings require caller-owned `lengths` / `parts`, and DELTA_BYTE_ARRAY
  additionally requires a caller-owned previous-value buffer.
- Callbacks are synchronous and infallible. On an error the consumer may have
  received a partial page; commit aggregates/output **after** `finish()`.
- CRC32 is verified if a PageHeader contains it. File-level footer integrity,
  page-index seek and logical type conversion are outside the page assembler.

## Verification

`tests/pages.rs` includes one-byte fragmentation, V1/V2 nullable data,
Snappy with overlapping copies, dictionary values/indices, delta integer and
string pages, CRC validation, and malformed inputs.

**The Rust tests are included but have not been run in this environment**:
`cargo` and `rustc` are not available. Before production use, run:

```sh
cargo fmt --all
cargo test
cargo clippy --all-targets -- -D warnings
```
