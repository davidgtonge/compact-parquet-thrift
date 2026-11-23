# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate
follows [Semantic Versioning](https://semver.org/) (pre-1.0: a minor bump may
break the API).

## [0.3.0] - 2026-11-23

First release prepared for crates.io.

### Breaking

- `Error`, `values::Error`, `pages::Error` and `encode::EncodeError` are
  `#[non_exhaustive]`; exhaustive `match`es need a wildcard arm.
- `values::PlainValue::from_le_bytes(&[u8]) -> Self` is replaced by
  `from_le_prefix(&[u8]) -> Option<Self>`, which cannot panic on short input.
- Removed `encode::write_footer_copied` and `CopiedColumnMeta`,
  `CopiedRowGroupMeta`, `CopiedSchemaLeaf`. They were unused and wrote a
  fixed `created_by` string; build footers with `write_footer`.
- The encoder returns `EncodeError::Overflow` when a schema, row-group or
  column count, or a fixed width, does not fit its Thrift field, instead of
  silently truncating it.

### Added

- `Display` and `core::error::Error` for every error type;
  `pages::Error::source` exposes the Thrift or value error underneath.
- `pages::PageKind::name`.
- `Clone`, `Copy` and `Debug` for `encode::Column` and `RowGroupFooter`;
  `Debug` for `pages::Identity`.
- Crate-level documentation with tested examples, docs on every public item
  including its error cases, and a README whose examples run as doctests.
- `tests/malformed.rs`: random and mutated inputs for every parser.
- Cargo metadata, `rust-version = "1.83"`, and a CI workflow (fmt, clippy,
  tests, docs, wasm32 build, MSRV check, package dry run).

### Fixed

- `parquet::scan_footer` no longer panics on a row group that repeats its
  column list with more than 512 columns in total.

### Changed

- Lints: `clippy::all` and `clippy::pedantic` denied, `unsafe_code`
  forbidden, `missing_docs` and panic/print restriction lints denied in
  library code.
- Decoder state lives in explicit enums (level framing, RLE runs, delta and
  Snappy phases, page stages) instead of optional fields; one varint
  implementation replaces three. Encoded output is byte-for-byte unchanged.
- The DuckDB and local-corpus tests are opt-in through the `DUCKDB` and
  `PARQUET_DATA_DIR` environment variables instead of fixed local paths.
- `DELTA-CHANGELOG.md` and `PAGE-CHANGELOG.md` are folded into the README.

## [0.2.0]

Internal version consumed by path: Thrift Compact codec, footer and page
header visitors, streaming page assembly with raw Snappy and CRC-32, value
decoders for PLAIN, RLE, dictionary and delta encodings, and the Parquet
encoder with chunk statistics.
