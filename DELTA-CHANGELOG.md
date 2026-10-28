# Delta value streaming additions

Added three dependency-free, `no_std`, allocation-free incremental Parquet decoders:

- `DeltaBinary::<i32/i64>` for `DELTA_BINARY_PACKED` (5).
- `DeltaLengthByteArrays` for `DELTA_LENGTH_BYTE_ARRAY` (6).
- `DeltaByteArrays` for `DELTA_BYTE_ARRAY` (7), including optional fixed-length validation.

## Memory model

- Integer decoder: a caller-provided array holding one block's mini-block widths; no materialization of decoded integer values.
- Length-array decoder: a caller-provided `u32` array of length equal to the number of physical values.
- Prefix-array decoder: a caller-provided `DeltaByteParts` array (eight bytes per value), plus an in-place previous-value buffer.
- All three accept arbitrarily fragmented input and return the exact input bytes consumed.
- No decompression or page assembly is included. The callback remains synchronous and infallible; on error, discard the page's partial results.

## Verification

`tests/delta.rs` includes independent encoding helpers and reference layout bytes. Tests exercise arbitrary chunk sizes, padding, wraparound, errors, and exact stream boundaries.

**Cargo tests were not executed**, because no Rust compiler is installed in the authoring environment. Before using in production:

```sh
cargo fmt --all
cargo test
cargo clippy --all-targets -- -D warnings
```

See `README.md` for the API and workspace requirements.
