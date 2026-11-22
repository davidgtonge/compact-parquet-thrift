//! Run with `cargo run --example stream_values`.
//! Simulate arbitrary chunks as if they were read from an R2 object body.

use compact_parquet_thrift::values::{ByteArrayEvent, ByteArrays, DictionaryIds, PlainValues};

fn main() -> Result<(), compact_parquet_thrift::values::Error> {
    // A Parquet PLAIN INT32 page: 10, 20, 30, 40.
    let mut ints = PlainValues::<i32>::new(4);
    let mut sum = 0i64;
    let encoded: [u8; 16] = [10, 0, 0, 0, 20, 0, 0, 0, 30, 0, 0, 0, 40, 0, 0, 0];
    for chunk in encoded.chunks(3) {
        let n = ints.feed(chunk, |value| sum += i64::from(value))?;
        assert_eq!(n, chunk.len());
    }
    ints.finish()?;
    assert_eq!(sum, 100);

    // PLAIN BYTE_ARRAY page, including a value split across transport chunks.
    let bytes = [5, 0, 0, 0, b'h', b'e', b'l', b'l', b'o'];
    let mut strings = ByteArrays::new(1, 1_024);
    for chunk in bytes.chunks(2) {
        strings.feed(chunk, |event| match event {
            ByteArrayEvent::Start(length) => println!("start ({length} bytes)"),
            ByteArrayEvent::Chunk(part) => println!("chunk: {part:?}"),
            ByteArrayEvent::End => println!("end"),
        })?;
    }
    strings.finish()?;

    // Dictionary indices: bit width 2, RLE run of four ID=2 values.
    let mut ids = DictionaryIds::new(4, 3)?;
    for chunk in [2, 8, 2].chunks(1) {
        ids.feed(chunk, |index| println!("dictionary id={index}"))?;
    }
    ids.finish()?;
    Ok(())
}
