//! cargo run --example stream_delta
//! Forward-only decode of Parquet DELTA_BINARY_PACKED and DELTA_BYTE_ARRAY.

use compact_parquet_thrift::values::{
    ByteArrayEvent, DeltaBinary, DeltaByteArrays, DeltaByteParts, Error,
};

fn main() -> Result<(), Error> {
    // INT32 values [1, 2, 3, 4, 5]: block size=128, 4 mini-blocks,
    // count=5, first=1, min delta=1, all width=0.
    let encoded = [0x80, 0x01, 4, 5, 2, 2, 0, 0, 0, 0];
    let mut widths = [0; 4];
    let mut ints = DeltaBinary::<i32>::new(5, &mut widths);
    let mut sum = 0;
    for part in encoded.chunks(2) {
        let consumed = ints.feed(part, |v| sum += v)?;
        assert_eq!(consumed, part.len());
    }
    ints.finish()?;
    assert_eq!(sum, 15);

    // A single DELTA_BYTE_ARRAY string "hello" has both encoded prefix
    // and suffix lengths followed by raw bytes. This fixture is deliberately
    // small, but the decoder can split inputs anywhere.
    let mut string = [0u8; 15];
    // Each length stream uses the standard four-field delta header.
    // Prefix length = 0, suffix length = 5.
    string[..5].copy_from_slice(&[0x80, 1, 4, 1, 0]);
    string[5..10].copy_from_slice(&[0x80, 1, 4, 1, 10]);
    string[10..].copy_from_slice(b"hello");
    let mut parts = [DeltaByteParts::default(); 1];
    let mut widths = [0u8; 4];
    let mut previous = [0u8; 64];
    let mut strings = DeltaByteArrays::new(1, &mut parts, &mut widths, &mut previous, 64)?;
    for part in string.chunks(3) {
        strings.feed(part, |event| match event {
            ByteArrayEvent::Start(n) => println!("start: {n}"),
            ByteArrayEvent::Chunk(bytes) => println!("part: {:?}", bytes),
            ByteArrayEvent::End => println!("end"),
        })?;
    }
    strings.finish()?;
    Ok(())
}
