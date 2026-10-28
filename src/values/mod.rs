//! Allocation-free, resumable Parquet *value* decoders.
//!
//! Each `feed` call consumes as many bytes as possible and returns the number
//! consumed. The caller owns network buffers and may split input anywhere,
//! including inside a number, varint, dictionary run or byte-array value.
//!
//! Values are delivered synchronously to callbacks. The decoders never own
//! output collections or allocate. They do not decompress pages, parse page
//! headers or interpret logical types.

mod plain;
mod rle;
mod levels;
mod delta_binary;
mod delta_bytes;

pub use plain::{ByteArrayEvent, ByteArrays, FixedByteArrays, Int96, PlainBool, PlainValue, PlainValues};
pub use rle::{DictionaryIds, HybridRle};
pub use levels::{Levels, RleBool};
pub use delta_binary::{DeltaBinary, DeltaInt};
pub use delta_bytes::{DeltaLengthByteArrays, DeltaByteArrays, DeltaByteParts};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Truncated,
    InvalidBitWidth(u8),
    InvalidVarint,
    InvalidRun,
    ExcessValues,
    ValueOutOfRange { value: u32, max: u32 },
    ValueTooLarge { length: u32, limit: u32 },
    InvalidFixedLength,
    FrameTooLarge { length: u32, limit: u32 },
    LengthMismatch,
    EmptyDictionary,
    InvalidDeltaHeader,
    InvalidDeltaLength(i32),
    InvalidPrefix { prefix: u32, previous_length: u32 },
    DeltaWorkspaceTooSmall { needed: usize, available: usize },
    OutputBufferTooSmall { needed: usize, available: usize },
}
