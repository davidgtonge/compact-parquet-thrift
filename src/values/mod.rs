//! Allocation-free, resumable Parquet *value* decoders.
//!
//! Each `feed` call consumes as many bytes as possible and returns the number
//! consumed. The caller owns network buffers and may split input anywhere,
//! including inside a number, varint, dictionary run or byte-array value.
//!
//! Values are delivered synchronously to callbacks. The decoders never own
//! output collections or allocate. They do not decompress pages, parse page
//! headers or interpret logical types.

mod delta_binary;
mod delta_bytes;
mod levels;
mod plain;
mod rle;

use core::fmt;

pub use delta_binary::{DeltaBinary, DeltaInt};
pub use delta_bytes::{DeltaByteArrays, DeltaByteParts, DeltaLengthByteArrays};
pub use levels::{Levels, RleBool};
pub use plain::{
    ByteArrayEvent, ByteArrays, FixedByteArrays, Int96, PlainBool, PlainValue, PlainValues,
};
pub use rle::{DictionaryIds, HybridRle};

/// Result of a value decoder.
pub type Result<T> = core::result::Result<T, Error>;

/// A malformed or out-of-bounds Parquet value stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The stream ended before its declared value count.
    Truncated,
    /// A bit width is larger than the physical type allows.
    InvalidBitWidth(u8),
    /// A ULEB128 varint is too long or too wide.
    InvalidVarint,
    /// An RLE / bit-packed run header is zero or too long.
    InvalidRun,
    /// More values arrived than were declared.
    ExcessValues,
    /// A level or dictionary index exceeds its maximum.
    ValueOutOfRange {
        /// Decoded value.
        value: u32,
        /// Largest allowed value.
        max: u32,
    },
    /// A byte-array value exceeds the configured limit.
    ValueTooLarge {
        /// Declared value length.
        length: u32,
        /// Configured limit.
        limit: u32,
    },
    /// A fixed-length byte array was declared with length zero.
    InvalidFixedLength,
    /// A length-prefixed level or boolean stream exceeds its page.
    FrameTooLarge {
        /// Declared frame length.
        length: u32,
        /// Bytes available in the page.
        limit: u32,
    },
    /// A declared count or length disagrees with the data.
    LengthMismatch,
    /// Dictionary indices were declared but the dictionary is empty.
    EmptyDictionary,
    /// A `DELTA_BINARY_PACKED` header has an invalid block layout or first value.
    InvalidDeltaHeader,
    /// A delta-encoded byte-array length is negative.
    InvalidDeltaLength(i32),
    /// A `DELTA_BYTE_ARRAY` prefix is longer than the previous value.
    InvalidPrefix {
        /// Declared prefix length.
        prefix: u32,
        /// Length of the previous value.
        previous_length: u32,
    },
    /// A caller-supplied delta workspace is too small.
    DeltaWorkspaceTooSmall {
        /// Elements required.
        needed: usize,
        /// Elements supplied.
        available: usize,
    },
    /// A caller-supplied output buffer is too small.
    OutputBufferTooSmall {
        /// Bytes required.
        needed: usize,
        /// Bytes supplied.
        available: usize,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Truncated => f.write_str("value stream is truncated"),
            Self::InvalidBitWidth(width) => write!(f, "invalid bit width {width}"),
            Self::InvalidVarint => f.write_str("invalid varint in value stream"),
            Self::InvalidRun => f.write_str("invalid RLE / bit-packed run header"),
            Self::ExcessValues => f.write_str("more values than declared"),
            Self::ValueOutOfRange { value, max } => {
                write!(f, "value {value} exceeds maximum {max}")
            }
            Self::ValueTooLarge { length, limit } => {
                write!(f, "value of {length} bytes exceeds limit of {limit}")
            }
            Self::InvalidFixedLength => f.write_str("fixed-length byte array has length zero"),
            Self::FrameTooLarge { length, limit } => {
                write!(f, "frame of {length} bytes exceeds limit of {limit}")
            }
            Self::LengthMismatch => f.write_str("declared count or length does not match data"),
            Self::EmptyDictionary => f.write_str("dictionary indices without a dictionary"),
            Self::InvalidDeltaHeader => f.write_str("invalid DELTA_BINARY_PACKED header"),
            Self::InvalidDeltaLength(length) => write!(f, "negative delta length {length}"),
            Self::InvalidPrefix {
                prefix,
                previous_length,
            } => write!(
                f,
                "prefix of {prefix} bytes exceeds previous value of {previous_length}"
            ),
            Self::DeltaWorkspaceTooSmall { needed, available } => write!(
                f,
                "delta workspace too small: need {needed}, have {available}"
            ),
            Self::OutputBufferTooSmall { needed, available } => write!(
                f,
                "output buffer too small: need {needed} bytes, have {available}"
            ),
        }
    }
}

impl core::error::Error for Error {}

impl From<crate::varint::Overflow> for Error {
    fn from(_: crate::varint::Overflow) -> Self {
        Self::InvalidVarint
    }
}

/// `Ok` once a decoder has emitted everything, else [`Error::Truncated`].
pub(crate) const fn complete(done: bool) -> Result<()> {
    if done {
        Ok(())
    } else {
        Err(Error::Truncated)
    }
}

/// How much of a value with `left` bytes outstanding fits in `available` input.
pub(crate) fn chunk_len(left: u32, available: usize) -> u32 {
    u32::try_from(available).map_or(left, |available| left.min(available))
}
