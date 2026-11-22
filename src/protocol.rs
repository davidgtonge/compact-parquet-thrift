use core::fmt;

/// Result of a Thrift Compact read or write.
pub type Result<T> = core::result::Result<T, Error>;

/// A Thrift Compact Protocol or Parquet footer framing error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The input ended before the value was complete. For a page header
    /// prefix this means "offer more bytes", not corruption.
    EndOfInput,
    /// A fixed-capacity sink has no room for the next write.
    OutputFull,
    /// A varint is longer than ten bytes or overflows 64 bits.
    InvalidVarint,
    /// A decoded integer does not fit its declared width, or a length or
    /// offset computation overflowed.
    IntegerOverflow,
    /// A type nibble is not a Compact Protocol type tag.
    InvalidWireType(u8),
    /// A field has a different wire type from the one its ID requires.
    UnexpectedWireType,
    /// A field header was written outside any struct.
    UnexpectedStop,
    /// A boolean collection element is not 0, 1 or 2.
    InvalidBool(u8),
    /// A string field is not valid UTF-8.
    InvalidUtf8,
    /// Nesting exceeded the reader's or writer's depth limit.
    DepthLimit,
    /// The Parquet trailer does not end in `PAR1`.
    InvalidMagic,
    /// The Parquet footer length does not fit inside the file.
    InvalidFooterLength,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EndOfInput => f.write_str("unexpected end of Thrift input"),
            Self::OutputFull => f.write_str("output buffer is full"),
            Self::InvalidVarint => f.write_str("invalid Thrift varint"),
            Self::IntegerOverflow => f.write_str("integer overflow in Thrift value"),
            Self::InvalidWireType(tag) => write!(f, "invalid Thrift Compact type tag {tag}"),
            Self::UnexpectedWireType => f.write_str("Thrift field has an unexpected wire type"),
            Self::UnexpectedStop => f.write_str("Thrift field written outside a struct"),
            Self::InvalidBool(byte) => write!(f, "invalid Thrift boolean element {byte}"),
            Self::InvalidUtf8 => f.write_str("Thrift string is not valid UTF-8"),
            Self::DepthLimit => f.write_str("Thrift nesting depth limit exceeded"),
            Self::InvalidMagic => f.write_str("Parquet trailer is missing PAR1 magic"),
            Self::InvalidFooterLength => f.write_str("Parquet footer length is out of range"),
        }
    }
}

impl core::error::Error for Error {}

/// Compact Protocol type tags (not Thrift binary-protocol `TType` values).
/// A boolean struct field encodes its value in the field's type tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WireType {
    /// Boolean `true` (field tag) or boolean element type (collections).
    BoolTrue = 1,
    /// Boolean `false` (field tag).
    BoolFalse = 2,
    /// Signed 8-bit integer.
    Byte = 3,
    /// Zigzag varint 16-bit integer.
    I16 = 4,
    /// Zigzag varint 32-bit integer.
    I32 = 5,
    /// Zigzag varint 64-bit integer.
    I64 = 6,
    /// Little-endian IEEE 754 double.
    Double = 7,
    /// Varint length followed by bytes (binary or string).
    Binary = 8,
    /// List header followed by elements.
    List = 9,
    /// Set header (same wire format as a list) followed by elements.
    Set = 10,
    /// Map header followed by key/value pairs.
    Map = 11,
    /// Nested struct terminated by a STOP byte.
    Struct = 12,
    /// Sixteen raw bytes.
    Uuid = 13,
}

impl WireType {
    /// The value of a boolean *struct field*, which lives in its type tag.
    ///
    /// # Errors
    ///
    /// [`Error::UnexpectedWireType`] if this is not a boolean tag.
    pub fn field_bool(self) -> Result<bool> {
        match self {
            Self::BoolTrue => Ok(true),
            Self::BoolFalse => Ok(false),
            _ => Err(Error::UnexpectedWireType),
        }
    }

    pub(crate) const fn is_bool(self) -> bool {
        matches!(self, Self::BoolTrue | Self::BoolFalse)
    }
}

impl TryFrom<u8> for WireType {
    type Error = Error;

    fn try_from(raw: u8) -> Result<Self> {
        Ok(match raw {
            1 => Self::BoolTrue,
            2 => Self::BoolFalse,
            3 => Self::Byte,
            4 => Self::I16,
            5 => Self::I32,
            6 => Self::I64,
            7 => Self::Double,
            8 => Self::Binary,
            9 => Self::List,
            10 => Self::Set,
            11 => Self::Map,
            12 => Self::Struct,
            13 => Self::Uuid,
            _ => return Err(Error::InvalidWireType(raw)),
        })
    }
}

// Zigzag maps signed to unsigned by reinterpreting bits; the casts are the encoding.
#[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
pub(crate) const fn zigzag_i64(n: i64) -> u64 {
    ((n as u64) << 1) ^ ((n >> 63) as u64)
}

#[allow(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
pub(crate) const fn unzigzag_u64(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}
