use core::convert::TryFrom;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    EndOfInput,
    OutputFull,
    InvalidVarint,
    IntegerOverflow,
    InvalidWireType(u8),
    UnexpectedWireType,
    UnexpectedStop,
    InvalidBool(u8),
    InvalidUtf8,
    DepthLimit,
    InvalidMagic,
    InvalidFooterLength,
}

/// Compact Protocol type tags (not Thrift binary-protocol TType values).
/// A boolean struct field encodes its value in the field's type tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WireType {
    BoolTrue = 1,
    BoolFalse = 2,
    Byte = 3,
    I16 = 4,
    I32 = 5,
    I64 = 6,
    Double = 7,
    Binary = 8,
    List = 9,
    Set = 10,
    Map = 11,
    Struct = 12,
    Uuid = 13,
}

impl WireType {
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

pub(crate) const fn zigzag_i64(n: i64) -> u64 {
    ((n as u64) << 1) ^ ((n >> 63) as u64)
}

pub(crate) const fn unzigzag_u64(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}
