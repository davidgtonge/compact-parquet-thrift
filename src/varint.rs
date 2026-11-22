//! ULEB128 varints shared by the Thrift writer, the value decoders and the
//! page encoder.

/// The longest ULEB128 encoding of a `u64`.
pub(crate) const MAX_LEN: usize = 10;

/// Encode `value` into the front of `buf`; returns the encoded length.
pub(crate) fn encode(mut value: u64, buf: &mut [u8; MAX_LEN]) -> usize {
    for (len, slot) in buf.iter_mut().enumerate() {
        let low = value.to_le_bytes()[0] & 0x7f;
        value >>= 7;
        if value == 0 {
            *slot = low;
            return len + 1;
        }
        *slot = low | 0x80;
    }
    MAX_LEN
}

/// A varint is longer than ten bytes or wider than 64 bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Overflow;

/// Resumable ULEB128 decoder for input split at arbitrary byte boundaries.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Decoder {
    value: u64,
    shift: u8,
}

impl Decoder {
    pub(crate) const fn new() -> Self {
        Self { value: 0, shift: 0 }
    }

    /// Feed one byte; returns the value once its last byte arrives.
    pub(crate) fn push(&mut self, byte: u8) -> Result<Option<u64>, Overflow> {
        // The tenth byte may only contribute bit 63.
        if self.shift == 63 && byte > 1 {
            return Err(Overflow);
        }
        self.value |= u64::from(byte & 0x7f) << self.shift;
        if byte & 0x80 == 0 {
            let value = self.value;
            *self = Self::new();
            Ok(Some(value))
        } else {
            self.shift += 7;
            Ok(None)
        }
    }
}
