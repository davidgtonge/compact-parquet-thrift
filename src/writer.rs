use crate::protocol::{zigzag_i64, Error, Result, WireType};

/// A minimal byte sink. Streaming transports can implement this trait;
/// the codec never asks for a growable `Vec` or ownership of its output.
pub trait Sink {
    /// Append `bytes` to the output.
    ///
    /// # Errors
    ///
    /// Implementation-defined; [`Error::OutputFull`] for bounded sinks.
    fn write(&mut self, bytes: &[u8]) -> Result<()>;
}

/// A fixed-capacity, caller-owned destination. Exhaustion returns [`Error::OutputFull`].
pub struct SliceSink<'a> {
    buffer: &'a mut [u8],
    used: usize,
}

impl<'a> SliceSink<'a> {
    /// An empty sink writing into the front of `buffer`.
    pub fn new(buffer: &'a mut [u8]) -> Self {
        Self { buffer, used: 0 }
    }

    /// Bytes written so far.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.used
    }

    /// Whether nothing has been written.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.used == 0
    }

    /// The written prefix of the buffer.
    #[must_use]
    pub fn as_written(&self) -> &[u8] {
        &self.buffer[..self.used]
    }
}

impl Sink for SliceSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        let end = self
            .used
            .checked_add(bytes.len())
            .ok_or(Error::OutputFull)?;
        let dst = self
            .buffer
            .get_mut(self.used..end)
            .ok_or(Error::OutputFull)?;
        dst.copy_from_slice(bytes);
        self.used = end;
        Ok(())
    }
}

/// Encodes Thrift Compact structures without allocating.
///
/// Every struct resets the field ID delta to zero; scope is restored even if
/// its closure returns an error. A write error leaves partial output in the
/// sink, which must then be discarded or reset by the caller.
pub struct Writer<S: Sink> {
    sink: S,
    last_id: i16,
    depth: u16,
    max_depth: u16,
}

impl<S: Sink> Writer<S> {
    /// A writer with the default nesting limit (64).
    pub const fn new(sink: S) -> Self {
        Self {
            sink,
            last_id: 0,
            depth: 0,
            max_depth: 64,
        }
    }

    /// A writer with a custom nesting limit.
    pub const fn with_max_depth(sink: S, max_depth: u16) -> Self {
        Self {
            sink,
            last_id: 0,
            depth: 0,
            max_depth,
        }
    }

    /// Release the sink.
    pub fn into_inner(self) -> S {
        self.sink
    }

    /// Write a struct: `f` writes its fields, then the STOP byte follows.
    ///
    /// # Errors
    ///
    /// [`Error::DepthLimit`], or any error from `f` or the sink.
    pub fn structure(&mut self, f: impl FnOnce(&mut Self) -> Result<()>) -> Result<()> {
        if self.depth >= self.max_depth {
            return Err(Error::DepthLimit);
        }
        self.depth += 1;
        let saved_id = core::mem::replace(&mut self.last_id, 0);
        let result = f(self).and_then(|()| self.sink.write(&[0]));
        self.last_id = saved_id;
        self.depth -= 1;
        result
    }

    /// Write a field header. Call this before its payload (except boolean
    /// fields, whose values live in their headers).
    ///
    /// # Errors
    ///
    /// [`Error::UnexpectedStop`] outside a struct, or a sink error.
    pub fn field_header(&mut self, id: i16, kind: WireType) -> Result<()> {
        if self.depth == 0 {
            return Err(Error::UnexpectedStop);
        }
        let delta = i32::from(id) - i32::from(self.last_id);
        if let Ok(delta @ 1..=15) = u8::try_from(delta) {
            self.sink.write(&[(delta << 4) | kind as u8])?;
        } else {
            self.sink.write(&[kind as u8])?;
            self.i16(id)?;
        }
        self.last_id = id;
        Ok(())
    }

    /// Write a boolean field (its value lives in the header).
    ///
    /// # Errors
    ///
    /// As [`Self::field_header`].
    pub fn field_bool(&mut self, id: i16, value: bool) -> Result<()> {
        self.field_header(
            id,
            if value {
                WireType::BoolTrue
            } else {
                WireType::BoolFalse
            },
        )
    }

    /// Write an `i16` field.
    ///
    /// # Errors
    ///
    /// As [`Self::field_header`].
    pub fn field_i16(&mut self, id: i16, value: i16) -> Result<()> {
        self.field_header(id, WireType::I16)?;
        self.i16(value)
    }

    /// Write an `i32` field.
    ///
    /// # Errors
    ///
    /// As [`Self::field_header`].
    pub fn field_i32(&mut self, id: i16, value: i32) -> Result<()> {
        self.field_header(id, WireType::I32)?;
        self.i32(value)
    }

    /// Write an `i64` field.
    ///
    /// # Errors
    ///
    /// As [`Self::field_header`].
    pub fn field_i64(&mut self, id: i16, value: i64) -> Result<()> {
        self.field_header(id, WireType::I64)?;
        self.i64(value)
    }

    /// Write a binary or string field.
    ///
    /// # Errors
    ///
    /// As [`Self::field_header`], plus [`Error::IntegerOverflow`] for values
    /// longer than `u32::MAX`.
    pub fn field_binary(&mut self, id: i16, value: &[u8]) -> Result<()> {
        self.field_header(id, WireType::Binary)?;
        self.binary(value)
    }

    /// Write a double field.
    ///
    /// # Errors
    ///
    /// As [`Self::field_header`].
    pub fn field_double(&mut self, id: i16, value: f64) -> Result<()> {
        self.field_header(id, WireType::Double)?;
        self.f64(value)
    }

    /// Write a nested struct field; `f` writes its fields.
    ///
    /// # Errors
    ///
    /// As [`Self::field_header`] and [`Self::structure`].
    pub fn field_struct(&mut self, id: i16, f: impl FnOnce(&mut Self) -> Result<()>) -> Result<()> {
        self.field_header(id, WireType::Struct)?;
        self.structure(f)
    }

    /// Preserve an unknown field's encoded value while regenerating its field
    /// header (because field-ID deltas depend on the preceding field).
    ///
    /// # Errors
    ///
    /// [`Error::UnexpectedWireType`] for a boolean with a payload, or as
    /// [`Self::field_header`].
    pub fn raw_field(&mut self, id: i16, kind: WireType, encoded_value: &[u8]) -> Result<()> {
        if kind.is_bool() && !encoded_value.is_empty() {
            return Err(Error::UnexpectedWireType);
        }
        self.field_header(id, kind)?;
        self.sink.write(encoded_value)
    }

    /// Write one raw byte.
    ///
    /// # Errors
    ///
    /// A sink error.
    pub fn byte(&mut self, value: u8) -> Result<()> {
        self.sink.write(&[value])
    }

    /// Write a signed byte.
    ///
    /// # Errors
    ///
    /// A sink error.
    pub fn i8(&mut self, value: i8) -> Result<()> {
        self.sink.write(&value.to_le_bytes())
    }

    /// Write an unsigned varint (ULEB128).
    ///
    /// # Errors
    ///
    /// A sink error.
    pub fn var_u64(&mut self, value: u64) -> Result<()> {
        let mut buf = [0u8; crate::varint::MAX_LEN];
        let len = crate::varint::encode(value, &mut buf);
        self.sink.write(&buf[..len])
    }

    /// Write an unsigned 32-bit varint.
    ///
    /// # Errors
    ///
    /// A sink error.
    pub fn var_u32(&mut self, value: u32) -> Result<()> {
        self.var_u64(u64::from(value))
    }

    /// Write a zigzag varint `i16`.
    ///
    /// # Errors
    ///
    /// A sink error.
    pub fn i16(&mut self, value: i16) -> Result<()> {
        // Thrift encodes signed i16 as zigzag i32; the zigzag of an i16 fits a u16 varint.
        self.var_u64(zigzag_i64(i64::from(value)))
    }

    /// Write a zigzag varint `i32`.
    ///
    /// # Errors
    ///
    /// A sink error.
    pub fn i32(&mut self, value: i32) -> Result<()> {
        self.var_u64(zigzag_i64(i64::from(value)))
    }

    /// Write a zigzag varint `i64`.
    ///
    /// # Errors
    ///
    /// A sink error.
    pub fn i64(&mut self, value: i64) -> Result<()> {
        self.var_u64(zigzag_i64(value))
    }

    /// Write a little-endian double.
    ///
    /// # Errors
    ///
    /// A sink error.
    pub fn f64(&mut self, value: f64) -> Result<()> {
        self.sink.write(&value.to_le_bytes())
    }

    /// Write a boolean collection element (1 true, 2 false).
    ///
    /// # Errors
    ///
    /// A sink error.
    pub fn bool_element(&mut self, value: bool) -> Result<()> {
        self.byte(if value { 1 } else { 2 })
    }

    /// Write a length-prefixed binary value.
    ///
    /// # Errors
    ///
    /// [`Error::IntegerOverflow`] if longer than `u32::MAX`, or a sink error.
    pub fn binary(&mut self, bytes: &[u8]) -> Result<()> {
        let len = u32::try_from(bytes.len()).map_err(|_| Error::IntegerOverflow)?;
        self.var_u32(len)?;
        self.sink.write(bytes)
    }

    /// Write a sixteen-byte UUID.
    ///
    /// # Errors
    ///
    /// A sink error.
    pub fn uuid(&mut self, bytes: &[u8; 16]) -> Result<()> {
        self.sink.write(bytes)
    }

    /// Write a list header; the caller then writes `len` elements.
    ///
    /// # Errors
    ///
    /// A sink error.
    pub fn list_header(&mut self, element: WireType, len: u32) -> Result<()> {
        if let Ok(short @ 0..15) = u8::try_from(len) {
            self.byte((short << 4) | element as u8)
        } else {
            self.byte(0xf0 | element as u8)?;
            self.var_u32(len)
        }
    }

    /// Write a set header (same wire format as a list).
    ///
    /// # Errors
    ///
    /// A sink error.
    pub fn set_header(&mut self, element: WireType, len: u32) -> Result<()> {
        self.list_header(element, len)
    }

    /// Write a map header; empty maps omit the key/value types.
    ///
    /// # Errors
    ///
    /// A sink error.
    pub fn map_header(&mut self, key: WireType, value: WireType, len: u32) -> Result<()> {
        self.var_u32(len)?;
        if len != 0 {
            self.byte(((key as u8) << 4) | value as u8)?;
        }
        Ok(())
    }
}
