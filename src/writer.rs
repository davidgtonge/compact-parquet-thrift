use crate::protocol::{zigzag_i64, Error, Result, WireType};

/// A minimal byte sink. Streaming transports can implement this trait;
/// the codec never asks for a growable `Vec` or ownership of its output.
pub trait Sink {
    fn write(&mut self, bytes: &[u8]) -> Result<()>;
}

/// A fixed-capacity, caller-owned destination. Exhaustion returns OutputFull.
pub struct SliceSink<'a> {
    buffer: &'a mut [u8],
    used: usize,
}

impl<'a> SliceSink<'a> {
    pub fn new(buffer: &'a mut [u8]) -> Self { Self { buffer, used: 0 } }
    pub fn len(&self) -> usize { self.used }
    pub fn is_empty(&self) -> bool { self.used == 0 }
    pub fn as_written(&self) -> &[u8] { &self.buffer[..self.used] }
}

impl Sink for SliceSink<'_> {
    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        let end = self.used.checked_add(bytes.len()).ok_or(Error::OutputFull)?;
        let dst = self.buffer.get_mut(self.used..end).ok_or(Error::OutputFull)?;
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
    pub fn new(sink: S) -> Self {
        Self { sink, last_id: 0, depth: 0, max_depth: 64 }
    }

    pub fn with_max_depth(sink: S, max_depth: u16) -> Self {
        Self { sink, last_id: 0, depth: 0, max_depth }
    }

    pub fn into_inner(self) -> S { self.sink }

    pub fn structure(&mut self, f: impl FnOnce(&mut Self) -> Result<()>) -> Result<()> {
        if self.depth >= self.max_depth { return Err(Error::DepthLimit); }
        self.depth += 1;
        let saved_id = core::mem::replace(&mut self.last_id, 0);
        let result = f(self).and_then(|_| self.sink.write(&[0]));
        self.last_id = saved_id;
        self.depth -= 1;
        result
    }

    /// Write a field header. Call this before its payload (except boolean
    /// fields, whose values live in their headers).
    pub fn field_header(&mut self, id: i16, kind: WireType) -> Result<()> {
        if self.depth == 0 { return Err(Error::UnexpectedStop); }
        let delta = (id as i32) - (self.last_id as i32);
        if (1..=15).contains(&delta) {
            self.sink.write(&[((delta as u8) << 4) | kind as u8])?;
        } else {
            self.sink.write(&[kind as u8])?;
            self.i16(id)?;
        }
        self.last_id = id;
        Ok(())
    }

    pub fn field_bool(&mut self, id: i16, value: bool) -> Result<()> {
        self.field_header(id, if value { WireType::BoolTrue } else { WireType::BoolFalse })
    }

    pub fn field_i16(&mut self, id: i16, value: i16) -> Result<()> {
        self.field_header(id, WireType::I16)?;
        self.i16(value)
    }

    pub fn field_i32(&mut self, id: i16, value: i32) -> Result<()> {
        self.field_header(id, WireType::I32)?;
        self.i32(value)
    }

    pub fn field_i64(&mut self, id: i16, value: i64) -> Result<()> {
        self.field_header(id, WireType::I64)?;
        self.i64(value)
    }

    pub fn field_binary(&mut self, id: i16, value: &[u8]) -> Result<()> {
        self.field_header(id, WireType::Binary)?;
        self.binary(value)
    }

    pub fn field_double(&mut self, id: i16, value: f64) -> Result<()> {
        self.field_header(id, WireType::Double)?;
        self.f64(value)
    }

    pub fn field_struct(&mut self, id: i16, f: impl FnOnce(&mut Self) -> Result<()>) -> Result<()> {
        self.field_header(id, WireType::Struct)?;
        self.structure(f)
    }

    /// Preserve an unknown field's encoded value while regenerating its field
    /// header (because field-ID deltas depend on the preceding field).
    pub fn raw_field(&mut self, id: i16, kind: WireType, encoded_value: &[u8]) -> Result<()> {
        if kind.is_bool() && !encoded_value.is_empty() {
            return Err(Error::UnexpectedWireType);
        }
        self.field_header(id, kind)?;
        self.sink.write(encoded_value)
    }

    pub fn byte(&mut self, value: u8) -> Result<()> {
        self.sink.write(&[value])
    }

    pub fn i8(&mut self, value: i8) -> Result<()> {
        self.byte(value as u8)
    }

    pub fn var_u64(&mut self, mut value: u64) -> Result<()> {
        let mut buf = [0u8; 10];
        let mut len = 0;
        loop {
            let b = (value & 0x7f) as u8;
            value >>= 7;
            buf[len] = b | if value != 0 { 0x80 } else { 0 };
            len += 1;
            if value == 0 { break; }
        }
        self.sink.write(&buf[..len])
    }

    pub fn var_u32(&mut self, value: u32) -> Result<()> {
        self.var_u64(value as u64)
    }

    pub fn i16(&mut self, value: i16) -> Result<()> {
        // Thrift encodes signed i16 as zigzag i32 (compatible with a u16 varint).
        self.var_u32(zigzag_i64(value as i64) as u32)
    }

    pub fn i32(&mut self, value: i32) -> Result<()> {
        self.var_u32(zigzag_i64(value as i64) as u32)
    }

    pub fn i64(&mut self, value: i64) -> Result<()> {
        self.var_u64(zigzag_i64(value))
    }

    pub fn f64(&mut self, value: f64) -> Result<()> {
        self.sink.write(&value.to_le_bytes())
    }

    pub fn bool_element(&mut self, value: bool) -> Result<()> {
        self.byte(if value { 1 } else { 2 })
    }

    pub fn binary(&mut self, bytes: &[u8]) -> Result<()> {
        let len = u32::try_from(bytes.len()).map_err(|_| Error::IntegerOverflow)?;
        self.var_u32(len)?;
        self.sink.write(bytes)
    }

    pub fn uuid(&mut self, bytes: &[u8; 16]) -> Result<()> {
        self.sink.write(bytes)
    }

    pub fn list_header(&mut self, element: WireType, len: u32) -> Result<()> {
        if len < 15 {
            self.byte(((len as u8) << 4) | element as u8)
        } else {
            self.byte(0xf0 | element as u8)?;
            self.var_u32(len)
        }
    }

    pub fn set_header(&mut self, element: WireType, len: u32) -> Result<()> {
        self.list_header(element, len)
    }

    pub fn map_header(&mut self, key: WireType, value: WireType, len: u32) -> Result<()> {
        self.var_u32(len)?;
        if len != 0 { self.byte(((key as u8) << 4) | value as u8)?; }
        Ok(())
    }
}
