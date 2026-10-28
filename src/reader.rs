use core::ops::{Deref, DerefMut};

use crate::protocol::{unzigzag_u64, Error, Result, WireType};

/// A zero-copy cursor over a complete, borrowed byte window.
///
/// For a Parquet page header, the window can be grown on `EndOfInput` and
/// reparsed. For a footer, read its length first, then pass the footer bytes.
#[derive(Debug)]
pub struct Reader<'a> {
    input: &'a [u8],
    offset: usize,
    depth: u16,
    max_depth: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Field {
    pub id: i16,
    pub kind: WireType,
}

impl Field {
    pub fn bool_value(self) -> Result<bool> {
        self.kind.field_bool()
    }

    pub fn expect(self, kind: WireType) -> Result<()> {
        if self.kind == kind { Ok(()) } else { Err(Error::UnexpectedWireType) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListHeader {
    pub len: u32,
    pub element: WireType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MapHeader {
    pub len: u32,
    /// Empty maps do not encode key/value types.
    pub types: Option<(WireType, WireType)>,
}

/// A scoped struct field iterator. The field-ID delta starts at zero for each
/// struct; nested structs borrow the same byte cursor and have independent IDs.
///
/// Call `next_field()` until it returns None (the STOP marker). Dropping an
/// unfinished struct simply releases the borrow; it does not skip its bytes.
pub struct StructReader<'r, 'a> {
    reader: &'r mut Reader<'a>,
    last_id: i16,
    ended: bool,
}

impl<'r, 'a> Deref for StructReader<'r, 'a> {
    type Target = Reader<'a>;
    fn deref(&self) -> &Self::Target { &*self.reader }
}

impl<'r, 'a> DerefMut for StructReader<'r, 'a> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut *self.reader }
}

impl<'r, 'a> Drop for StructReader<'r, 'a> {
    fn drop(&mut self) {
        self.reader.depth -= 1;
    }
}

impl<'r, 'a> StructReader<'r, 'a> {
    pub fn next_field(&mut self) -> Result<Option<Field>> {
        if self.ended { return Ok(None); }
        let header = self.reader.byte()?;
        if header == 0 {
            self.ended = true;
            return Ok(None);
        }
        let kind = WireType::try_from(header & 15)?;
        let delta = (header >> 4) as i16;
        let id = if delta == 0 {
            self.reader.i16()?
        } else {
            self.last_id.checked_add(delta).ok_or(Error::IntegerOverflow)?
        };
        self.last_id = id;
        Ok(Some(Field { id, kind }))
    }
}

impl<'a> Reader<'a> {
    pub const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0, depth: 0, max_depth: 64 }
    }

    pub const fn with_max_depth(input: &'a [u8], max_depth: u16) -> Self {
        Self { input, offset: 0, depth: 0, max_depth }
    }

    pub const fn position(&self) -> usize { self.offset }
    pub fn remaining(&self) -> usize { self.input.len() - self.offset }

    pub fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(len).ok_or(Error::IntegerOverflow)?;
        let result = self.input.get(self.offset..end).ok_or(Error::EndOfInput)?;
        self.offset = end;
        Ok(result)
    }

    pub fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn i8(&mut self) -> Result<i8> {
        Ok(self.byte()? as i8)
    }

    /// Unsigned varint (ULEB128), with strict overflow checks.
    pub fn var_u64(&mut self) -> Result<u64> {
        let mut result = 0_u64;
        for shift in (0..=63).step_by(7) {
            let b = self.byte()?;
            if shift == 63 && b > 1 { return Err(Error::InvalidVarint); }
            result |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 { return Ok(result); }
        }
        Err(Error::InvalidVarint)
    }

    pub fn var_u32(&mut self) -> Result<u32> {
        self.var_u64()?.try_into().map_err(|_| Error::IntegerOverflow)
    }

    pub fn i16(&mut self) -> Result<i16> {
        unzigzag_u64(self.var_u32()? as u64).try_into().map_err(|_| Error::IntegerOverflow)
    }

    pub fn i32(&mut self) -> Result<i32> {
        unzigzag_u64(self.var_u32()? as u64).try_into().map_err(|_| Error::IntegerOverflow)
    }

    pub fn i64(&mut self) -> Result<i64> {
        Ok(unzigzag_u64(self.var_u64()?))
    }

    pub fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().map_err(|_| Error::EndOfInput)?))
    }

    /// The Compact Protocol uses 1 for true, 2 for false in collections.
    /// A zero false representation is also accepted for compatibility.
    pub fn bool_element(&mut self) -> Result<bool> {
        match self.byte()? {
            1 => Ok(true),
            0 | 2 => Ok(false),
            x => Err(Error::InvalidBool(x)),
        }
    }

    pub fn binary(&mut self) -> Result<&'a [u8]> {
        let len = self.var_u32()? as usize;
        self.take(len)
    }

    pub fn string(&mut self) -> Result<&'a str> {
        core::str::from_utf8(self.binary()?).map_err(|_| Error::InvalidUtf8)
    }

    pub fn uuid(&mut self) -> Result<&'a [u8; 16]> {
        self.take(16)?.try_into().map_err(|_| Error::EndOfInput)
    }

    pub fn list_header(&mut self) -> Result<ListHeader> {
        let b = self.byte()?;
        let len = if b >> 4 == 15 { self.var_u32()? } else { (b >> 4) as u32 };
        let element = WireType::try_from(b & 15)?;
        Ok(ListHeader { len, element })
    }

    /// Sets have the same wire representation as lists.
    pub fn set_header(&mut self) -> Result<ListHeader> { self.list_header() }

    pub fn map_header(&mut self) -> Result<MapHeader> {
        let len = self.var_u32()?;
        if len == 0 { return Ok(MapHeader { len, types: None }); }
        let b = self.byte()?;
        let key = WireType::try_from(b >> 4)?;
        let val = WireType::try_from(b & 15)?;
        Ok(MapHeader { len, types: Some((key, val)) })
    }

    pub fn structure(&mut self) -> Result<StructReader<'_, 'a>> {
        if self.depth >= self.max_depth { return Err(Error::DepthLimit); }
        self.depth += 1;
        Ok(StructReader { reader: self, last_id: 0, ended: false })
    }

    /// Consume the wire representation of an unknown value without allocating.
    pub fn skip(&mut self, kind: WireType) -> Result<()> {
        self.skip_inner(kind, self.max_depth.saturating_sub(self.depth), true)
    }

    /// Borrow the complete encoded value (not its field header).
    /// The result is safe to pass to `Writer::raw_field()`.
    pub fn raw_value(&mut self, kind: WireType) -> Result<&'a [u8]> {
        let start = self.offset;
        self.skip(kind)?;
        Ok(&self.input[start..self.offset])
    }

    fn skip_inner(&mut self, kind: WireType, budget: u16, is_field: bool) -> Result<()> {
        match kind {
            WireType::BoolTrue | WireType::BoolFalse => {
                if !is_field { self.bool_element()?; }
            }
            WireType::Byte => { self.byte()?; }
            WireType::I16 => { self.i16()?; }
            WireType::I32 => { self.i32()?; }
            WireType::I64 => { self.i64()?; }
            WireType::Double => { self.take(8)?; }
            WireType::Binary => { self.binary()?; }
            WireType::Uuid => { self.take(16)?; }
            WireType::List | WireType::Set => {
                if budget == 0 { return Err(Error::DepthLimit); }
                let header = self.list_header()?;
                for _ in 0..header.len {
                    self.skip_inner(header.element, budget - 1, false)?;
                }
            }
            WireType::Map => {
                if budget == 0 { return Err(Error::DepthLimit); }
                let header = self.map_header()?;
                if let Some((key, value)) = header.types {
                    for _ in 0..header.len {
                        self.skip_inner(key, budget - 1, false)?;
                        self.skip_inner(value, budget - 1, false)?;
                    }
                }
            }
            WireType::Struct => {
                if budget == 0 { return Err(Error::DepthLimit); }
                let mut s = self.structure()?;
                while let Some(field) = s.next_field()? {
                    s.reader.skip_inner(field.kind, budget - 1, true)?;
                }
            }
        }
        Ok(())
    }
}
