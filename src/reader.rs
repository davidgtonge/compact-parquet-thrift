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

/// One struct field header: its ID and wire type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Field {
    /// Thrift field ID.
    pub id: i16,
    /// Wire type of the field's value.
    pub kind: WireType,
}

impl Field {
    /// The value of a boolean field, which is carried by its header.
    ///
    /// # Errors
    ///
    /// [`Error::UnexpectedWireType`] if the field is not boolean.
    pub fn bool_value(self) -> Result<bool> {
        self.kind.field_bool()
    }

    /// Check that the field has the wire type its ID requires.
    ///
    /// # Errors
    ///
    /// [`Error::UnexpectedWireType`] on a mismatch.
    pub fn expect(self, kind: WireType) -> Result<()> {
        if self.kind == kind {
            Ok(())
        } else {
            Err(Error::UnexpectedWireType)
        }
    }
}

/// A list or set header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListHeader {
    /// Number of elements.
    pub len: u32,
    /// Wire type of every element.
    pub element: WireType,
}

/// A map header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MapHeader {
    /// Number of key/value pairs.
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

impl<'a> Deref for StructReader<'_, 'a> {
    type Target = Reader<'a>;
    fn deref(&self) -> &Self::Target {
        &*self.reader
    }
}

impl DerefMut for StructReader<'_, '_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.reader
    }
}

impl Drop for StructReader<'_, '_> {
    fn drop(&mut self) {
        self.reader.depth -= 1;
    }
}

impl StructReader<'_, '_> {
    /// The next field header, or `None` at the struct's STOP byte.
    ///
    /// # Errors
    ///
    /// Truncated input, an invalid type tag, or a field ID that overflows `i16`.
    pub fn next_field(&mut self) -> Result<Option<Field>> {
        if self.ended {
            return Ok(None);
        }
        let header = self.reader.byte()?;
        if header == 0 {
            self.ended = true;
            return Ok(None);
        }
        let kind = WireType::try_from(header & 15)?;
        let delta = i16::from(header >> 4);
        let id = if delta == 0 {
            self.reader.i16()?
        } else {
            self.last_id
                .checked_add(delta)
                .ok_or(Error::IntegerOverflow)?
        };
        self.last_id = id;
        Ok(Some(Field { id, kind }))
    }
}

impl<'a> Reader<'a> {
    /// A cursor at the start of `input` with the default nesting limit (64).
    #[must_use]
    pub const fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            offset: 0,
            depth: 0,
            max_depth: 64,
        }
    }

    /// A cursor with a custom nesting limit for structs and collections.
    #[must_use]
    pub const fn with_max_depth(input: &'a [u8], max_depth: u16) -> Self {
        Self {
            input,
            offset: 0,
            depth: 0,
            max_depth,
        }
    }

    /// Bytes consumed from the start of the window.
    #[must_use]
    pub const fn position(&self) -> usize {
        self.offset
    }

    /// Bytes left in the window.
    #[must_use]
    pub const fn remaining(&self) -> usize {
        self.input.len() - self.offset
    }

    /// Independent cursor over the same window. Depth starts at zero so a
    /// nested value can be walked twice without sharing the parent's budget.
    ///
    /// # Errors
    ///
    /// [`Error::EndOfInput`] if `offset` is past the window.
    pub fn fork_at(&self, offset: usize) -> Result<Reader<'a>> {
        if offset > self.input.len() {
            return Err(Error::EndOfInput);
        }
        Ok(Reader {
            input: self.input,
            offset,
            depth: 0,
            max_depth: self.max_depth,
        })
    }

    /// Move this cursor. Depth is unchanged; do not call while a child
    /// `StructReader` is still live.
    ///
    /// # Errors
    ///
    /// [`Error::EndOfInput`] if `offset` is past the window.
    pub fn set_position(&mut self, offset: usize) -> Result<()> {
        if offset > self.input.len() {
            return Err(Error::EndOfInput);
        }
        self.offset = offset;
        Ok(())
    }

    /// Borrow the next `len` bytes.
    ///
    /// # Errors
    ///
    /// [`Error::EndOfInput`] if fewer than `len` bytes remain.
    pub fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(len).ok_or(Error::IntegerOverflow)?;
        let result = self.input.get(self.offset..end).ok_or(Error::EndOfInput)?;
        self.offset = end;
        Ok(result)
    }

    /// Read one byte.
    ///
    /// # Errors
    ///
    /// [`Error::EndOfInput`] at the end of the window.
    pub fn byte(&mut self) -> Result<u8> {
        let byte = *self.input.get(self.offset).ok_or(Error::EndOfInput)?;
        self.offset += 1;
        Ok(byte)
    }

    /// Read a signed byte.
    ///
    /// # Errors
    ///
    /// [`Error::EndOfInput`] at the end of the window.
    pub fn i8(&mut self) -> Result<i8> {
        Ok(i8::from_le_bytes([self.byte()?]))
    }

    /// Unsigned varint (ULEB128), with strict overflow checks.
    ///
    /// # Errors
    ///
    /// [`Error::EndOfInput`] if truncated, [`Error::InvalidVarint`] if longer
    /// than ten bytes or wider than 64 bits.
    pub fn var_u64(&mut self) -> Result<u64> {
        let mut result = 0_u64;
        for shift in (0..=63).step_by(7) {
            let b = self.byte()?;
            if shift == 63 && b > 1 {
                return Err(Error::InvalidVarint);
            }
            result |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
        }
        Err(Error::InvalidVarint)
    }

    /// Unsigned varint that must fit in 32 bits.
    ///
    /// # Errors
    ///
    /// As [`Self::var_u64`], plus [`Error::IntegerOverflow`].
    pub fn var_u32(&mut self) -> Result<u32> {
        self.var_u64()?
            .try_into()
            .map_err(|_| Error::IntegerOverflow)
    }

    /// Zigzag varint `i16`.
    ///
    /// # Errors
    ///
    /// As [`Self::var_u64`], plus [`Error::IntegerOverflow`].
    pub fn i16(&mut self) -> Result<i16> {
        unzigzag_u64(u64::from(self.var_u32()?))
            .try_into()
            .map_err(|_| Error::IntegerOverflow)
    }

    /// Zigzag varint `i32`.
    ///
    /// # Errors
    ///
    /// As [`Self::var_u64`], plus [`Error::IntegerOverflow`].
    pub fn i32(&mut self) -> Result<i32> {
        unzigzag_u64(u64::from(self.var_u32()?))
            .try_into()
            .map_err(|_| Error::IntegerOverflow)
    }

    /// Zigzag varint `i64`.
    ///
    /// # Errors
    ///
    /// As [`Self::var_u64`].
    pub fn i64(&mut self) -> Result<i64> {
        Ok(unzigzag_u64(self.var_u64()?))
    }

    /// Little-endian double.
    ///
    /// # Errors
    ///
    /// [`Error::EndOfInput`] if fewer than eight bytes remain.
    pub fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| Error::EndOfInput)?,
        ))
    }

    /// The Compact Protocol uses 1 for true, 2 for false in collections.
    /// A zero false representation is also accepted for compatibility.
    ///
    /// # Errors
    ///
    /// [`Error::EndOfInput`] or [`Error::InvalidBool`].
    pub fn bool_element(&mut self) -> Result<bool> {
        match self.byte()? {
            1 => Ok(true),
            0 | 2 => Ok(false),
            x => Err(Error::InvalidBool(x)),
        }
    }

    /// Borrow a length-prefixed binary value.
    ///
    /// # Errors
    ///
    /// A malformed length or truncated payload.
    pub fn binary(&mut self) -> Result<&'a [u8]> {
        let len = usize::try_from(self.var_u32()?).map_err(|_| Error::IntegerOverflow)?;
        self.take(len)
    }

    /// Borrow a length-prefixed UTF-8 string.
    ///
    /// # Errors
    ///
    /// As [`Self::binary`], plus [`Error::InvalidUtf8`].
    pub fn string(&mut self) -> Result<&'a str> {
        core::str::from_utf8(self.binary()?).map_err(|_| Error::InvalidUtf8)
    }

    /// Borrow a sixteen-byte UUID.
    ///
    /// # Errors
    ///
    /// [`Error::EndOfInput`] if fewer than sixteen bytes remain.
    pub fn uuid(&mut self) -> Result<&'a [u8; 16]> {
        self.take(16)?.try_into().map_err(|_| Error::EndOfInput)
    }

    /// Read a list header.
    ///
    /// # Errors
    ///
    /// Truncated input, a malformed length or an invalid element type.
    pub fn list_header(&mut self) -> Result<ListHeader> {
        let b = self.byte()?;
        let len = if b >> 4 == 15 {
            self.var_u32()?
        } else {
            u32::from(b >> 4)
        };
        let element = WireType::try_from(b & 15)?;
        Ok(ListHeader { len, element })
    }

    /// Sets have the same wire representation as lists.
    ///
    /// # Errors
    ///
    /// As [`Self::list_header`].
    pub fn set_header(&mut self) -> Result<ListHeader> {
        self.list_header()
    }

    /// Read a map header.
    ///
    /// # Errors
    ///
    /// Truncated input, a malformed length or an invalid key/value type.
    pub fn map_header(&mut self) -> Result<MapHeader> {
        let len = self.var_u32()?;
        if len == 0 {
            return Ok(MapHeader { len, types: None });
        }
        let b = self.byte()?;
        let key = WireType::try_from(b >> 4)?;
        let val = WireType::try_from(b & 15)?;
        Ok(MapHeader {
            len,
            types: Some((key, val)),
        })
    }

    /// Enter a struct. Read its fields with [`StructReader::next_field`].
    ///
    /// # Errors
    ///
    /// [`Error::DepthLimit`] if nesting is too deep.
    pub fn structure(&mut self) -> Result<StructReader<'_, 'a>> {
        if self.depth >= self.max_depth {
            return Err(Error::DepthLimit);
        }
        self.depth += 1;
        Ok(StructReader {
            reader: self,
            last_id: 0,
            ended: false,
        })
    }

    /// Consume the wire representation of an unknown value without allocating.
    ///
    /// # Errors
    ///
    /// Any malformed or truncated content, or [`Error::DepthLimit`].
    pub fn skip(&mut self, kind: WireType) -> Result<()> {
        self.skip_inner(kind, self.max_depth.saturating_sub(self.depth), true)
    }

    /// Borrow the complete encoded value (not its field header).
    /// The result is safe to pass to [`crate::Writer::raw_field`].
    ///
    /// # Errors
    ///
    /// As [`Self::skip`].
    pub fn raw_value(&mut self, kind: WireType) -> Result<&'a [u8]> {
        let start = self.offset;
        self.skip(kind)?;
        self.input.get(start..self.offset).ok_or(Error::EndOfInput)
    }

    fn skip_inner(&mut self, kind: WireType, budget: u16, is_field: bool) -> Result<()> {
        match kind {
            WireType::BoolTrue | WireType::BoolFalse => {
                if !is_field {
                    self.bool_element()?;
                }
            }
            WireType::Byte => {
                self.byte()?;
            }
            WireType::I16 => {
                self.i16()?;
            }
            WireType::I32 => {
                self.i32()?;
            }
            WireType::I64 => {
                self.i64()?;
            }
            WireType::Double => {
                self.take(8)?;
            }
            WireType::Binary => {
                self.binary()?;
            }
            WireType::Uuid => {
                self.take(16)?;
            }
            WireType::List | WireType::Set => {
                if budget == 0 {
                    return Err(Error::DepthLimit);
                }
                let header = self.list_header()?;
                for _ in 0..header.len {
                    self.skip_inner(header.element, budget - 1, false)?;
                }
            }
            WireType::Map => {
                if budget == 0 {
                    return Err(Error::DepthLimit);
                }
                let header = self.map_header()?;
                if let Some((key, value)) = header.types {
                    for _ in 0..header.len {
                        self.skip_inner(key, budget - 1, false)?;
                        self.skip_inner(value, budget - 1, false)?;
                    }
                }
            }
            WireType::Struct => {
                if budget == 0 {
                    return Err(Error::DepthLimit);
                }
                let mut s = self.structure()?;
                while let Some(field) = s.next_field()? {
                    s.reader.skip_inner(field.kind, budget - 1, true)?;
                }
            }
        }
        Ok(())
    }
}
