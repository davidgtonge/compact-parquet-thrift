//! Hybrid RLE / bit-packed encoder.
//!
//! Runs of eight or more equal values become RLE. Anything shorter is flushed
//! as one 8-value bit-packed group, padded with zeros. The logical value count
//! lives in the page header, so that padding is not another value.

use super::{Cursor, EncodeError, Result};

pub struct HybridRleWriter {
    bit_width: u8,
    literals: [u32; 8],
    nlit: u8,
    in_repeat: bool,
    repeat_value: u32,
    repeat_count: u32,
}

impl HybridRleWriter {
    pub fn new(bit_width: u8) -> Result<Self> {
        if bit_width > 32 { return Err(EncodeError::ValueOutOfRange); }
        Ok(Self {
            bit_width,
            literals: [0; 8],
            nlit: 0,
            in_repeat: false,
            repeat_value: 0,
            repeat_count: 0,
        })
    }

    pub fn push(&mut self, value: u32, out: &mut Cursor<'_>) -> Result<()> {
        if value > mask(self.bit_width) { return Err(EncodeError::ValueOutOfRange); }
        if self.in_repeat {
            if value == self.repeat_value {
                if self.repeat_count == 0x7fff_ffff {
                    self.flush_repeat(out)?;
                    self.in_repeat = true;
                    self.repeat_value = value;
                    self.repeat_count = 1;
                } else {
                    self.repeat_count += 1;
                }
                return Ok(());
            }
            self.flush_repeat(out)?;
        }
        self.literals[self.nlit as usize] = value;
        self.nlit += 1;
        if self.nlit == 8 {
            if self.literals.iter().all(|&v| v == self.literals[0]) {
                self.in_repeat = true;
                self.repeat_value = self.literals[0];
                self.repeat_count = 8;
                self.nlit = 0;
            } else {
                self.flush_literals(out)?;
            }
        }
        Ok(())
    }

    pub fn finish(&mut self, out: &mut Cursor<'_>) -> Result<()> {
        if self.in_repeat { self.flush_repeat(out)?; }
        self.flush_literals(out)
    }

    fn flush_repeat(&mut self, out: &mut Cursor<'_>) -> Result<()> {
        write_rle(self.bit_width, self.repeat_count, self.repeat_value, out)?;
        self.in_repeat = false;
        self.repeat_count = 0;
        Ok(())
    }

    fn flush_literals(&mut self, out: &mut Cursor<'_>) -> Result<()> {
        if self.nlit == 0 { return Ok(()); }
        let mut group = [0u32; 8];
        group[..self.nlit as usize].copy_from_slice(&self.literals[..self.nlit as usize]);
        write_bit_packed(self.bit_width, &group, out)?;
        self.nlit = 0;
        Ok(())
    }
}

pub fn encode_hybrid(bit_width: u8, values: &[u32], out: &mut [u8]) -> Result<usize> {
    let mut writer = HybridRleWriter::new(bit_width)?;
    let mut cursor = Cursor::new(out);
    for &value in values {
        writer.push(value, &mut cursor)?;
    }
    writer.finish(&mut cursor)?;
    Ok(cursor.position())
}

/// V1 definition levels for a flat optional column: a 4-byte length followed
/// by a bit-width-1 hybrid stream. A set bit in `present` means the row has a
/// value. `present` must cover `rows` bits.
pub fn encode_definition_levels(rows: usize, present: &[u8], out: &mut [u8]) -> Result<usize> {
    let need = rows.div_ceil(8);
    if present.len() < need { return Err(EncodeError::ValueCount); }
    if out.len() < 4 { return Err(EncodeError::OutputFull); }
    let n = {
        let mut writer = HybridRleWriter::new(1)?;
        let mut cursor = Cursor::new(&mut out[4..]);
        for row in 0..rows {
            let bit = u32::from(present[row >> 3] & (1 << (row & 7)) != 0);
            writer.push(bit, &mut cursor)?;
        }
        writer.finish(&mut cursor)?;
        cursor.position()
    };
    let len = u32::try_from(n).map_err(|_| EncodeError::Overflow)?;
    out[..4].copy_from_slice(&len.to_le_bytes());
    Ok(4 + n)
}

pub fn index_bit_width(dictionary_len: u32) -> u8 {
    if dictionary_len <= 1 { 0 }
    else { (32 - (dictionary_len - 1).leading_zeros()) as u8 }
}

/// Dictionary-id payload: one bit-width byte, then the hybrid stream.
pub fn encode_dictionary_ids(dictionary_len: u32, ids: &[u32], out: &mut [u8]) -> Result<usize> {
    if ids.is_empty() { return Ok(0); }
    if dictionary_len == 0 { return Err(EncodeError::EmptyDictionary); }
    let width = index_bit_width(dictionary_len);
    let limit = if dictionary_len == 0 { 0 } else { dictionary_len - 1 };
    if ids.iter().any(|&id| id > limit) { return Err(EncodeError::ValueOutOfRange); }
    if out.is_empty() { return Err(EncodeError::OutputFull); }
    out[0] = width;
    let n = encode_hybrid(width, ids, &mut out[1..])?;
    Ok(1 + n)
}

fn write_rle(bit_width: u8, count: u32, value: u32, out: &mut Cursor<'_>) -> Result<()> {
    out.write_uleb(u64::from(count) << 1)?;
    let nbytes = usize::from(bit_width.div_ceil(8));
    if nbytes == 0 { return Ok(()); }
    out.write(&value.to_le_bytes()[..nbytes])
}

fn write_bit_packed(bit_width: u8, values: &[u32; 8], out: &mut Cursor<'_>) -> Result<()> {
    // One group. Header count is the number of groups, not values.
    out.write_uleb((1u64 << 1) | 1)?;
    let nbytes = usize::from(bit_width);
    if nbytes == 0 { return Ok(()); }
    let mut packed = [0u8; 32];
    for (i, &value) in values.iter().enumerate() {
        let start = i * usize::from(bit_width);
        for bit in 0..bit_width {
            if (value >> bit) & 1 != 0 {
                let pos = start + usize::from(bit);
                packed[pos / 8] |= 1 << (pos & 7);
            }
        }
    }
    out.write(&packed[..nbytes])
}

fn mask(bit_width: u8) -> u32 {
    if bit_width >= 32 { u32::MAX } else { (1u32 << bit_width) - 1 }
}
