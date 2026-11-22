//! Hybrid RLE / bit-packed encoder.
//!
//! Runs of eight or more equal values become RLE. Anything shorter is flushed
//! as one 8-value bit-packed group, padded with zeros. The logical value count
//! lives in the page header, so that padding is not another value.

use super::{is_present, Cursor, EncodeError, Result};

/// A run of equal values not yet written.
#[derive(Debug, Clone, Copy)]
struct Repeat {
    value: u32,
    count: u32,
}

/// Streaming hybrid encoder: up to eight pending literals, or one repeat run.
pub(crate) struct HybridRleWriter {
    bit_width: u8,
    literals: [u32; 8],
    nlit: usize,
    repeat: Option<Repeat>,
}

impl HybridRleWriter {
    pub(crate) const fn new(bit_width: u8) -> Result<Self> {
        if bit_width > 32 {
            return Err(EncodeError::ValueOutOfRange);
        }
        Ok(Self {
            bit_width,
            literals: [0; 8],
            nlit: 0,
            repeat: None,
        })
    }

    pub(crate) fn push(&mut self, value: u32, out: &mut Cursor<'_>) -> Result<()> {
        if value > mask(self.bit_width) {
            return Err(EncodeError::ValueOutOfRange);
        }
        if let Some(run) = &mut self.repeat {
            if value == run.value && run.count < MAX_RUN {
                run.count += 1;
                return Ok(());
            }
            let full = value == run.value;
            self.flush_repeat(out)?;
            if full {
                // The run hit Parquet's length cap; start another.
                self.repeat = Some(Repeat { value, count: 1 });
                return Ok(());
            }
        }
        self.literals[self.nlit] = value;
        self.nlit += 1;
        if self.nlit == self.literals.len() {
            if self.literals.iter().all(|&v| v == value) {
                self.repeat = Some(Repeat { value, count: 8 });
                self.nlit = 0;
            } else {
                self.flush_literals(out)?;
            }
        }
        Ok(())
    }

    pub(crate) fn finish(&mut self, out: &mut Cursor<'_>) -> Result<()> {
        self.flush_repeat(out)?;
        self.flush_literals(out)
    }

    fn flush_repeat(&mut self, out: &mut Cursor<'_>) -> Result<()> {
        match self.repeat.take() {
            Some(run) => write_rle(self.bit_width, run.count, run.value, out),
            None => Ok(()),
        }
    }

    fn flush_literals(&mut self, out: &mut Cursor<'_>) -> Result<()> {
        if self.nlit == 0 {
            return Ok(());
        }
        let mut group = [0u32; 8];
        group[..self.nlit].copy_from_slice(&self.literals[..self.nlit]);
        write_bit_packed(self.bit_width, &group, out)?;
        self.nlit = 0;
        Ok(())
    }
}

/// Parquet caps a run at 2^31 - 1 values.
const MAX_RUN: u32 = 0x7fff_ffff;

/// Hybrid RLE / bit-packed encoding of `values` at `bit_width`; returns
/// bytes written.
///
/// # Errors
///
/// [`EncodeError::ValueOutOfRange`] for a width above 32 or a value wider
/// than `bit_width`, or [`EncodeError::OutputFull`].
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
pub(crate) fn encode_definition_levels(rows: usize, present: &[u8], out: &mut [u8]) -> Result<usize> {
    let need = rows.div_ceil(8);
    if present.len() < need {
        return Err(EncodeError::ValueCount);
    }
    if out.len() < 4 {
        return Err(EncodeError::OutputFull);
    }
    let n = {
        let mut writer = HybridRleWriter::new(1)?;
        let mut cursor = Cursor::new(&mut out[4..]);
        for row in 0..rows {
            writer.push(u32::from(is_present(present, row)), &mut cursor)?;
        }
        writer.finish(&mut cursor)?;
        cursor.position()
    };
    let len = u32::try_from(n).map_err(|_| EncodeError::Overflow)?;
    out[..4].copy_from_slice(&len.to_le_bytes());
    Ok(4 + n)
}

/// Bits needed for the largest index of a dictionary (at most 32).
#[allow(clippy::cast_possible_truncation)]
pub(crate) const fn index_bit_width(dictionary_len: u32) -> u8 {
    (u32::BITS - dictionary_len.saturating_sub(1).leading_zeros()) as u8
}

/// Dictionary-id payload: one bit-width byte, then the hybrid stream.
pub(crate) fn encode_dictionary_ids(dictionary_len: u32, ids: &[u32], out: &mut [u8]) -> Result<usize> {
    if ids.is_empty() {
        return Ok(0);
    }
    if dictionary_len == 0 {
        return Err(EncodeError::EmptyDictionary);
    }
    let width = index_bit_width(dictionary_len);
    if ids.iter().any(|&id| id >= dictionary_len) {
        return Err(EncodeError::ValueOutOfRange);
    }
    let (first, rest) = out.split_first_mut().ok_or(EncodeError::OutputFull)?;
    *first = width;
    Ok(1 + encode_hybrid(width, ids, rest)?)
}

fn write_rle(bit_width: u8, count: u32, value: u32, out: &mut Cursor<'_>) -> Result<()> {
    out.write_uleb(u64::from(count) << 1)?;
    let nbytes = usize::from(bit_width.div_ceil(8));
    if nbytes == 0 {
        return Ok(());
    }
    out.write(&value.to_le_bytes()[..nbytes])
}

fn write_bit_packed(bit_width: u8, values: &[u32; 8], out: &mut Cursor<'_>) -> Result<()> {
    // One group. Header count is the number of groups, not values.
    out.write_uleb((1u64 << 1) | 1)?;
    let nbytes = usize::from(bit_width);
    if nbytes == 0 {
        return Ok(());
    }
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

const fn mask(bit_width: u8) -> u32 {
    if bit_width >= 32 {
        u32::MAX
    } else {
        (1u32 << bit_width) - 1
    }
}
