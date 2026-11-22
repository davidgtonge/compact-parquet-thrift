//! Raw Snappy block compressor.
//!
//! `table` is a caller-owned hash table of previous positions. An empty table,
//! or one whose length is not a power of two, emits literals only. Literals
//! are valid Snappy and round-trip through `RawSnappy`. Matches use the
//! two-byte offset form (lengths 4 through 64).

use super::{Cursor, EncodeError, Result};

/// Longest match one two-byte-offset copy element can express.
const MAX_MATCH: usize = 64;
/// Longest literal written with a one-byte tag.
const MAX_SHORT_LITERAL: usize = 60;

/// Compress `input` as one raw Snappy block into `out`; returns bytes written.
///
/// # Errors
///
/// [`EncodeError::Overflow`] if `input` is longer than `u32::MAX`, or
/// [`EncodeError::OutputFull`] if `out` is too small.
pub fn compress(input: &[u8], out: &mut [u8], table: &mut [u32]) -> Result<usize> {
    let mut cursor = Cursor::new(out);
    let len = u32::try_from(input.len()).map_err(|_| EncodeError::Overflow)?;
    cursor.write_uleb(u64::from(len))?;
    if input.is_empty() {
        return Ok(cursor.position());
    }

    let hashed = table.len() >= 2 && table.len().is_power_of_two();
    if hashed {
        table.fill(0);
    }
    let mask = table.len().wrapping_sub(1);

    // A byte-position scan with jumps over matches; positions fit u32
    // because the whole input does.
    let mut i = 0;
    let mut literal_at = 0;
    while i < input.len() {
        let Some(word) = input[i..].first_chunk::<4>().filter(|_| hashed) else {
            i += 1;
            continue;
        };
        let slot = (u32::from_le_bytes(*word).wrapping_mul(0x1e35_a7bd) >> 16) as usize & mask;
        let prev = core::mem::replace(&mut table[slot], position(i) + 1);
        if let Some(from) = (prev as usize).checked_sub(1) {
            let offset = i - from;
            if (1..=65535).contains(&offset) && input[from..].starts_with(word) {
                write_literals(&mut cursor, &input[literal_at..i])?;
                let match_len = 4 + input[from + 4..]
                    .iter()
                    .zip(&input[i + 4..])
                    .take(MAX_MATCH - 4)
                    .take_while(|(a, b)| a == b)
                    .count();
                write_copy(&mut cursor, offset, match_len)?;
                i += match_len;
                literal_at = i;
                continue;
            }
        }
        i += 1;
    }
    write_literals(&mut cursor, &input[literal_at..])?;
    Ok(cursor.position())
}

// `compress` has checked that the input length fits a u32.
#[allow(clippy::cast_possible_truncation)]
const fn position(i: usize) -> u32 {
    i as u32
}

/// A copy element with a two-byte offset (tag type 2).
fn write_copy(out: &mut Cursor<'_>, offset: usize, len: usize) -> Result<()> {
    let len = u8::try_from(len - 1).map_err(|_| EncodeError::ValueOutOfRange)?;
    let offset = u16::try_from(offset).map_err(|_| EncodeError::ValueOutOfRange)?;
    let [lo, hi] = offset.to_le_bytes();
    out.write(&[(len << 2) | 2, lo, hi])
}

fn write_literals(out: &mut Cursor<'_>, bytes: &[u8]) -> Result<()> {
    bytes.chunks(MAX_SHORT_LITERAL).try_for_each(|chunk| {
        let len = u8::try_from(chunk.len() - 1).map_err(|_| EncodeError::ValueOutOfRange)?;
        out.write(&[len << 2])?;
        out.write(chunk)
    })
}
