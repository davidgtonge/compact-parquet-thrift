//! Raw Snappy block compressor.
//!
//! `table` is a caller-owned hash table of previous positions. An empty table,
//! or one whose length is not a power of two, emits literals only. Literals
//! are valid Snappy and round-trip through `RawSnappy`. Matches use the
//! two-byte offset form (lengths 4 through 64).

use super::{Cursor, EncodeError, Result};

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

    let mut i = 0;
    let mut literal_at = 0;
    while i < input.len() {
        if hashed && i + 4 <= input.len() {
            let word = u32::from_le_bytes(input[i..i + 4].try_into().unwrap());
            let slot = (word.wrapping_mul(0x1e35_a7bd) >> 16) as usize & mask;
            let prev = table[slot];
            table[slot] = (i as u32).wrapping_add(1);
            if prev > 0 {
                let from = (prev - 1) as usize;
                let offset = i - from;
                if (1..=65535).contains(&offset)
                    && from + 4 <= input.len()
                    && input[from..from + 4] == input[i..i + 4]
                {
                    write_literals(&mut cursor, &input[literal_at..i])?;
                    let mut match_len = 4;
                    while match_len < 64
                        && i + match_len < input.len()
                        && from + match_len < input.len()
                        && input[from + match_len] == input[i + match_len]
                    {
                        match_len += 1;
                    }
                    let tag = (((match_len - 1) as u8) << 2) | 2;
                    cursor.write(&[tag, offset as u8, (offset >> 8) as u8])?;
                    i += match_len;
                    literal_at = i;
                    continue;
                }
            }
        }
        i += 1;
    }
    write_literals(&mut cursor, &input[literal_at..])?;
    Ok(cursor.position())
}

fn write_literals(out: &mut Cursor<'_>, bytes: &[u8]) -> Result<()> {
    let mut at = 0;
    while at < bytes.len() {
        let n = (bytes.len() - at).min(60);
        out.write(&[(n as u8 - 1) << 2])?;
        out.write(&bytes[at..at + n])?;
        at += n;
    }
    Ok(())
}
