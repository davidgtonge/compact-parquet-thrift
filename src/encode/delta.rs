//! `DELTA_BINARY_PACKED`, `DELTA_LENGTH_BYTE_ARRAY`, and `DELTA_BYTE_ARRAY`.
//!
//! Blocks are 128 values and each block has four mini-blocks of 32. That is
//! the layout this crate's decoder sizes its width scratch for. The last
//! block still stores four width bytes. Only mini-blocks that contain a real
//! value have a body, and that body is padded out to 32 residuals.

use super::{Cursor, EncodeError, Result};

const BLOCK: usize = 128;
const MINI_COUNT: usize = 4;
const MINI: usize = 32;

/// Upper bound on the encoded size of `count` delta integers of
/// `width_bytes` bytes each (4 for `INT32`, 8 for `INT64`).
#[must_use]
pub fn delta_binary_bound(count: usize, width_bytes: usize) -> Option<usize> {
    let header = 32usize;
    if count <= 1 {
        return Some(header);
    }
    let blocks = (count - 1).div_ceil(BLOCK);
    let per_block = 16usize.checked_add(BLOCK.checked_mul(width_bytes)?)?;
    header.checked_add(blocks.checked_mul(per_block)?)
}

/// Upper bound on the `DELTA_LENGTH_BYTE_ARRAY` encoding of `count` values
/// totalling `payload_bytes`.
#[must_use]
pub fn delta_length_bound(count: usize, payload_bytes: usize) -> Option<usize> {
    delta_binary_bound(count, 4)?.checked_add(payload_bytes)
}

/// Upper bound on the `DELTA_BYTE_ARRAY` encoding of `count` values
/// totalling `payload_bytes`.
#[must_use]
pub fn delta_byte_array_bound(count: usize, payload_bytes: usize) -> Option<usize> {
    let lengths = delta_binary_bound(count, 4)?;
    lengths.checked_mul(2)?.checked_add(payload_bytes)
}

/// `DELTA_BINARY_PACKED` `INT32` values; returns bytes written.
///
/// # Errors
///
/// [`EncodeError::OutputFull`] if `out` is too small.
pub fn encode_delta_i32(values: &[i32], out: &mut [u8]) -> Result<usize> {
    encode_values(values.len(), &|i| values[i], out)
}

/// `DELTA_BINARY_PACKED` `INT64` values; returns bytes written.
///
/// # Errors
///
/// [`EncodeError::OutputFull`] if `out` is too small.
pub fn encode_delta_i64(values: &[i64], out: &mut [u8]) -> Result<usize> {
    encode_values(values.len(), &|i| values[i], out)
}

/// `DELTA_LENGTH_BYTE_ARRAY` values; returns bytes written.
///
/// # Errors
///
/// [`EncodeError::Overflow`] for a value longer than `i32::MAX`, or
/// [`EncodeError::OutputFull`].
pub fn encode_delta_length_bytes(values: &[&[u8]], out: &mut [u8]) -> Result<usize> {
    check_byte_lengths(values)?;
    let n = encode_values(values.len(), &|i| byte_len(values[i].len()), out)?;
    copy_bytes(values.iter().copied(), &mut out[n..]).map(|payload| n + payload)
}

/// `DELTA_BYTE_ARRAY` values (prefix-compressed against the previous
/// value); returns bytes written.
///
/// # Errors
///
/// As [`encode_delta_length_bytes`].
pub fn encode_delta_bytes(values: &[&[u8]], out: &mut [u8]) -> Result<usize> {
    check_byte_lengths(values)?;
    let prefixes = encode_values(values.len(), &|i| byte_len(prefix_len(values, i)), out)?;
    let suffixes = encode_values(
        values.len(),
        &|i| byte_len(values[i].len() - prefix_len(values, i)),
        &mut out[prefixes..],
    )?;
    let at = prefixes + suffixes;
    let payload = copy_bytes(
        values
            .iter()
            .enumerate()
            .map(|(i, value)| &value[prefix_len(values, i)..]),
        &mut out[at..],
    )?;
    Ok(at + payload)
}

fn check_byte_lengths(values: &[&[u8]]) -> Result<()> {
    if values
        .iter()
        .any(|value| i32::try_from(value.len()).is_err())
    {
        return Err(EncodeError::Overflow);
    }
    Ok(())
}

/// A value, prefix or suffix length, already bounded by [`check_byte_lengths`].
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
const fn byte_len(len: usize) -> i32 {
    len as i32
}

fn prefix_len(values: &[&[u8]], index: usize) -> usize {
    let Some(previous) = index.checked_sub(1).map(|i| values[i]) else {
        return 0;
    };
    previous
        .iter()
        .zip(values[index])
        .take_while(|(a, b)| a == b)
        .count()
}

fn copy_bytes<'a>(values: impl Iterator<Item = &'a [u8]>, out: &mut [u8]) -> Result<usize> {
    let mut at = 0usize;
    for value in values {
        let end = at.checked_add(value.len()).ok_or(EncodeError::Overflow)?;
        if end > out.len() {
            return Err(EncodeError::OutputFull);
        }
        out[at..end].copy_from_slice(value);
        at = end;
    }
    Ok(at)
}

trait DeltaInt: Copy + Ord {
    const BITS: u8;
    fn zero() -> Self;
    fn delta(self, previous: Self) -> Self;
    fn zigzag(self) -> u64;
    fn residual(self, min_delta: Self) -> u64;
}

// Zigzag and residuals reinterpret two's-complement bits at the physical width.
#[allow(clippy::cast_sign_loss)]
impl DeltaInt for i32 {
    const BITS: u8 = 32;
    fn zero() -> Self {
        0
    }
    fn delta(self, previous: Self) -> Self {
        self.wrapping_sub(previous)
    }
    fn zigzag(self) -> u64 {
        u64::from(((self as u32) << 1) ^ ((self >> 31) as u32))
    }
    fn residual(self, min_delta: Self) -> u64 {
        u64::from(self.wrapping_sub(min_delta) as u32)
    }
}

#[allow(clippy::cast_sign_loss)]
impl DeltaInt for i64 {
    const BITS: u8 = 64;
    fn zero() -> Self {
        0
    }
    fn delta(self, previous: Self) -> Self {
        self.wrapping_sub(previous)
    }
    fn zigzag(self) -> u64 {
        ((self as u64) << 1) ^ ((self >> 63) as u64)
    }
    fn residual(self, min_delta: Self) -> u64 {
        self.wrapping_sub(min_delta) as u64
    }
}

fn encode_values<T: DeltaInt>(
    count: usize,
    value_at: &impl Fn(usize) -> T,
    out: &mut [u8],
) -> Result<usize> {
    let mut cursor = Cursor::new(out);
    cursor.write_uleb(BLOCK as u64)?;
    cursor.write_uleb(MINI_COUNT as u64)?;
    cursor.write_uleb(count as u64)?;
    let first = if count == 0 { T::zero() } else { value_at(0) };
    cursor.write_uleb(first.zigzag())?;
    let mut index = 1usize;
    while index < count {
        let end = (index + BLOCK).min(count);
        let min_delta = block_min(value_at, index, end);
        cursor.write_uleb(min_delta.zigzag())?;
        // Mini-blocks with a real value; trailing widths stay zero.
        let minis = (index..end)
            .step_by(MINI)
            .map(|start| (start, (start + MINI).min(end)));
        let mut widths = [0u8; MINI_COUNT];
        for ((start, stop), width) in minis.clone().zip(&mut widths) {
            *width = mini_width(value_at, start, stop, min_delta);
        }
        cursor.write(&widths)?;
        for ((start, stop), &width) in minis.zip(&widths) {
            pack_mini(&mut cursor, value_at, start, stop, min_delta, width)?;
        }
        index = end;
    }
    Ok(cursor.position())
}

fn block_min<T: DeltaInt>(value_at: &impl Fn(usize) -> T, start: usize, end: usize) -> T {
    let mut previous = value_at(start - 1);
    let mut min_delta = value_at(start).delta(previous);
    for index in start..end {
        let current = value_at(index);
        let delta = current.delta(previous);
        if delta < min_delta {
            min_delta = delta;
        }
        previous = current;
    }
    min_delta
}

fn mini_width<T: DeltaInt>(
    value_at: &impl Fn(usize) -> T,
    start: usize,
    end: usize,
    min_delta: T,
) -> u8 {
    let mut previous = value_at(start - 1);
    let mut max_residual = 0u64;
    for index in start..end {
        let current = value_at(index);
        max_residual = max_residual.max(current.delta(previous).residual(min_delta));
        previous = current;
    }
    bit_width(max_residual)
}

// At most 64, so the narrowing cast cannot truncate.
#[allow(clippy::cast_possible_truncation)]
const fn bit_width(value: u64) -> u8 {
    (u64::BITS - value.leading_zeros()) as u8
}

fn pack_mini<T: DeltaInt>(
    out: &mut Cursor<'_>,
    value_at: &impl Fn(usize) -> T,
    start: usize,
    end: usize,
    min_delta: T,
    width: u8,
) -> Result<()> {
    if width == 0 {
        return Ok(());
    }
    if width > T::BITS {
        return Err(EncodeError::ValueOutOfRange);
    }
    let nbytes = MINI * usize::from(width) / 8;
    let mut packed = [0u8; 256];
    if nbytes > packed.len() {
        return Err(EncodeError::Overflow);
    }
    let mut previous = value_at(start - 1);
    let mut acc = 0u128;
    let mut nbits = 0u32;
    let mut at = 0usize;
    for pos in 0..MINI {
        let residual = if start + pos < end {
            let current = value_at(start + pos);
            let residual = current.delta(previous).residual(min_delta);
            previous = current;
            residual
        } else {
            0
        };
        if width < 64 && residual >> width != 0 {
            return Err(EncodeError::ValueOutOfRange);
        }
        acc |= u128::from(residual) << nbits;
        nbits += u32::from(width);
        while nbits >= 8 {
            packed[at] = acc.to_le_bytes()[0];
            at += 1;
            acc >>= 8;
            nbits -= 8;
        }
    }
    if at != nbytes || nbits != 0 {
        return Err(EncodeError::ValueOutOfRange);
    }
    out.write(&packed[..nbytes])
}
