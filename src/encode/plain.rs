//! PLAIN value encoding. Bytes are appended to a caller-owned buffer.

use super::{EncodeError, Result};

/// Little-endian fixed-width numbers, `N` bytes each.
fn encode_numbers<T: Copy, const N: usize>(
    values: &[T],
    out: &mut [u8],
    to_le_bytes: impl Fn(T) -> [u8; N],
) -> Result<usize> {
    let bytes = values.len().checked_mul(N).ok_or(EncodeError::Overflow)?;
    let out = out.get_mut(..bytes).ok_or(EncodeError::OutputFull)?;
    for (dst, &value) in out.chunks_exact_mut(N).zip(values) {
        dst.copy_from_slice(&to_le_bytes(value));
    }
    Ok(bytes)
}

pub(crate) fn encode_i32(values: &[i32], out: &mut [u8]) -> Result<usize> {
    encode_numbers(values, out, i32::to_le_bytes)
}

pub(crate) fn encode_i64(values: &[i64], out: &mut [u8]) -> Result<usize> {
    encode_numbers(values, out, i64::to_le_bytes)
}

pub(crate) fn encode_f32(values: &[f32], out: &mut [u8]) -> Result<usize> {
    encode_numbers(values, out, f32::to_le_bytes)
}

pub(crate) fn encode_f64(values: &[f64], out: &mut [u8]) -> Result<usize> {
    encode_numbers(values, out, f64::to_le_bytes)
}

/// Bits packed LSB first, eight values per byte.
pub(crate) fn encode_bool(values: &[bool], out: &mut [u8]) -> Result<usize> {
    let bytes = values.len().div_ceil(8);
    let out = out.get_mut(..bytes).ok_or(EncodeError::OutputFull)?;
    for (byte, chunk) in out.iter_mut().zip(values.chunks(8)) {
        *byte = chunk
            .iter()
            .enumerate()
            .fold(0, |byte, (bit, &value)| byte | (u8::from(value) << bit));
    }
    Ok(bytes)
}

pub(crate) fn encode_fixed(width: u32, values: &[&[u8]], out: &mut [u8]) -> Result<usize> {
    let width = usize::try_from(width).map_err(|_| EncodeError::Overflow)?;
    let bytes = values
        .len()
        .checked_mul(width)
        .ok_or(EncodeError::Overflow)?;
    let out = out.get_mut(..bytes).ok_or(EncodeError::OutputFull)?;
    for (i, value) in values.iter().enumerate() {
        if value.len() != width {
            return Err(EncodeError::ValueOutOfRange);
        }
        out[i * width..(i + 1) * width].copy_from_slice(value);
    }
    Ok(bytes)
}

/// Each value as a 4-byte little-endian length and its bytes.
pub(crate) fn encode_bytes(values: &[&[u8]], out: &mut [u8]) -> Result<usize> {
    let mut at: usize = 0;
    for value in values {
        let len = u32::try_from(value.len()).map_err(|_| EncodeError::Overflow)?;
        let end = at
            .checked_add(4 + value.len())
            .ok_or(EncodeError::Overflow)?;
        let dst = out.get_mut(at..end).ok_or(EncodeError::OutputFull)?;
        dst[..4].copy_from_slice(&len.to_le_bytes());
        dst[4..].copy_from_slice(value);
        at = end;
    }
    Ok(at)
}
