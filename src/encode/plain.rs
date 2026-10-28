//! PLAIN value encoding. Bytes are appended to a caller-owned buffer.

use super::{EncodeError, Result};

pub fn encode_i32(values: &[i32], out: &mut [u8]) -> Result<usize> {
    let bytes = values.len().checked_mul(4).ok_or(EncodeError::Overflow)?;
    if out.len() < bytes { return Err(EncodeError::OutputFull); }
    for (i, value) in values.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&value.to_le_bytes());
    }
    Ok(bytes)
}

pub fn encode_i64(values: &[i64], out: &mut [u8]) -> Result<usize> {
    let bytes = values.len().checked_mul(8).ok_or(EncodeError::Overflow)?;
    if out.len() < bytes { return Err(EncodeError::OutputFull); }
    for (i, value) in values.iter().enumerate() {
        out[i * 8..i * 8 + 8].copy_from_slice(&value.to_le_bytes());
    }
    Ok(bytes)
}

pub fn encode_f32(values: &[f32], out: &mut [u8]) -> Result<usize> {
    let bytes = values.len().checked_mul(4).ok_or(EncodeError::Overflow)?;
    if out.len() < bytes { return Err(EncodeError::OutputFull); }
    for (i, value) in values.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&value.to_le_bytes());
    }
    Ok(bytes)
}

pub fn encode_f64(values: &[f64], out: &mut [u8]) -> Result<usize> {
    let bytes = values.len().checked_mul(8).ok_or(EncodeError::Overflow)?;
    if out.len() < bytes { return Err(EncodeError::OutputFull); }
    for (i, value) in values.iter().enumerate() {
        out[i * 8..i * 8 + 8].copy_from_slice(&value.to_le_bytes());
    }
    Ok(bytes)
}

pub fn encode_bool(values: &[bool], out: &mut [u8]) -> Result<usize> {
    let bytes = values.len().div_ceil(8);
    if out.len() < bytes { return Err(EncodeError::OutputFull); }
    for byte in &mut out[..bytes] { *byte = 0; }
    for (i, &value) in values.iter().enumerate() {
        if value { out[i >> 3] |= 1 << (i & 7); }
    }
    Ok(bytes)
}

pub fn encode_bytes(values: &[&[u8]], out: &mut [u8]) -> Result<usize> {
    let mut at: usize = 0;
    for value in values {
        let len = u32::try_from(value.len()).map_err(|_| EncodeError::Overflow)?;
        let end = at.checked_add(4 + value.len()).ok_or(EncodeError::Overflow)?;
        if end > out.len() { return Err(EncodeError::OutputFull); }
        out[at..at + 4].copy_from_slice(&len.to_le_bytes());
        out[at + 4..end].copy_from_slice(value);
        at = end;
    }
    Ok(at)
}
