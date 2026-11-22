//! Parquet PLAIN encoding. All types use borrowed inputs and fixed-size state.

use super::{Error, Result};
use core::marker::PhantomData;

mod sealed {
    pub trait Sealed {}
    impl Sealed for i32 {}
    impl Sealed for i64 {}
    impl Sealed for f32 {}
    impl Sealed for f64 {}
    impl Sealed for super::Int96 {}
}

/// Legacy INT96 is exposed as raw bytes; timestamp interpretation is separate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Int96(pub [u8; 12]);

/// Physical Parquet types with a fixed-width PLAIN encoding.
/// Sealed so WIDTH is always <= the internal 12-byte carry buffer.
pub trait PlainValue: sealed::Sealed + Copy {
    const WIDTH: usize;
    fn from_le_bytes(bytes: &[u8]) -> Self;
}

impl PlainValue for i32 {
    const WIDTH: usize = 4;
    fn from_le_bytes(bytes: &[u8]) -> Self {
        i32::from_le_bytes(bytes.try_into().expect("i32 width"))
    }
}
impl PlainValue for i64 {
    const WIDTH: usize = 8;
    fn from_le_bytes(bytes: &[u8]) -> Self {
        i64::from_le_bytes(bytes.try_into().expect("i64 width"))
    }
}
impl PlainValue for f32 {
    const WIDTH: usize = 4;
    fn from_le_bytes(bytes: &[u8]) -> Self {
        f32::from_le_bytes(bytes.try_into().expect("f32 width"))
    }
}
impl PlainValue for f64 {
    const WIDTH: usize = 8;
    fn from_le_bytes(bytes: &[u8]) -> Self {
        f64::from_le_bytes(bytes.try_into().expect("f64 width"))
    }
}
impl PlainValue for Int96 {
    const WIDTH: usize = 12;
    fn from_le_bytes(bytes: &[u8]) -> Self {
        let mut value = [0; 12];
        value.copy_from_slice(bytes);
        Self(value)
    }
}

/// Incrementally decode PLAIN fixed-width numbers without an output array.
/// `feed` stops once `count` values have been emitted, even if input has more.
#[derive(Debug)]
pub struct PlainValues<T: PlainValue> {
    remaining: u64,
    carry: [u8; 12],
    filled: usize,
    marker: PhantomData<T>,
}

impl<T: PlainValue> PlainValues<T> {
    pub const fn new(count: u64) -> Self {
        Self {
            remaining: count,
            carry: [0; 12],
            filled: 0,
            marker: PhantomData,
        }
    }

    pub const fn is_complete(&self) -> bool {
        self.remaining == 0 && self.filled == 0
    }

    pub fn finish(&self) -> Result<()> {
        if self.is_complete() {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }

    pub fn feed(&mut self, input: &[u8], emit: impl FnMut(T)) -> Result<usize> {
        self.feed_limited(input, usize::MAX, emit)
    }

    /// Emit at most `max_values` scalars; stops before consuming bytes beyond the last emitted value.
    pub fn feed_limited(
        &mut self,
        input: &[u8],
        max_values: usize,
        mut emit: impl FnMut(T),
    ) -> Result<usize> {
        if max_values == 0 {
            return Ok(0);
        }
        let mut at = 0;
        let mut emitted = 0usize;
        while self.remaining > 0 && at < input.len() && emitted < max_values {
            if self.filled > 0 {
                let n = (T::WIDTH - self.filled).min(input.len() - at);
                self.carry[self.filled..self.filled + n].copy_from_slice(&input[at..at + n]);
                self.filled += n;
                at += n;
                if self.filled == T::WIDTH {
                    emit(T::from_le_bytes(&self.carry[..T::WIDTH]));
                    self.filled = 0;
                    self.remaining -= 1;
                    emitted += 1;
                }
            } else if input.len() - at >= T::WIDTH {
                emit(T::from_le_bytes(&input[at..at + T::WIDTH]));
                at += T::WIDTH;
                self.remaining -= 1;
                emitted += 1;
            } else {
                let n = input.len() - at;
                self.carry[..n].copy_from_slice(&input[at..]);
                self.filled = n;
                at += n;
            }
        }
        Ok(at)
    }
}

/// BOOLEAN PLAIN uses LSB-first bits; final byte may contain padding bits.
///
/// A limited feed may stop in the middle of a byte. The unread bits stay in `held` so the
/// next call continues them; `used` counts only bytes the caller can drop.
#[derive(Debug, Clone, Copy)]
pub struct PlainBool {
    remaining: u64,
    bit: u8,
    held: Option<u8>,
}

impl PlainBool {
    pub const fn new(count: u64) -> Self {
        Self {
            remaining: count,
            bit: 0,
            held: None,
        }
    }
    pub const fn is_complete(&self) -> bool {
        self.remaining == 0
    }
    pub fn finish(&self) -> Result<()> {
        if self.is_complete() {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }
    pub fn feed(&mut self, input: &[u8], emit: impl FnMut(bool)) -> Result<usize> {
        self.feed_limited(input, usize::MAX, emit)
    }

    pub fn feed_limited(
        &mut self,
        input: &[u8],
        max_values: usize,
        mut emit: impl FnMut(bool),
    ) -> Result<usize> {
        if max_values == 0 {
            return Ok(0);
        }
        let mut at = 0;
        let mut emitted = 0usize;
        while self.remaining > 0 && emitted < max_values {
            if self.held.is_none() {
                if at >= input.len() {
                    break;
                }
                self.held = Some(input[at]);
                at += 1;
                self.bit = 0;
            }
            let byte = self.held.unwrap_or(0);
            if self.bit >= 8 {
                self.held = None;
                self.bit = 0;
                continue;
            }
            emit(byte & (1 << self.bit) != 0);
            self.bit += 1;
            self.remaining -= 1;
            emitted += 1;
            if self.bit == 8 || self.remaining == 0 {
                self.held = None;
                self.bit = 0;
            }
        }
        Ok(at)
    }
}

/// A borrowed event for a single BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY value.
/// A value may have multiple Chunk events when it crosses input boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteArrayEvent<'a> {
    Start(u32),
    Chunk(&'a [u8]),
    End,
}

#[derive(Debug, Clone, Copy)]
enum ByteState {
    Length { bytes: [u8; 4], filled: usize },
    Body { left: u32 },
}

/// PLAIN BYTE_ARRAY: a 4-byte little-endian length before each value.
/// The configured `max_value_bytes` guards against untrusted length prefixes.
#[derive(Debug, Clone, Copy)]
pub struct ByteArrays {
    remaining: u64,
    max_value_bytes: u32,
    state: ByteState,
}

impl ByteArrays {
    pub const fn new(count: u64, max_value_bytes: u32) -> Self {
        Self {
            remaining: count,
            max_value_bytes,
            state: ByteState::Length {
                bytes: [0; 4],
                filled: 0,
            },
        }
    }
    pub const fn is_complete(&self) -> bool {
        self.remaining == 0
    }
    pub fn finish(&self) -> Result<()> {
        if self.is_complete() {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }

    pub fn feed<'a>(
        &mut self,
        input: &'a [u8],
        mut emit: impl FnMut(ByteArrayEvent<'a>),
    ) -> Result<usize> {
        let mut at = 0;
        while self.remaining > 0 {
            match &mut self.state {
                ByteState::Length { bytes, filled } => {
                    if at == input.len() {
                        break;
                    }
                    let n = (4 - *filled).min(input.len() - at);
                    bytes[*filled..*filled + n].copy_from_slice(&input[at..at + n]);
                    *filled += n;
                    at += n;
                    if *filled == 4 {
                        let length = u32::from_le_bytes(*bytes);
                        if length > self.max_value_bytes {
                            return Err(Error::ValueTooLarge {
                                length,
                                limit: self.max_value_bytes,
                            });
                        }
                        self.state = ByteState::Body { left: length };
                        emit(ByteArrayEvent::Start(length));
                    }
                }
                ByteState::Body { left } => {
                    if *left == 0 {
                        emit(ByteArrayEvent::End);
                        self.remaining -= 1;
                        self.state = ByteState::Length {
                            bytes: [0; 4],
                            filled: 0,
                        };
                        continue;
                    }
                    if at == input.len() {
                        break;
                    }
                    let n = (*left as usize).min(input.len() - at);
                    emit(ByteArrayEvent::Chunk(&input[at..at + n]));
                    *left -= n as u32;
                    at += n;
                }
            }
        }
        Ok(at)
    }
}

/// PLAIN FIXED_LEN_BYTE_ARRAY. Byte slices are borrowed, potentially split
/// across multiple `Chunk` callbacks for one value.
#[derive(Debug, Clone, Copy)]
pub struct FixedByteArrays {
    remaining: u64,
    length: u32,
    left: u32,
}

impl FixedByteArrays {
    pub const fn new(count: u64, length: u32) -> Result<Self> {
        if length == 0 {
            return Err(Error::InvalidFixedLength);
        }
        Ok(Self {
            remaining: count,
            length,
            left: 0,
        })
    }
    pub const fn is_complete(&self) -> bool {
        self.remaining == 0
    }
    pub fn finish(&self) -> Result<()> {
        if self.is_complete() {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }
    pub fn feed<'a>(
        &mut self,
        input: &'a [u8],
        mut emit: impl FnMut(ByteArrayEvent<'a>),
    ) -> Result<usize> {
        let mut at = 0;
        while self.remaining > 0 && at < input.len() {
            if self.left == 0 {
                self.left = self.length;
                emit(ByteArrayEvent::Start(self.length));
            }
            let n = (self.left as usize).min(input.len() - at);
            emit(ByteArrayEvent::Chunk(&input[at..at + n]));
            at += n;
            self.left -= n as u32;
            if self.left == 0 {
                emit(ByteArrayEvent::End);
                self.remaining -= 1;
            }
        }
        Ok(at)
    }
}
