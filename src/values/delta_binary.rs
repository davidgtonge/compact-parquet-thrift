//! Incremental Parquet DELTA_BINARY_PACKED decoder (INT32 and INT64).
//!
//! State is constant-size apart from caller-owned mini-block bit widths.
//! A block stores all widths *before* its packed residuals, which makes
//! retaining those few bytes unavoidable for a single forward pass.
//!
//! `feed` consumes whole encoded mini-blocks, including the padding in the
//! final mini-block, before reporting completion. Bytes belonging to the
//! next stream are left untouched.

use core::marker::PhantomData;
use super::{Error, Result};

mod sealed {
    pub trait Sealed {}
    impl Sealed for i32 {}
    impl Sealed for i64 {}
}

/// Parquet integer types supported by DELTA_BINARY_PACKED.
/// Signed delta arithmetic uses wrapping physical-width operations.
pub trait DeltaInt: sealed::Sealed + Copy {
    const BITS: u8;
    fn zigzag(value: u64) -> Option<Self>;
    fn advance(self, min_delta: Self, residual: u64) -> Self;
}

impl DeltaInt for i32 {
    const BITS: u8 = 32;
    fn zigzag(value: u64) -> Option<Self> {
        let value = u32::try_from(value).ok()?;
        Some(((value >> 1) as i32) ^ (-((value & 1) as i32)))
    }
    fn advance(self, min_delta: Self, residual: u64) -> Self {
        self.wrapping_add(min_delta).wrapping_add(residual as i32)
    }
}

impl DeltaInt for i64 {
    const BITS: u8 = 64;
    fn zigzag(value: u64) -> Option<Self> {
        Some(((value >> 1) as i64) ^ (-((value & 1) as i64)))
    }
    fn advance(self, min_delta: Self, residual: u64) -> Self {
        self.wrapping_add(min_delta).wrapping_add(residual as i64)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase { BlockSize, MiniCount, Count, First, Minimum, Widths, Packed, Done }

#[derive(Debug, Clone, Copy)]
struct Varint { value: u64, shift: u8 }

impl Varint {
    const fn new() -> Self { Self { value: 0, shift: 0 } }

    fn push(&mut self, b: u8) -> Result<Option<u64>> {
        if self.shift == 63 && (b & 0x7f) > 1 {
            return Err(Error::InvalidVarint);
        }
        self.value |= u64::from(b & 0x7f) << self.shift;
        if b & 0x80 == 0 {
            let result = self.value;
            *self = Self::new();
            Ok(Some(result))
        } else if self.shift == 63 {
            Err(Error::InvalidVarint)
        } else {
            self.shift += 7;
            Ok(None)
        }
    }
}

/// Borrowing, allocation-free, resuming decoder for a single delta integer
/// stream. `widths` is the caller's scratch space for one block's mini-block
/// widths (4 bytes for the common Parquet layout).
///
/// `expected` must be the number of physically present values in this page.
/// The encoded header's count is checked against it.
#[derive(Debug)]
pub struct DeltaBinary<'a, T: DeltaInt> {
    widths: &'a mut [u8],
    expected: u64,
    produced: u64,
    phase: Phase,
    varint: Varint,
    block_size: usize,
    mini_count: usize,
    mini_size: usize,
    widths_read: usize,
    mini_index: usize,
    mini_position: usize,
    packed: u128,
    packed_bits: u8,
    minimum: Option<T>,
    previous: Option<T>,
    marker: PhantomData<T>,
}

impl<'a, T: DeltaInt> DeltaBinary<'a, T> {
    pub const fn new(expected: u64, widths: &'a mut [u8]) -> Self {
        Self {
            widths, expected, produced: 0, phase: Phase::BlockSize,
            varint: Varint::new(), block_size: 0, mini_count: 0,
            mini_size: 0, widths_read: 0, mini_index: 0,
            mini_position: 0, packed: 0, packed_bits: 0,
            minimum: None, previous: None, marker: PhantomData,
        }
    }

    /// Reuse both the decoder and the caller's width scratch for the next
    /// consecutive delta stream, such as prefix lengths then suffix lengths.
    pub fn reset(&mut self, expected: u64) {
        self.expected = expected;
        self.produced = 0;
        self.phase = Phase::BlockSize;
        self.varint = Varint::new();
        self.block_size = 0;
        self.mini_count = 0;
        self.mini_size = 0;
        self.widths_read = 0;
        self.mini_index = 0;
        self.mini_position = 0;
        self.packed = 0;
        self.packed_bits = 0;
        self.minimum = None;
        self.previous = None;
    }

    pub const fn is_complete(&self) -> bool { matches!(self.phase, Phase::Done) }
    pub const fn emitted(&self) -> u64 { self.produced }
    pub fn finish(&self) -> Result<()> {
        if self.is_complete() { Ok(()) } else { Err(Error::Truncated) }
    }

    /// Returns the exact number of bytes belonging to this encoding.
    /// The callback is invoked synchronously and does not retain input bytes.
    pub fn feed(&mut self, input: &[u8], mut emit: impl FnMut(T)) -> Result<usize> {
        let mut at = 0;
        loop {
            match self.phase {
                Phase::BlockSize | Phase::MiniCount | Phase::Count | Phase::First | Phase::Minimum => {
                    if at == input.len() { break; }
                    let b = input[at];
                    at += 1;
                    let Some(value) = self.varint.push(b)? else { continue; };
                    match self.phase {
                        Phase::BlockSize => {
                            let n = usize::try_from(value).map_err(|_| Error::InvalidDeltaHeader)?;
                            if n < 128 || n % 128 != 0 || n > u32::MAX as usize {
                                return Err(Error::InvalidDeltaHeader);
                            }
                            self.block_size = n;
                            self.phase = Phase::MiniCount;
                        }
                        Phase::MiniCount => {
                            let n = usize::try_from(value).map_err(|_| Error::InvalidDeltaHeader)?;
                            if n == 0 || self.block_size % n != 0 ||
                                (self.block_size / n) % 32 != 0 {
                                return Err(Error::InvalidDeltaHeader);
                            }
                            if n > self.widths.len() {
                                return Err(Error::DeltaWorkspaceTooSmall {
                                    needed: n, available: self.widths.len(),
                                });
                            }
                            self.mini_count = n;
                            self.mini_size = self.block_size / n;
                            self.phase = Phase::Count;
                        }
                        Phase::Count => {
                            if value != self.expected { return Err(Error::LengthMismatch); }
                            self.phase = Phase::First;
                        }
                        Phase::First => {
                            let first = T::zigzag(value).ok_or(Error::InvalidDeltaHeader)?;
                            self.previous = Some(first);
                            if self.expected > 0 {
                                self.produced = 1;
                                emit(first);
                            }
                            self.phase = if self.produced == self.expected {
                                Phase::Done
                            } else {
                                Phase::Minimum
                            };
                        }
                        Phase::Minimum => {
                            self.minimum = Some(T::zigzag(value).ok_or(Error::InvalidDeltaHeader)?);
                            self.widths_read = 0;
                            self.phase = Phase::Widths;
                        }
                        _ => unreachable!(),
                    }
                }
                Phase::Widths => {
                    let available = input.len() - at;
                    let need = self.mini_count - self.widths_read;
                    let n = available.min(need);
                    self.widths[self.widths_read..self.widths_read + n]
                        .copy_from_slice(&input[at..at + n]);
                    self.widths_read += n;
                    at += n;
                    if self.widths_read < self.mini_count { break; }
                    self.mini_index = 0;
                    self.mini_position = 0;
                    self.packed = 0;
                    self.packed_bits = 0;
                    self.phase = Phase::Packed;
                }
                Phase::Packed => {
                    if self.mini_index == self.mini_count {
                        self.phase = Phase::Minimum;
                        continue;
                    }
                    // Unused widths in the final block may contain *any*
                    // byte. There are no corresponding mini-block bodies.
                    // A resumed mini-block (mini_position > 0) can already
                    // have emitted its last real value; its padding bits are
                    // still part of this stream and must be consumed.
                    if self.produced == self.expected && self.mini_position == 0 {
                        self.phase = Phase::Done;
                        continue;
                    }
                    let width = self.widths[self.mini_index];
                    if width > T::BITS { return Err(Error::InvalidBitWidth(width)); }
                    if width == 0 {
                        let n = (self.expected - self.produced).min(self.mini_size as u64);
                        for _ in 0..n {
                            let next = self.previous.expect("first value parsed")
                                .advance(self.minimum.expect("block minimum parsed"), 0);
                            self.previous = Some(next);
                            self.produced += 1;
                            emit(next);
                        }
                        // Any padding values in this mini-block require 0 bits.
                        self.mini_position = self.mini_size;
                    } else {
                        while self.mini_position < self.mini_size {
                            while self.packed_bits < width {
                                if at == input.len() { return Ok(at); }
                                self.packed |= u128::from(input[at]) << self.packed_bits;
                                self.packed_bits += 8;
                                at += 1;
                            }
                            let value = self.packed & ((1u128 << width) - 1);
                            self.packed >>= width;
                            self.packed_bits -= width;
                            self.mini_position += 1;
                            if self.produced < self.expected {
                                let next = self.previous.expect("first value parsed")
                                    .advance(self.minimum.expect("block minimum parsed"), value as u64);
                                self.previous = Some(next);
                                self.produced += 1;
                                emit(next);
                            }
                        }
                    }
                    // Mini-block lengths are multiples of 32, hence packed
                    // data always ends on a byte boundary, including padding.
                    debug_assert_eq!(self.packed_bits, 0);
                    self.mini_position = 0;
                    self.mini_index += 1;
                    if self.produced == self.expected { self.phase = Phase::Done; }
                    else if self.mini_index == self.mini_count { self.phase = Phase::Minimum; }
                }
                Phase::Done => break,
            }
        }
        Ok(at)
    }
}
