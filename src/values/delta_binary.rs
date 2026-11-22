//! Incremental Parquet `DELTA_BINARY_PACKED` decoder (INT32 and INT64).
//!
//! State is constant-size apart from caller-owned mini-block bit widths.
//! A block stores all widths *before* its packed residuals, which makes
//! retaining those few bytes unavoidable for a single forward pass.
//!
//! `feed` consumes whole encoded mini-blocks, including the padding in the
//! final mini-block, before reporting completion. Bytes belonging to the
//! next stream are left untouched.

use super::{complete, Error, Result};
use crate::varint;

mod sealed {
    pub trait Sealed {}
    impl Sealed for i32 {}
    impl Sealed for i64 {}
}

/// Parquet integer types supported by `DELTA_BINARY_PACKED`.
/// Signed delta arithmetic uses wrapping physical-width operations.
pub trait DeltaInt: sealed::Sealed + Copy {
    /// Physical width in bits.
    const BITS: u8;
    /// Undo zigzag encoding; `None` if the value does not fit the type.
    fn zigzag(value: u64) -> Option<Self>;
    /// `self + min_delta + residual`, wrapping at the physical width.
    #[must_use]
    fn advance(self, min_delta: Self, residual: u64) -> Self;
}

// Zigzag and residual arithmetic reinterpret bits at the physical width;
// truncation and wrapping are what the format specifies.
#[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
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

#[allow(clippy::cast_possible_wrap)]
impl DeltaInt for i64 {
    const BITS: u8 = 64;
    fn zigzag(value: u64) -> Option<Self> {
        Some(((value >> 1) as i64) ^ (-((value & 1) as i64)))
    }
    fn advance(self, min_delta: Self, residual: u64) -> Self {
        self.wrapping_add(min_delta).wrapping_add(residual as i64)
    }
}

/// The varint fields of the stream header and of each block header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Header<T> {
    BlockSize,
    MiniCount,
    Count,
    First,
    /// A block's minimum delta; `previous` is the last value emitted.
    Minimum {
        previous: T,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase<T> {
    Header(Header<T>),
    /// Reading a block's mini-block bit widths.
    Widths {
        previous: T,
        minimum: T,
    },
    /// Unpacking a block's mini-blocks.
    Packed {
        previous: T,
        minimum: T,
    },
    Done,
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
    phase: Phase<T>,
    varint: varint::Decoder,
    block_size: usize,
    mini_count: usize,
    mini_size: usize,
    widths_read: usize,
    mini_index: usize,
    mini_position: usize,
    packed: u128,
    packed_bits: u8,
}

impl<'a, T: DeltaInt> DeltaBinary<'a, T> {
    /// A decoder for `expected` values using `widths` as mini-block scratch.
    pub const fn new(expected: u64, widths: &'a mut [u8]) -> Self {
        Self {
            widths,
            expected,
            produced: 0,
            phase: Phase::Header(Header::BlockSize),
            varint: varint::Decoder::new(),
            block_size: 0,
            mini_count: 0,
            mini_size: 0,
            widths_read: 0,
            mini_index: 0,
            mini_position: 0,
            packed: 0,
            packed_bits: 0,
        }
    }

    /// Reuse both the decoder and the caller's width scratch for the next
    /// consecutive delta stream, such as prefix lengths then suffix lengths.
    pub fn reset(&mut self, expected: u64) {
        let widths = core::mem::take(&mut self.widths);
        *self = Self::new(expected, widths);
    }

    /// Whether the stream, including mini-block padding, is fully consumed.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self.phase, Phase::Done)
    }

    /// Values emitted so far.
    #[must_use]
    pub const fn emitted(&self) -> u64 {
        self.produced
    }

    /// Check that the stream ended after its last value.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] if values are missing.
    pub fn finish(&self) -> Result<()> {
        complete(self.is_complete())
    }

    /// Returns the exact number of bytes belonging to this encoding.
    /// The callback is invoked synchronously and does not retain input bytes.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidDeltaHeader`] for an invalid block layout or first
    /// value, [`Error::LengthMismatch`] if the encoded count differs from
    /// `expected`, [`Error::DeltaWorkspaceTooSmall`] if `widths` cannot hold
    /// a block's widths, [`Error::InvalidBitWidth`] or [`Error::InvalidVarint`].
    pub fn feed(&mut self, input: &[u8], mut emit: impl FnMut(T)) -> Result<usize> {
        let mut at = 0;
        loop {
            let progressed = match self.phase {
                Phase::Header(header) => {
                    let Some(&byte) = input.get(at) else {
                        break;
                    };
                    at += 1;
                    if let Some(value) = self.varint.push(byte)? {
                        self.phase = self.header_field(header, value, &mut emit)?;
                    }
                    true
                }
                Phase::Widths { previous, minimum } => {
                    self.read_widths(input, &mut at, previous, minimum)
                }
                Phase::Packed { previous, minimum } => {
                    self.unpack(input, &mut at, previous, minimum, &mut emit)?
                }
                Phase::Done => false,
            };
            if !progressed {
                break;
            }
        }
        Ok(at)
    }

    /// Apply one decoded header varint; returns the next phase.
    fn header_field(
        &mut self,
        header: Header<T>,
        value: u64,
        emit: &mut impl FnMut(T),
    ) -> Result<Phase<T>> {
        Ok(match header {
            Header::BlockSize => {
                let n = usize::try_from(value).map_err(|_| Error::InvalidDeltaHeader)?;
                if n < 128 || n % 128 != 0 || u32::try_from(n).is_err() {
                    return Err(Error::InvalidDeltaHeader);
                }
                self.block_size = n;
                Phase::Header(Header::MiniCount)
            }
            Header::MiniCount => {
                let n = usize::try_from(value).map_err(|_| Error::InvalidDeltaHeader)?;
                if n == 0 || self.block_size % n != 0 || (self.block_size / n) % 32 != 0 {
                    return Err(Error::InvalidDeltaHeader);
                }
                if n > self.widths.len() {
                    return Err(Error::DeltaWorkspaceTooSmall {
                        needed: n,
                        available: self.widths.len(),
                    });
                }
                self.mini_count = n;
                self.mini_size = self.block_size / n;
                Phase::Header(Header::Count)
            }
            Header::Count => {
                if value != self.expected {
                    return Err(Error::LengthMismatch);
                }
                Phase::Header(Header::First)
            }
            Header::First => {
                let first = T::zigzag(value).ok_or(Error::InvalidDeltaHeader)?;
                if self.expected > 0 {
                    self.produced = 1;
                    emit(first);
                }
                if self.produced == self.expected {
                    Phase::Done
                } else {
                    Phase::Header(Header::Minimum { previous: first })
                }
            }
            Header::Minimum { previous } => {
                let minimum = T::zigzag(value).ok_or(Error::InvalidDeltaHeader)?;
                self.widths_read = 0;
                Phase::Widths { previous, minimum }
            }
        })
    }

    /// Copy the block's width bytes; returns whether they are all present.
    fn read_widths(&mut self, input: &[u8], at: &mut usize, previous: T, minimum: T) -> bool {
        let n = (self.mini_count - self.widths_read).min(input.len() - *at);
        self.widths[self.widths_read..self.widths_read + n].copy_from_slice(&input[*at..*at + n]);
        self.widths_read += n;
        *at += n;
        if self.widths_read < self.mini_count {
            return false;
        }
        self.mini_index = 0;
        self.mini_position = 0;
        self.packed = 0;
        self.packed_bits = 0;
        self.phase = Phase::Packed { previous, minimum };
        true
    }

    /// Unpack the current mini-block. Returns `false` when input runs out
    /// mid-block; the bit accumulator and position resume on the next call.
    fn unpack(
        &mut self,
        input: &[u8],
        at: &mut usize,
        mut previous: T,
        minimum: T,
        emit: &mut impl FnMut(T),
    ) -> Result<bool> {
        if self.mini_index == self.mini_count {
            self.phase = Phase::Header(Header::Minimum { previous });
            return Ok(true);
        }
        // Unused widths in the final block may contain *any* byte. There are
        // no corresponding mini-block bodies. A resumed mini-block
        // (mini_position > 0) can already have emitted its last real value;
        // its padding bits are still part of this stream and must be consumed.
        if self.produced == self.expected && self.mini_position == 0 {
            self.phase = Phase::Done;
            return Ok(true);
        }
        let width = self.widths[self.mini_index];
        if width > T::BITS {
            return Err(Error::InvalidBitWidth(width));
        }
        if width == 0 {
            let n = (self.expected - self.produced).min(self.mini_size as u64);
            for _ in 0..n {
                previous = previous.advance(minimum, 0);
                emit(previous);
            }
            self.produced += n;
            // Any padding values in this mini-block require 0 bits.
            self.mini_position = self.mini_size;
        } else {
            // A byte-at-a-time loop: the bit accumulator spans input chunks.
            let mask = (1u128 << width) - 1;
            while self.mini_position < self.mini_size {
                while self.packed_bits < width {
                    let Some(&byte) = input.get(*at) else {
                        self.phase = Phase::Packed { previous, minimum };
                        return Ok(false);
                    };
                    self.packed |= u128::from(byte) << self.packed_bits;
                    self.packed_bits += 8;
                    *at += 1;
                }
                let residual = residual(self.packed & mask);
                self.packed >>= width;
                self.packed_bits -= width;
                self.mini_position += 1;
                if self.produced < self.expected {
                    previous = previous.advance(minimum, residual);
                    self.produced += 1;
                    emit(previous);
                }
            }
        }
        // Mini-block lengths are multiples of 32, hence packed data always
        // ends on a byte boundary, including padding.
        debug_assert_eq!(self.packed_bits, 0);
        self.mini_position = 0;
        self.mini_index += 1;
        self.phase = if self.produced == self.expected {
            Phase::Done
        } else if self.mini_index == self.mini_count {
            Phase::Header(Header::Minimum { previous })
        } else {
            Phase::Packed { previous, minimum }
        };
        Ok(true)
    }
}

// Masked to a bit width of at most 64.
#[allow(clippy::cast_possible_truncation)]
const fn residual(masked: u128) -> u64 {
    masked as u64
}
