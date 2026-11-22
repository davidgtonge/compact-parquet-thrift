//! Streaming decompression with caller-owned buffers.
//!
//! `Identity` has zero-copy output. `RawSnappy` implements the raw Snappy
//! block format used by Parquet (not Snappy's framed stream format). A bounded
//! history window is required for back-references; offsets beyond its capacity
//! fail explicitly instead of silently allocating.

use super::{Error, Result};

pub trait Inflate {
    fn codec_id(&self) -> i32;

    /// Reset for one *compressed section*, not necessarily a whole page:
    /// V2 definition levels bypass decompression.
    fn reset(&mut self, uncompressed_size: usize) -> Result<()>;

    /// Consume the entire offered input and synchronously emit decompressed
    /// fragments. Output fragments are valid only during the callback.
    fn push(&mut self, input: &[u8], emit: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()>;

    fn finish(&mut self, emit: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()>;
}

#[derive(Default)]
pub struct Identity {
    expected: usize,
    seen: usize,
}

impl Inflate for Identity {
    fn codec_id(&self) -> i32 {
        0
    }
    fn reset(&mut self, uncompressed_size: usize) -> Result<()> {
        self.expected = uncompressed_size;
        self.seen = 0;
        Ok(())
    }
    fn push(&mut self, input: &[u8], emit: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        self.seen = self
            .seen
            .checked_add(input.len())
            .ok_or(Error::LengthMismatch)?;
        if self.seen > self.expected {
            return Err(Error::LengthMismatch);
        }
        if !input.is_empty() {
            emit(input)?;
        }
        Ok(())
    }
    fn finish(&mut self, _: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        if self.seen == self.expected {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }
}

#[derive(Clone, Copy)]
enum SnappyState {
    Length {
        value: u64,
        shift: u8,
    },
    Tag,
    LiteralExtra {
        left: u8,
        value: u32,
        shift: u8,
    },
    Literal {
        left: usize,
    },
    Offset {
        left: u8,
        offset: u32,
        shift: u8,
        copy_len: usize,
    },
    Done,
}

/// Raw Snappy with no heap allocation. `history` determines the largest
/// supported backward copy distance; `output` bounds callback fragment sizes.
/// Both may be as small as one byte, but 64 KiB of history and 32 KiB of
/// output are useful initial settings (larger Snappy offsets may occur).
///
/// Parquet ZSTD/GZIP/etc. can use the same `Inflate` trait with independently
/// managed, reusable decoder contexts; they are intentionally not bundled.
pub struct RawSnappy<'a> {
    history: &'a mut [u8],
    output: &'a mut [u8],
    state: SnappyState,
    expected: usize,
    produced: usize,
    history_pos: usize,
    output_len: usize,
}

impl<'a> RawSnappy<'a> {
    pub fn new(history: &'a mut [u8], output: &'a mut [u8]) -> Result<Self> {
        if history.is_empty() || output.is_empty() {
            return Err(Error::WorkspaceTooSmall {
                needed: 1,
                available: history.len().min(output.len()),
            });
        }
        Ok(Self {
            history,
            output,
            state: SnappyState::Done,
            expected: 0,
            produced: 0,
            history_pos: 0,
            output_len: 0,
        })
    }

    fn flush(&mut self, emit: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        if self.output_len > 0 {
            emit(&self.output[..self.output_len])?;
            self.output_len = 0;
        }
        Ok(())
    }

    fn byte(&mut self, value: u8, emit: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        if self.produced == self.expected {
            return Err(Error::LengthMismatch);
        }
        self.history[self.history_pos] = value;
        self.history_pos += 1;
        if self.history_pos == self.history.len() {
            self.history_pos = 0;
        }
        self.output[self.output_len] = value;
        self.output_len += 1;
        self.produced += 1;
        if self.output_len == self.output.len() {
            self.flush(emit)?;
        }
        Ok(())
    }

    fn copy(
        &mut self,
        offset: usize,
        length: usize,
        emit: &mut impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        if offset == 0 || offset > self.produced {
            return Err(Error::CorruptSnappy);
        }
        if offset > self.history.len() {
            return Err(Error::SnappyHistoryTooSmall {
                offset,
                capacity: self.history.len(),
            });
        }
        if length > self.expected - self.produced {
            return Err(Error::LengthMismatch);
        }
        for _ in 0..length {
            let from = if self.history_pos >= offset {
                self.history_pos - offset
            } else {
                self.history.len() + self.history_pos - offset
            };
            let value = self.history[from];
            self.byte(value, emit)?;
        }
        Ok(())
    }
}

impl Inflate for RawSnappy<'_> {
    fn codec_id(&self) -> i32 {
        1
    }
    fn reset(&mut self, uncompressed_size: usize) -> Result<()> {
        // Raw Snappy's uncompressed-length prefix is a u32 varint.
        if uncompressed_size > u32::MAX as usize {
            return Err(Error::InvalidPageSize);
        }
        self.state = SnappyState::Length { value: 0, shift: 0 };
        self.expected = uncompressed_size;
        self.produced = 0;
        self.history_pos = 0;
        self.output_len = 0;
        Ok(())
    }

    fn push(&mut self, input: &[u8], emit: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        let mut at = 0;
        while at < input.len() {
            match self.state {
                SnappyState::Length { value, shift } => {
                    let b = input[at];
                    at += 1;
                    let next = value | (u64::from(b & 0x7f) << shift);
                    if next > u32::MAX as u64 {
                        return Err(Error::CorruptSnappy);
                    }
                    if b & 0x80 != 0 {
                        if shift >= 28 {
                            return Err(Error::CorruptSnappy);
                        }
                        self.state = SnappyState::Length {
                            value: next,
                            shift: shift + 7,
                        };
                    } else {
                        if next as usize != self.expected {
                            return Err(Error::LengthMismatch);
                        }
                        self.state = if self.expected == 0 {
                            SnappyState::Done
                        } else {
                            SnappyState::Tag
                        };
                    }
                }
                SnappyState::Tag => {
                    let tag = input[at];
                    at += 1;
                    match tag & 3 {
                        0 => {
                            let n = tag >> 2;
                            self.state = if n < 60 {
                                SnappyState::Literal {
                                    left: (n as usize) + 1,
                                }
                            } else {
                                SnappyState::LiteralExtra {
                                    left: n - 59,
                                    value: 0,
                                    shift: 0,
                                }
                            };
                        }
                        1 => {
                            let offset = u32::from(tag & 0xe0) << 3;
                            let len = 4 + usize::from((tag >> 2) & 7);
                            self.state = SnappyState::Offset {
                                left: 1,
                                offset,
                                shift: 0,
                                copy_len: len,
                            };
                        }
                        code => {
                            self.state = SnappyState::Offset {
                                left: if code == 2 { 2 } else { 4 },
                                offset: 0,
                                shift: 0,
                                copy_len: 1 + usize::from(tag >> 2),
                            };
                        }
                    }
                }
                SnappyState::LiteralExtra { left, value, shift } => {
                    let b = input[at];
                    at += 1;
                    let next = value | (u32::from(b) << shift);
                    if left == 1 {
                        let len = next.checked_add(1).ok_or(Error::CorruptSnappy)? as usize;
                        self.state = SnappyState::Literal { left: len };
                    } else {
                        self.state = SnappyState::LiteralExtra {
                            left: left - 1,
                            value: next,
                            shift: shift + 8,
                        };
                    }
                }
                SnappyState::Literal { left } => {
                    if left > self.expected - self.produced {
                        return Err(Error::LengthMismatch);
                    }
                    let n = left.min(input.len() - at);
                    for &b in &input[at..at + n] {
                        self.byte(b, emit)?;
                    }
                    at += n;
                    self.state = if left == n {
                        if self.produced == self.expected {
                            SnappyState::Done
                        } else {
                            SnappyState::Tag
                        }
                    } else {
                        SnappyState::Literal { left: left - n }
                    };
                }
                SnappyState::Offset {
                    left,
                    offset,
                    shift,
                    copy_len,
                } => {
                    let b = input[at];
                    at += 1;
                    let next = offset | (u32::from(b) << shift);
                    if left == 1 {
                        self.copy(next as usize, copy_len, emit)?;
                        self.state = if self.produced == self.expected {
                            SnappyState::Done
                        } else {
                            SnappyState::Tag
                        };
                    } else {
                        self.state = SnappyState::Offset {
                            left: left - 1,
                            offset: next,
                            shift: shift + 8,
                            copy_len,
                        };
                    }
                }
                SnappyState::Done => return Err(Error::CorruptSnappy),
            }
        }
        Ok(())
    }

    fn finish(&mut self, emit: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        if !matches!(self.state, SnappyState::Done) || self.produced != self.expected {
            return Err(Error::Truncated);
        }
        self.flush(emit)
    }
}
