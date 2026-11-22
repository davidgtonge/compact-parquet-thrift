//! Streaming decompression with caller-owned buffers.
//!
//! `Identity` has zero-copy output. `RawSnappy` implements the raw Snappy
//! block format used by Parquet (not Snappy's framed stream format). A bounded
//! history window is required for back-references; offsets beyond its capacity
//! fail explicitly instead of silently allocating.

use super::{Error, Result};

/// A streaming decompressor for one page section at a time.
pub trait Inflate {
    /// The `parquet.thrift` `CompressionCodec` this decoder handles.
    fn codec_id(&self) -> i32;

    /// Reset for one *compressed section*, not necessarily a whole page:
    /// V2 definition levels bypass decompression.
    ///
    /// # Errors
    ///
    /// Implementation-defined; for example a size the codec cannot represent.
    fn reset(&mut self, uncompressed_size: usize) -> Result<()>;

    /// Consume the entire offered input and synchronously emit decompressed
    /// fragments. Output fragments are valid only during the callback.
    ///
    /// # Errors
    ///
    /// Corrupt input, output beyond the declared size, or an error returned
    /// by `emit`.
    fn push(&mut self, input: &[u8], emit: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()>;

    /// Flush buffered output and check that exactly the declared size was produced.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] if the section ended early, or an error from `emit`.
    fn finish(&mut self, emit: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()>;
}

/// The uncompressed codec: input fragments pass through unchanged.
#[derive(Debug, Default)]
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

#[derive(Debug, Clone, Copy)]
enum SnappyState {
    /// Consuming one byte at a time.
    Header(Header),
    /// Copying `left` literal bytes straight from the input.
    Literal { left: usize },
    /// All declared output produced.
    Done,
}

/// Snappy states that consume exactly one input byte per step.
#[derive(Debug, Clone, Copy)]
enum Header {
    /// The uncompressed-length varint.
    Length { value: u64, shift: u8 },
    /// An element tag.
    Tag,
    /// `left` more little-endian bytes of a long literal's length.
    LiteralExtra { left: u8, value: u32, shift: u8 },
    /// `left` more little-endian bytes of a copy offset.
    Offset {
        left: u8,
        offset: u32,
        shift: u8,
        copy_len: usize,
    },
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
    /// A decoder using caller-owned `history` (back-reference window) and
    /// `output` (fragment buffer).
    ///
    /// # Errors
    ///
    /// [`Error::WorkspaceTooSmall`] if either buffer is empty.
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

impl RawSnappy<'_> {
    /// Copy literal bytes from `input`; returns how many were taken.
    fn literal(
        &mut self,
        left: usize,
        input: &[u8],
        emit: &mut impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<usize> {
        if left > self.expected - self.produced {
            return Err(Error::LengthMismatch);
        }
        let n = left.min(input.len());
        input[..n].iter().try_for_each(|&b| self.byte(b, emit))?;
        Ok(n)
    }

    /// The state after a complete element: another tag, or the end.
    const fn next_element(&self) -> SnappyState {
        if self.produced == self.expected {
            SnappyState::Done
        } else {
            SnappyState::Header(Header::Tag)
        }
    }

    /// Advance a byte-at-a-time state (length varint, tag, literal length
    /// or copy offset) by one input byte.
    fn header_byte(
        &mut self,
        state: Header,
        b: u8,
        emit: &mut impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<SnappyState> {
        Ok(match state {
            Header::Length { value, shift } => {
                let next = value | (u64::from(b & 0x7f) << shift);
                if next > u64::from(u32::MAX) {
                    return Err(Error::CorruptSnappy);
                }
                if b & 0x80 != 0 {
                    if shift >= 28 {
                        return Err(Error::CorruptSnappy);
                    }
                    SnappyState::Header(Header::Length {
                        value: next,
                        shift: shift + 7,
                    })
                } else {
                    if usize::try_from(next) != Ok(self.expected) {
                        return Err(Error::LengthMismatch);
                    }
                    self.next_element()
                }
            }
            Header::Tag => tag(b),
            Header::LiteralExtra { left, value, shift } => {
                let next = value | (u32::from(b) << shift);
                if left == 1 {
                    let len = next.checked_add(1).ok_or(Error::CorruptSnappy)?;
                    SnappyState::Literal {
                        left: usize::try_from(len).map_err(|_| Error::CorruptSnappy)?,
                    }
                } else {
                    SnappyState::Header(Header::LiteralExtra {
                        left: left - 1,
                        value: next,
                        shift: shift + 8,
                    })
                }
            }
            Header::Offset {
                left,
                offset,
                shift,
                copy_len,
            } => {
                let next = offset | (u32::from(b) << shift);
                if left == 1 {
                    let offset = usize::try_from(next).map_err(|_| Error::CorruptSnappy)?;
                    self.copy(offset, copy_len, emit)?;
                    self.next_element()
                } else {
                    SnappyState::Header(Header::Offset {
                        left: left - 1,
                        offset: next,
                        shift: shift + 8,
                        copy_len,
                    })
                }
            }
        })
    }
}

/// Decode an element tag: a literal, or a copy with 1, 2 or 4 offset bytes.
fn tag(tag: u8) -> SnappyState {
    match tag & 3 {
        0 => {
            let n = tag >> 2;
            if n < 60 {
                SnappyState::Literal {
                    left: usize::from(n) + 1,
                }
            } else {
                SnappyState::Header(Header::LiteralExtra {
                    left: n - 59,
                    value: 0,
                    shift: 0,
                })
            }
        }
        1 => SnappyState::Header(Header::Offset {
            left: 1,
            offset: u32::from(tag & 0xe0) << 3,
            shift: 0,
            copy_len: 4 + usize::from((tag >> 2) & 7),
        }),
        code => SnappyState::Header(Header::Offset {
            left: if code == 2 { 2 } else { 4 },
            offset: 0,
            shift: 0,
            copy_len: 1 + usize::from(tag >> 2),
        }),
    }
}

impl Inflate for RawSnappy<'_> {
    fn codec_id(&self) -> i32 {
        1
    }
    fn reset(&mut self, uncompressed_size: usize) -> Result<()> {
        // Raw Snappy's uncompressed-length prefix is a u32 varint.
        if u32::try_from(uncompressed_size).is_err() {
            return Err(Error::InvalidPageSize);
        }
        self.state = SnappyState::Header(Header::Length { value: 0, shift: 0 });
        self.expected = uncompressed_size;
        self.produced = 0;
        self.history_pos = 0;
        self.output_len = 0;
        Ok(())
    }

    fn push(&mut self, input: &[u8], emit: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        let mut at = 0;
        while let Some(&byte) = input.get(at) {
            self.state = match self.state {
                SnappyState::Literal { left } => {
                    let n = self.literal(left, &input[at..], emit)?;
                    at += n;
                    if n == left {
                        self.next_element()
                    } else {
                        SnappyState::Literal { left: left - n }
                    }
                }
                SnappyState::Done => return Err(Error::CorruptSnappy),
                SnappyState::Header(state) => {
                    at += 1;
                    self.header_byte(state, byte, emit)?
                }
            };
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
