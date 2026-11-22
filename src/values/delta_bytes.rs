//! Streaming `DELTA_LENGTH_BYTE_ARRAY` and `DELTA_BYTE_ARRAY`.
//!
//! These formats place *all* the encoded lengths before the byte payloads.
//! A single-pass reader must therefore retain lengths until the values
//! arrive. The caller owns the exact-size metadata arrays; nothing allocates.
//! For `DELTA_BYTE_ARRAY`, the caller also owns a bounded buffer holding the
//! previous reconstructed value for prefix references.

use super::{chunk_len, complete, ByteArrayEvent, DeltaBinary, Error, Result};

fn valid_length(value: i32, max_length: u32) -> Result<u32> {
    let length = u32::try_from(value).map_err(|_| Error::InvalidDeltaLength(value))?;
    if length > max_length {
        return Err(Error::ValueTooLarge {
            length,
            limit: max_length,
        });
    }
    Ok(length)
}

/// Decode one delta-packed length stream from `input` into `store`, which
/// receives each validated length with its value index. Returns bytes
/// consumed; the stream is complete once `delta.is_complete()`.
fn read_lengths(
    delta: &mut DeltaBinary<'_, i32>,
    input: &[u8],
    max_length: u32,
    mut store: impl FnMut(u32),
) -> Result<usize> {
    // The value callback is infallible, so remember the first bad length.
    let mut error = None;
    let used = delta.feed(input, |value| {
        if error.is_none() {
            match valid_length(value, max_length) {
                Ok(length) => store(length),
                Err(e) => error = Some(e),
            }
        }
    })?;
    error.map_or(Ok(used), Err)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LengthPhase {
    Lengths,
    /// Emitting value bodies; `left` is set while a value is in progress.
    Bodies {
        left: Option<u32>,
    },
    Done,
}

/// `DELTA_LENGTH_BYTE_ARRAY` = delta-binary-packed lengths followed by a
/// contiguous concatenation of byte-array contents.
///
/// `lengths` needs at least `count` elements. `widths` needs at least one
/// byte per mini-block in the encoded delta header (commonly four).
#[derive(Debug)]
pub struct DeltaLengthByteArrays<'a> {
    lengths: &'a mut [u32],
    delta: DeltaBinary<'a, i32>,
    count: usize,
    max_length: u32,
    parsed: usize,
    index: usize,
    phase: LengthPhase,
}

impl<'a> DeltaLengthByteArrays<'a> {
    /// A decoder for `count` values of at most `max_length` bytes.
    ///
    /// # Errors
    ///
    /// [`Error::DeltaWorkspaceTooSmall`] if `lengths` holds fewer than `count`.
    pub fn new(
        count: usize,
        lengths: &'a mut [u32],
        widths: &'a mut [u8],
        max_length: u32,
    ) -> Result<Self> {
        if lengths.len() < count {
            return Err(Error::DeltaWorkspaceTooSmall {
                needed: count,
                available: lengths.len(),
            });
        }
        Ok(Self {
            lengths,
            delta: DeltaBinary::new(count as u64, widths),
            count,
            max_length,
            parsed: 0,
            index: 0,
            phase: LengthPhase::Lengths,
        })
    }

    /// Whether every value has been emitted.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self.phase, LengthPhase::Done)
    }

    /// Check that the stream ended after its last value.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] if values are missing.
    pub fn finish(&self) -> Result<()> {
        complete(self.is_complete())
    }

    /// Decode as much as `input` holds; returns bytes consumed. `Chunk`
    /// events borrow `input`.
    ///
    /// # Errors
    ///
    /// Any [`DeltaBinary`] error, [`Error::InvalidDeltaLength`] for a
    /// negative length or [`Error::ValueTooLarge`].
    pub fn feed<'input>(
        &mut self,
        input: &'input [u8],
        mut emit: impl FnMut(ByteArrayEvent<'input>),
    ) -> Result<usize> {
        let mut at = 0;
        loop {
            match self.phase {
                LengthPhase::Lengths => {
                    let lengths = &mut *self.lengths;
                    let parsed = &mut self.parsed;
                    at += read_lengths(&mut self.delta, &input[at..], self.max_length, |len| {
                        lengths[*parsed] = len;
                        *parsed += 1;
                    })?;
                    if !self.delta.is_complete() {
                        break;
                    }
                    debug_assert_eq!(self.parsed, self.count);
                    self.phase = LengthPhase::Bodies { left: None };
                }
                LengthPhase::Bodies { left: None } => {
                    if self.index == self.count {
                        self.phase = LengthPhase::Done;
                        continue;
                    }
                    let length = self.lengths[self.index];
                    emit(ByteArrayEvent::Start(length));
                    self.phase = LengthPhase::Bodies { left: Some(length) };
                }
                LengthPhase::Bodies { left: Some(0) } => {
                    emit(ByteArrayEvent::End);
                    self.index += 1;
                    self.phase = LengthPhase::Bodies { left: None };
                }
                LengthPhase::Bodies { left: Some(left) } => {
                    if at == input.len() {
                        break;
                    }
                    let n = chunk_len(left, input.len() - at);
                    let end = at + n as usize;
                    emit(ByteArrayEvent::Chunk(&input[at..end]));
                    at = end;
                    self.phase = LengthPhase::Bodies {
                        left: Some(left - n),
                    };
                }
                LengthPhase::Done => break,
            }
        }
        Ok(at)
    }
}

/// Caller-owned per-value workspace for `DELTA_BYTE_ARRAY`.
/// Only eight bytes per physical value are needed for prefix/suffix lengths.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeltaByteParts {
    /// Bytes shared with the previous value.
    pub prefix: u32,
    /// Bytes of new suffix following the prefix.
    pub suffix: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BytesPhase {
    Prefixes,
    Suffixes,
    /// Emitting value bodies; `left` is set while a suffix is in progress.
    Bodies {
        left: Option<u32>,
    },
    Done,
}

/// `DELTA_BYTE_ARRAY` = delta-binary-packed prefix lengths, then delta-
/// binary-packed suffix lengths, followed by all concatenated suffix bytes.
///
/// `previous` stores the last value in-place. On the next value its prefix
/// stays intact, and its suffix is overwritten by the new suffix. Events
/// borrow either the previous-value workspace or the incoming byte chunk;
/// neither needs a heap allocation or a full materialized page.
#[derive(Debug)]
pub struct DeltaByteArrays<'a> {
    parts: &'a mut [DeltaByteParts],
    previous: &'a mut [u8],
    delta: DeltaBinary<'a, i32>,
    count: usize,
    max_length: u32,
    fixed_length: Option<u32>,
    parsed: usize,
    index: usize,
    previous_length: u32,
    current_length: u32,
    write_at: usize,
    phase: BytesPhase,
}

impl<'a> DeltaByteArrays<'a> {
    /// A decoder for `count` values of at most `max_length` bytes, using
    /// `previous` (at least `max_length` bytes) to rebuild shared prefixes.
    ///
    /// # Errors
    ///
    /// [`Error::DeltaWorkspaceTooSmall`] if `parts` holds fewer than `count`.
    pub fn new(
        count: usize,
        parts: &'a mut [DeltaByteParts],
        widths: &'a mut [u8],
        previous: &'a mut [u8],
        max_length: u32,
    ) -> Result<Self> {
        if parts.len() < count {
            return Err(Error::DeltaWorkspaceTooSmall {
                needed: count,
                available: parts.len(),
            });
        }
        Ok(Self {
            parts,
            previous,
            delta: DeltaBinary::new(count as u64, widths),
            count,
            max_length,
            fixed_length: None,
            parsed: 0,
            index: 0,
            previous_length: 0,
            current_length: 0,
            write_at: 0,
            phase: BytesPhase::Prefixes,
        })
    }

    /// For physical `FIXED_LEN_BYTE_ARRAY`, validate the reconstructed length
    /// for every value (the wire encoding still stores both length streams).
    ///
    /// # Errors
    ///
    /// [`Error::InvalidFixedLength`] for zero, [`Error::ValueTooLarge`] above
    /// `max_length`, [`Error::OutputBufferTooSmall`] if `previous` cannot
    /// hold one value.
    pub fn with_fixed_length(mut self, length: u32) -> Result<Self> {
        if length == 0 {
            return Err(Error::InvalidFixedLength);
        }
        if length > self.max_length {
            return Err(Error::ValueTooLarge {
                length,
                limit: self.max_length,
            });
        }
        self.check_previous_capacity(length)?;
        self.fixed_length = Some(length);
        Ok(self)
    }

    /// Whether every value has been emitted.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self.phase, BytesPhase::Done)
    }

    /// Check that the stream ended after its last value.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] if values are missing.
    pub fn finish(&self) -> Result<()> {
        complete(self.is_complete())
    }

    /// The callback is higher-ranked because `Chunk` may refer to either the
    /// caller's network buffer or this decoder's previous-value workspace.
    /// Callers must consume each event before the callback returns.
    ///
    /// # Errors
    ///
    /// Any [`DeltaBinary`] error, [`Error::InvalidDeltaLength`],
    /// [`Error::ValueTooLarge`], [`Error::InvalidPrefix`] for a prefix longer
    /// than the previous value, [`Error::LengthMismatch`] for a wrong fixed
    /// length, or [`Error::OutputBufferTooSmall`].
    pub fn feed(
        &mut self,
        input: &[u8],
        mut emit: impl for<'part> FnMut(ByteArrayEvent<'part>),
    ) -> Result<usize> {
        let mut at = 0;
        loop {
            match self.phase {
                BytesPhase::Prefixes | BytesPhase::Suffixes => {
                    let prefixes = self.phase == BytesPhase::Prefixes;
                    let parts = &mut *self.parts;
                    let parsed = &mut self.parsed;
                    at += read_lengths(&mut self.delta, &input[at..], self.max_length, |len| {
                        let part = &mut parts[*parsed];
                        if prefixes {
                            part.prefix = len;
                        } else {
                            part.suffix = len;
                        }
                        *parsed += 1;
                    })?;
                    if !self.delta.is_complete() {
                        break;
                    }
                    debug_assert_eq!(self.parsed, self.count);
                    self.phase = if prefixes {
                        self.delta.reset(self.count as u64);
                        self.parsed = 0;
                        BytesPhase::Suffixes
                    } else {
                        BytesPhase::Bodies { left: None }
                    };
                }
                BytesPhase::Bodies { left: None } => {
                    if self.index == self.count {
                        self.phase = BytesPhase::Done;
                        continue;
                    }
                    let part = self.parts[self.index];
                    let length = self.start_value(part)?;
                    emit(ByteArrayEvent::Start(length));
                    if part.prefix != 0 {
                        emit(ByteArrayEvent::Chunk(&self.previous[..self.write_at]));
                    }
                    self.phase = BytesPhase::Bodies {
                        left: Some(part.suffix),
                    };
                }
                BytesPhase::Bodies { left: Some(0) } => {
                    emit(ByteArrayEvent::End);
                    self.previous_length = self.current_length;
                    self.index += 1;
                    self.phase = BytesPhase::Bodies { left: None };
                }
                BytesPhase::Bodies { left: Some(left) } => {
                    if at == input.len() {
                        break;
                    }
                    let n = chunk_len(left, input.len() - at);
                    let suffix = &input[at..at + n as usize];
                    self.previous[self.write_at..self.write_at + suffix.len()]
                        .copy_from_slice(suffix);
                    emit(ByteArrayEvent::Chunk(suffix));
                    self.write_at += suffix.len();
                    at += suffix.len();
                    self.phase = BytesPhase::Bodies {
                        left: Some(left - n),
                    };
                }
                BytesPhase::Done => break,
            }
        }
        Ok(at)
    }

    /// Validate the next value's prefix and length; returns its full length
    /// and positions `write_at` after the shared prefix.
    fn start_value(&mut self, part: DeltaByteParts) -> Result<u32> {
        if part.prefix > self.previous_length {
            return Err(Error::InvalidPrefix {
                prefix: part.prefix,
                previous_length: self.previous_length,
            });
        }
        let length = part
            .prefix
            .checked_add(part.suffix)
            .ok_or(Error::InvalidDeltaHeader)?;
        if length > self.max_length {
            return Err(Error::ValueTooLarge {
                length,
                limit: self.max_length,
            });
        }
        if self.fixed_length.is_some_and(|fixed| fixed != length) {
            return Err(Error::LengthMismatch);
        }
        self.check_previous_capacity(length)?;
        self.current_length = length;
        self.write_at = part.prefix as usize;
        Ok(length)
    }

    fn check_previous_capacity(&self, length: u32) -> Result<()> {
        let needed = length as usize;
        if needed > self.previous.len() {
            return Err(Error::OutputBufferTooSmall {
                needed,
                available: self.previous.len(),
            });
        }
        Ok(())
    }
}
