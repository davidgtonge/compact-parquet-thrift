//! Streaming DELTA_LENGTH_BYTE_ARRAY and DELTA_BYTE_ARRAY.
//!
//! These formats place *all* the encoded lengths before the byte payloads.
//! A single-pass reader must therefore retain lengths until the values
//! arrive. The caller owns the exact-size metadata arrays; nothing allocates.
//! For DELTA_BYTE_ARRAY, the caller also owns a bounded buffer holding the
//! previous reconstructed value for prefix references.

use super::{ByteArrayEvent, DeltaBinary, Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase { Lengths, Prefixes, Suffixes, Bodies, Done }

fn valid_length(value: i32, max_length: u32) -> Result<u32> {
    if value < 0 { return Err(Error::InvalidDeltaLength(value)); }
    let value = value as u32;
    if value > max_length {
        return Err(Error::ValueTooLarge { length: value, limit: max_length });
    }
    Ok(value)
}

/// DELTA_LENGTH_BYTE_ARRAY = delta-binary-packed lengths followed by a
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
    left: u32,
    started: bool,
    phase: Phase,
}

impl<'a> DeltaLengthByteArrays<'a> {
    pub fn new(
        count: usize,
        lengths: &'a mut [u32],
        widths: &'a mut [u8],
        max_length: u32,
    ) -> Result<Self> {
        if lengths.len() < count {
            return Err(Error::DeltaWorkspaceTooSmall {
                needed: count, available: lengths.len(),
            });
        }
        Ok(Self {
            lengths, delta: DeltaBinary::new(count as u64, widths),
            count, max_length, parsed: 0, index: 0, left: 0,
            started: false, phase: Phase::Lengths,
        })
    }

    pub const fn is_complete(&self) -> bool { matches!(self.phase, Phase::Done) }
    pub fn finish(&self) -> Result<()> {
        if self.is_complete() { Ok(()) } else { Err(Error::Truncated) }
    }

    pub fn feed<'input>(
        &mut self,
        input: &'input [u8],
        mut emit: impl FnMut(ByteArrayEvent<'input>),
    ) -> Result<usize> {
        let mut at = 0;
        loop {
            match self.phase {
                Phase::Lengths => {
                    let mut error = None;
                    let max_length = self.max_length;
                    let lengths = &mut *self.lengths;
                    let parsed = &mut self.parsed;
                    let used = self.delta.feed(&input[at..], |value| {
                        if error.is_some() { return; }
                        match valid_length(value, max_length) {
                            Ok(length) => {
                                lengths[*parsed] = length;
                                *parsed += 1;
                            }
                            Err(e) => error = Some(e),
                        }
                    })?;
                    at += used;
                    if let Some(e) = error { return Err(e); }
                    if !self.delta.is_complete() { break; }
                    debug_assert_eq!(self.parsed, self.count);
                    self.phase = Phase::Bodies;
                }
                Phase::Bodies => {
                    if self.index == self.count {
                        self.phase = Phase::Done;
                        continue;
                    }
                    if !self.started {
                        self.left = self.lengths[self.index];
                        self.started = true;
                        emit(ByteArrayEvent::Start(self.left));
                    }
                    if self.left == 0 {
                        emit(ByteArrayEvent::End);
                        self.index += 1;
                        self.started = false;
                        continue;
                    }
                    if at == input.len() { break; }
                    let n = (self.left as usize).min(input.len() - at);
                    emit(ByteArrayEvent::Chunk(&input[at..at + n]));
                    self.left -= n as u32;
                    at += n;
                }
                Phase::Done => break,
                _ => unreachable!(),
            }
        }
        Ok(at)
    }
}

/// Caller-owned per-value workspace for DELTA_BYTE_ARRAY.
/// Only eight bytes per physical value are needed for prefix/suffix lengths.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeltaByteParts {
    pub prefix: u32,
    pub suffix: u32,
}

/// DELTA_BYTE_ARRAY = delta-binary-packed prefix lengths, then delta-
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
    left: u32,
    started: bool,
    phase: Phase,
}

impl<'a> DeltaByteArrays<'a> {
    pub fn new(
        count: usize,
        parts: &'a mut [DeltaByteParts],
        widths: &'a mut [u8],
        previous: &'a mut [u8],
        max_length: u32,
    ) -> Result<Self> {
        if parts.len() < count {
            return Err(Error::DeltaWorkspaceTooSmall {
                needed: count, available: parts.len(),
            });
        }
        Ok(Self {
            parts, previous, delta: DeltaBinary::new(count as u64, widths),
            count, max_length, fixed_length: None, parsed: 0, index: 0,
            previous_length: 0, current_length: 0, write_at: 0,
            left: 0, started: false, phase: Phase::Prefixes,
        })
    }

    /// For physical FIXED_LEN_BYTE_ARRAY, validate the reconstructed length
    /// for every value (the wire encoding still stores both length streams).
    pub fn with_fixed_length(mut self, length: u32) -> Result<Self> {
        if length == 0 { return Err(Error::InvalidFixedLength); }
        if length > self.max_length {
            return Err(Error::ValueTooLarge { length, limit: self.max_length });
        }
        let needed = length as usize;
        if needed > self.previous.len() {
            return Err(Error::OutputBufferTooSmall {
                needed, available: self.previous.len(),
            });
        }
        self.fixed_length = Some(length);
        Ok(self)
    }

    pub const fn is_complete(&self) -> bool { matches!(self.phase, Phase::Done) }
    pub fn finish(&self) -> Result<()> {
        if self.is_complete() { Ok(()) } else { Err(Error::Truncated) }
    }

    /// The callback is higher-ranked because `Chunk` may refer to either the
    /// caller's network buffer or this decoder's previous-value workspace.
    /// Callers must consume each event before the callback returns.
    pub fn feed(
        &mut self,
        input: &[u8],
        mut emit: impl for<'part> FnMut(ByteArrayEvent<'part>),
    ) -> Result<usize> {
        let mut at = 0;
        loop {
            match self.phase {
                Phase::Prefixes | Phase::Suffixes => {
                    let prefixes = self.phase == Phase::Prefixes;
                    let mut error = None;
                    let max_length = self.max_length;
                    let parts = &mut *self.parts;
                    let parsed = &mut self.parsed;
                    let used = self.delta.feed(&input[at..], |value| {
                        if error.is_some() { return; }
                        match valid_length(value, max_length) {
                            Ok(length) => {
                                if prefixes { parts[*parsed].prefix = length; }
                                else { parts[*parsed].suffix = length; }
                                *parsed += 1;
                            }
                            Err(e) => error = Some(e),
                        }
                    })?;
                    at += used;
                    if let Some(e) = error { return Err(e); }
                    if !self.delta.is_complete() { break; }
                    debug_assert_eq!(self.parsed, self.count);
                    if prefixes {
                        self.delta.reset(self.count as u64);
                        self.parsed = 0;
                        self.phase = Phase::Suffixes;
                    } else {
                        self.phase = Phase::Bodies;
                    }
                }
                Phase::Bodies => {
                    if self.index == self.count {
                        self.phase = Phase::Done;
                        continue;
                    }
                    if !self.started {
                        let current = self.parts[self.index];
                        if current.prefix > self.previous_length {
                            return Err(Error::InvalidPrefix {
                                prefix: current.prefix,
                                previous_length: self.previous_length,
                            });
                        }
                        let length = current.prefix.checked_add(current.suffix)
                            .ok_or(Error::InvalidDeltaHeader)?;
                        if length > self.max_length {
                            return Err(Error::ValueTooLarge { length, limit: self.max_length });
                        }
                        if let Some(fixed) = self.fixed_length {
                            if length != fixed { return Err(Error::LengthMismatch); }
                        }
                        let needed = length as usize;
                        if needed > self.previous.len() {
                            return Err(Error::OutputBufferTooSmall {
                                needed, available: self.previous.len(),
                            });
                        }
                        self.current_length = length;
                        self.left = current.suffix;
                        self.write_at = current.prefix as usize;
                        self.started = true;
                        emit(ByteArrayEvent::Start(length));
                        if current.prefix != 0 {
                            emit(ByteArrayEvent::Chunk(&self.previous[..current.prefix as usize]));
                        }
                    }
                    if self.left == 0 {
                        emit(ByteArrayEvent::End);
                        self.previous_length = self.current_length;
                        self.index += 1;
                        self.started = false;
                        continue;
                    }
                    if at == input.len() { break; }
                    let n = (self.left as usize).min(input.len() - at);
                    self.previous[self.write_at..self.write_at + n]
                        .copy_from_slice(&input[at..at + n]);
                    emit(ByteArrayEvent::Chunk(&input[at..at + n]));
                    self.write_at += n;
                    self.left -= n as u32;
                    at += n;
                }
                Phase::Done => break,
                _ => unreachable!(),
            }
        }
        Ok(at)
    }
}
