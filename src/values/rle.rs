//! Incremental Parquet RLE / bit-packed hybrid decoding.
//!
//! A run is a ULEB128 header followed by either an RLE literal (even header)
//! or groups of eight bit-packed values (odd header). The bit width is fixed
//! per stream. Group data may be split at any input boundary.

use super::{complete, Error, Result};
use crate::varint;

/// Parquet caps a run at 2^31 - 1 values.
const MAX_RUN: u64 = 0x7fff_ffff;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Reading a run header varint.
    Header,
    /// Reading the repeated value of an RLE run into `scratch`.
    RepeatValue,
    /// Emitting an RLE run's value.
    Repeat { value: u32 },
    /// Inside a bit-packed run; `group` is set while a loaded group of eight
    /// values is partially emitted.
    Packed { group: Option<PackedGroup> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PackedGroup {
    /// Logical values in this group (fewer than eight only at the stream end).
    total: u8,
    /// Next value index within the group (0..total).
    at: u8,
}

/// RLE / bit-packed hybrid decoder; uses a small, fixed-size internal
/// state. No page buffering, heap allocation or intermediate decoded array.
///
/// The last bit-packed run may contain more 8-value groups than the logical
/// values still remaining. Those extra groups are consumed and discarded:
/// writers flush a fixed-size literal block, and the value count ends the
/// stream. The caller frames the encoded bytes separately (V1 length prefix,
/// V2 level byte length, or the page's remaining values section).
#[derive(Debug, Clone)]
pub struct HybridRle {
    bit_width: u8,
    remaining: u64,
    max_value: u32,
    mode: Mode,
    varint: varint::Decoder,
    /// Values (RLE) or 8-value groups (bit-packed) left in the current run.
    run_remaining: u64,
    scratch: [u8; 32],
    scratch_len: usize,
}

impl HybridRle {
    /// A decoder for `count` values of `bit_width` bits.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidBitWidth`] if `bit_width` exceeds 32.
    pub const fn new(bit_width: u8, count: u64) -> Result<Self> {
        if bit_width > 32 {
            return Err(Error::InvalidBitWidth(bit_width));
        }
        Ok(Self {
            bit_width,
            remaining: count,
            max_value: u32::MAX,
            mode: Mode::Header,
            varint: varint::Decoder::new(),
            run_remaining: 0,
            scratch: [0; 32],
            scratch_len: 0,
        })
    }

    /// Validate emitted indices / levels against a known dictionary or
    /// definition-level maximum before passing them to the consumer.
    #[must_use]
    pub const fn with_max_value(mut self, max_value: u32) -> Self {
        self.max_value = max_value;
        self
    }

    /// Values not yet emitted.
    #[must_use]
    pub const fn remaining_values(&self) -> u64 {
        self.remaining
    }

    /// Whether every value has been emitted and no run is in progress.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.remaining == 0 && matches!(self.mode, Mode::Header)
    }

    /// Check that the stream ended after its last value.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] if values are missing.
    pub fn finish(&self) -> Result<()> {
        complete(self.is_complete())
    }

    /// Returns bytes consumed. If the logical value count has been reached,
    /// trailing bytes are left for the caller to validate against its frame.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidVarint`] or [`Error::InvalidRun`] for a malformed run
    /// header, [`Error::ValueOutOfRange`] for a value above the maximum.
    pub fn feed(&mut self, input: &[u8], emit: impl FnMut(u32)) -> Result<usize> {
        self.feed_limited(input, usize::MAX, emit)
    }

    /// Emit at most `max_values` scalars; stops before consuming indices beyond the last emit.
    ///
    /// # Errors
    ///
    /// As [`Self::feed`].
    pub fn feed_limited(
        &mut self,
        input: &[u8],
        max_values: usize,
        mut emit: impl FnMut(u32),
    ) -> Result<usize> {
        let mut at = 0;
        let mut emitted = 0usize;
        while emitted < max_values {
            let budget = max_values - emitted;
            let step = match self.mode {
                Mode::Header => self.read_header(input, &mut at)?,
                Mode::RepeatValue => self.read_repeat_value(input, &mut at)?,
                Mode::Repeat { value } => Some(self.emit_repeat(value, budget, &mut emit)),
                Mode::Packed { group } => self.packed(group, input, &mut at, budget, &mut emit)?,
            };
            match step {
                Some(n) => emitted += n,
                None => break,
            }
        }
        Ok(at)
    }

    /// Steps return `Some(values emitted)` to continue, `None` to wait for input.
    fn read_header(&mut self, input: &[u8], at: &mut usize) -> Result<Option<usize>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let Some(&byte) = input.get(*at) else {
            return Ok(None);
        };
        *at += 1;
        let Some(header) = self.varint.push(byte)? else {
            return Ok(Some(0));
        };
        let count = header >> 1;
        self.scratch_len = 0;
        if header & 1 == 0 {
            if count == 0 || count > MAX_RUN {
                return Err(Error::InvalidRun);
            }
            // A repeated run has one payload. Extra repetitions
            // past the logical count are not stored again.
            self.run_remaining = count.min(self.remaining);
            self.mode = Mode::RepeatValue;
        } else {
            if count == 0 || count > MAX_RUN / 8 {
                return Err(Error::InvalidRun);
            }
            // Groups past the logical count are padding and are
            // consumed without being emitted.
            self.run_remaining = count;
            self.mode = Mode::Packed { group: None };
        }
        Ok(Some(0))
    }

    fn read_repeat_value(&mut self, input: &[u8], at: &mut usize) -> Result<Option<usize>> {
        let width = usize::from(self.bit_width).div_ceil(8);
        if !self.fill_scratch(input, at, width) {
            return Ok(None);
        }
        let mut bytes = [0u8; 4];
        bytes[..width].copy_from_slice(&self.scratch[..width]);
        let value = u32::from_le_bytes(bytes) & mask(self.bit_width);
        self.check(value)?;
        self.mode = Mode::Repeat { value };
        Ok(Some(0))
    }

    fn emit_repeat(&mut self, value: u32, budget: usize, emit: &mut impl FnMut(u32)) -> usize {
        let take = self
            .run_remaining
            .min(self.remaining)
            .min(u64::try_from(budget).unwrap_or(u64::MAX));
        for _ in 0..take {
            emit(value);
        }
        self.run_remaining -= take;
        self.remaining -= take;
        if self.run_remaining == 0 {
            self.mode = Mode::Header;
        }
        // `take` is bounded by `budget`, a usize.
        usize::try_from(take).unwrap_or(budget)
    }

    fn packed(
        &mut self,
        group: Option<PackedGroup>,
        input: &[u8],
        at: &mut usize,
        budget: usize,
        emit: &mut impl FnMut(u32),
    ) -> Result<Option<usize>> {
        if self.run_remaining == 0 {
            self.mode = Mode::Header;
            return Ok(Some(0));
        }
        if self.remaining == 0 {
            return Ok(self.skip_padding_group(input, at).then_some(0));
        }
        let width = usize::from(self.bit_width);
        let group = if let Some(group) = group {
            group
        } else {
            if !self.fill_scratch(input, at, width) {
                return Ok(None);
            }
            PackedGroup {
                total: u8::try_from(self.remaining.min(8)).unwrap_or(8),
                at: 0,
            }
        };
        // A group holds at most 8 values; saturate rather than truncate a large budget.
        let take = (group.total - group.at).min(u8::try_from(budget).unwrap_or(u8::MAX));
        decode_group(
            self.bit_width,
            self.max_value,
            &self.scratch[..width],
            group.at,
            take,
            emit,
        )?;
        self.remaining -= u64::from(take);
        let next = group.at + take;
        if next < group.total {
            self.mode = Mode::Packed {
                group: Some(PackedGroup { at: next, ..group }),
            };
        } else {
            self.end_group();
        }
        Ok(Some(usize::from(take)))
    }

    /// Consume one padding group after the last logical value. Returns
    /// whether progress was made.
    fn skip_padding_group(&mut self, input: &[u8], at: &mut usize) -> bool {
        if self.bit_width == 0 {
            self.run_remaining = 0;
            self.mode = Mode::Header;
            return true;
        }
        if !self.fill_scratch(input, at, usize::from(self.bit_width)) {
            return false;
        }
        self.end_group();
        true
    }

    fn end_group(&mut self) {
        self.scratch_len = 0;
        self.run_remaining -= 1;
        self.mode = if self.run_remaining == 0 {
            Mode::Header
        } else {
            Mode::Packed { group: None }
        };
    }

    /// Top `scratch` up to `width` bytes from `input`; returns whether it is full.
    fn fill_scratch(&mut self, input: &[u8], at: &mut usize, width: usize) -> bool {
        let n = (width - self.scratch_len).min(input.len() - *at);
        self.scratch[self.scratch_len..self.scratch_len + n].copy_from_slice(&input[*at..*at + n]);
        self.scratch_len += n;
        *at += n;
        self.scratch_len == width
    }

    const fn check(&self, value: u32) -> Result<()> {
        if value > self.max_value {
            return Err(Error::ValueOutOfRange {
                value,
                max: self.max_value,
            });
        }
        Ok(())
    }
}

const fn mask(bit_width: u8) -> u32 {
    if bit_width >= 32 {
        u32::MAX
    } else {
        (1u32 << bit_width) - 1
    }
}

// `mask` keeps each extracted value within 32 bits.
#[allow(clippy::cast_possible_truncation)]
fn decode_group(
    bit_width: u8,
    max_value: u32,
    group: &[u8],
    start: u8,
    count: u8,
    emit: &mut impl FnMut(u32),
) -> Result<()> {
    let mask = u64::from(mask(bit_width));
    for idx in usize::from(start)..usize::from(start) + usize::from(count) {
        let bit_offset = idx * usize::from(bit_width);
        let byte_offset = bit_offset / 8;
        let shift = bit_offset % 8;
        // At most 5 bytes cover a 32-bit value starting at any bit offset.
        // A fixed loop: this runs once per decoded level or dictionary id.
        let mut window = 0u64;
        for j in 0..5 {
            if let Some(&b) = group.get(byte_offset + j) {
                window |= u64::from(b) << (j * 8);
            }
        }
        let value = ((window >> shift) & mask) as u32;
        if value > max_value {
            return Err(Error::ValueOutOfRange {
                value,
                max: max_value,
            });
        }
        emit(value);
    }
    Ok(())
}

/// Dictionary data pages begin with a one-byte bit width, followed by the
/// unframed RLE / bit-packed hybrid stream. Produces dictionary *indices*,
/// not materialized values; the dictionary can remain encoded in its own page.
#[derive(Debug, Clone)]
pub struct DictionaryIds {
    expected_values: u64,
    dictionary_length: u32,
    indices: Option<HybridRle>,
}

impl DictionaryIds {
    /// A decoder for `expected_values` indices into a dictionary of
    /// `dictionary_length` entries.
    ///
    /// # Errors
    ///
    /// [`Error::EmptyDictionary`] if values are expected but the dictionary is empty.
    pub const fn new(expected_values: u64, dictionary_length: u32) -> Result<Self> {
        if expected_values > 0 && dictionary_length == 0 {
            return Err(Error::EmptyDictionary);
        }
        Ok(Self {
            expected_values,
            dictionary_length,
            indices: None,
        })
    }

    /// Whether every index has been emitted.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.expected_values == 0 || self.indices.as_ref().is_some_and(HybridRle::is_complete)
    }

    /// Check that the stream ended after its last index.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] if indices are missing.
    pub fn finish(&self) -> Result<()> {
        complete(self.is_complete())
    }

    /// Decode as many indices as `input` holds; returns bytes consumed.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidBitWidth`] for a width byte above 32, or any
    /// [`HybridRle`] error, including an index outside the dictionary.
    pub fn feed(&mut self, input: &[u8], emit: impl FnMut(u32)) -> Result<usize> {
        self.feed_limited(input, usize::MAX, emit)
    }

    /// Emit at most `max_values` indices.
    ///
    /// # Errors
    ///
    /// As [`Self::feed`].
    pub fn feed_limited(
        &mut self,
        input: &[u8],
        max_values: usize,
        emit: impl FnMut(u32),
    ) -> Result<usize> {
        if self.expected_values == 0 || max_values == 0 {
            return Ok(0);
        }
        let (at, indices) = if let Some(indices) = self.indices.as_mut() {
            (0, indices)
        } else {
            // The stream starts with the bit width of the indices.
            let Some(&width) = input.first() else {
                return Ok(0);
            };
            let decoder = HybridRle::new(width, self.expected_values)?
                .with_max_value(self.dictionary_length - 1);
            (1, self.indices.insert(decoder))
        };
        Ok(at + indices.feed_limited(&input[at..], max_values, emit)?)
    }
}
