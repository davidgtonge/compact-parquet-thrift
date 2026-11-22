//! Incremental Parquet RLE / bit-packed hybrid decoding.
//!
//! A run is a ULEB128 header followed by either an RLE literal (even header)
//! or groups of eight bit-packed values (odd header). The bit width is fixed
//! per stream. Group data may be split at any input boundary.

use super::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Header,
    RepeatValue,
    Repeat,
    Packed,
}

#[derive(Debug, Clone, Copy)]
struct PackedGroup {
    /// Values still to emit from `scratch[..width]` for this bit-packed group.
    total: u8,
    /// Next value index within the group (0..total).
    at: u8,
}

#[derive(Debug, Clone, Copy)]
struct Varint {
    value: u64,
    shift: u8,
}

impl Varint {
    const fn new() -> Self {
        Self { value: 0, shift: 0 }
    }

    fn byte(&mut self, b: u8) -> Result<Option<u64>> {
        if self.shift == 63 && b > 1 {
            return Err(Error::InvalidVarint);
        }
        self.value |= u64::from(b & 0x7f) << self.shift;
        if b & 0x80 == 0 {
            let result = self.value;
            *self = Self::new();
            Ok(Some(result))
        } else if self.shift >= 63 {
            Err(Error::InvalidVarint)
        } else {
            self.shift += 7;
            Ok(None)
        }
    }
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
    varint: Varint,
    run_remaining: u64,
    repeat_value: u32,
    scratch: [u8; 32],
    scratch_len: usize,
    /// When set, `scratch` holds a full bit-packed group with partial emits in flight.
    packed_group: Option<PackedGroup>,
}

impl HybridRle {
    pub const fn new(bit_width: u8, count: u64) -> Result<Self> {
        if bit_width > 32 {
            return Err(Error::InvalidBitWidth(bit_width));
        }
        Ok(Self {
            bit_width,
            remaining: count,
            max_value: u32::MAX,
            mode: Mode::Header,
            varint: Varint::new(),
            run_remaining: 0,
            repeat_value: 0,
            scratch: [0; 32],
            scratch_len: 0,
            packed_group: None,
        })
    }

    /// Validate emitted indices / levels against a known dictionary or
    /// definition-level maximum before passing them to the consumer.
    pub const fn with_max_value(mut self, max_value: u32) -> Self {
        self.max_value = max_value;
        self
    }

    pub const fn remaining_values(&self) -> u64 {
        self.remaining
    }
    pub const fn is_complete(&self) -> bool {
        self.remaining == 0 && matches!(self.mode, Mode::Header)
    }
    pub fn finish(&self) -> Result<()> {
        if self.is_complete() {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }

    /// Returns bytes consumed. If the logical value count has been reached,
    /// trailing bytes are left for the caller to validate against its frame.
    pub fn feed(&mut self, input: &[u8], emit: impl FnMut(u32)) -> Result<usize> {
        self.feed_limited(input, usize::MAX, emit)
    }

    /// Emit at most `max_values` scalars; stops before consuming indices beyond the last emit.
    pub fn feed_limited(
        &mut self,
        input: &[u8],
        max_values: usize,
        mut emit: impl FnMut(u32),
    ) -> Result<usize> {
        if max_values == 0 {
            return Ok(0);
        }
        let mut at = 0;
        let mut emitted = 0usize;
        loop {
            if emitted >= max_values {
                break;
            }
            match self.mode {
                Mode::Header => {
                    if self.remaining == 0 || at == input.len() {
                        break;
                    }
                    let b = input[at];
                    at += 1;
                    let Some(header) = self.varint.byte(b)? else {
                        continue;
                    };
                    let count = header >> 1;
                    if count == 0 {
                        return Err(Error::InvalidRun);
                    }
                    // Parquet caps run length at 2^31-1 values.
                    if header & 1 == 0 {
                        if count > 0x7fff_ffff {
                            return Err(Error::InvalidRun);
                        }
                        // A repeated run has one payload. Extra repetitions
                        // past the logical count are not stored again.
                        self.run_remaining = count.min(self.remaining);
                        self.scratch_len = 0;
                        self.mode = Mode::RepeatValue;
                    } else {
                        if count > 0x7fff_ffff / 8 {
                            return Err(Error::InvalidRun);
                        }
                        // Groups past the logical count are padding and are
                        // consumed below without being emitted.
                        self.run_remaining = count;
                        self.scratch_len = 0;
                        self.mode = Mode::Packed;
                    }
                }
                Mode::RepeatValue => {
                    let width = usize::from(self.bit_width).div_ceil(8);
                    if width > 0 {
                        if at == input.len() {
                            break;
                        }
                        let n = (width - self.scratch_len).min(input.len() - at);
                        self.scratch[self.scratch_len..self.scratch_len + n]
                            .copy_from_slice(&input[at..at + n]);
                        self.scratch_len += n;
                        at += n;
                        if self.scratch_len != width {
                            continue;
                        }
                    }
                    let mut bytes = [0u8; 4];
                    bytes[..width].copy_from_slice(&self.scratch[..width]);
                    let raw = u32::from_le_bytes(bytes);
                    self.repeat_value = raw & mask(self.bit_width);
                    if self.repeat_value > self.max_value {
                        return Err(Error::ValueOutOfRange {
                            value: self.repeat_value,
                            max: self.max_value,
                        });
                    }
                    self.mode = Mode::Repeat;
                }
                Mode::Repeat => {
                    let cap = max_values - emitted;
                    let take = self.run_remaining.min(self.remaining).min(cap as u64);
                    for _ in 0..take {
                        emit(self.repeat_value);
                        emitted += 1;
                    }
                    self.run_remaining -= take;
                    self.remaining -= take;
                    if self.run_remaining == 0 {
                        self.mode = Mode::Header;
                    }
                }
                Mode::Packed => {
                    if self.run_remaining == 0 {
                        self.mode = Mode::Header;
                        self.packed_group = None;
                        continue;
                    }
                    let width = usize::from(self.bit_width);
                    if self.remaining == 0 {
                        self.packed_group = None;
                        if width == 0 {
                            self.run_remaining = 0;
                            self.mode = Mode::Header;
                            continue;
                        }
                        if self.scratch_len < width {
                            if at == input.len() {
                                break;
                            }
                            let n = (width - self.scratch_len).min(input.len() - at);
                            if n == 0 {
                                break;
                            }
                            self.scratch[self.scratch_len..self.scratch_len + n]
                                .copy_from_slice(&input[at..at + n]);
                            self.scratch_len += n;
                            at += n;
                            if self.scratch_len < width {
                                continue;
                            }
                        }
                        self.scratch_len = 0;
                        self.run_remaining -= 1;
                        if self.run_remaining == 0 {
                            self.mode = Mode::Header;
                        }
                        continue;
                    }

                    // Resume or load a full bit-packed group into scratch.
                    if self.packed_group.is_none() {
                        if self.scratch_len == 0 && input.len().saturating_sub(at) >= width {
                            self.scratch[..width].copy_from_slice(&input[at..at + width]);
                            self.scratch_len = width;
                            at += width;
                        } else {
                            if at == input.len() && width != 0 {
                                break;
                            }
                            let n = (width - self.scratch_len).min(input.len() - at);
                            if n == 0 {
                                break;
                            }
                            self.scratch[self.scratch_len..self.scratch_len + n]
                                .copy_from_slice(&input[at..at + n]);
                            self.scratch_len += n;
                            at += n;
                            if self.scratch_len < width {
                                continue;
                            }
                        }
                        let total = self.remaining.min(8) as u8;
                        if total == 0 {
                            break;
                        }
                        self.packed_group = Some(PackedGroup { total, at: 0 });
                    }

                    let group_at = self.packed_group.as_ref().expect("set above").at;
                    let group_total = self.packed_group.as_ref().expect("set above").total;
                    let left_in_group = group_total.saturating_sub(group_at);
                    let cap = max_values.saturating_sub(emitted);
                    if cap == 0 {
                        break;
                    }
                    // A group holds at most 8 values; saturate rather than truncate a large cap.
                    let take = left_in_group.min(u8::try_from(cap).unwrap_or(u8::MAX));
                    if take == 0 {
                        break;
                    }
                    decode_group(
                        self.bit_width,
                        self.max_value,
                        &self.scratch[..width],
                        group_at,
                        take,
                        &mut |v| {
                            emit(v);
                            emitted += 1;
                        },
                    )?;
                    let new_at = group_at + take;
                    self.remaining -= u64::from(take);
                    if emitted >= max_values && new_at < group_total {
                        if let Some(g) = &mut self.packed_group {
                            g.at = new_at;
                        }
                        break;
                    }
                    if new_at >= group_total {
                        self.packed_group = None;
                        self.scratch_len = 0;
                        self.run_remaining -= 1;
                        if self.run_remaining == 0 {
                            self.mode = Mode::Header;
                        }
                    } else if let Some(g) = &mut self.packed_group {
                        g.at = new_at;
                    }
                }
            }
        }
        Ok(at)
    }
}

fn mask(bit_width: u8) -> u32 {
    if bit_width == 32 {
        u32::MAX
    } else {
        (1u32 << bit_width) - 1
    }
}

fn decode_group(
    bit_width: u8,
    max_value: u32,
    group: &[u8],
    start: u8,
    count: u8,
    emit: &mut impl FnMut(u32),
) -> Result<()> {
    let mask = u64::from(mask(bit_width));
    for i in 0..usize::from(count) {
        let idx = usize::from(start) + i;
        let bit_offset = idx * usize::from(bit_width);
        let byte_offset = bit_offset / 8;
        let shift = bit_offset % 8;
        // At most 5 bytes cover a 32-bit value starting at any bit offset.
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
    pub fn is_complete(&self) -> bool {
        if self.expected_values == 0 {
            return true;
        }
        self.indices.as_ref().is_some_and(HybridRle::is_complete)
    }
    pub fn finish(&self) -> Result<()> {
        if self.is_complete() {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }
    pub fn feed(&mut self, input: &[u8], emit: impl FnMut(u32)) -> Result<usize> {
        self.feed_limited(input, usize::MAX, emit)
    }

    pub fn feed_limited(
        &mut self,
        input: &[u8],
        max_values: usize,
        emit: impl FnMut(u32),
    ) -> Result<usize> {
        if self.expected_values == 0 || max_values == 0 {
            return Ok(0);
        }
        let mut at = 0;
        if self.indices.is_none() {
            if input.is_empty() {
                return Ok(0);
            }
            let width = input[0];
            self.indices = Some(
                HybridRle::new(width, self.expected_values)?
                    .with_max_value(self.dictionary_length - 1),
            );
            at = 1;
        }
        at += self
            .indices
            .as_mut()
            .expect("initialized above")
            .feed_limited(&input[at..], max_values, emit)?;
        Ok(at)
    }
}
