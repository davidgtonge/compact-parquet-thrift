//! Parquet definition / repetition level stream framing.
//!
//! Data Page V1 prefixes each present RLE-encoded levels stream with its
//! 4-byte little-endian *encoded byte length*. V2 records each length in the
//! page header and omits the prefix. Neither form prefixes dictionary ids.

use super::{chunk_len, complete, Error, HybridRle, Result};

#[derive(Debug, Clone, Copy)]
enum Frame {
    /// V1: the 4-byte length prefix is still arriving.
    Prefix {
        bytes: [u8; 4],
        filled: usize,
        limit: u32,
    },
    /// Inside the encoded levels, with this many frame bytes unread.
    Body { left: u32 },
}

/// Decode one encoded definition OR repetition-level stream. For a required
/// field (`max_level` = 0), levels are absent: do not construct this decoder.
/// It emits u32 levels, which callers may convert to a nullable bitmap.
#[derive(Debug, Clone)]
pub struct Levels {
    frame: Frame,
    decoder: HybridRle,
}

impl Levels {
    /// A Data Page V1 stream: a 4-byte length prefix of at most
    /// `max_encoded_bytes`, then the hybrid-encoded levels.
    ///
    /// # Errors
    ///
    /// Never for levels up to `u32::MAX`; the `Result` mirrors [`HybridRle::new`].
    pub fn v1(max_level: u32, value_count: u64, max_encoded_bytes: u32) -> Result<Self> {
        Ok(Self {
            frame: Frame::Prefix {
                bytes: [0; 4],
                filled: 0,
                limit: max_encoded_bytes,
            },
            decoder: level_decoder(max_level, value_count)?,
        })
    }

    /// A Data Page V2 stream of exactly `encoded_bytes`, with no prefix.
    ///
    /// # Errors
    ///
    /// As [`Self::v1`].
    pub fn v2(max_level: u32, value_count: u64, encoded_bytes: u32) -> Result<Self> {
        Ok(Self {
            frame: Frame::Body {
                left: encoded_bytes,
            },
            decoder: level_decoder(max_level, value_count)?,
        })
    }

    /// Whether every level and every framed byte has been consumed.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        matches!(self.frame, Frame::Body { left: 0 }) && self.decoder.is_complete()
    }

    /// Check that the stream ended after its last level.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] if levels or framed bytes are missing.
    pub fn finish(&self) -> Result<()> {
        complete(self.is_complete())
    }

    /// Parse at most the encoded levels stream; leave following page values
    /// unconsumed. The length prefix in V1 is counted in the returned bytes.
    ///
    /// # Errors
    ///
    /// [`Error::FrameTooLarge`] for an oversized V1 prefix, or any
    /// [`HybridRle`] error.
    pub fn feed(&mut self, input: &[u8], emit: impl FnMut(u32)) -> Result<usize> {
        self.feed_limited(input, usize::MAX, emit)
    }

    /// Emit at most `max_values` levels. A short read leaves the rest of the frame for the next call.
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
        if max_values == 0 {
            return Ok(0);
        }
        let (at, left) = self.frame_body(input)?;
        let Some(left) = left else {
            return Ok(at);
        };
        let take = chunk_len(left, input.len() - at);
        let body = &input[at..at + take as usize];
        let used = self.decoder.feed_limited(body, max_values, emit)?;
        // HybridRle stops once all logical levels are emitted, but the framed
        // buffer may still contain writer padding. Snappy (and other codecs)
        // can deliver that buffer in multiple decompressed fragments, so a
        // short `used` with an incomplete decoder is a normal partial read.
        let consumed = if max_values == usize::MAX && self.decoder.is_complete() {
            take
        } else {
            u32::try_from(used).map_or(take, |used| used.min(take))
        };
        self.frame = Frame::Body {
            left: left - consumed,
        };
        Ok(at + consumed as usize)
    }

    /// Complete the V1 prefix from `input` if needed. Returns the prefix
    /// bytes consumed and, once known, the frame bytes still unread.
    fn frame_body(&mut self, input: &[u8]) -> Result<(usize, Option<u32>)> {
        match &mut self.frame {
            Frame::Body { left } => Ok((0, Some(*left))),
            Frame::Prefix {
                bytes,
                filled,
                limit,
            } => {
                let n = (bytes.len() - *filled).min(input.len());
                bytes[*filled..*filled + n].copy_from_slice(&input[..n]);
                *filled += n;
                if *filled < bytes.len() {
                    return Ok((n, None));
                }
                let length = u32::from_le_bytes(*bytes);
                if length > *limit {
                    return Err(Error::FrameTooLarge {
                        length,
                        limit: *limit,
                    });
                }
                self.frame = Frame::Body { left: length };
                Ok((n, Some(length)))
            }
        }
    }
}

fn level_decoder(max_level: u32, value_count: u64) -> Result<HybridRle> {
    Ok(HybridRle::new(bit_width(max_level), value_count)?.with_max_value(max_level))
}

// At most 32, so the narrowing cast cannot truncate.
#[allow(clippy::cast_possible_truncation)]
const fn bit_width(max_level: u32) -> u8 {
    (u32::BITS - max_level.leading_zeros()) as u8
}

/// Parquet BOOLEAN with RLE encoding. Unlike levels, this value stream has
/// the 4-byte byte-length prefix in both V1 and V2 data pages.
#[derive(Debug, Clone)]
pub struct RleBool(Levels);

impl RleBool {
    /// A decoder for `value_count` booleans framed in at most
    /// `max_encoded_bytes`.
    ///
    /// # Errors
    ///
    /// As [`Levels::v1`].
    pub fn new(value_count: u64, max_encoded_bytes: u32) -> Result<Self> {
        Ok(Self(Levels::v1(1, value_count, max_encoded_bytes)?))
    }

    /// Whether every value has been emitted.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.0.is_complete()
    }

    /// Check that the stream ended after its last value.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] if values are missing.
    pub fn finish(&self) -> Result<()> {
        self.0.finish()
    }

    /// Decode as many booleans as `input` holds; returns bytes consumed.
    ///
    /// # Errors
    ///
    /// As [`Levels::feed`].
    pub fn feed(&mut self, input: &[u8], emit: impl FnMut(bool)) -> Result<usize> {
        self.feed_limited(input, usize::MAX, emit)
    }

    /// Emit at most `max_values` booleans and stop before consuming the next value.
    ///
    /// # Errors
    ///
    /// As [`Levels::feed`].
    pub fn feed_limited(
        &mut self,
        input: &[u8],
        max_values: usize,
        mut emit: impl FnMut(bool),
    ) -> Result<usize> {
        self.0
            .feed_limited(input, max_values, |level| emit(level != 0))
    }
}
