//! Parquet definition / repetition level stream framing.
//!
//! Data Page V1 prefixes each present RLE-encoded levels stream with its
//! 4-byte little-endian *encoded byte length*. V2 records each length in the
//! page header and omits the prefix. Neither form prefixes dictionary ids.

use super::{Error, HybridRle, Result};

#[derive(Debug, Clone, Copy)]
enum Framing {
    V1 {
        prefix: [u8; 4],
        filled: usize,
        limit: u32,
    },
    V2,
}

/// Decode one encoded definition OR repetition-level stream. For a required
/// field (max_level = 0), levels are absent: do not construct this decoder.
/// It emits u32 levels, which callers may convert to a nullable bitmap.
#[derive(Debug, Clone)]
pub struct Levels {
    frame: Framing,
    bytes_left: Option<u32>,
    decoder: HybridRle,
}

impl Levels {
    pub fn v1(max_level: u32, value_count: u64, max_encoded_bytes: u32) -> Result<Self> {
        let decoder = HybridRle::new(bit_width(max_level), value_count)?.with_max_value(max_level);
        Ok(Self {
            frame: Framing::V1 {
                prefix: [0; 4],
                filled: 0,
                limit: max_encoded_bytes,
            },
            bytes_left: None,
            decoder,
        })
    }

    pub fn v2(max_level: u32, value_count: u64, encoded_bytes: u32) -> Result<Self> {
        let decoder = HybridRle::new(bit_width(max_level), value_count)?.with_max_value(max_level);
        Ok(Self {
            frame: Framing::V2,
            bytes_left: Some(encoded_bytes),
            decoder,
        })
    }

    pub fn is_complete(&self) -> bool {
        self.bytes_left == Some(0) && self.decoder.is_complete()
    }
    pub fn finish(&self) -> Result<()> {
        if self.is_complete() {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }

    /// Parse at most the encoded levels stream; leave following page values
    /// unconsumed. The length prefix in V1 is counted in the returned bytes.
    pub fn feed(&mut self, input: &[u8], emit: impl FnMut(u32)) -> Result<usize> {
        self.feed_limited(input, usize::MAX, emit)
    }

    /// Emit at most `max_values` levels. A short read leaves the rest of the frame for the next call.
    pub fn feed_limited(
        &mut self,
        input: &[u8],
        max_values: usize,
        emit: impl FnMut(u32),
    ) -> Result<usize> {
        if max_values == 0 {
            return Ok(0);
        }
        let mut at = 0;
        if let Framing::V1 {
            prefix,
            filled,
            limit,
        } = &mut self.frame
        {
            if self.bytes_left.is_none() {
                let n = (4 - *filled).min(input.len());
                prefix[*filled..*filled + n].copy_from_slice(&input[..n]);
                *filled += n;
                at += n;
                if *filled < 4 {
                    return Ok(at);
                }
                let length = u32::from_le_bytes(*prefix);
                if length > *limit {
                    return Err(Error::FrameTooLarge {
                        length,
                        limit: *limit,
                    });
                }
                self.bytes_left = Some(length);
            }
        }
        let left = self.bytes_left.expect("V2 or parsed V1 prefix");
        let take = (left as usize).min(input.len() - at);
        let used = self
            .decoder
            .feed_limited(&input[at..at + take], max_values, emit)?;
        // HybridRle stops once all logical levels are emitted, but the framed
        // buffer may still contain writer padding. Snappy (and other codecs)
        // can deliver that buffer in multiple decompressed fragments, so a
        // short `used` with an incomplete decoder is a normal partial read.
        let consumed = if max_values == usize::MAX && self.decoder.is_complete() && used < take {
            take
        } else {
            used
        };
        self.bytes_left = Some(left - consumed as u32);
        Ok(at + consumed)
    }
}

fn bit_width(max_level: u32) -> u8 {
    (32 - max_level.leading_zeros()) as u8
}

/// Parquet BOOLEAN with RLE encoding. Unlike levels, this value stream has
/// the 4-byte byte-length prefix in both V1 and V2 data pages.
#[derive(Debug, Clone)]
pub struct RleBool(Levels);

impl RleBool {
    pub fn new(value_count: u64, max_encoded_bytes: u32) -> Result<Self> {
        Ok(Self(Levels::v1(1, value_count, max_encoded_bytes)?))
    }
    pub fn is_complete(&self) -> bool {
        self.0.is_complete()
    }
    pub fn finish(&self) -> Result<()> {
        self.0.finish()
    }
    pub fn feed(&mut self, input: &[u8], emit: impl FnMut(bool)) -> Result<usize> {
        self.feed_limited(input, usize::MAX, emit)
    }

    /// Emit at most `max_values` booleans and stop before consuming the next value.
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
