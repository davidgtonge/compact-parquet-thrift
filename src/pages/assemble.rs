//! Bind one page's compressed payload to its incremental value decoder.
//!
//! The page header has already been parsed. `feed` accepts only bytes from
//! that page's compressed payload; excess input belongs to the next header.
//! For V2, definition levels bypass the decompressor. For V1, the entire
//! payload is decompressed before interpreting levels/values.

use super::{ColumnSpec, DecodeWorkspace, Error, FlatPage, Inflate, PageSink, Result};
use crate::parquet::PageHeader;

pub struct PageAssembler<'codec, 'workspace, C: Inflate> {
    page: FlatPage<'workspace>,
    codec: &'codec mut C,
    payload_remaining: usize,
    uncompressed_expected: usize,
    decoded: usize,
    uncompressed_levels_remaining: usize,
    compressed: bool,
    crc: u32,
    expected_crc: Option<u32>,
    completed: bool,
}

impl<'codec, 'workspace, C: Inflate> PageAssembler<'codec, 'workspace, C> {
    pub fn new(
        header: PageHeader,
        spec: ColumnSpec,
        workspace: DecodeWorkspace<'workspace>,
        codec: &'codec mut C,
    ) -> Result<Self> {
        let page = FlatPage::new(header, spec, workspace)?;
        let uncompressed_expected = page.uncompressed_size();
        let levels = page.v2_levels_size();
        let compressed = page.values_compressed();
        if compressed {
            if codec.codec_id() != spec.codec {
                return Err(Error::UnsupportedCodec(spec.codec));
            }
            codec.reset(uncompressed_expected - levels)?;
        }
        let payload_remaining = page.compressed_size();
        Ok(Self {
            page,
            codec,
            payload_remaining,
            uncompressed_expected,
            decoded: 0,
            uncompressed_levels_remaining: levels,
            compressed,
            crc: !0u32,
            expected_crc: header.crc.map(|x| x as u32),
            completed: false,
        })
    }

    pub fn page_kind(&self) -> &'static str {
        self.page.page_kind()
    }
    pub fn remaining(&self) -> usize {
        self.payload_remaining
    }

    /// All values and borrowed byte fragments are delivered synchronously.
    /// A malformed page may have emitted a prefix of values before returning
    /// an error: downstream state must be transactional per page.
    pub fn feed(&mut self, input: &[u8], emit: &mut impl for<'ev> PageSink<'ev>) -> Result<usize> {
        if self.completed {
            return Err(Error::InvalidHeader);
        }
        let n = input.len().min(self.payload_remaining);
        let input = &input[..n];
        self.crc = update_crc32(self.crc, input);
        let mut at = 0;
        if self.uncompressed_levels_remaining > 0 {
            let levels = self.uncompressed_levels_remaining.min(n);
            let used = self.page.feed(&input[..levels], emit)?;
            if used != levels {
                return Err(Error::LengthMismatch);
            }
            self.decoded += levels;
            self.uncompressed_levels_remaining -= levels;
            at = levels;
        }
        if at < n {
            if self.compressed {
                let page = &mut self.page;
                let decoded = &mut self.decoded;
                let max = self.uncompressed_expected;
                self.codec.push(&input[at..], &mut |out| {
                    *decoded = decoded
                        .checked_add(out.len())
                        .ok_or(Error::InvalidPageSize)?;
                    if *decoded > max {
                        return Err(Error::LengthMismatch);
                    }
                    if page.feed(out, emit)? != out.len() {
                        return Err(Error::LengthMismatch);
                    }
                    Ok(())
                })?;
            } else {
                self.decoded = self
                    .decoded
                    .checked_add(n - at)
                    .ok_or(Error::InvalidPageSize)?;
                if self.decoded > self.uncompressed_expected {
                    return Err(Error::LengthMismatch);
                }
                if self.page.feed(&input[at..], emit)? != n - at {
                    return Err(Error::LengthMismatch);
                }
            }
        }
        self.payload_remaining -= n;
        Ok(n)
    }

    /// Ends one page and checks all lengths, value counts and optional CRC32.
    pub fn finish(&mut self, emit: &mut impl for<'ev> PageSink<'ev>) -> Result<()> {
        if self.completed {
            return Err(Error::InvalidHeader);
        }
        if self.payload_remaining != 0 || self.uncompressed_levels_remaining != 0 {
            return Err(Error::Truncated);
        }
        if self.compressed {
            let page = &mut self.page;
            let decoded = &mut self.decoded;
            let max = self.uncompressed_expected;
            self.codec.finish(&mut |out| {
                *decoded = decoded
                    .checked_add(out.len())
                    .ok_or(Error::InvalidPageSize)?;
                if *decoded > max {
                    return Err(Error::LengthMismatch);
                }
                if page.feed(out, emit)? != out.len() {
                    return Err(Error::LengthMismatch);
                }
                Ok(())
            })?;
        }
        if self.decoded != self.uncompressed_expected {
            return Err(Error::LengthMismatch);
        }
        self.page.finish(emit)?;
        if let Some(expected) = self.expected_crc {
            let actual = !self.crc;
            if actual != expected {
                return Err(Error::ChecksumMismatch { actual, expected });
            }
        }
        self.completed = true;
        Ok(())
    }
}

// Reflected CRC-32 with a tiny, compile-time-generated nibble table: no
// allocation, just 64 static bytes, and 2 lookups per payload byte.
const fn crc_nibble_table() -> [u32; 16] {
    let mut result = [0u32; 16];
    let mut i = 0;
    while i < 16 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 4 {
            crc = (crc >> 1) ^ (if crc & 1 != 0 { 0xedb8_8320 } else { 0 });
            bit += 1;
        }
        result[i] = crc;
        i += 1;
    }
    result
}

const CRC_NIBBLES: [u32; 16] = crc_nibble_table();

// The checksum covers compressed page-body bytes, including raw V2 levels.
fn update_crc32(mut crc: u32, bytes: &[u8]) -> u32 {
    for &b in bytes {
        crc ^= u32::from(b);
        crc = (crc >> 4) ^ CRC_NIBBLES[(crc & 15) as usize];
        crc = (crc >> 4) ^ CRC_NIBBLES[(crc & 15) as usize];
    }
    crc
}
