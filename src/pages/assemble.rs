//! Bind one page's compressed payload to its incremental value decoder.
//!
//! The page header has already been parsed. `feed` accepts only bytes from
//! that page's compressed payload; excess input belongs to the next header.
//! For V2, definition levels bypass the decompressor. For V1, the entire
//! payload is decompressed before interpreting levels/values.

use super::{ColumnSpec, DecodeWorkspace, Error, FlatPage, Inflate, PageSink, Result};
use crate::parquet::PageHeader;

/// Drives one page: verifies its CRC, routes V2 levels past the codec,
/// decompresses the rest and feeds the result to a [`FlatPage`].
pub struct PageAssembler<'codec, 'workspace, C: Inflate> {
    page: FlatPage<'workspace>,
    codec: &'codec mut C,
    payload_remaining: usize,
    /// Decompressed bytes delivered to the page so far, levels included.
    decoded: Decoded,
    uncompressed_levels_remaining: usize,
    compressed: bool,
    crc: u32,
    expected_crc: Option<u32>,
    completed: bool,
}

/// Decompressed byte accounting against the header's uncompressed size.
#[derive(Debug, Clone, Copy)]
struct Decoded {
    seen: usize,
    expected: usize,
}

impl Decoded {
    /// Pass `out` to the page after checking it fits the declared size.
    fn deliver(
        &mut self,
        page: &mut FlatPage<'_>,
        out: &[u8],
        emit: &mut impl for<'ev> PageSink<'ev>,
    ) -> Result<()> {
        self.seen = self
            .seen
            .checked_add(out.len())
            .ok_or(Error::InvalidPageSize)?;
        if self.seen > self.expected {
            return Err(Error::LengthMismatch);
        }
        if page.feed(out, emit)? != out.len() {
            return Err(Error::LengthMismatch);
        }
        Ok(())
    }
}

impl<'codec, 'workspace, C: Inflate> PageAssembler<'codec, 'workspace, C> {
    /// Start a page whose header has been parsed. `codec` is reset for the
    /// page's compressed section when the page is compressed.
    ///
    /// # Errors
    ///
    /// Any [`FlatPage::new`] error, [`Error::UnsupportedCodec`] if `codec`
    /// does not match the column, or a codec reset error.
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
            decoded: Decoded {
                seen: 0,
                expected: uncompressed_expected,
            },
            uncompressed_levels_remaining: levels,
            compressed,
            crc: !0u32,
            // The header stores the CRC's bits in a signed Thrift i32.
            expected_crc: header.crc.map(|crc| u32::from_le_bytes(crc.to_le_bytes())),
            completed: false,
        })
    }

    /// The page type's `parquet.thrift` name.
    #[must_use]
    pub const fn page_kind(&self) -> &'static str {
        self.page.page_kind()
    }

    /// Compressed payload bytes still expected.
    #[must_use]
    pub const fn remaining(&self) -> usize {
        self.payload_remaining
    }

    /// All values and borrowed byte fragments are delivered synchronously.
    /// A malformed page may have emitted a prefix of values before returning
    /// an error: downstream state must be transactional per page.
    ///
    /// Returns the bytes of `input` that belong to this page; the rest
    /// belongs to the next page header.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidHeader`] after [`Self::finish`], any codec or page
    /// decoding error, or [`Error::LengthMismatch`] when decoded sizes
    /// disagree with the header.
    pub fn feed(&mut self, input: &[u8], emit: &mut impl for<'ev> PageSink<'ev>) -> Result<usize> {
        if self.completed {
            return Err(Error::InvalidHeader);
        }
        let input = &input[..input.len().min(self.payload_remaining)];
        self.crc = update_crc32(self.crc, input);
        let (levels, values) = input.split_at(self.uncompressed_levels_remaining.min(input.len()));
        if !levels.is_empty() {
            self.decoded.deliver(&mut self.page, levels, emit)?;
            self.uncompressed_levels_remaining -= levels.len();
        }
        if !values.is_empty() {
            if self.compressed {
                let page = &mut self.page;
                let decoded = &mut self.decoded;
                self.codec
                    .push(values, &mut |out| decoded.deliver(page, out, emit))?;
            } else {
                self.decoded.deliver(&mut self.page, values, emit)?;
            }
        }
        self.payload_remaining -= input.len();
        Ok(input.len())
    }

    /// Ends one page and checks all lengths, value counts and optional CRC32.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] if payload bytes are missing, any codec or page
    /// error, [`Error::LengthMismatch`] or [`Error::ChecksumMismatch`].
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
            self.codec
                .finish(&mut |out| decoded.deliver(page, out, emit))?;
        }
        if self.decoded.seen != self.decoded.expected {
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
    let mut i = 0u32;
    while i < 16 {
        let mut crc = i;
        let mut bit = 0;
        while bit < 4 {
            crc = (crc >> 1) ^ (if crc & 1 != 0 { 0xedb8_8320 } else { 0 });
            bit += 1;
        }
        result[i as usize] = crc;
        i += 1;
    }
    result
}

const CRC_NIBBLES: [u32; 16] = crc_nibble_table();

// The checksum covers compressed page-body bytes, including raw V2 levels.
fn update_crc32(crc: u32, bytes: &[u8]) -> u32 {
    bytes.iter().fold(crc, |crc, &b| {
        let crc = crc ^ u32::from(b);
        let crc = (crc >> 4) ^ CRC_NIBBLES[(crc & 15) as usize];
        (crc >> 4) ^ CRC_NIBBLES[(crc & 15) as usize]
    })
}
