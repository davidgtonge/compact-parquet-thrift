//! Bounded-memory Parquet page streaming.
//!
//! The byte framer, the compressed-payload driver, and the physical-value
//! decoder are deliberately separate. They never own input or output rows.
//! Workspaces are supplied by the caller; the callbacks borrow their bytes
//! for the duration of each call only.
//!
//! Scope: flat scalar columns (maximum repetition level zero, definition
//! level zero or one), V1 and V2 data pages, and PLAIN dictionary pages.
//! Nested schemas and unsupported physical encodings fail explicitly.

mod assemble;
mod decode;
mod frame;
mod inflate;

use core::fmt;

pub use assemble::PageAssembler;
pub use decode::{
    ColumnSpec, DecodeWorkspace, FlatPage, Location, PageEvent, PageSink, Physical, Value,
};
pub use frame::{FrameEvent, PageFramer};
pub use inflate::{Identity, Inflate, RawSnappy};

use crate::parquet::PageHeader;

/// Result of page framing, decompression or decoding.
pub type Result<T> = core::result::Result<T, Error>;

/// A page that cannot be framed, decompressed or decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The page header is malformed Thrift.
    Thrift(crate::Error),
    /// The page's levels or values are malformed.
    Value(crate::values::Error),
    /// Input ended inside a page.
    Truncated,
    /// A page header does not fit the caller's header buffer.
    HeaderTooLarge,
    /// A required header field is missing or negative, or the decoder was
    /// driven out of order.
    InvalidHeader,
    /// A page size is negative, inconsistent or above the configured limit.
    InvalidPageSize,
    /// A page declares more values than the configured limit.
    InvalidValueCount,
    /// The page type is not data V1, data V2 or dictionary.
    UnsupportedPage(i32),
    /// The encoding is not supported for this physical type.
    UnsupportedEncoding {
        /// Column physical type.
        physical: Physical,
        /// Parquet encoding number (-1 when absent).
        encoding: i32,
    },
    /// The page is compressed with a codec the supplied [`Inflate`] does not handle.
    UnsupportedCodec(i32),
    /// The column is repeated or has more than one definition level.
    NestedColumn,
    /// A caller-supplied workspace is too small for this page.
    WorkspaceTooSmall {
        /// Bytes or elements required.
        needed: usize,
        /// Bytes or elements supplied.
        available: usize,
    },
    /// Decoded bytes or levels disagree with the header's sizes.
    LengthMismatch,
    /// More values were decoded than the page declares.
    TooManyValues,
    /// The page ended before all its values were decoded.
    MissingValues,
    /// The null count disagrees with the definition levels or row count.
    InvalidNullCount,
    /// The raw Snappy stream is malformed.
    CorruptSnappy,
    /// A Snappy back-reference reaches further than the history buffer.
    SnappyHistoryTooSmall {
        /// Back-reference distance.
        offset: usize,
        /// History buffer size.
        capacity: usize,
    },
    /// The page body does not match the header's CRC-32.
    ChecksumMismatch {
        /// CRC-32 of the bytes received.
        actual: u32,
        /// CRC-32 from the header.
        expected: u32,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Thrift(e) => write!(f, "page header: {e}"),
            Self::Value(e) => write!(f, "page values: {e}"),
            Self::Truncated => f.write_str("page is truncated"),
            Self::HeaderTooLarge => f.write_str("page header exceeds the header buffer"),
            Self::InvalidHeader => f.write_str("page header is missing a required field"),
            Self::InvalidPageSize => f.write_str("page size is invalid or above the limit"),
            Self::InvalidValueCount => f.write_str("page value count is above the limit"),
            Self::UnsupportedPage(kind) => write!(f, "unsupported page type {kind}"),
            Self::UnsupportedEncoding { physical, encoding } => {
                write!(f, "unsupported encoding {encoding} for {physical:?}")
            }
            Self::UnsupportedCodec(codec) => write!(f, "unsupported compression codec {codec}"),
            Self::NestedColumn => f.write_str("nested or repeated columns are not supported"),
            Self::WorkspaceTooSmall { needed, available } => write!(
                f,
                "page workspace too small: need {needed}, have {available}"
            ),
            Self::LengthMismatch => f.write_str("decoded length does not match the page header"),
            Self::TooManyValues => f.write_str("page has more values than declared"),
            Self::MissingValues => f.write_str("page has fewer values than declared"),
            Self::InvalidNullCount => f.write_str("page null count is inconsistent"),
            Self::CorruptSnappy => f.write_str("corrupt Snappy data"),
            Self::SnappyHistoryTooSmall { offset, capacity } => write!(
                f,
                "Snappy back-reference of {offset} bytes exceeds history of {capacity}"
            ),
            Self::ChecksumMismatch { actual, expected } => write!(
                f,
                "page CRC-32 mismatch: got {actual:#010x}, expected {expected:#010x}"
            ),
        }
    }
}

impl core::error::Error for Error {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Thrift(e) => Some(e),
            Self::Value(e) => Some(e),
            _ => None,
        }
    }
}

impl From<crate::Error> for Error {
    fn from(e: crate::Error) -> Self {
        Self::Thrift(e)
    }
}

impl From<crate::values::Error> for Error {
    fn from(e: crate::values::Error) -> Self {
        Self::Value(e)
    }
}

/// A required, nonnegative `i32` header field as a `usize`.
pub(crate) fn nonnegative(value: Option<i32>) -> Result<usize> {
    usize::try_from(value.ok_or(Error::InvalidHeader)?).map_err(|_| Error::InvalidHeader)
}

/// The page types this crate decodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind {
    /// `DATA_PAGE`.
    V1,
    /// `DATA_PAGE_V2`.
    V2,
    /// `DICTIONARY_PAGE`.
    Dictionary,
}

impl PageKind {
    /// The `PageType` name from `parquet.thrift`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::V1 => "DATA_PAGE",
            Self::V2 => "DATA_PAGE_V2",
            Self::Dictionary => "DICTIONARY_PAGE",
        }
    }
}

/// Summary of a validated page header for callers that decode outside [`FlatPage`].
#[derive(Debug, Clone, Copy)]
pub struct PageLayout {
    /// Page type.
    pub kind: PageKind,
    /// Values in the page, nulls included (dictionary entries for a dictionary page).
    pub count: usize,
    /// Encoding of the values section.
    pub encoding: i32,
    /// Page body size on disk.
    pub compressed_bytes: usize,
    /// Page body size after decompression.
    pub uncompressed_bytes: usize,
    /// V2 level bytes at the start of the body, which bypass decompression.
    pub v2_levels_bytes: usize,
    /// Whether the values section passes through the codec.
    pub values_compressed: bool,
    /// V2 null count from the header, checked against the levels.
    pub expected_nulls: Option<usize>,
}

/// The kind-specific part of a layout.
struct Body {
    count: usize,
    encoding: i32,
    levels_bytes: usize,
    values_compressed: bool,
    nulls: Option<usize>,
}

impl PageLayout {
    pub(crate) fn from_header(header: PageHeader, spec: ColumnSpec) -> Result<Self> {
        let compressed_bytes = nonnegative(header.compressed_size)?;
        let uncompressed_bytes = nonnegative(header.uncompressed_size)?;
        if compressed_bytes > spec.max_page_bytes || uncompressed_bytes > spec.max_page_bytes {
            return Err(Error::InvalidPageSize);
        }
        let kind = match header.page_type.ok_or(Error::InvalidHeader)? {
            0 => PageKind::V1,
            2 => PageKind::Dictionary,
            3 => PageKind::V2,
            p => return Err(Error::UnsupportedPage(p)),
        };
        let body = match kind {
            PageKind::V1 => v1_body(header, spec)?,
            PageKind::V2 => v2_body(header, spec, compressed_bytes, uncompressed_bytes)?,
            PageKind::Dictionary => dictionary_body(header, spec)?,
        };
        if body.count > spec.max_page_values || u32::try_from(body.count).is_err() {
            return Err(Error::InvalidValueCount);
        }
        if !body.values_compressed && compressed_bytes != uncompressed_bytes {
            return Err(Error::InvalidPageSize);
        }
        Ok(Self {
            kind,
            count: body.count,
            encoding: body.encoding,
            compressed_bytes,
            uncompressed_bytes,
            v2_levels_bytes: body.levels_bytes,
            values_compressed: body.values_compressed,
            expected_nulls: body.nulls,
        })
    }
}

fn v1_body(header: PageHeader, spec: ColumnSpec) -> Result<Body> {
    let count = nonnegative(header.data_v1_num_values)?;
    if spec.max_definition_level == 1 && header.data_v1_definition_encoding != Some(3) {
        return Err(Error::UnsupportedEncoding {
            physical: spec.physical,
            encoding: header.data_v1_definition_encoding.unwrap_or(-1),
        });
    }
    Ok(Body {
        count,
        encoding: header.data_v1_encoding.ok_or(Error::InvalidHeader)?,
        levels_bytes: 0,
        values_compressed: spec.codec != 0,
        nulls: None,
    })
}

fn v2_body(
    header: PageHeader,
    spec: ColumnSpec,
    compressed_bytes: usize,
    uncompressed_bytes: usize,
) -> Result<Body> {
    let v2 = header.data_v2.ok_or(Error::InvalidHeader)?;
    let count = nonnegative(v2.num_values)?;
    let nulls = nonnegative(v2.num_nulls)?;
    let rows = nonnegative(v2.num_rows)?;
    // Flat schema: each logical value is exactly one row.
    if rows != count || nulls > count || (spec.max_definition_level == 0 && nulls > 0) {
        return Err(Error::InvalidNullCount);
    }
    let reps = nonnegative(v2.repetition_levels_bytes)?;
    let defs = nonnegative(v2.definition_levels_bytes)?;
    if reps != 0 || (spec.max_definition_level == 0 && defs != 0) {
        return Err(Error::NestedColumn);
    }
    let levels_bytes = defs.checked_add(reps).ok_or(Error::InvalidPageSize)?;
    if levels_bytes > compressed_bytes || levels_bytes > uncompressed_bytes {
        return Err(Error::InvalidPageSize);
    }
    if !v2.is_compressed && compressed_bytes != uncompressed_bytes {
        return Err(Error::InvalidPageSize);
    }
    Ok(Body {
        count,
        encoding: v2.encoding.ok_or(Error::InvalidHeader)?,
        levels_bytes,
        values_compressed: v2.is_compressed && spec.codec != 0,
        nulls: Some(nulls),
    })
}

fn dictionary_body(header: PageHeader, spec: ColumnSpec) -> Result<Body> {
    // The dictionary payload is PLAIN. Older writers still label the page
    // PLAIN_DICTIONARY (2).
    match header.dictionary_encoding {
        Some(0 | 2) => {}
        other => {
            return Err(Error::UnsupportedEncoding {
                physical: spec.physical,
                encoding: other.unwrap_or(-1),
            })
        }
    }
    Ok(Body {
        count: nonnegative(header.dictionary_num_values)?,
        encoding: 0,
        levels_bytes: 0,
        values_compressed: spec.codec != 0,
        nulls: None,
    })
}

/// Validate a page header against a column and summarize its layout.
///
/// # Errors
///
/// Any header the page decoder would reject: unsupported page types or
/// encodings, missing or negative sizes, sizes or counts above the
/// [`ColumnSpec`] limits, or inconsistent V2 level and null counts.
pub fn page_layout(header: PageHeader, spec: ColumnSpec) -> Result<PageLayout> {
    PageLayout::from_header(header, spec)
}
