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

mod frame;
mod decode;
mod inflate;
mod assemble;

pub use frame::{PageFramer, FrameEvent};
pub use decode::{ColumnSpec, Physical, DecodeWorkspace, FlatPage, Location, PageEvent, PageSink, Value};
pub use inflate::{Inflate, Identity, RawSnappy};
pub use assemble::PageAssembler;

use crate::parquet::PageHeader;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Thrift(crate::Error),
    Value(crate::values::Error),
    Truncated,
    HeaderTooLarge,
    InvalidHeader,
    InvalidPageSize,
    InvalidValueCount,
    UnsupportedPage(i32),
    UnsupportedEncoding { physical: Physical, encoding: i32 },
    UnsupportedCodec(i32),
    NestedColumn,
    WorkspaceTooSmall { needed: usize, available: usize },
    LengthMismatch,
    TooManyValues,
    MissingValues,
    InvalidNullCount,
    CorruptSnappy,
    SnappyHistoryTooSmall { offset: usize, capacity: usize },
    ChecksumMismatch { actual: u32, expected: u32 },
}

impl From<crate::Error> for Error {
    fn from(e: crate::Error) -> Self { Self::Thrift(e) }
}

impl From<crate::values::Error> for Error {
    fn from(e: crate::values::Error) -> Self { Self::Value(e) }
}

pub(crate) fn nonnegative(value: Option<i32>) -> Result<usize> {
    usize::try_from(value.ok_or(Error::InvalidHeader)?)
        .map_err(|_| Error::InvalidHeader)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind { V1, V2, Dictionary }

#[derive(Debug, Clone, Copy)]
pub(crate) struct Layout {
    pub kind: PageKind,
    pub count: usize,
    pub encoding: i32,
    pub compressed_bytes: usize,
    pub uncompressed_bytes: usize,
    pub v2_levels_bytes: usize,
    pub values_compressed: bool,
    pub expected_nulls: Option<usize>,
}

impl Layout {
    pub fn from_header(header: PageHeader, spec: ColumnSpec) -> Result<Self> {
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
        let (count, encoding, levels_bytes, values_compressed, nulls) = match kind {
            PageKind::V1 => {
                let count = nonnegative(header.data_v1_num_values)?;
                if spec.max_definition_level == 1 && header.data_v1_definition_encoding != Some(3) {
                    return Err(Error::UnsupportedEncoding {
                        physical: spec.physical, encoding: header.data_v1_definition_encoding.unwrap_or(-1),
                    });
                }
                (count, header.data_v1_encoding.ok_or(Error::InvalidHeader)?, 0,
                    spec.codec != 0, None)
            }
            PageKind::V2 => {
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
                let compressed = v2.is_compressed && spec.codec != 0;
                if !v2.is_compressed && compressed_bytes != uncompressed_bytes {
                    return Err(Error::InvalidPageSize);
                }
                (count, v2.encoding.ok_or(Error::InvalidHeader)?, levels_bytes, compressed, Some(nulls))
            }
            PageKind::Dictionary => {
                // The dictionary payload is PLAIN. Older writers still label
                // the page PLAIN_DICTIONARY (2).
                match header.dictionary_encoding {
                    Some(0 | 2) => {}
                    other => return Err(Error::UnsupportedEncoding {
                        physical: spec.physical, encoding: other.unwrap_or(-1),
                    }),
                }
                (nonnegative(header.dictionary_num_values)?, 0, 0, spec.codec != 0, None)
            }
        };
        if count > spec.max_page_values || count > u32::MAX as usize {
            return Err(Error::InvalidValueCount);
        }
        if !values_compressed && compressed_bytes != uncompressed_bytes {
            return Err(Error::InvalidPageSize);
        }
        Ok(Self { kind, count, encoding, compressed_bytes, uncompressed_bytes,
            v2_levels_bytes: levels_bytes, values_compressed, expected_nulls: nulls })
    }
}

/// Summary of a parsed page header for callers that decode outside [`FlatPage`].
#[derive(Debug, Clone, Copy)]
pub struct PageLayout {
    pub kind: PageKind,
    pub count: usize,
    pub encoding: i32,
    pub compressed_bytes: usize,
    pub uncompressed_bytes: usize,
    pub v2_levels_bytes: usize,
    pub values_compressed: bool,
    pub expected_nulls: Option<usize>,
}

impl From<Layout> for PageLayout {
    fn from(l: Layout) -> Self {
        Self {
            kind: l.kind,
            count: l.count,
            encoding: l.encoding,
            compressed_bytes: l.compressed_bytes,
            uncompressed_bytes: l.uncompressed_bytes,
            v2_levels_bytes: l.v2_levels_bytes,
            values_compressed: l.values_compressed,
            expected_nulls: l.expected_nulls,
        }
    }
}

pub fn page_layout(header: PageHeader, spec: ColumnSpec) -> Result<PageLayout> {
    Layout::from_header(header, spec).map(PageLayout::from)
}
