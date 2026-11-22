//! Flat column decoding with one-bit-per-row null bookkeeping.
//!
//! Parquet places all definition levels before the values. A forward-only
//! decoder cannot emit correctly positioned nullable values until those levels
//! have arrived; the reusable presence bitmap is the minimal general buffer.

use super::{Error, PageKind, PageLayout, Result};
use crate::parquet::PageHeader;
use crate::values::{
    ByteArrayEvent, ByteArrays, DeltaBinary, DeltaByteArrays, DeltaByteParts,
    DeltaLengthByteArrays, DictionaryIds, FixedByteArrays, Int96, Levels, PlainBool, PlainValues,
    RleBool,
};

/// Parquet physical types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Physical {
    /// `BOOLEAN`.
    Boolean,
    /// `INT32`.
    Int32,
    /// `INT64`.
    Int64,
    /// `INT96` (legacy timestamps).
    Int96,
    /// `FLOAT`.
    Float,
    /// `DOUBLE`.
    Double,
    /// `BYTE_ARRAY`.
    ByteArray,
    /// `FIXED_LEN_BYTE_ARRAY` of the given length.
    FixedByteArray(u32),
}

/// What the page decoder needs to know about a column, and the limits it
/// enforces on untrusted page headers.
#[derive(Debug, Clone, Copy)]
pub struct ColumnSpec {
    /// Physical type.
    pub physical: Physical,
    /// `parquet.thrift` `CompressionCodec` of the column chunk.
    pub codec: i32,
    /// 0 for a required column, 1 for an optional one.
    pub max_definition_level: u8,
    /// Must be zero: repeated columns are not supported.
    pub max_repetition_level: u8,
    /// Entries in the chunk's dictionary page, for dictionary-encoded pages.
    pub dictionary_size: Option<u32>,
    /// Largest accepted compressed or uncompressed page body.
    pub max_page_bytes: usize,
    /// Largest accepted value count per page.
    pub max_page_values: usize,
    /// Largest accepted single byte-array value.
    pub max_value_bytes: u32,
}

impl ColumnSpec {
    /// A required flat column with default limits (16 MiB pages,
    /// 1 Mi values per page, 1 MiB values).
    #[must_use]
    pub const fn flat(physical: Physical, codec: i32) -> Self {
        Self {
            physical,
            codec,
            max_definition_level: 0,
            max_repetition_level: 0,
            dictionary_size: None,
            max_page_bytes: 16 * 1024 * 1024,
            max_page_values: 1_048_576,
            max_value_bytes: 1_048_576,
        }
    }
    /// Mark the column optional (one definition level).
    #[must_use]
    pub const fn optional(mut self) -> Self {
        self.max_definition_level = 1;
        self
    }
    /// Set the dictionary size used to bound dictionary ids.
    #[must_use]
    pub const fn with_dictionary(mut self, length: u32) -> Self {
        self.dictionary_size = Some(length);
        self
    }
}

/// All workspaces are borrowed. Unused slices can be empty; delta-string
/// paths need `lengths` or `parts` sized for *non-null values*, not rows.
pub struct DecodeWorkspace<'a> {
    /// One bit per row for optional columns: `ceil(rows / 8)` bytes.
    pub present: &'a mut [u8],
    /// Mini-block widths for delta encodings (commonly 4 bytes).
    pub widths: &'a mut [u8],
    /// One length per value for `DELTA_LENGTH_BYTE_ARRAY`.
    pub lengths: &'a mut [u32],
    /// One prefix/suffix pair per value for `DELTA_BYTE_ARRAY`.
    pub parts: &'a mut [DeltaByteParts],
    /// The longest value, for `DELTA_BYTE_ARRAY` prefix reconstruction.
    pub previous: &'a mut [u8],
}

#[derive(Default)]
struct ValueScratch<'a> {
    widths: &'a mut [u8],
    lengths: &'a mut [u32],
    parts: &'a mut [DeltaByteParts],
    previous: &'a mut [u8],
}

/// Where a decoded value belongs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Location {
    /// Row index within a data page.
    Row(u32),
    /// Entry index within a dictionary page.
    Dictionary(u32),
}

/// One decoded physical value, or one piece of a byte array.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value<'a> {
    /// `BOOLEAN`.
    Boolean(bool),
    /// `INT32`.
    I32(i32),
    /// `INT64`.
    I64(i64),
    /// `INT96` raw bytes.
    Int96(Int96),
    /// `FLOAT`.
    F32(f32),
    /// `DOUBLE`.
    F64(f64),
    /// An index into the column's dictionary page.
    DictionaryId(u32),
    /// A byte array of this many bytes begins.
    BytesStart(u32),
    /// The next fragment of the current byte array.
    BytesChunk(&'a [u8]),
    /// The current byte array is complete.
    BytesEnd,
}

/// One event from a page decoder.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageEvent<'a> {
    /// The row is null.
    Null(u32),
    /// A value, or byte-array piece, at a location.
    Value(Location, Value<'a>),
}

/// One page event at a time. The lifetime is per call, so a closure can be
/// stored and reused across `feed` and `finish` without allocating.
///
/// Annotate a stored closure as `|event: PageEvent<'_>|`. Borrowed
/// `BytesChunk` data refers to either the caller's input or a caller-owned
/// scratch buffer, and only for the duration of `on_page`.
pub trait PageSink<'a> {
    /// Receive one event.
    fn on_page(&mut self, event: PageEvent<'a>);
}

impl<'a, F> PageSink<'a> for F
where
    F: FnMut(PageEvent<'a>),
{
    fn on_page(&mut self, event: PageEvent<'a>) {
        self(event);
    }
}

/// Assigns decoded values to rows, emitting nulls for absent rows.
struct RowCursor {
    count: u32,
    index: u32,
    active_binary: Option<Location>,
    location: fn(u32) -> Location,
    optional: bool,
}

impl RowCursor {
    fn new(count: u32, kind: PageKind, optional: bool) -> Self {
        Self {
            count,
            index: 0,
            active_binary: None,
            location: if kind == PageKind::Dictionary {
                Location::Dictionary
            } else {
                Location::Row
            },
            optional,
        }
    }

    fn is_present(&self, bits: &[u8], row: u32) -> bool {
        !self.optional || (bits[(row >> 3) as usize] & (1 << (row & 7))) != 0
    }

    fn next(&mut self, bits: &[u8], emit: &mut impl for<'ev> PageSink<'ev>) -> Result<Location> {
        while self.index < self.count {
            let index = self.index;
            self.index += 1;
            if self.is_present(bits, index) {
                return Ok((self.location)(index));
            }
            emit.on_page(PageEvent::Null(index));
        }
        Err(Error::TooManyValues)
    }

    fn scalar<'v>(
        &mut self,
        bits: &[u8],
        value: Value<'v>,
        emit: &mut impl for<'ev> PageSink<'ev>,
    ) -> Result<()> {
        if self.active_binary.is_some() {
            return Err(Error::InvalidHeader);
        }
        let at = self.next(bits, emit)?;
        emit.on_page(PageEvent::Value(at, value));
        Ok(())
    }

    fn bytes<'v>(
        &mut self,
        bits: &[u8],
        event: ByteArrayEvent<'v>,
        emit: &mut impl for<'ev> PageSink<'ev>,
    ) -> Result<()> {
        match event {
            ByteArrayEvent::Start(length) => {
                if self.active_binary.is_some() {
                    return Err(Error::InvalidHeader);
                }
                let at = self.next(bits, emit)?;
                self.active_binary = Some(at);
                emit.on_page(PageEvent::Value(at, Value::BytesStart(length)));
            }
            ByteArrayEvent::Chunk(bytes) => {
                let at = self.active_binary.ok_or(Error::InvalidHeader)?;
                emit.on_page(PageEvent::Value(at, Value::BytesChunk(bytes)));
            }
            ByteArrayEvent::End => {
                let at = self.active_binary.take().ok_or(Error::InvalidHeader)?;
                emit.on_page(PageEvent::Value(at, Value::BytesEnd));
            }
        }
        Ok(())
    }

    fn finish(&mut self, bits: &[u8], emit: &mut impl for<'ev> PageSink<'ev>) -> Result<()> {
        if self.active_binary.is_some() {
            return Err(Error::MissingValues);
        }
        while self.index < self.count {
            let index = self.index;
            self.index += 1;
            if self.is_present(bits, index) {
                return Err(Error::MissingValues);
            }
            emit.on_page(PageEvent::Null(index));
        }
        Ok(())
    }
}

enum Values<'a> {
    Bool(PlainBool),
    BoolRle(RleBool),
    I32(PlainValues<i32>),
    I64(PlainValues<i64>),
    Int96(PlainValues<Int96>),
    F32(PlainValues<f32>),
    F64(PlainValues<f64>),
    Bytes(ByteArrays),
    Fixed(FixedByteArrays),
    Dictionary(DictionaryIds),
    Delta32(DeltaBinary<'a, i32>),
    Delta64(DeltaBinary<'a, i64>),
    DeltaLength(DeltaLengthByteArrays<'a>),
    DeltaBytes(DeltaByteArrays<'a>),
}

impl<'a> Values<'a> {
    // The scratch borrows move into whichever decoder needs them.
    #[allow(clippy::needless_pass_by_value)]
    fn new(spec: ColumnSpec, encoding: i32, count: usize, ws: ValueScratch<'a>) -> Result<Self> {
        let count64 = count as u64;
        Ok(match (encoding, spec.physical) {
            (0, Physical::Boolean) => Self::Bool(PlainBool::new(count64)),
            (0, Physical::Int32) => Self::I32(PlainValues::new(count64)),
            (0, Physical::Int64) => Self::I64(PlainValues::new(count64)),
            (0, Physical::Int96) => Self::Int96(PlainValues::new(count64)),
            (0, Physical::Float) => Self::F32(PlainValues::new(count64)),
            (0, Physical::Double) => Self::F64(PlainValues::new(count64)),
            (0, Physical::ByteArray) => Self::Bytes(ByteArrays::new(count64, spec.max_value_bytes)),
            (0, Physical::FixedByteArray(n)) => Self::Fixed(FixedByteArrays::new(count64, n)?),
            (3, Physical::Boolean) => {
                Self::BoolRle(RleBool::new(count64, saturating_u32(spec.max_page_bytes))?)
            }
            (2 | 8, _) => Self::Dictionary(DictionaryIds::new(
                count64,
                spec.dictionary_size.ok_or(Error::InvalidHeader)?,
            )?),
            (5, Physical::Int32) => Self::Delta32(DeltaBinary::new(count64, ws.widths)),
            (5, Physical::Int64) => Self::Delta64(DeltaBinary::new(count64, ws.widths)),
            (6, Physical::ByteArray) => Self::DeltaLength(DeltaLengthByteArrays::new(
                count,
                ws.lengths,
                ws.widths,
                spec.max_value_bytes,
            )?),
            (7, Physical::ByteArray) => Self::DeltaBytes(DeltaByteArrays::new(
                count,
                ws.parts,
                ws.widths,
                ws.previous,
                spec.max_value_bytes,
            )?),
            (7, Physical::FixedByteArray(n)) => Self::DeltaBytes(
                DeltaByteArrays::new(
                    count,
                    ws.parts,
                    ws.widths,
                    ws.previous,
                    spec.max_value_bytes,
                )?
                .with_fixed_length(n)?,
            ),
            _ => {
                return Err(Error::UnsupportedEncoding {
                    physical: spec.physical,
                    encoding,
                })
            }
        })
    }

    fn is_complete(&self) -> bool {
        match self {
            Self::Bool(d) => d.is_complete(),
            Self::BoolRle(d) => d.is_complete(),
            Self::I32(d) => d.is_complete(),
            Self::I64(d) => d.is_complete(),
            Self::Int96(d) => d.is_complete(),
            Self::F32(d) => d.is_complete(),
            Self::F64(d) => d.is_complete(),
            Self::Bytes(d) => d.is_complete(),
            Self::Fixed(d) => d.is_complete(),
            Self::Dictionary(d) => d.is_complete(),
            Self::Delta32(d) => d.is_complete(),
            Self::Delta64(d) => d.is_complete(),
            Self::DeltaLength(d) => d.is_complete(),
            Self::DeltaBytes(d) => d.is_complete(),
        }
    }

    fn feed(
        &mut self,
        input: &[u8],
        rows: &mut RowCursor,
        bits: &[u8],
        emit: &mut impl for<'ev> PageSink<'ev>,
    ) -> Result<usize> {
        // The low-level decoders deliberately have infallible callbacks.
        // Capture a row-alignment error, then surface it after decoding.
        macro_rules! scalar {
            ($decoder:expr, $variant:ident) => {{
                let mut error = None;
                let used = $decoder.feed(input, |v| {
                    if error.is_none() {
                        error = rows.scalar(bits, Value::$variant(v), emit).err();
                    }
                })?;
                if let Some(e) = error {
                    Err(e)
                } else {
                    Ok(used)
                }
            }};
        }
        macro_rules! binary {
            ($decoder:expr) => {{
                let mut error = None;
                let used = $decoder.feed(input, |event| {
                    if error.is_none() {
                        error = rows.bytes(bits, event, emit).err();
                    }
                })?;
                if let Some(e) = error {
                    Err(e)
                } else {
                    Ok(used)
                }
            }};
        }
        match self {
            Self::Bool(d) => scalar!(d, Boolean),
            Self::BoolRle(d) => scalar!(d, Boolean),
            Self::I32(d) => scalar!(d, I32),
            Self::I64(d) => scalar!(d, I64),
            Self::Int96(d) => scalar!(d, Int96),
            Self::F32(d) => scalar!(d, F32),
            Self::F64(d) => scalar!(d, F64),
            Self::Dictionary(d) => scalar!(d, DictionaryId),
            Self::Delta32(d) => scalar!(d, I32),
            Self::Delta64(d) => scalar!(d, I64),
            Self::Bytes(d) => binary!(d),
            Self::Fixed(d) => binary!(d),
            Self::DeltaLength(d) => binary!(d),
            Self::DeltaBytes(d) => binary!(d),
        }
    }
}

/// One page at a time, consuming **decompressed** bytes. For V2, feed the
/// uncompressed definition levels before feeding decompressed values. The
/// outer [`PageAssembler`](super::PageAssembler) handles the codec and
/// compressed payload boundaries.
pub struct FlatPage<'a> {
    layout: PageLayout,
    spec: ColumnSpec,
    present: &'a mut [u8],
    stage: Stage<'a>,
    rows: RowCursor,
}

/// Parquet stores all definition levels before any value.
enum Stage<'a> {
    /// Optional column: levels fill the presence bitmap; `levels` counts
    /// rows seen and `present` the rows with a value.
    Levels {
        decoder: Levels,
        scratch: ValueScratch<'a>,
        levels: usize,
        present: usize,
    },
    Values(Values<'a>),
}

impl<'a> FlatPage<'a> {
    /// Start decoding the page described by `header`.
    ///
    /// # Errors
    ///
    /// [`Error::NestedColumn`] for a repeated or multi-level column, any
    /// [`page_layout`](super::page_layout) error, an unsupported encoding,
    /// or [`Error::WorkspaceTooSmall`].
    // The workspace's borrows move into the decoder.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(header: PageHeader, spec: ColumnSpec, ws: DecodeWorkspace<'a>) -> Result<Self> {
        if spec.max_repetition_level != 0 || spec.max_definition_level > 1 {
            return Err(Error::NestedColumn);
        }
        let layout = PageLayout::from_header(header, spec)?;
        let rows = u32::try_from(layout.count).map_err(|_| Error::InvalidValueCount)?;
        let optional = spec.max_definition_level == 1 && layout.kind != PageKind::Dictionary;
        let scratch = ValueScratch {
            widths: ws.widths,
            lengths: ws.lengths,
            parts: ws.parts,
            previous: ws.previous,
        };
        let stage = if optional {
            let needed = layout.count.div_ceil(8);
            let available = ws.present.len();
            ws.present
                .get_mut(..needed)
                .ok_or(Error::WorkspaceTooSmall { needed, available })?
                .fill(0);
            let decoder = if layout.kind == PageKind::V2 {
                let bytes =
                    u32::try_from(layout.v2_levels_bytes).map_err(|_| Error::InvalidPageSize)?;
                Levels::v2(1, u64::from(rows), bytes)?
            } else {
                Levels::v1(
                    1,
                    u64::from(rows),
                    saturating_u32(layout.uncompressed_bytes),
                )?
            };
            Stage::Levels {
                decoder,
                scratch,
                levels: 0,
                present: 0,
            }
        } else {
            Stage::Values(Values::new(spec, layout.encoding, layout.count, scratch)?)
        };
        Ok(Self {
            layout,
            spec,
            present: ws.present,
            stage,
            rows: RowCursor::new(rows, layout.kind, optional),
        })
    }

    /// Page body size on disk.
    #[must_use]
    pub const fn compressed_size(&self) -> usize {
        self.layout.compressed_bytes
    }

    /// Page body size after decompression.
    #[must_use]
    pub const fn uncompressed_size(&self) -> usize {
        self.layout.uncompressed_bytes
    }

    /// Uncompressed V2 level bytes at the start of the body.
    #[must_use]
    pub const fn v2_levels_size(&self) -> usize {
        self.layout.v2_levels_bytes
    }

    /// Whether the values section passes through the codec.
    #[must_use]
    pub const fn values_compressed(&self) -> bool {
        self.layout.values_compressed
    }

    /// The page type's `parquet.thrift` name.
    #[must_use]
    pub const fn page_kind(&self) -> &'static str {
        self.layout.kind.name()
    }

    /// Decode decompressed body bytes; returns bytes consumed. Once every
    /// value is decoded, trailing writer padding is consumed too.
    ///
    /// # Errors
    ///
    /// Malformed levels or values, a level or value count that disagrees
    /// with the header, or an inconsistent V2 null count.
    pub fn feed(&mut self, input: &[u8], emit: &mut impl for<'ev> PageSink<'ev>) -> Result<usize> {
        let mut at = 0;
        if matches!(self.stage, Stage::Levels { .. }) {
            at = self.feed_levels(input)?;
        }
        let Stage::Values(values) = &mut self.stage else {
            // The levels need more input.
            return Ok(at);
        };
        while at < input.len() && !values.is_complete() {
            let used = values.feed(&input[at..], &mut self.rows, self.present, emit)?;
            if used == 0 {
                break;
            }
            at += used;
        }
        if values.is_complete() {
            // The value count ends the page. Writers, notably fastparquet,
            // append unused bytes after the last value.
            at = input.len();
        }
        Ok(at)
    }

    /// Fill the presence bitmap; once the levels are complete, start the
    /// value decoder for the present rows.
    fn feed_levels(&mut self, input: &[u8]) -> Result<usize> {
        let Stage::Levels {
            decoder,
            scratch,
            levels,
            present,
        } = &mut self.stage
        else {
            return Ok(0);
        };
        let count = self.layout.count;
        let bits = &mut *self.present;
        let mut at = 0;
        let mut err = None;
        while at < input.len() && !decoder.is_complete() {
            let used = decoder.feed(&input[at..], |level| {
                if *levels >= count {
                    err = Some(Error::TooManyValues);
                    return;
                }
                if level == 1 {
                    bits[*levels >> 3] |= 1 << (*levels & 7);
                    *present += 1;
                }
                *levels += 1;
            })?;
            if used == 0 {
                break;
            }
            at += used;
        }
        if let Some(e) = err {
            return Err(e);
        }
        if !decoder.is_complete() {
            return Ok(at);
        }
        if *levels != count {
            return Err(Error::LengthMismatch);
        }
        let physical = *present;
        if self
            .layout
            .expected_nulls
            .is_some_and(|nulls| count - physical != nulls)
        {
            return Err(Error::InvalidNullCount);
        }
        let scratch = core::mem::take(scratch);
        self.stage = Stage::Values(Values::new(
            self.spec,
            self.layout.encoding,
            physical,
            scratch,
        )?);
        Ok(at)
    }

    /// End the page, emitting any trailing nulls.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] if levels or values are missing, or
    /// [`Error::MissingValues`] if present rows received no value.
    pub fn finish(&mut self, emit: &mut impl for<'ev> PageSink<'ev>) -> Result<()> {
        if matches!(self.stage, Stage::Levels { .. }) {
            return Err(Error::Truncated);
        }
        // Some string decoders emit their final End event without consuming a
        // byte. Flush that event before checking completion.
        self.feed(&[], emit)?;
        match &self.stage {
            Stage::Values(values) if values.is_complete() => self.rows.finish(self.present, emit),
            _ => Err(Error::Truncated),
        }
    }
}

fn saturating_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}
