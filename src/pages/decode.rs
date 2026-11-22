//! Flat column decoding with one-bit-per-row null bookkeeping.
//!
//! Parquet places all definition levels before the values. A forward-only
//! decoder cannot emit correctly positioned nullable values until those levels
//! have arrived; the reusable presence bitmap is the minimal general buffer.

use super::{Error, Layout, PageKind, Result};
use crate::parquet::PageHeader;
use crate::values::{
    ByteArrayEvent, ByteArrays, DeltaBinary, DeltaByteArrays, DeltaByteParts,
    DeltaLengthByteArrays, DictionaryIds, FixedByteArrays, Int96, Levels, PlainBool, PlainValues,
    RleBool,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Physical {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedByteArray(u32),
}

#[derive(Debug, Clone, Copy)]
pub struct ColumnSpec {
    pub physical: Physical,
    pub codec: i32,               // parquet.thrift CompressionCodec
    pub max_definition_level: u8, // 0 = required; 1 = optional
    pub max_repetition_level: u8, // must be zero
    pub dictionary_size: Option<u32>,
    pub max_page_bytes: usize,
    pub max_page_values: usize,
    pub max_value_bytes: u32,
}

impl ColumnSpec {
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
    pub const fn optional(mut self) -> Self {
        self.max_definition_level = 1;
        self
    }
    pub const fn with_dictionary(mut self, length: u32) -> Self {
        self.dictionary_size = Some(length);
        self
    }
}

/// All workspaces are borrowed. Unused slices can be empty; delta-string
/// paths need `lengths` or `parts` sized for *non-null values*, not rows.
pub struct DecodeWorkspace<'a> {
    pub present: &'a mut [u8],
    pub widths: &'a mut [u8],
    pub lengths: &'a mut [u32],
    pub parts: &'a mut [DeltaByteParts],
    pub previous: &'a mut [u8],
}

struct ValueScratch<'a> {
    widths: &'a mut [u8],
    lengths: &'a mut [u32],
    parts: &'a mut [DeltaByteParts],
    previous: &'a mut [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Location {
    Row(u32),
    Dictionary(u32),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value<'a> {
    Boolean(bool),
    I32(i32),
    I64(i64),
    Int96(Int96),
    F32(f32),
    F64(f64),
    DictionaryId(u32),
    BytesStart(u32),
    BytesChunk(&'a [u8]),
    BytesEnd,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageEvent<'a> {
    Null(u32),
    Value(Location, Value<'a>),
}

/// One page event at a time. The lifetime is per call, so a closure can be
/// stored and reused across `feed` and `finish` without allocating.
///
/// Annotate a stored closure as `|event: PageEvent<'_>|`. Borrowed
/// `BytesChunk` data refers to either the caller's input or a caller-owned
/// scratch buffer, and only for the duration of `on_page`.
pub trait PageSink<'a> {
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

struct RowCursor {
    count: usize,
    index: usize,
    active_binary: Option<Location>,
    dictionary: bool,
    optional: bool,
}

impl RowCursor {
    fn new(count: usize, kind: PageKind, optional: bool) -> Self {
        Self {
            count,
            index: 0,
            active_binary: None,
            dictionary: kind == PageKind::Dictionary,
            optional,
        }
    }

    fn is_present(&self, bits: &[u8], row: usize) -> bool {
        !self.optional || (bits[row >> 3] & (1 << (row & 7))) != 0
    }

    fn next(&mut self, bits: &[u8], emit: &mut impl for<'ev> PageSink<'ev>) -> Result<Location> {
        while self.index < self.count {
            let index = self.index;
            self.index += 1;
            if self.is_present(bits, index) {
                let index = index as u32; // max_page_values is capped at < u32::MAX
                return Ok(if self.dictionary {
                    Location::Dictionary(index)
                } else {
                    Location::Row(index)
                });
            }
            emit.on_page(PageEvent::Null(index as u32));
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
            emit.on_page(PageEvent::Null(index as u32));
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
            (3, Physical::Boolean) => Self::BoolRle(RleBool::new(
                count64,
                spec.max_page_bytes.min(u32::MAX as usize) as u32,
            )?),
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
/// outer PageAssembler handles the codec and compressed payload boundaries.
pub struct FlatPage<'a> {
    layout: Layout,
    spec: ColumnSpec,
    present: &'a mut [u8],
    level: Option<Levels>,
    level_count: usize,
    physical_count: usize,
    scratch: Option<ValueScratch<'a>>,
    values: Option<Values<'a>>,
    rows: RowCursor,
}

impl<'a> FlatPage<'a> {
    pub fn new(header: PageHeader, spec: ColumnSpec, ws: DecodeWorkspace<'a>) -> Result<Self> {
        if spec.max_repetition_level != 0 || spec.max_definition_level > 1 {
            return Err(Error::NestedColumn);
        }
        let layout = Layout::from_header(header, spec)?;
        let optional = spec.max_definition_level == 1 && layout.kind != PageKind::Dictionary;
        let needed = layout.count.div_ceil(8);
        if optional && ws.present.len() < needed {
            return Err(Error::WorkspaceTooSmall {
                needed,
                available: ws.present.len(),
            });
        }
        if optional {
            ws.present[..needed].fill(0);
        }
        let level = if optional {
            Some(if layout.kind == PageKind::V2 {
                Levels::v2(1, layout.count as u64, layout.v2_levels_bytes as u32)?
            } else {
                Levels::v1(
                    1,
                    layout.count as u64,
                    layout.uncompressed_bytes.min(u32::MAX as usize) as u32,
                )?
            })
        } else {
            None
        };
        let value_scratch = ValueScratch {
            widths: ws.widths,
            lengths: ws.lengths,
            parts: ws.parts,
            previous: ws.previous,
        };
        let (scratch, values) = if optional {
            (Some(value_scratch), None)
        } else {
            (
                None,
                Some(Values::new(
                    spec,
                    layout.encoding,
                    layout.count,
                    value_scratch,
                )?),
            )
        };
        Ok(Self {
            layout,
            spec,
            present: ws.present,
            level,
            level_count: 0,
            physical_count: if optional { 0 } else { layout.count },
            scratch,
            values,
            rows: RowCursor::new(layout.count, layout.kind, optional),
        })
    }

    pub fn compressed_size(&self) -> usize {
        self.layout.compressed_bytes
    }
    pub fn uncompressed_size(&self) -> usize {
        self.layout.uncompressed_bytes
    }
    pub fn v2_levels_size(&self) -> usize {
        self.layout.v2_levels_bytes
    }
    pub fn values_compressed(&self) -> bool {
        self.layout.values_compressed
    }
    pub fn page_kind(&self) -> &'static str {
        match self.layout.kind {
            PageKind::V1 => "DATA_PAGE",
            PageKind::V2 => "DATA_PAGE_V2",
            PageKind::Dictionary => "DICTIONARY_PAGE",
        }
    }

    pub fn feed(&mut self, input: &[u8], emit: &mut impl for<'ev> PageSink<'ev>) -> Result<usize> {
        let mut at = 0;
        if let Some(level) = &mut self.level {
            let count = self.layout.count;
            let bits = &mut *self.present;
            let levels = &mut self.level_count;
            let physical = &mut self.physical_count;
            let mut err = None;
            while at < input.len() && !level.is_complete() {
                let used = level.feed(&input[at..], |value| {
                    if *levels >= count {
                        err = Some(Error::TooManyValues);
                        return;
                    }
                    if value == 1 {
                        bits[*levels >> 3] |= 1 << (*levels & 7);
                        *physical += 1;
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
            if !level.is_complete() {
                return Ok(at);
            }
            if self.level_count != count {
                return Err(Error::LengthMismatch);
            }
            if let Some(expected_nulls) = self.layout.expected_nulls {
                if count - self.physical_count != expected_nulls {
                    return Err(Error::InvalidNullCount);
                }
            }
            self.level = None;
            let scratch = self.scratch.take().ok_or(Error::InvalidHeader)?;
            self.values = Some(Values::new(
                self.spec,
                self.layout.encoding,
                self.physical_count,
                scratch,
            )?);
        }
        while at < input.len() {
            let values = self.values.as_mut().ok_or(Error::InvalidHeader)?;
            if values.is_complete() {
                at = input.len();
                break;
            }
            let used = values.feed(&input[at..], &mut self.rows, self.present, emit)?;
            if used == 0 {
                break;
            }
            at += used;
        }
        if self.values.as_ref().is_some_and(Values::is_complete) {
            // The value count ends the page. Writers, notably fastparquet,
            // append unused bytes after the last value.
            at = input.len();
        }
        Ok(at)
    }

    pub fn finish(&mut self, emit: &mut impl for<'ev> PageSink<'ev>) -> Result<()> {
        // Some string decoders emit their final End event without consuming a
        // byte. Flush that event before checking completion.
        if self.level.is_some() {
            return Err(Error::Truncated);
        }
        self.feed(&[], emit)?;
        if !self.values.as_ref().is_some_and(Values::is_complete) {
            return Err(Error::Truncated);
        }
        self.rows.finish(self.present, emit)
    }
}
