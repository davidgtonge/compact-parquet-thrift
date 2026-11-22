//! Parquet file encoder for the same subset the page reader accepts.
//!
//! Flat columns, definition level 0 or 1, PLAIN, dictionary, or delta values,
//! V1 data pages, uncompressed or raw Snappy. The caller owns the output
//! buffer, the uncompressed-page scratch, and the optional Snappy hash table.
//! Dictionary entries are supplied by the caller; this module does not build one.
//!
//! A set bit in [`Column::present`] means that row has a value. The value
//! slices contain only those present rows, in order.

mod delta;
mod page;
mod plain;
mod rle;
mod snappy;
mod stats;

use crate::{SliceSink, WireType, Writer};
use page::write_column_pages;

pub use delta::{
    delta_binary_bound, delta_byte_array_bound, delta_length_bound, encode_delta_bytes,
    encode_delta_i32, encode_delta_i64, encode_delta_length_bytes,
};
pub use rle::encode_hybrid;
pub use snappy::compress as compress_snappy;
pub use stats::{ChunkStats, ValueRange};

pub const UNCOMPRESSED: i32 = 0;
pub const SNAPPY: i32 = 1;

pub type Result<T> = core::result::Result<T, EncodeError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeError {
    OutputFull,
    Overflow,
    ValueCount,
    ValueOutOfRange,
    UnsupportedCodec,
    EmptyDictionary,
    Thrift,
}

#[derive(Debug, Clone, Copy)]
pub enum ColumnValues<'a> {
    Bool(&'a [bool]),
    I32(&'a [i32]),
    I64(&'a [i64]),
    F32(&'a [f32]),
    F64(&'a [f64]),
    Bytes(&'a [&'a [u8]]),
    /// PLAIN FIXED_LEN_BYTE_ARRAY. Every value has length `width`.
    Fixed {
        width: u32,
        values: &'a [&'a [u8]],
    },
    /// INT32 with converted type DATE.
    Date(&'a [i32]),
    /// `ids` has one entry per present row and indexes `dictionary`.
    DictI32 {
        dictionary: &'a [i32],
        ids: &'a [u32],
    },
    DictI64 {
        dictionary: &'a [i64],
        ids: &'a [u32],
    },
    DictF32 {
        dictionary: &'a [f32],
        ids: &'a [u32],
    },
    DictF64 {
        dictionary: &'a [f64],
        ids: &'a [u32],
    },
    DictBytes {
        dictionary: &'a [&'a [u8]],
        ids: &'a [u32],
    },
    DictFixed {
        width: u32,
        dictionary: &'a [&'a [u8]],
        ids: &'a [u32],
    },
    /// DELTA_BINARY_PACKED, blocks of 128 and four mini-blocks.
    DeltaI32(&'a [i32]),
    DeltaI64(&'a [i64]),
    /// DELTA_LENGTH_BYTE_ARRAY.
    DeltaLengthBytes(&'a [&'a [u8]]),
    /// DELTA_BYTE_ARRAY. Shared prefixes are taken against the previous value.
    DeltaBytes(&'a [&'a [u8]]),
}

pub struct Column<'a> {
    pub name: &'a str,
    pub rows: usize,
    /// 0 uncompressed, 1 Snappy.
    pub codec: i32,
    pub present: Option<&'a [u8]>,
    pub values: ColumnValues<'a>,
}

/// Where one column chunk landed in the file, and what its footer says about its values.
/// Offsets are absolute.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkPlace {
    pub offset: u64,
    pub data_page_offset: u64,
    pub dictionary_page_offset: Option<u64>,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub stats: ChunkStats,
}

pub(crate) struct Cursor<'a> {
    buf: &'a mut [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, at: 0 }
    }
    pub(crate) fn position(&self) -> usize {
        self.at
    }

    pub(crate) fn write(&mut self, bytes: &[u8]) -> Result<()> {
        let end = self
            .at
            .checked_add(bytes.len())
            .ok_or(EncodeError::Overflow)?;
        let dst = self
            .buf
            .get_mut(self.at..end)
            .ok_or(EncodeError::OutputFull)?;
        dst.copy_from_slice(bytes);
        self.at = end;
        Ok(())
    }

    pub(crate) fn write_uleb(&mut self, mut value: u64) -> Result<()> {
        let mut buf = [0u8; 10];
        let mut len = 0;
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            buf[len] = byte | if value != 0 { 0x80 } else { 0 };
            len += 1;
            if value == 0 {
                break;
            }
        }
        self.write(&buf[..len])
    }
}

pub(crate) fn thrift_error(error: crate::Error) -> EncodeError {
    match error {
        crate::Error::OutputFull => EncodeError::OutputFull,
        crate::Error::IntegerOverflow => EncodeError::Overflow,
        _ => EncodeError::Thrift,
    }
}

/// Bytes of scratch required for one column. `value_bytes` is the larger of
/// the dictionary page's PLAIN payload and the data page's value payload.
/// For delta columns, pass [`delta_binary_bound`], [`delta_length_bound`], or
/// [`delta_byte_array_bound`].
pub fn scratch_len(rows: usize, value_bytes: usize) -> Option<usize> {
    rows.checked_mul(2)?
        .checked_add(16)?
        .checked_add(value_bytes)
}

pub const MAGIC_LEN: usize = 4;
pub const TRAILER_LEN: usize = 8;

/// Leading `PAR1` magic (4 bytes).
pub fn write_magic(out: &mut [u8]) -> Result<usize> {
    if out.len() < MAGIC_LEN {
        return Err(EncodeError::OutputFull);
    }
    out[..MAGIC_LEN].copy_from_slice(b"PAR1");
    Ok(MAGIC_LEN)
}

/// Metadata for one completed row group in the file footer.
pub struct RowGroupFooter<'a> {
    pub num_rows: i64,
    pub columns: &'a [Column<'a>],
    pub places: &'a [ChunkPlace],
}

/// Encode one row group's column pages into `out`.
///
/// `places` are filled with offsets **relative to the start of `out`**. The caller
/// adds the absolute file offset before retaining them for [`write_footer`].
pub fn write_row_group(
    out: &mut [u8],
    num_rows: i64,
    columns: &[Column<'_>],
    places: &mut [ChunkPlace],
    scratch: &mut [u8],
    snappy_table: &mut [u32],
) -> Result<usize> {
    if columns.len() != places.len() {
        return Err(EncodeError::ValueCount);
    }
    if i32::try_from(columns.len()).is_err() {
        return Err(EncodeError::Overflow);
    }
    let mut at = 0usize;
    for (column, place) in columns.iter().zip(places.iter_mut()) {
        let present = check_column(column, num_rows)?;
        let stat = write_column_pages(
            &mut out[at..],
            column.rows,
            column.codec,
            column.present,
            &column.values,
            scratch,
            snappy_table,
        )?;
        let offset = u64::try_from(at).map_err(|_| EncodeError::Overflow)?;
        *place = ChunkPlace {
            offset,
            data_page_offset: offset
                + u64::try_from(stat.dictionary_on_disk.unwrap_or(0))
                    .map_err(|_| EncodeError::Overflow)?,
            dictionary_page_offset: stat.dictionary_on_disk.map(|_| offset),
            compressed_size: u64::try_from(stat.on_disk).map_err(|_| EncodeError::Overflow)?,
            uncompressed_size: u64::try_from(stat.uncompressed_with_header)
                .map_err(|_| EncodeError::Overflow)?,
            stats: stats::chunk_stats(column.rows, &column.values, present),
        };
        at = at.checked_add(stat.on_disk).ok_or(EncodeError::Overflow)?;
    }
    Ok(at)
}

/// Thrift file metadata plus the 8-byte Parquet trailer.
///
/// `schema_columns` defines the schema leaves (row counts are ignored). Each
/// [`RowGroupFooter`] supplies that group's row count, per-column chunk metadata,
/// and absolute chunk offsets in `places`.
pub fn write_footer(
    out: &mut [u8],
    schema_columns: &[Column<'_>],
    row_groups: &[RowGroupFooter<'_>],
) -> Result<usize> {
    if schema_columns.is_empty() {
        return Err(EncodeError::ValueCount);
    }
    for rg in row_groups {
        if rg.columns.len() != schema_columns.len() || rg.places.len() != schema_columns.len() {
            return Err(EncodeError::ValueCount);
        }
    }
    let num_rows: i64 = row_groups.iter().try_fold(0i64, |acc, rg| {
        acc.checked_add(rg.num_rows).ok_or(EncodeError::Overflow)
    })?;
    let footer_thrift = write_footer_thrift(out, schema_columns, row_groups, num_rows)?;
    let footer_len = u32::try_from(footer_thrift).map_err(|_| EncodeError::Overflow)?;
    let at = footer_thrift;
    if out.len() < at + TRAILER_LEN {
        return Err(EncodeError::OutputFull);
    }
    out[at..at + 4].copy_from_slice(&footer_len.to_le_bytes());
    out[at + 4..at + TRAILER_LEN].copy_from_slice(b"PAR1");
    Ok(at + TRAILER_LEN)
}

fn offset_places(places: &mut [ChunkPlace], base: u64) -> Result<()> {
    for place in places.iter_mut() {
        place.offset = place
            .offset
            .checked_add(base)
            .ok_or(EncodeError::Overflow)?;
        place.data_page_offset = place
            .data_page_offset
            .checked_add(base)
            .ok_or(EncodeError::Overflow)?;
        if let Some(dict) = place.dictionary_page_offset {
            place.dictionary_page_offset =
                Some(dict.checked_add(base).ok_or(EncodeError::Overflow)?);
        }
    }
    Ok(())
}

/// Write `PAR1`, one row group, the footer, and the 8-byte trailer.
///
/// `places` is filled one entry per column with **absolute** file offsets.
/// `scratch` must hold the largest uncompressed page body ([`scratch_len`]).
pub fn write_file(
    out: &mut [u8],
    num_rows: i64,
    columns: &[Column<'_>],
    places: &mut [ChunkPlace],
    scratch: &mut [u8],
    snappy_table: &mut [u32],
) -> Result<usize> {
    if columns.len() != places.len() {
        return Err(EncodeError::ValueCount);
    }
    let mut at = write_magic(out)?;
    let rg_len = write_row_group(
        &mut out[at..],
        num_rows,
        columns,
        places,
        scratch,
        snappy_table,
    )?;
    offset_places(
        places,
        u64::try_from(at).map_err(|_| EncodeError::Overflow)?,
    )?;
    at = at.checked_add(rg_len).ok_or(EncodeError::Overflow)?;
    let footer = write_footer(
        &mut out[at..],
        columns,
        &[RowGroupFooter {
            num_rows,
            columns,
            places,
        }],
    )?;
    Ok(at.checked_add(footer).ok_or(EncodeError::Overflow)?)
}

/// Validate one column; returns how many rows are present.
fn check_column(column: &Column<'_>, num_rows: i64) -> Result<usize> {
    if column.codec != UNCOMPRESSED && column.codec != SNAPPY {
        return Err(EncodeError::UnsupportedCodec);
    }
    let rows = i64::try_from(column.rows).map_err(|_| EncodeError::Overflow)?;
    if rows != num_rows {
        return Err(EncodeError::ValueCount);
    }
    let present = present_count(column.rows, column.present)?;
    if present != value_count(&column.values) {
        return Err(EncodeError::ValueCount);
    }
    Ok(present)
}

fn present_count(rows: usize, present: Option<&[u8]>) -> Result<usize> {
    let Some(bits) = present else {
        return Ok(rows);
    };
    if bits.len() < rows.div_ceil(8) {
        return Err(EncodeError::ValueCount);
    }
    let mut count = 0usize;
    for row in 0..rows {
        if bits[row >> 3] & (1 << (row & 7)) != 0 {
            count += 1;
        }
    }
    Ok(count)
}

fn value_count(values: &ColumnValues<'_>) -> usize {
    match values {
        ColumnValues::Bool(v) => v.len(),
        ColumnValues::I32(v) => v.len(),
        ColumnValues::I64(v) => v.len(),
        ColumnValues::F32(v) => v.len(),
        ColumnValues::F64(v) => v.len(),
        ColumnValues::Bytes(v) => v.len(),
        ColumnValues::Fixed { values, .. } => values.len(),
        ColumnValues::Date(v) => v.len(),
        ColumnValues::DictI32 { ids, .. }
        | ColumnValues::DictI64 { ids, .. }
        | ColumnValues::DictF32 { ids, .. }
        | ColumnValues::DictF64 { ids, .. }
        | ColumnValues::DictBytes { ids, .. }
        | ColumnValues::DictFixed { ids, .. } => ids.len(),
        ColumnValues::DeltaI32(v) => v.len(),
        ColumnValues::DeltaI64(v) => v.len(),
        ColumnValues::DeltaLengthBytes(v) | ColumnValues::DeltaBytes(v) => v.len(),
    }
}

pub(crate) fn value_encoding(values: &ColumnValues<'_>) -> i32 {
    match values {
        ColumnValues::DictI32 { .. }
        | ColumnValues::DictI64 { .. }
        | ColumnValues::DictF32 { .. }
        | ColumnValues::DictF64 { .. }
        | ColumnValues::DictBytes { .. }
        | ColumnValues::DictFixed { .. } => 8,
        ColumnValues::DeltaI32(_) | ColumnValues::DeltaI64(_) => 5,
        ColumnValues::DeltaLengthBytes(_) => 6,
        ColumnValues::DeltaBytes(_) => 7,
        _ => 0,
    }
}

fn write_footer_thrift(
    out: &mut [u8],
    schema_columns: &[Column<'_>],
    row_groups: &[RowGroupFooter<'_>],
    num_rows: i64,
) -> Result<usize> {
    let mut writer = Writer::new(SliceSink::new(out));
    writer
        .structure(|w| {
            w.field_i32(1, 1)?;
            w.field_header(2, WireType::List)?;
            w.list_header(WireType::Struct, (1 + schema_columns.len()) as u32)?;
            w.structure(|w| {
                w.field_binary(4, b"schema")?;
                w.field_i32(5, schema_columns.len() as i32)
            })?;
            for column in schema_columns {
                w.structure(|w| write_schema_leaf(w, column))?;
            }
            w.field_i64(3, num_rows)?;
            w.field_header(4, WireType::List)?;
            w.list_header(WireType::Struct, row_groups.len() as u32)?;
            for rg in row_groups {
                w.structure(|w| write_row_group_meta(w, rg.columns, rg.places, rg.num_rows))?;
            }
            w.field_binary(6, b"compact-parquet-thrift")
        })
        .map_err(thrift_error)?;
    Ok(writer.into_inner().len())
}

fn write_schema_leaf(w: &mut Writer<SliceSink<'_>>, column: &Column<'_>) -> crate::Result<()> {
    w.field_i32(1, physical_type(&column.values))?;
    if let Some(width) = fixed_width(&column.values) {
        w.field_i32(2, width as i32)?;
    }
    w.field_i32(3, if column.present.is_some() { 1 } else { 0 })?;
    w.field_binary(4, column.name.as_bytes())?;
    if matches!(column.values, ColumnValues::Date(_)) {
        w.field_i32(6, 6)?; // DATE
    } else if matches!(
        column.values,
        ColumnValues::Bytes(_)
            | ColumnValues::DictBytes { .. }
            | ColumnValues::DeltaLengthBytes(_)
            | ColumnValues::DeltaBytes(_)
    ) {
        w.field_i32(6, 0)?; // UTF8
    }
    Ok(())
}

fn fixed_width(values: &ColumnValues<'_>) -> Option<u32> {
    match values {
        ColumnValues::Fixed { width, .. } | ColumnValues::DictFixed { width, .. } => Some(*width),
        _ => None,
    }
}

fn write_row_group_meta(
    w: &mut Writer<SliceSink<'_>>,
    columns: &[Column<'_>],
    places: &[ChunkPlace],
    num_rows: i64,
) -> crate::Result<()> {
    let uncompressed = sum_i64(places.iter().map(|p| p.uncompressed_size))?;
    let compressed = sum_i64(places.iter().map(|p| p.compressed_size))?;
    w.field_header(1, WireType::List)?;
    w.list_header(WireType::Struct, columns.len() as u32)?;
    for (column, place) in columns.iter().zip(places) {
        w.structure(|w| write_column_chunk(w, column, place))?;
    }
    w.field_i64(2, uncompressed)?;
    w.field_i64(3, num_rows)?;
    w.field_i64(5, compressed)
}

fn sum_i64(values: impl Iterator<Item = u64>) -> crate::Result<i64> {
    let mut total = 0i64;
    for value in values {
        let value = i64::try_from(value).map_err(|_| crate::Error::IntegerOverflow)?;
        total = total
            .checked_add(value)
            .ok_or(crate::Error::IntegerOverflow)?;
    }
    Ok(total)
}

fn write_column_chunk(
    w: &mut Writer<SliceSink<'_>>,
    column: &Column<'_>,
    place: &ChunkPlace,
) -> crate::Result<()> {
    let offset = i64::try_from(place.offset).map_err(|_| crate::Error::IntegerOverflow)?;
    let uncompressed =
        i64::try_from(place.uncompressed_size).map_err(|_| crate::Error::IntegerOverflow)?;
    let compressed =
        i64::try_from(place.compressed_size).map_err(|_| crate::Error::IntegerOverflow)?;
    let data_page =
        i64::try_from(place.data_page_offset).map_err(|_| crate::Error::IntegerOverflow)?;
    w.field_i64(2, offset)?;
    w.field_struct(3, |w| {
        w.field_i32(1, physical_type(&column.values))?;
        let (encodings, n) = encoding_list(column);
        w.field_header(2, WireType::List)?;
        w.list_header(WireType::I32, u32::from(n))?;
        for encoding in &encodings[..usize::from(n)] {
            w.i32(*encoding)?;
        }
        w.field_header(3, WireType::List)?;
        w.list_header(WireType::Binary, 1)?;
        w.binary(column.name.as_bytes())?;
        w.field_i32(4, column.codec)?;
        w.field_i64(
            5,
            i64::try_from(column.rows).map_err(|_| crate::Error::IntegerOverflow)?,
        )?;
        w.field_i64(6, uncompressed)?;
        w.field_i64(7, compressed)?;
        w.field_i64(9, data_page)?;
        if let Some(dictionary) = place.dictionary_page_offset {
            w.field_i64(
                11,
                i64::try_from(dictionary).map_err(|_| crate::Error::IntegerOverflow)?,
            )?;
        }
        stats::write_statistics(w, &place.stats)
    })
}

fn encoding_list(column: &Column<'_>) -> ([i32; 3], u8) {
    let mut list = [0i32; 3];
    let mut n = 1u8; // PLAIN, used by plain values or the dictionary page
    if column.present.is_some() {
        list[usize::from(n)] = 3; // RLE definition levels
        n += 1;
    }
    let data = value_encoding(&column.values);
    if data != 0 {
        list[usize::from(n)] = data;
        n += 1;
    }
    (list, n)
}

fn physical_type(values: &ColumnValues<'_>) -> i32 {
    match values {
        ColumnValues::Bool(_) => 0,
        ColumnValues::I32(_)
        | ColumnValues::Date(_)
        | ColumnValues::DictI32 { .. }
        | ColumnValues::DeltaI32(_) => 1,
        ColumnValues::I64(_) | ColumnValues::DictI64 { .. } | ColumnValues::DeltaI64(_) => 2,
        ColumnValues::F32(_) | ColumnValues::DictF32 { .. } => 4,
        ColumnValues::F64(_) | ColumnValues::DictF64 { .. } => 5,
        ColumnValues::Bytes(_)
        | ColumnValues::DictBytes { .. }
        | ColumnValues::DeltaLengthBytes(_)
        | ColumnValues::DeltaBytes(_) => 6,
        ColumnValues::Fixed { .. } | ColumnValues::DictFixed { .. } => 7,
    }
}

/// Metadata for one column chunk assembled by copying compressed pages (no decode).
#[derive(Debug, Clone, Copy)]
pub struct CopiedColumnMeta<'a> {
    pub name: &'a str,
    pub physical: i32,
    pub codec: i32,
    pub max_def: u8,
    pub num_values: i64,
    pub uncompressed_size: i64,
    pub compressed_size: i64,
    pub data_page_offset: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct CopiedRowGroupMeta<'a> {
    pub num_rows: i64,
    pub columns: &'a [CopiedColumnMeta<'a>],
}

pub struct CopiedSchemaLeaf<'a> {
    pub name: &'a str,
    pub physical: i32,
    pub max_def: u8,
}

/// Write Thrift footer + 8-byte trailer for page-copied row groups (no statistics/indexes).
pub fn write_footer_copied<'a>(
    out: &mut [u8],
    schema: &[CopiedSchemaLeaf<'a>],
    row_groups: &[CopiedRowGroupMeta<'a>],
    total_rows: i64,
) -> Result<usize> {
    if row_groups.is_empty() || schema.is_empty() {
        return Err(EncodeError::ValueCount);
    }
    let rg_len = u32::try_from(row_groups.len()).map_err(|_| EncodeError::Overflow)?;
    let mut w = Writer::new(SliceSink::new(out));
    w.structure(|w| {
        w.field_i32(1, 1)?;
        w.field_header(2, WireType::List)?;
        w.list_header(WireType::Struct, (1 + schema.len()) as u32)?;
        w.structure(|w| {
            w.field_binary(4, b"schema")?;
            w.field_i32(5, schema.len() as i32)
        })?;
        for leaf in schema {
            w.structure(|w| {
                w.field_i32(1, leaf.physical)?;
                w.field_i32(3, if leaf.max_def > 0 { 1 } else { 0 })?;
                w.field_binary(4, leaf.name.as_bytes())
            })?;
        }
        w.field_i64(3, total_rows)?;
        w.field_header(4, WireType::List)?;
        w.list_header(WireType::Struct, rg_len)?;
        for group in row_groups {
            write_copied_row_group(w, group)?;
        }
        w.field_binary(6, b"wcol-page-compact")
    })
    .map_err(|_| EncodeError::Thrift)?;
    let footer_thrift = w.into_inner().as_written().len();
    let footer_len = u32::try_from(footer_thrift).map_err(|_| EncodeError::Overflow)?;
    let at = footer_thrift;
    if out.len() < at + TRAILER_LEN {
        return Err(EncodeError::OutputFull);
    }
    out[at..at + 4].copy_from_slice(&footer_len.to_le_bytes());
    out[at + 4..at + TRAILER_LEN].copy_from_slice(b"PAR1");
    Ok(at + TRAILER_LEN)
}

fn write_copied_row_group<'a>(
    w: &mut Writer<SliceSink<'_>>,
    group: &CopiedRowGroupMeta<'a>,
) -> crate::Result<()> {
    let uncompressed = sum_i64(group.columns.iter().map(|c| c.uncompressed_size as u64))?;
    let compressed = sum_i64(group.columns.iter().map(|c| c.compressed_size as u64))?;
    w.field_header(1, WireType::List)?;
    w.list_header(
        WireType::Struct,
        u32::try_from(group.columns.len()).map_err(|_| crate::Error::IntegerOverflow)?,
    )?;
    for column in group.columns {
        write_copied_column_chunk(w, column)?;
    }
    w.field_i64(2, uncompressed)?;
    w.field_i64(3, group.num_rows)?;
    w.field_i64(5, compressed)
}

fn write_copied_column_chunk<'a>(
    w: &mut Writer<SliceSink<'_>>,
    column: &CopiedColumnMeta<'a>,
) -> crate::Result<()> {
    let chunk_start =
        i64::try_from(column.data_page_offset).map_err(|_| crate::Error::IntegerOverflow)?;
    let data_page = chunk_start;
    w.field_i64(2, chunk_start)?;
    w.field_struct(3, |w| {
        w.field_i32(1, column.physical)?;
        let (encodings, n) = copied_encodings(column.max_def);
        w.field_header(2, WireType::List)?;
        w.list_header(WireType::I32, u32::from(n))?;
        for encoding in &encodings[..usize::from(n)] {
            w.i32(*encoding)?;
        }
        w.field_header(3, WireType::List)?;
        w.list_header(WireType::Binary, 1)?;
        w.binary(column.name.as_bytes())?;
        w.field_i32(4, column.codec)?;
        w.field_i64(5, column.num_values)?;
        w.field_i64(6, column.uncompressed_size)?;
        w.field_i64(7, column.compressed_size)?;
        w.field_i64(9, data_page)?;
        Ok(())
    })
}

fn copied_encodings(max_def: u8) -> ([i32; 2], u8) {
    if max_def > 0 {
        ([0, 3], 2)
    } else {
        ([0, 0], 1)
    }
}
