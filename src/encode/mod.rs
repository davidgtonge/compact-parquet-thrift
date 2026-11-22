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

use core::fmt;

use crate::{SliceSink, WireType, Writer};
use page::write_column_pages;

pub use delta::{
    delta_binary_bound, delta_byte_array_bound, delta_length_bound, encode_delta_bytes,
    encode_delta_i32, encode_delta_i64, encode_delta_length_bytes,
};
pub use rle::encode_hybrid;
pub use snappy::compress as compress_snappy;
pub use stats::{ChunkStats, ValueRange};

/// `CompressionCodec.UNCOMPRESSED`.
pub const UNCOMPRESSED: i32 = 0;
/// `CompressionCodec.SNAPPY` (raw Snappy blocks).
pub const SNAPPY: i32 = 1;

/// Result of an encoding step.
pub type Result<T> = core::result::Result<T, EncodeError>;

/// Why a column, page or footer could not be encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EncodeError {
    /// The output or scratch buffer is too small; retry with a larger one.
    OutputFull,
    /// A size, count or offset does not fit its Parquet field.
    Overflow,
    /// Value, row, column or place counts disagree.
    ValueCount,
    /// A value does not fit its encoding (bit width, dictionary id or fixed width).
    ValueOutOfRange,
    /// The codec is neither [`UNCOMPRESSED`] nor [`SNAPPY`].
    UnsupportedCodec,
    /// Dictionary ids were supplied with an empty dictionary.
    EmptyDictionary,
    /// The Thrift writer rejected the metadata (nesting or field order).
    Thrift,
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::OutputFull => "output buffer is full",
            Self::Overflow => "size or count overflows its Parquet field",
            Self::ValueCount => "value, row or column counts disagree",
            Self::ValueOutOfRange => "value does not fit its encoding",
            Self::UnsupportedCodec => "unsupported compression codec",
            Self::EmptyDictionary => "dictionary ids without dictionary entries",
            Self::Thrift => "invalid Thrift metadata structure",
        })
    }
}

impl core::error::Error for EncodeError {}

/// The values of one column chunk and how to encode them.
#[derive(Debug, Clone, Copy)]
pub enum ColumnValues<'a> {
    /// PLAIN `BOOLEAN`.
    Bool(&'a [bool]),
    /// PLAIN `INT32`.
    I32(&'a [i32]),
    /// PLAIN `INT64`.
    I64(&'a [i64]),
    /// PLAIN `FLOAT`.
    F32(&'a [f32]),
    /// PLAIN `DOUBLE`.
    F64(&'a [f64]),
    /// PLAIN `BYTE_ARRAY`, annotated UTF8.
    Bytes(&'a [&'a [u8]]),
    /// PLAIN `FIXED_LEN_BYTE_ARRAY`. Every value has length `width`.
    Fixed {
        /// Byte length of every value.
        width: u32,
        /// The values.
        values: &'a [&'a [u8]],
    },
    /// INT32 with converted type DATE.
    Date(&'a [i32]),
    /// `ids` has one entry per present row and indexes `dictionary`.
    DictI32 {
        /// Dictionary entries.
        dictionary: &'a [i32],
        /// One dictionary index per present row.
        ids: &'a [u32],
    },
    /// Dictionary-encoded `INT64`.
    DictI64 {
        /// Dictionary entries.
        dictionary: &'a [i64],
        /// One dictionary index per present row.
        ids: &'a [u32],
    },
    /// Dictionary-encoded `FLOAT`.
    DictF32 {
        /// Dictionary entries.
        dictionary: &'a [f32],
        /// One dictionary index per present row.
        ids: &'a [u32],
    },
    /// Dictionary-encoded `DOUBLE`.
    DictF64 {
        /// Dictionary entries.
        dictionary: &'a [f64],
        /// One dictionary index per present row.
        ids: &'a [u32],
    },
    /// Dictionary-encoded `BYTE_ARRAY`, annotated UTF8.
    DictBytes {
        /// Dictionary entries.
        dictionary: &'a [&'a [u8]],
        /// One dictionary index per present row.
        ids: &'a [u32],
    },
    /// Dictionary-encoded `FIXED_LEN_BYTE_ARRAY`.
    DictFixed {
        /// Byte length of every entry.
        width: u32,
        /// Dictionary entries.
        dictionary: &'a [&'a [u8]],
        /// One dictionary index per present row.
        ids: &'a [u32],
    },
    /// `DELTA_BINARY_PACKED` `INT32`, blocks of 128 and four mini-blocks.
    DeltaI32(&'a [i32]),
    /// `DELTA_BINARY_PACKED` `INT64`.
    DeltaI64(&'a [i64]),
    /// `DELTA_LENGTH_BYTE_ARRAY`, annotated UTF8.
    DeltaLengthBytes(&'a [&'a [u8]]),
    /// `DELTA_BYTE_ARRAY`, annotated UTF8. Shared prefixes are taken against the previous value.
    DeltaBytes(&'a [&'a [u8]]),
}

impl<'a> ColumnValues<'a> {
    /// Physical values supplied (dictionary ids for dictionary columns).
    pub(crate) const fn len(&self) -> usize {
        match self {
            Self::Bool(v) => v.len(),
            Self::I32(v) | Self::Date(v) | Self::DeltaI32(v) => v.len(),
            Self::I64(v) | Self::DeltaI64(v) => v.len(),
            Self::F32(v) => v.len(),
            Self::F64(v) => v.len(),
            Self::Bytes(v) | Self::DeltaLengthBytes(v) | Self::DeltaBytes(v) => v.len(),
            Self::Fixed { values, .. } => values.len(),
            Self::DictI32 { ids, .. }
            | Self::DictI64 { ids, .. }
            | Self::DictF32 { ids, .. }
            | Self::DictF64 { ids, .. }
            | Self::DictBytes { ids, .. }
            | Self::DictFixed { ids, .. } => ids.len(),
        }
    }

    /// For a dictionary column: its entries as PLAIN values, and the ids.
    pub(crate) const fn dictionary(&self) -> Option<(ColumnValues<'a>, &'a [u32])> {
        match *self {
            Self::DictI32 { dictionary, ids } => Some((Self::I32(dictionary), ids)),
            Self::DictI64 { dictionary, ids } => Some((Self::I64(dictionary), ids)),
            Self::DictF32 { dictionary, ids } => Some((Self::F32(dictionary), ids)),
            Self::DictF64 { dictionary, ids } => Some((Self::F64(dictionary), ids)),
            Self::DictBytes { dictionary, ids } => Some((Self::Bytes(dictionary), ids)),
            Self::DictFixed {
                width,
                dictionary,
                ids,
            } => Some((
                Self::Fixed {
                    width,
                    values: dictionary,
                },
                ids,
            )),
            _ => None,
        }
    }

    /// Data page value `Encoding`.
    pub(crate) const fn encoding(&self) -> i32 {
        match self {
            Self::DictI32 { .. }
            | Self::DictI64 { .. }
            | Self::DictF32 { .. }
            | Self::DictF64 { .. }
            | Self::DictBytes { .. }
            | Self::DictFixed { .. } => 8, // RLE_DICTIONARY
            Self::DeltaI32(_) | Self::DeltaI64(_) => 5,
            Self::DeltaLengthBytes(_) => 6,
            Self::DeltaBytes(_) => 7,
            _ => 0, // PLAIN
        }
    }

    /// Parquet physical `Type`.
    const fn physical_type(&self) -> i32 {
        match self {
            Self::Bool(_) => 0,
            Self::I32(_) | Self::Date(_) | Self::DictI32 { .. } | Self::DeltaI32(_) => 1,
            Self::I64(_) | Self::DictI64 { .. } | Self::DeltaI64(_) => 2,
            Self::F32(_) | Self::DictF32 { .. } => 4,
            Self::F64(_) | Self::DictF64 { .. } => 5,
            Self::Bytes(_)
            | Self::DictBytes { .. }
            | Self::DeltaLengthBytes(_)
            | Self::DeltaBytes(_) => 6,
            Self::Fixed { .. } | Self::DictFixed { .. } => 7,
        }
    }

    /// `ConvertedType` annotation, if any.
    const fn converted_type(&self) -> Option<i32> {
        match self {
            Self::Date(_) => Some(6), // DATE
            Self::Bytes(_)
            | Self::DictBytes { .. }
            | Self::DeltaLengthBytes(_)
            | Self::DeltaBytes(_) => Some(0), // UTF8
            _ => None,
        }
    }

    const fn fixed_width(&self) -> Option<u32> {
        match self {
            Self::Fixed { width, .. } | Self::DictFixed { width, .. } => Some(*width),
            _ => None,
        }
    }
}

/// One column chunk to encode.
#[derive(Debug, Clone, Copy)]
pub struct Column<'a> {
    /// Flat column name (one path component).
    pub name: &'a str,
    /// Rows in the chunk, nulls included.
    pub rows: usize,
    /// [`UNCOMPRESSED`] or [`SNAPPY`].
    pub codec: i32,
    /// Presence bitmap (bit set = row has a value) for an optional column;
    /// `None` for a required one.
    pub present: Option<&'a [u8]>,
    /// The present rows' values.
    pub values: ColumnValues<'a>,
}

/// Where one column chunk landed in the file, and what its footer says about its values.
/// Offsets are absolute.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkPlace {
    /// Offset of the chunk's first page.
    pub offset: u64,
    /// Offset of the data page.
    pub data_page_offset: u64,
    /// Offset of the dictionary page, for dictionary columns.
    pub dictionary_page_offset: Option<u64>,
    /// Bytes on disk, page headers included.
    pub compressed_size: u64,
    /// Bytes after decompression, page headers included.
    pub uncompressed_size: u64,
    /// Statistics recorded in the footer.
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

    pub(crate) const fn position(&self) -> usize {
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

    pub(crate) fn write_uleb(&mut self, value: u64) -> Result<()> {
        let mut buf = [0u8; crate::varint::MAX_LEN];
        let len = crate::varint::encode(value, &mut buf);
        self.write(&buf[..len])
    }
}

pub(crate) const fn thrift_error(error: crate::Error) -> EncodeError {
    match error {
        crate::Error::OutputFull => EncodeError::OutputFull,
        crate::Error::IntegerOverflow => EncodeError::Overflow,
        _ => EncodeError::Thrift,
    }
}

/// Whether row `row` is set in a presence bitmap.
pub(crate) fn is_present(bits: &[u8], row: usize) -> bool {
    bits[row >> 3] & (1 << (row & 7)) != 0
}

/// Bytes of scratch required for one column. `value_bytes` is the larger of
/// the dictionary page's PLAIN payload and the data page's value payload.
/// For delta columns, pass [`delta_binary_bound`], [`delta_length_bound`], or
/// [`delta_byte_array_bound`].
#[must_use]
pub fn scratch_len(rows: usize, value_bytes: usize) -> Option<usize> {
    rows.checked_mul(2)?
        .checked_add(16)?
        .checked_add(value_bytes)
}

/// Length of the leading `PAR1` magic.
pub const MAGIC_LEN: usize = 4;
/// Length of the footer-length-plus-`PAR1` trailer.
pub const TRAILER_LEN: usize = 8;

/// Leading `PAR1` magic (4 bytes).
///
/// # Errors
///
/// [`EncodeError::OutputFull`] if `out` is shorter than four bytes.
pub fn write_magic(out: &mut [u8]) -> Result<usize> {
    out.get_mut(..MAGIC_LEN)
        .ok_or(EncodeError::OutputFull)?
        .copy_from_slice(b"PAR1");
    Ok(MAGIC_LEN)
}

/// Metadata for one completed row group in the file footer.
#[derive(Debug, Clone, Copy)]
pub struct RowGroupFooter<'a> {
    /// Rows in the group.
    pub num_rows: i64,
    /// The group's columns, in schema order.
    pub columns: &'a [Column<'a>],
    /// Absolute placement of each column chunk.
    pub places: &'a [ChunkPlace],
}

/// Encode one row group's column pages into `out`.
///
/// `places` are filled with offsets **relative to the start of `out`**. The caller
/// adds the absolute file offset before retaining them for [`write_footer`].
///
/// # Errors
///
/// [`EncodeError::ValueCount`] if counts disagree, [`EncodeError::UnsupportedCodec`],
/// [`EncodeError::OutputFull`] if `out` or `scratch` is too small, or any
/// value encoding error.
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
        let stat = write_column_pages(&mut out[at..], column, scratch, snappy_table)?;
        let offset = to_u64(at)?;
        *place = ChunkPlace {
            offset,
            data_page_offset: offset + to_u64(stat.dictionary_on_disk.unwrap_or(0))?,
            dictionary_page_offset: stat.dictionary_on_disk.map(|_| offset),
            compressed_size: to_u64(stat.on_disk)?,
            uncompressed_size: to_u64(stat.uncompressed_with_header)?,
            stats: stats::chunk_stats(column.rows, &column.values, present),
        };
        at = at.checked_add(stat.on_disk).ok_or(EncodeError::Overflow)?;
    }
    Ok(at)
}

fn to_u64(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| EncodeError::Overflow)
}

/// Thrift file metadata plus the 8-byte Parquet trailer.
///
/// `schema_columns` defines the schema leaves (row counts are ignored). Each
/// [`RowGroupFooter`] supplies that group's row count, per-column chunk metadata,
/// and absolute chunk offsets in `places`.
///
/// # Errors
///
/// [`EncodeError::ValueCount`] for an empty schema or mismatched row-group
/// columns, [`EncodeError::Overflow`], or [`EncodeError::OutputFull`] if the
/// footer does not fit `out`.
pub fn write_footer(
    out: &mut [u8],
    schema_columns: &[Column<'_>],
    row_groups: &[RowGroupFooter<'_>],
) -> Result<usize> {
    if schema_columns.is_empty() {
        return Err(EncodeError::ValueCount);
    }
    let leaves = schema_columns.len();
    if row_groups
        .iter()
        .any(|rg| rg.columns.len() != leaves || rg.places.len() != leaves)
    {
        return Err(EncodeError::ValueCount);
    }
    let num_rows = row_groups.iter().try_fold(0i64, |acc, rg| {
        acc.checked_add(rg.num_rows).ok_or(EncodeError::Overflow)
    })?;
    let metadata = write_file_metadata(out, schema_columns, row_groups, num_rows)?;
    write_trailer(out, metadata)
}

/// Write `PAR1`, one row group, the footer, and the 8-byte trailer.
///
/// `places` is filled one entry per column with **absolute** file offsets.
/// `scratch` must hold the largest uncompressed page body ([`scratch_len`]).
///
/// # Errors
///
/// As [`write_row_group`] and [`write_footer`].
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
    let magic = write_magic(out)?;
    let rg_len = write_row_group(
        &mut out[magic..],
        num_rows,
        columns,
        places,
        scratch,
        snappy_table,
    )?;
    offset_places(places, to_u64(magic)?)?;
    let at = magic.checked_add(rg_len).ok_or(EncodeError::Overflow)?;
    let footer = write_footer(
        &mut out[at..],
        columns,
        &[RowGroupFooter {
            num_rows,
            columns,
            places,
        }],
    )?;
    at.checked_add(footer).ok_or(EncodeError::Overflow)
}

fn offset_places(places: &mut [ChunkPlace], base: u64) -> Result<()> {
    let shift = |offset: u64| offset.checked_add(base).ok_or(EncodeError::Overflow);
    places.iter_mut().try_for_each(|place| {
        place.offset = shift(place.offset)?;
        place.data_page_offset = shift(place.data_page_offset)?;
        place.dictionary_page_offset = place.dictionary_page_offset.map(shift).transpose()?;
        Ok(())
    })
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
    if present != column.values.len() {
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
    Ok((0..rows).filter(|&row| is_present(bits, row)).count())
}

/// Append the 4-byte footer length and `PAR1` after `metadata_len` bytes of
/// Thrift `FileMetaData` at the front of `out`.
fn write_trailer(out: &mut [u8], metadata_len: usize) -> Result<usize> {
    let footer_len = u32::try_from(metadata_len).map_err(|_| EncodeError::Overflow)?;
    let end = metadata_len
        .checked_add(TRAILER_LEN)
        .ok_or(EncodeError::Overflow)?;
    let trailer = out
        .get_mut(metadata_len..end)
        .ok_or(EncodeError::OutputFull)?;
    trailer[..4].copy_from_slice(&footer_len.to_le_bytes());
    trailer[4..].copy_from_slice(b"PAR1");
    Ok(end)
}

fn write_file_metadata(
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
            w.list_header(WireType::Struct, thrift_len(1 + schema_columns.len())?)?;
            w.structure(|w| {
                w.field_binary(4, b"schema")?;
                w.field_i32(5, thrift_i32(schema_columns.len())?)
            })?;
            for column in schema_columns {
                w.structure(|w| write_schema_leaf(w, column))?;
            }
            w.field_i64(3, num_rows)?;
            w.field_header(4, WireType::List)?;
            w.list_header(WireType::Struct, thrift_len(row_groups.len())?)?;
            for rg in row_groups {
                w.structure(|w| write_row_group_meta(w, rg))?;
            }
            w.field_binary(6, b"compact-parquet-thrift")
        })
        .map_err(thrift_error)?;
    Ok(writer.into_inner().len())
}

fn thrift_len(len: usize) -> crate::Result<u32> {
    u32::try_from(len).map_err(|_| crate::Error::IntegerOverflow)
}

fn thrift_i32(value: usize) -> crate::Result<i32> {
    i32::try_from(value).map_err(|_| crate::Error::IntegerOverflow)
}

fn thrift_i64(value: u64) -> crate::Result<i64> {
    i64::try_from(value).map_err(|_| crate::Error::IntegerOverflow)
}

fn write_schema_leaf(w: &mut Writer<SliceSink<'_>>, column: &Column<'_>) -> crate::Result<()> {
    w.field_i32(1, column.values.physical_type())?;
    if let Some(width) = column.values.fixed_width() {
        let width = i32::try_from(width).map_err(|_| crate::Error::IntegerOverflow)?;
        w.field_i32(2, width)?;
    }
    // FieldRepetitionType: 0 REQUIRED, 1 OPTIONAL.
    w.field_i32(3, i32::from(column.present.is_some()))?;
    w.field_binary(4, column.name.as_bytes())?;
    if let Some(converted) = column.values.converted_type() {
        w.field_i32(6, converted)?;
    }
    Ok(())
}

fn write_row_group_meta(
    w: &mut Writer<SliceSink<'_>>,
    rg: &RowGroupFooter<'_>,
) -> crate::Result<()> {
    let total = |size: fn(&ChunkPlace) -> u64| {
        rg.places.iter().try_fold(0i64, |acc, place| {
            acc.checked_add(thrift_i64(size(place))?)
                .ok_or(crate::Error::IntegerOverflow)
        })
    };
    let uncompressed = total(|p| p.uncompressed_size)?;
    let compressed = total(|p| p.compressed_size)?;
    w.field_header(1, WireType::List)?;
    w.list_header(WireType::Struct, thrift_len(rg.columns.len())?)?;
    for (column, place) in rg.columns.iter().zip(rg.places) {
        w.structure(|w| write_column_chunk(w, column, place))?;
    }
    w.field_i64(2, uncompressed)?;
    w.field_i64(3, rg.num_rows)?;
    w.field_i64(5, compressed)
}

fn write_column_chunk(
    w: &mut Writer<SliceSink<'_>>,
    column: &Column<'_>,
    place: &ChunkPlace,
) -> crate::Result<()> {
    w.field_i64(2, thrift_i64(place.offset)?)?;
    w.field_struct(3, |w| {
        w.field_i32(1, column.values.physical_type())?;
        let (encodings, n) = encoding_list(column);
        w.field_header(2, WireType::List)?;
        w.list_header(WireType::I32, u32::from(n))?;
        for &encoding in &encodings[..usize::from(n)] {
            w.i32(encoding)?;
        }
        w.field_header(3, WireType::List)?;
        w.list_header(WireType::Binary, 1)?;
        w.binary(column.name.as_bytes())?;
        w.field_i32(4, column.codec)?;
        let rows = i64::try_from(column.rows).map_err(|_| crate::Error::IntegerOverflow)?;
        w.field_i64(5, rows)?;
        w.field_i64(6, thrift_i64(place.uncompressed_size)?)?;
        w.field_i64(7, thrift_i64(place.compressed_size)?)?;
        w.field_i64(9, thrift_i64(place.data_page_offset)?)?;
        if let Some(dictionary) = place.dictionary_page_offset {
            w.field_i64(11, thrift_i64(dictionary)?)?;
        }
        stats::write_statistics(w, &place.stats)
    })
}

/// `ColumnMetaData.encodings`: PLAIN, then RLE for definition levels, then
/// the data page's value encoding when it is not PLAIN.
fn encoding_list(column: &Column<'_>) -> ([i32; 3], u8) {
    let mut list = [0i32; 3];
    let mut n = 1u8; // PLAIN, used by plain values or the dictionary page
    if column.present.is_some() {
        list[usize::from(n)] = 3; // RLE definition levels
        n += 1;
    }
    let data = column.values.encoding();
    if data != 0 {
        list[usize::from(n)] = data;
        n += 1;
    }
    (list, n)
}
