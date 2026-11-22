//! Minimal zero-allocation Parquet metadata inspection.
//!
//! These are *visitors*, not a complete generated model of `parquet.thrift`.
//! Unknown fields are skipped, including nested structs and collections.

use core::ops::Range;

use crate::{Error, Field, Reader, Result, WireType};

/// Locate the serialized Thrift `FileMetaData` using the final eight bytes.
/// The first four bytes of the object should independently be checked for `PAR1`.
///
/// # Errors
///
/// [`Error::InvalidMagic`] if the trailer does not end in `PAR1`, and
/// [`Error::InvalidFooterLength`] if the footer would not fit after the
/// leading magic.
pub fn footer_range(file_size: u64, tail: &[u8; 8]) -> Result<Range<u64>> {
    let [l0, l1, l2, l3, magic @ ..] = *tail;
    if magic != *b"PAR1" {
        return Err(Error::InvalidMagic);
    }
    let len = u64::from(u32::from_le_bytes([l0, l1, l2, l3]));
    let end = file_size.checked_sub(8).ok_or(Error::InvalidFooterLength)?;
    let start = end.checked_sub(len).ok_or(Error::InvalidFooterLength)?;
    if start < 4 {
        return Err(Error::InvalidFooterLength);
    }
    Ok(start..end)
}

/// File-level totals gathered while scanning a footer.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FooterSummary {
    /// `FileMetaData.version`.
    pub version: Option<i32>,
    /// `FileMetaData.num_rows`.
    pub num_rows: Option<i64>,
    /// Number of row groups.
    pub row_groups: u32,
    /// Column chunks across all row groups.
    pub columns: u64,
}

/// Column offsets from `FileMetaData` -> `RowGroup` -> `ColumnChunk` -> `ColumnMetaData`.
///
/// Offsets and sizes are signed in the Thrift IDL; validate nonnegative values
/// before converting them to object-store range requests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ColumnChunkInfo<'a> {
    /// Zero-based row group index.
    pub row_group: u32,
    /// Zero-based column index within the row group.
    pub index: u32,
    /// `ColumnChunk.file_path`, when the chunk lives in another file.
    pub external_file: Option<&'a [u8]>,
    /// Only populated when `path_in_schema` has exactly one component.
    pub flat_name: Option<&'a [u8]>,
    /// `ColumnMetaData.type` (Parquet physical type).
    pub physical_type: Option<i32>,
    /// `ColumnMetaData.codec` (Parquet compression codec).
    pub codec: Option<i32>,
    /// `ColumnMetaData.num_values`, including nulls.
    pub num_values: Option<i64>,
    /// `ColumnChunk.file_offset` (deprecated by most writers).
    pub file_offset: Option<i64>,
    /// Offset of the first data page.
    pub data_page_offset: Option<i64>,
    /// Offset of the dictionary page, if any.
    pub dictionary_page_offset: Option<i64>,
    /// `ColumnMetaData.total_compressed_size`, page headers included.
    pub compressed_size: Option<i64>,
    /// `ColumnMetaData.total_uncompressed_size`, page headers included.
    pub uncompressed_size: Option<i64>,
    /// Offset of the page offset index.
    pub offset_index_offset: Option<i64>,
    /// Offset of the page column index.
    pub column_index_offset: Option<i64>,
    /// Offset of the bloom filter.
    pub bloom_filter_offset: Option<i64>,
    /// `RowGroup.num_rows` for the group that owns this column. Not inferred from `num_values`.
    pub row_group_rows: Option<i64>,
    /// Deprecated signed min (field 2). Prefer [`Self::min_value`].
    pub min: Option<&'a [u8]>,
    /// Deprecated signed max (field 1). Prefer [`Self::max_value`].
    pub max: Option<&'a [u8]>,
    /// `Statistics.min_value`, plain-encoded.
    pub min_value: Option<&'a [u8]>,
    /// `Statistics.max_value`, plain-encoded.
    pub max_value: Option<&'a [u8]>,
    /// `Statistics.null_count`.
    pub null_count: Option<i64>,
    /// `Statistics.distinct_count`.
    pub distinct_count: Option<i64>,
}

/// Visit one `ColumnChunkInfo` at a time instead of collecting a Vec.
/// Schema parsing and full compatibility validation belong to an upper layer.
///
/// # Errors
///
/// Any malformed Thrift, a field with the wrong wire type, or an error
/// returned by `visit` (which stops the scan).
pub fn scan_footer<'a>(
    metadata: &'a [u8],
    mut visit: impl FnMut(ColumnChunkInfo<'a>) -> Result<()>,
) -> Result<FooterSummary> {
    let mut reader = Reader::new(metadata);
    let mut root = reader.structure()?;
    let mut summary = FooterSummary::default();
    while let Some(field) = root.next_field()? {
        match field.id {
            1 => summary.version = i32_field(&mut root, field)?,
            3 => summary.num_rows = i64_field(&mut root, field)?,
            4 => {
                let len = struct_list(&mut root, field)?;
                summary.row_groups = len;
                for rg in 0..len {
                    let columns = visit_row_group(&mut root, rg, &mut visit)?;
                    summary.columns = summary
                        .columns
                        .checked_add(u64::from(columns))
                        .ok_or(Error::IntegerOverflow)?;
                }
            }
            _ => root.skip(field.kind)?,
        }
    }
    Ok(summary)
}

fn i32_field(reader: &mut Reader<'_>, field: Field) -> Result<Option<i32>> {
    field.expect(WireType::I32)?;
    reader.i32().map(Some)
}

fn i64_field(reader: &mut Reader<'_>, field: Field) -> Result<Option<i64>> {
    field.expect(WireType::I64)?;
    reader.i64().map(Some)
}

fn binary_field<'a>(reader: &mut Reader<'a>, field: Field) -> Result<Option<&'a [u8]>> {
    field.expect(WireType::Binary)?;
    reader.binary().map(Some)
}

/// Header of a `list<struct>` field; returns its length.
fn struct_list(reader: &mut Reader<'_>, field: Field) -> Result<u32> {
    field.expect(WireType::List)?;
    let header = reader.list_header()?;
    if header.element != WireType::Struct {
        return Err(Error::UnexpectedWireType);
    }
    Ok(header.len)
}

/// Columns that fit here are parsed in one pass. Wider row groups rewind
/// and take a skip-then-emit walk so the stack frame stays bounded.
const INLINE_COLUMNS: usize = 512;

fn visit_row_group<'a>(
    reader: &mut Reader<'a>,
    row_group: u32,
    visit: &mut impl FnMut(ColumnChunkInfo<'a>) -> Result<()>,
) -> Result<u32> {
    let start = reader.position();
    if let Some(count) = try_inline_row_group(reader, row_group, visit)? {
        return Ok(count);
    }
    reader.set_position(start)?;
    let rows = row_group_num_rows(&mut reader.fork_at(start)?)?;
    emit_row_group_columns(reader, row_group, rows, visit)
}

// `RowGroup.num_rows` (field 3) follows the column list (field 1), yet every
// visited column carries it. Buffering a typical row group's columns on the
// stack avoids a second walk over its bytes.
#[allow(clippy::large_stack_arrays)]
fn try_inline_row_group<'a>(
    reader: &mut Reader<'a>,
    row_group: u32,
    visit: &mut impl FnMut(ColumnChunkInfo<'a>) -> Result<()>,
) -> Result<Option<u32>> {
    let mut group = reader.structure()?;
    let mut count = 0;
    let mut rows = None;
    let mut pending = [ColumnChunkInfo::default(); INLINE_COLUMNS];
    while let Some(field) = group.next_field()? {
        match field.id {
            1 => {
                count = struct_list(&mut group, field)?;
                let Some(slots) = usize::try_from(count)
                    .ok()
                    .and_then(|len| pending.get_mut(..len))
                else {
                    return Ok(None);
                };
                for (index, slot) in (0..count).zip(slots) {
                    *slot = column_chunk(&mut group, row_group, index, None)?;
                }
            }
            3 => rows = i64_field(&mut group, field)?,
            _ => group.skip(field.kind)?,
        }
    }
    let stored = usize::try_from(count).map_err(|_| Error::IntegerOverflow)?;
    pending.iter().take(stored).try_for_each(|&info| {
        visit(ColumnChunkInfo {
            row_group_rows: rows,
            ..info
        })
    })?;
    Ok(Some(count))
}

fn row_group_num_rows(reader: &mut Reader<'_>) -> Result<Option<i64>> {
    let mut group = reader.structure()?;
    let mut rows = None;
    while let Some(field) = group.next_field()? {
        if field.id == 3 {
            rows = i64_field(&mut group, field)?;
        } else {
            group.skip(field.kind)?;
        }
    }
    Ok(rows)
}

fn emit_row_group_columns<'a>(
    reader: &mut Reader<'a>,
    row_group: u32,
    rows: Option<i64>,
    visit: &mut impl FnMut(ColumnChunkInfo<'a>) -> Result<()>,
) -> Result<u32> {
    let mut group = reader.structure()?;
    let mut count = 0;
    while let Some(field) = group.next_field()? {
        if field.id == 1 {
            count = struct_list(&mut group, field)?;
            for index in 0..count {
                visit(column_chunk(&mut group, row_group, index, rows)?)?;
            }
        } else {
            group.skip(field.kind)?;
        }
    }
    Ok(count)
}

fn column_chunk<'a>(
    reader: &mut Reader<'a>,
    row_group: u32,
    index: u32,
    row_group_rows: Option<i64>,
) -> Result<ColumnChunkInfo<'a>> {
    let mut info = ColumnChunkInfo {
        row_group,
        index,
        row_group_rows,
        ..ColumnChunkInfo::default()
    };
    let mut chunk = reader.structure()?;
    while let Some(field) = chunk.next_field()? {
        match field.id {
            1 => info.external_file = binary_field(&mut chunk, field)?,
            2 => info.file_offset = i64_field(&mut chunk, field)?,
            3 => {
                field.expect(WireType::Struct)?;
                visit_column_metadata(&mut chunk, &mut info)?;
            }
            4 => info.offset_index_offset = i64_field(&mut chunk, field)?,
            6 => info.column_index_offset = i64_field(&mut chunk, field)?,
            _ => chunk.skip(field.kind)?,
        }
    }
    Ok(info)
}

fn visit_column_metadata<'a>(
    reader: &mut Reader<'a>,
    info: &mut ColumnChunkInfo<'a>,
) -> Result<()> {
    let mut meta = reader.structure()?;
    while let Some(field) = meta.next_field()? {
        match field.id {
            1 => info.physical_type = i32_field(&mut meta, field)?,
            3 => info.flat_name = flat_path(&mut meta, field)?,
            4 => info.codec = i32_field(&mut meta, field)?,
            5 => info.num_values = i64_field(&mut meta, field)?,
            6 => info.uncompressed_size = i64_field(&mut meta, field)?,
            7 => info.compressed_size = i64_field(&mut meta, field)?,
            9 => info.data_page_offset = i64_field(&mut meta, field)?,
            11 => info.dictionary_page_offset = i64_field(&mut meta, field)?,
            12 => {
                field.expect(WireType::Struct)?;
                visit_statistics(&mut meta, info)?;
            }
            14 => info.bloom_filter_offset = i64_field(&mut meta, field)?,
            _ => meta.skip(field.kind)?,
        }
    }
    Ok(())
}

/// `path_in_schema`: the single component of a flat column, else `None`.
fn flat_path<'a>(reader: &mut Reader<'a>, field: Field) -> Result<Option<&'a [u8]>> {
    field.expect(WireType::List)?;
    let header = reader.list_header()?;
    if header.element != WireType::Binary {
        return Err(Error::UnexpectedWireType);
    }
    if header.len == 1 {
        return reader.binary().map(Some);
    }
    for _ in 0..header.len {
        reader.binary()?;
    }
    Ok(None)
}

fn visit_statistics<'a>(reader: &mut Reader<'a>, info: &mut ColumnChunkInfo<'a>) -> Result<()> {
    let mut stats = reader.structure()?;
    while let Some(field) = stats.next_field()? {
        match field.id {
            1 => info.max = binary_field(&mut stats, field)?,
            2 => info.min = binary_field(&mut stats, field)?,
            3 => info.null_count = i64_field(&mut stats, field)?,
            4 => info.distinct_count = i64_field(&mut stats, field)?,
            5 => info.max_value = binary_field(&mut stats, field)?,
            6 => info.min_value = binary_field(&mut stats, field)?,
            _ => stats.skip(field.kind)?,
        }
    }
    Ok(())
}

/// The fields of `DataPageHeaderV2` this crate reads.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DataPageV2 {
    /// Values in the page, nulls included.
    pub num_values: Option<i32>,
    /// Null values in the page.
    pub num_nulls: Option<i32>,
    /// Rows in the page.
    pub num_rows: Option<i32>,
    /// Encoding of the values section.
    pub encoding: Option<i32>,
    /// Byte length of the definition levels (never compressed).
    pub definition_levels_bytes: Option<i32>,
    /// Byte length of the repetition levels (never compressed).
    pub repetition_levels_bytes: Option<i32>,
    /// Defaults to true in the Parquet IDL when absent.
    pub is_compressed: bool,
}

/// The fields of a Parquet `PageHeader` this crate reads, flattened.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    /// `PageType`: 0 data V1, 2 dictionary, 3 data V2.
    pub page_type: Option<i32>,
    /// Page body size after decompression.
    pub uncompressed_size: Option<i32>,
    /// Page body size on disk.
    pub compressed_size: Option<i32>,
    /// CRC-32 of the on-disk page body.
    pub crc: Option<i32>,
    /// `DataPageHeader.num_values`, nulls included.
    pub data_v1_num_values: Option<i32>,
    /// `DataPageHeader.encoding`.
    pub data_v1_encoding: Option<i32>,
    /// `DataPageHeader.definition_level_encoding`.
    pub data_v1_definition_encoding: Option<i32>,
    /// `DataPageHeader.repetition_level_encoding`.
    pub data_v1_repetition_encoding: Option<i32>,
    /// `DictionaryPageHeader.num_values`.
    pub dictionary_num_values: Option<i32>,
    /// `DictionaryPageHeader.encoding`.
    pub dictionary_encoding: Option<i32>,
    /// `DataPageHeaderV2`, for V2 data pages.
    pub data_v2: Option<DataPageV2>,
}

/// Parse one Thrift `PageHeader` from a prefix. Returns its exact byte length;
/// bytes after that length belong to the page payload.
///
/// # Errors
///
/// [`Error::EndOfInput`] means more bytes are needed for the complete
/// header. Any other error means the header is malformed.
pub fn page_header(prefix: &[u8]) -> Result<(PageHeader, usize)> {
    let mut reader = Reader::new(prefix);
    let mut root = reader.structure()?;
    let mut out = PageHeader::default();
    while let Some(field) = root.next_field()? {
        match field.id {
            1 => out.page_type = i32_field(&mut root, field)?,
            2 => out.uncompressed_size = i32_field(&mut root, field)?,
            3 => out.compressed_size = i32_field(&mut root, field)?,
            4 => out.crc = i32_field(&mut root, field)?,
            5 => {
                field.expect(WireType::Struct)?;
                data_page_v1(&mut root, &mut out)?;
            }
            7 => {
                field.expect(WireType::Struct)?;
                dictionary_page(&mut root, &mut out)?;
            }
            8 => {
                field.expect(WireType::Struct)?;
                out.data_v2 = Some(data_page_v2(&mut root)?);
            }
            _ => root.skip(field.kind)?,
        }
    }
    // `root` still owns a mutable borrow of `reader` until its scope ends.
    drop(root);
    Ok((out, reader.position()))
}

fn data_page_v1(reader: &mut Reader<'_>, out: &mut PageHeader) -> Result<()> {
    let mut page = reader.structure()?;
    while let Some(field) = page.next_field()? {
        match field.id {
            1 => out.data_v1_num_values = i32_field(&mut page, field)?,
            2 => out.data_v1_encoding = i32_field(&mut page, field)?,
            3 => out.data_v1_definition_encoding = i32_field(&mut page, field)?,
            4 => out.data_v1_repetition_encoding = i32_field(&mut page, field)?,
            _ => page.skip(field.kind)?,
        }
    }
    Ok(())
}

fn dictionary_page(reader: &mut Reader<'_>, out: &mut PageHeader) -> Result<()> {
    let mut page = reader.structure()?;
    while let Some(field) = page.next_field()? {
        match field.id {
            1 => out.dictionary_num_values = i32_field(&mut page, field)?,
            2 => out.dictionary_encoding = i32_field(&mut page, field)?,
            _ => page.skip(field.kind)?,
        }
    }
    Ok(())
}

fn data_page_v2(reader: &mut Reader<'_>) -> Result<DataPageV2> {
    let mut page = reader.structure()?;
    let mut v2 = DataPageV2 {
        is_compressed: true,
        ..DataPageV2::default()
    };
    while let Some(field) = page.next_field()? {
        match field.id {
            1 => v2.num_values = i32_field(&mut page, field)?,
            2 => v2.num_nulls = i32_field(&mut page, field)?,
            3 => v2.num_rows = i32_field(&mut page, field)?,
            4 => v2.encoding = i32_field(&mut page, field)?,
            5 => v2.definition_levels_bytes = i32_field(&mut page, field)?,
            6 => v2.repetition_levels_bytes = i32_field(&mut page, field)?,
            7 => v2.is_compressed = field.bool_value()?,
            _ => page.skip(field.kind)?,
        }
    }
    Ok(v2)
}
