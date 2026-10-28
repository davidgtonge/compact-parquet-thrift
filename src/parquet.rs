//! Minimal zero-allocation Parquet metadata inspection.
//!
//! These are *visitors*, not a complete generated model of parquet.thrift.
//! Unknown fields are skipped, including nested structs and collections.

use core::ops::Range;

use crate::{Error, Reader, Result, WireType};

/// Locate the serialized Thrift FileMetaData using the final eight bytes.
/// The first four bytes of the object should independently be checked for PAR1.
pub fn footer_range(file_size: u64, tail: &[u8; 8]) -> Result<Range<u64>> {
    if &tail[4..8] != b"PAR1" { return Err(Error::InvalidMagic); }
    let len = u32::from_le_bytes(tail[..4].try_into().map_err(|_| Error::InvalidFooterLength)?) as u64;
    let end = file_size.checked_sub(8).ok_or(Error::InvalidFooterLength)?;
    let start = end.checked_sub(len).ok_or(Error::InvalidFooterLength)?;
    if start < 4 { return Err(Error::InvalidFooterLength); }
    Ok(start..end)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FooterSummary {
    pub version: Option<i32>,
    pub num_rows: Option<i64>,
    pub row_groups: u32,
    pub columns: u64,
}

/// Column offsets from FileMetaData -> RowGroup -> ColumnChunk -> ColumnMetaData.
///
/// Offsets and sizes are signed in the Thrift IDL; validate nonnegative values
/// before converting them to object-store range requests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ColumnChunkInfo<'a> {
    pub row_group: u32,
    pub index: u32,
    pub external_file: Option<&'a [u8]>,
    /// Only populated when path_in_schema has exactly one component.
    pub flat_name: Option<&'a [u8]>,
    pub physical_type: Option<i32>,
    pub codec: Option<i32>,
    pub num_values: Option<i64>,
    pub file_offset: Option<i64>,
    pub data_page_offset: Option<i64>,
    pub dictionary_page_offset: Option<i64>,
    pub compressed_size: Option<i64>,
    pub uncompressed_size: Option<i64>,
    pub offset_index_offset: Option<i64>,
    pub column_index_offset: Option<i64>,
    pub bloom_filter_offset: Option<i64>,
    /// `RowGroup.num_rows` for the group that owns this column. Not inferred from `num_values`.
    pub row_group_rows: Option<i64>,
}

/// Visit one `ColumnChunkInfo` at a time instead of collecting a Vec.
/// Schema parsing and full compatibility validation belong to an upper layer.
pub fn scan_footer<'a>(
    metadata: &'a [u8],
    mut visit: impl FnMut(ColumnChunkInfo<'a>) -> Result<()>,
) -> Result<FooterSummary> {
    let mut reader = Reader::new(metadata);
    let mut root = reader.structure()?;
    let mut summary = FooterSummary::default();
    while let Some(field) = root.next_field()? {
        match field.id {
            1 => { field.expect(WireType::I32)?; summary.version = Some(root.i32()?); }
            3 => { field.expect(WireType::I64)?; summary.num_rows = Some(root.i64()?); }
            4 => {
                field.expect(WireType::List)?;
                let list = root.list_header()?;
                if list.element != WireType::Struct { return Err(Error::UnexpectedWireType); }
                summary.row_groups = list.len;
                for rg in 0..list.len {
                    summary.columns = summary.columns.checked_add(
                        visit_row_group(&mut root, rg, &mut visit)? as u64
                    ).ok_or(Error::IntegerOverflow)?;
                }
            }
            _ => root.skip(field.kind)?,
        }
    }
    Ok(summary)
}

fn visit_row_group<'a>(
    reader: &mut Reader<'a>,
    row_group: u32,
    visit: &mut impl FnMut(ColumnChunkInfo<'a>) -> Result<()>,
) -> Result<u32> {
    let mut group = reader.structure()?;
    let mut count = 0;
    let mut rows = None;
    let mut stored = 0usize;
    let mut pending = [ColumnChunkInfo {
        row_group: 0,
        index: 0,
        ..Default::default()
    }; 512];
    while let Some(field) = group.next_field()? {
        match field.id {
            1 => {
                field.expect(WireType::List)?;
                let header = group.list_header()?;
                if header.element != WireType::Struct { return Err(Error::UnexpectedWireType); }
                count = header.len;
                for index in 0..header.len {
                    if stored >= pending.len() { return Err(Error::IntegerOverflow); }
                    let mut info = ColumnChunkInfo { row_group, index, ..Default::default() };
                    visit_column_chunk(&mut group, &mut info)?;
                    pending[stored] = info;
                    stored += 1;
                }
            }
            3 => {
                field.expect(WireType::I64)?;
                rows = Some(group.i64()?);
            }
            _ => group.skip(field.kind)?,
        }
    }
    for index in 0..stored {
        pending[index].row_group_rows = rows;
        visit(pending[index])?;
    }
    Ok(count)
}

fn visit_column_chunk<'a>(reader: &mut Reader<'a>, info: &mut ColumnChunkInfo<'a>) -> Result<()> {
    let mut chunk = reader.structure()?;
    while let Some(field) = chunk.next_field()? {
        match field.id {
            1 => { field.expect(WireType::Binary)?; info.external_file = Some(chunk.binary()?); }
            2 => { field.expect(WireType::I64)?; info.file_offset = Some(chunk.i64()?); }
            3 => { field.expect(WireType::Struct)?; visit_column_metadata(&mut chunk, info)?; }
            4 => { field.expect(WireType::I64)?; info.offset_index_offset = Some(chunk.i64()?); }
            6 => { field.expect(WireType::I64)?; info.column_index_offset = Some(chunk.i64()?); }
            _ => chunk.skip(field.kind)?,
        }
    }
    Ok(())
}

fn visit_column_metadata<'a>(reader: &mut Reader<'a>, info: &mut ColumnChunkInfo<'a>) -> Result<()> {
    let mut meta = reader.structure()?;
    while let Some(field) = meta.next_field()? {
        match field.id {
            1 => { field.expect(WireType::I32)?; info.physical_type = Some(meta.i32()?); }
            3 => {
                field.expect(WireType::List)?;
                let header = meta.list_header()?;
                if header.element != WireType::Binary { return Err(Error::UnexpectedWireType); }
                if header.len == 1 { info.flat_name = Some(meta.binary()?); }
                else {
                    for _ in 0..header.len { meta.binary()?; }
                }
            }
            4 => { field.expect(WireType::I32)?; info.codec = Some(meta.i32()?); }
            5 => { field.expect(WireType::I64)?; info.num_values = Some(meta.i64()?); }
            6 => { field.expect(WireType::I64)?; info.uncompressed_size = Some(meta.i64()?); }
            7 => { field.expect(WireType::I64)?; info.compressed_size = Some(meta.i64()?); }
            9 => { field.expect(WireType::I64)?; info.data_page_offset = Some(meta.i64()?); }
            11 => { field.expect(WireType::I64)?; info.dictionary_page_offset = Some(meta.i64()?); }
            14 => { field.expect(WireType::I64)?; info.bloom_filter_offset = Some(meta.i64()?); }
            _ => meta.skip(field.kind)?,
        }
    }
    Ok(())
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DataPageV2 {
    pub num_values: Option<i32>,
    pub num_nulls: Option<i32>,
    pub num_rows: Option<i32>,
    pub encoding: Option<i32>,
    pub definition_levels_bytes: Option<i32>,
    pub repetition_levels_bytes: Option<i32>,
    /// Defaults to true in the Parquet IDL when absent.
    pub is_compressed: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    pub page_type: Option<i32>,
    pub uncompressed_size: Option<i32>,
    pub compressed_size: Option<i32>,
    pub crc: Option<i32>,
    pub data_v1_num_values: Option<i32>,
    pub data_v1_encoding: Option<i32>,
    pub data_v1_definition_encoding: Option<i32>,
    pub data_v1_repetition_encoding: Option<i32>,
    pub dictionary_num_values: Option<i32>,
    pub dictionary_encoding: Option<i32>,
    pub data_v2: Option<DataPageV2>,
}

/// Parse one Thrift PageHeader from a prefix. Returns its exact byte length;
/// bytes after that length belong to the page payload.
/// `EndOfInput` means more bytes are needed for the complete header.
pub fn page_header(prefix: &[u8]) -> Result<(PageHeader, usize)> {
    let mut reader = Reader::new(prefix);
    let mut root = reader.structure()?;
    let mut out = PageHeader::default();
    while let Some(field) = root.next_field()? {
        match field.id {
            1 => { field.expect(WireType::I32)?; out.page_type = Some(root.i32()?); }
            2 => { field.expect(WireType::I32)?; out.uncompressed_size = Some(root.i32()?); }
            3 => { field.expect(WireType::I32)?; out.compressed_size = Some(root.i32()?); }
            4 => { field.expect(WireType::I32)?; out.crc = Some(root.i32()?); }
            5 => {
                field.expect(WireType::Struct)?;
                let mut sub = root.structure()?;
                while let Some(f) = sub.next_field()? {
                    match f.id {
                        1 => { f.expect(WireType::I32)?; out.data_v1_num_values = Some(sub.i32()?); }
                        2 => { f.expect(WireType::I32)?; out.data_v1_encoding = Some(sub.i32()?); }
                        3 => { f.expect(WireType::I32)?; out.data_v1_definition_encoding = Some(sub.i32()?); }
                        4 => { f.expect(WireType::I32)?; out.data_v1_repetition_encoding = Some(sub.i32()?); }
                        _ => sub.skip(f.kind)?,
                    }
                }
            }
            7 => {
                field.expect(WireType::Struct)?;
                let mut sub = root.structure()?;
                while let Some(f) = sub.next_field()? {
                    match f.id {
                        1 => { f.expect(WireType::I32)?; out.dictionary_num_values = Some(sub.i32()?); }
                        2 => { f.expect(WireType::I32)?; out.dictionary_encoding = Some(sub.i32()?); }
                        _ => sub.skip(f.kind)?,
                    }
                }
            }
            8 => {
                field.expect(WireType::Struct)?;
                let mut sub = root.structure()?;
                let mut v2 = DataPageV2 { is_compressed: true, ..Default::default() };
                while let Some(f) = sub.next_field()? {
                    match f.id {
                        1 => { f.expect(WireType::I32)?; v2.num_values = Some(sub.i32()?); }
                        2 => { f.expect(WireType::I32)?; v2.num_nulls = Some(sub.i32()?); }
                        3 => { f.expect(WireType::I32)?; v2.num_rows = Some(sub.i32()?); }
                        4 => { f.expect(WireType::I32)?; v2.encoding = Some(sub.i32()?); }
                        5 => { f.expect(WireType::I32)?; v2.definition_levels_bytes = Some(sub.i32()?); }
                        6 => { f.expect(WireType::I32)?; v2.repetition_levels_bytes = Some(sub.i32()?); }
                        7 => { v2.is_compressed = f.bool_value()?; }
                        _ => sub.skip(f.kind)?,
                    }
                }
                out.data_v2 = Some(v2);
            }
            _ => root.skip(field.kind)?,
        }
    }
    // `root` still owns a mutable borrow of `reader` until its scope ends.
    drop(root);
    Ok((out, reader.position()))
}
