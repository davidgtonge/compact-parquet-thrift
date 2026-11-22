//! One V1 data page or PLAIN dictionary page, uncompressed or Snappy.
//!
//! The uncompressed body is assembled in `scratch`. Compressed bytes are
//! written at the front of `out` and shifted right once the Thrift header
//! length is known.

use super::delta::{
    encode_delta_bytes, encode_delta_i32, encode_delta_i64, encode_delta_length_bytes,
};
use super::plain::{
    encode_bool, encode_bytes, encode_f32, encode_f64, encode_fixed, encode_i32, encode_i64,
};
use super::rle::{encode_definition_levels, encode_dictionary_ids};
use super::snappy::compress;
use super::{thrift_error, Column, ColumnValues, EncodeError, Result, SNAPPY, UNCOMPRESSED};
use crate::{SliceSink, Writer};

const HEADER_ROOM: usize = 96;

/// `PageType` values written here.
const DATA_PAGE: i32 = 0;
const DICTIONARY_PAGE: i32 = 2;

pub(crate) struct PageStat {
    pub on_disk: usize,
    /// On-disk header plus the uncompressed page body.
    pub uncompressed_with_header: usize,
    /// Present when a dictionary page was written in front of the data page.
    pub dictionary_on_disk: Option<usize>,
}

/// Write a column chunk: an optional dictionary page, then one data page.
pub(crate) fn write_column_pages(
    out: &mut [u8],
    column: &Column<'_>,
    scratch: &mut [u8],
    table: &mut [u32],
) -> Result<PageStat> {
    let dictionary = column
        .values
        .dictionary()
        .map(|(entries, _)| write_dictionary_page(out, column.codec, &entries, scratch, table))
        .transpose()?;
    let at = dictionary.as_ref().map_or(0, |page| page.on_disk);
    let data = write_data_page(&mut out[at..], column, scratch, table)?;
    Ok(PageStat {
        on_disk: at + data.on_disk,
        uncompressed_with_header: dictionary
            .as_ref()
            .map_or(0, |page| page.uncompressed_with_header)
            + data.uncompressed_with_header,
        dictionary_on_disk: dictionary.map(|page| page.on_disk),
    })
}

fn write_dictionary_page(
    out: &mut [u8],
    codec: i32,
    entries: &ColumnValues<'_>,
    scratch: &mut [u8],
    table: &mut [u32],
) -> Result<PageStat> {
    let n = encode_direct(entries, scratch)?;
    let count = i32::try_from(entries.len()).map_err(|_| EncodeError::Overflow)?;
    frame_page(out, DICTIONARY_PAGE, count, 0, &scratch[..n], codec, table)
}

fn write_data_page(
    out: &mut [u8],
    column: &Column<'_>,
    scratch: &mut [u8],
    table: &mut [u32],
) -> Result<PageStat> {
    let reserve = match column.present {
        Some(_) => definition_reserve(column.rows)?,
        None => 0,
    };
    if scratch.len() < reserve {
        return Err(EncodeError::OutputFull);
    }
    let value_len = encode_values(&column.values, &mut scratch[reserve..])?;
    let body_len = if let Some(bits) = column.present {
        let level_len = encode_definition_levels(column.rows, bits, &mut scratch[..reserve])?;
        scratch.copy_within(reserve..reserve + value_len, level_len);
        level_len + value_len
    } else {
        value_len
    };
    let count = i32::try_from(column.rows).map_err(|_| EncodeError::Overflow)?;
    frame_page(
        out,
        DATA_PAGE,
        count,
        column.values.encoding(),
        &scratch[..body_len],
        column.codec,
        table,
    )
}

/// The data page's values section.
fn encode_values(values: &ColumnValues<'_>, out: &mut [u8]) -> Result<usize> {
    match values.dictionary() {
        Some((entries, ids)) => {
            let len = u32::try_from(entries.len()).map_err(|_| EncodeError::Overflow)?;
            encode_dictionary_ids(len, ids, out)
        }
        None => encode_direct(values, out),
    }
}

/// Values that are not dictionary ids: PLAIN or delta.
fn encode_direct(values: &ColumnValues<'_>, out: &mut [u8]) -> Result<usize> {
    match values {
        ColumnValues::Bool(v) => encode_bool(v, out),
        ColumnValues::I32(v) | ColumnValues::Date(v) => encode_i32(v, out),
        ColumnValues::I64(v) => encode_i64(v, out),
        ColumnValues::F32(v) => encode_f32(v, out),
        ColumnValues::F64(v) => encode_f64(v, out),
        ColumnValues::Bytes(v) => encode_bytes(v, out),
        ColumnValues::Fixed { width, values } => encode_fixed(*width, values, out),
        ColumnValues::DeltaI32(v) => encode_delta_i32(v, out),
        ColumnValues::DeltaI64(v) => encode_delta_i64(v, out),
        ColumnValues::DeltaLengthBytes(v) => encode_delta_length_bytes(v, out),
        ColumnValues::DeltaBytes(v) => encode_delta_bytes(v, out),
        ColumnValues::DictI32 { .. }
        | ColumnValues::DictI64 { .. }
        | ColumnValues::DictF32 { .. }
        | ColumnValues::DictF64 { .. }
        | ColumnValues::DictBytes { .. }
        | ColumnValues::DictFixed { .. } => Err(EncodeError::ValueCount),
    }
}

fn definition_reserve(rows: usize) -> Result<usize> {
    // A width-1 RLE value is a short header plus one payload byte.
    rows.checked_mul(2)
        .and_then(|n| n.checked_add(16))
        .ok_or(EncodeError::Overflow)
}

fn frame_page(
    out: &mut [u8],
    page_type: i32,
    num_values: i32,
    value_encoding: i32,
    body: &[u8],
    codec: i32,
    table: &mut [u32],
) -> Result<PageStat> {
    let compressed_len = match codec {
        UNCOMPRESSED => {
            out.get_mut(..body.len())
                .ok_or(EncodeError::OutputFull)?
                .copy_from_slice(body);
            body.len()
        }
        SNAPPY => compress(body, out, table)?,
        _ => return Err(EncodeError::UnsupportedCodec),
    };
    let mut header = [0u8; HEADER_ROOM];
    let header_len = page_header(
        &mut header,
        page_type,
        num_values,
        value_encoding,
        body.len(),
        compressed_len,
    )?;
    let total = header_len
        .checked_add(compressed_len)
        .ok_or(EncodeError::Overflow)?;
    if out.len() < total {
        return Err(EncodeError::OutputFull);
    }
    out.copy_within(0..compressed_len, header_len);
    out[..header_len].copy_from_slice(&header[..header_len]);
    Ok(PageStat {
        on_disk: total,
        uncompressed_with_header: header_len + body.len(),
        dictionary_on_disk: None,
    })
}

fn page_header(
    bytes: &mut [u8],
    page_type: i32,
    num_values: i32,
    value_encoding: i32,
    uncompressed: usize,
    compressed: usize,
) -> Result<usize> {
    let uncompressed = i32::try_from(uncompressed).map_err(|_| EncodeError::Overflow)?;
    let compressed = i32::try_from(compressed).map_err(|_| EncodeError::Overflow)?;
    let mut writer = Writer::new(SliceSink::new(bytes));
    writer
        .structure(|w| {
            w.field_i32(1, page_type)?;
            w.field_i32(2, uncompressed)?;
            w.field_i32(3, compressed)?;
            if page_type == DICTIONARY_PAGE {
                w.field_struct(7, |w| {
                    w.field_i32(1, num_values)?;
                    w.field_i32(2, 0) // PLAIN
                })
            } else {
                w.field_struct(5, |w| {
                    w.field_i32(1, num_values)?;
                    w.field_i32(2, value_encoding)?;
                    w.field_i32(3, 3)?; // definition levels: RLE
                    w.field_i32(4, 3) // repetition levels: RLE
                })
            }
        })
        .map_err(thrift_error)?;
    Ok(writer.into_inner().len())
}
