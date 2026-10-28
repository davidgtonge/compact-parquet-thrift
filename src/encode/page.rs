//! One V1 data page or PLAIN dictionary page, uncompressed or Snappy.
//!
//! The uncompressed body is assembled in `scratch`. Compressed bytes are
//! written at the front of `out` and shifted right once the Thrift header
//! length is known.

use super::delta::{encode_delta_bytes, encode_delta_i32, encode_delta_i64, encode_delta_length_bytes};
use super::plain::{encode_bool, encode_bytes, encode_f32, encode_f64, encode_i32, encode_i64};
use super::rle::{encode_definition_levels, encode_dictionary_ids};
use super::snappy::compress;
use super::{thrift_error, value_encoding, ColumnValues, EncodeError, Result};
use crate::{SliceSink, Writer};

const HEADER_ROOM: usize = 96;

pub struct PageStat {
    pub on_disk: usize,
    /// On-disk header plus the uncompressed page body.
    pub uncompressed_with_header: usize,
    /// Present when a dictionary page was written in front of the data page.
    pub dictionary_on_disk: Option<usize>,
}

pub fn write_column_pages(
    out: &mut [u8],
    rows: usize,
    codec: i32,
    present: Option<&[u8]>,
    values: &ColumnValues<'_>,
    scratch: &mut [u8],
    table: &mut [u32],
) -> Result<PageStat> {
    let mut at = 0;
    let mut uncompressed = 0usize;
    let dictionary_on_disk = match values {
        ColumnValues::DictI32 { dictionary, .. } => {
            let stat = write_dictionary_page(
                &mut out[at..], codec, &ColumnValues::I32(dictionary), scratch, table,
            )?;
            at += stat.on_disk;
            uncompressed += stat.uncompressed_with_header;
            Some(stat.on_disk)
        }
        ColumnValues::DictBytes { dictionary, .. } => {
            let stat = write_dictionary_page(
                &mut out[at..], codec, &ColumnValues::Bytes(dictionary), scratch, table,
            )?;
            at += stat.on_disk;
            uncompressed += stat.uncompressed_with_header;
            Some(stat.on_disk)
        }
        _ => None,
    };
    let encoding = value_encoding(values);
    let data = write_data_page(
        &mut out[at..], rows, encoding, codec, present, values, scratch, table,
    )?;
    Ok(PageStat {
        on_disk: at + data.on_disk,
        uncompressed_with_header: uncompressed + data.uncompressed_with_header,
        dictionary_on_disk,
    })
}

fn write_dictionary_page(
    out: &mut [u8],
    codec: i32,
    values: &ColumnValues<'_>,
    scratch: &mut [u8],
    table: &mut [u32],
) -> Result<PageStat> {
    let n = encode_plain(values, scratch)?;
    let count = i32::try_from(plain_len(values)).map_err(|_| EncodeError::Overflow)?;
    let stat = frame_page(out, 2, count, 0, &scratch[..n], codec, table)?;
    Ok(PageStat { dictionary_on_disk: None, ..stat })
}

fn write_data_page(
    out: &mut [u8],
    rows: usize,
    encoding: i32,
    codec: i32,
    present: Option<&[u8]>,
    values: &ColumnValues<'_>,
    scratch: &mut [u8],
    table: &mut [u32],
) -> Result<PageStat> {
    let reserve = if present.is_some() { definition_reserve(rows)? } else { 0 };
    if scratch.len() < reserve { return Err(EncodeError::OutputFull); }
    let value_len = encode_data_values(values, &mut scratch[reserve..])?;
    let body_len = if let Some(bits) = present {
        let level_len = encode_definition_levels(rows, bits, &mut scratch[..reserve])?;
        scratch.copy_within(reserve..reserve + value_len, level_len);
        level_len + value_len
    } else {
        value_len
    };
    let count = i32::try_from(rows).map_err(|_| EncodeError::Overflow)?;
    frame_page(out, 0, count, encoding, &scratch[..body_len], codec, table)
}

fn encode_data_values(values: &ColumnValues<'_>, out: &mut [u8]) -> Result<usize> {
    match values {
        ColumnValues::Bool(v) => encode_bool(v, out),
        ColumnValues::I32(v) => encode_i32(v, out),
        ColumnValues::I64(v) => encode_i64(v, out),
        ColumnValues::F32(v) => encode_f32(v, out),
        ColumnValues::F64(v) => encode_f64(v, out),
        ColumnValues::Bytes(v) => encode_bytes(v, out),
        ColumnValues::DictI32 { dictionary, ids } => encode_dictionary_ids(dict_len(dictionary.len())?, ids, out),
        ColumnValues::DictBytes { dictionary, ids } => encode_dictionary_ids(dict_len(dictionary.len())?, ids, out),
        ColumnValues::DeltaI32(v) => encode_delta_i32(v, out),
        ColumnValues::DeltaI64(v) => encode_delta_i64(v, out),
        ColumnValues::DeltaLengthBytes(v) => encode_delta_length_bytes(v, out),
        ColumnValues::DeltaBytes(v) => encode_delta_bytes(v, out),
    }
}

fn encode_plain(values: &ColumnValues<'_>, out: &mut [u8]) -> Result<usize> {
    match values {
        ColumnValues::DictI32 { .. } | ColumnValues::DictBytes { .. } => Err(EncodeError::ValueCount),
        other => encode_data_values(other, out),
    }
}

fn plain_len(values: &ColumnValues<'_>) -> usize {
    match values {
        ColumnValues::Bool(v) => v.len(),
        ColumnValues::I32(v) => v.len(),
        ColumnValues::I64(v) => v.len(),
        ColumnValues::F32(v) => v.len(),
        ColumnValues::F64(v) => v.len(),
        ColumnValues::Bytes(v) | ColumnValues::DeltaLengthBytes(v) | ColumnValues::DeltaBytes(v) => v.len(),
        ColumnValues::DictI32 { .. } | ColumnValues::DictBytes { .. } => 0,
        ColumnValues::DeltaI32(v) => v.len(),
        ColumnValues::DeltaI64(v) => v.len(),
    }
}

fn dict_len(len: usize) -> Result<u32> {
    u32::try_from(len).map_err(|_| EncodeError::Overflow)
}

fn definition_reserve(rows: usize) -> Result<usize> {
    // A width-1 RLE value is a short header plus one payload byte.
    rows.checked_mul(2).and_then(|n| n.checked_add(16)).ok_or(EncodeError::Overflow)
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
        0 => {
            if out.len() < body.len() { return Err(EncodeError::OutputFull); }
            out[..body.len()].copy_from_slice(body);
            body.len()
        }
        1 => compress(body, out, table)?,
        _ => return Err(EncodeError::UnsupportedCodec),
    };
    let mut header = [0u8; HEADER_ROOM];
    let header_len = page_header(&mut header, page_type, num_values, value_encoding, body.len(), compressed_len)?;
    let total = header_len.checked_add(compressed_len).ok_or(EncodeError::Overflow)?;
    if out.len() < total { return Err(EncodeError::OutputFull); }
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
    writer.structure(|w| {
        w.field_i32(1, page_type)?;
        w.field_i32(2, uncompressed)?;
        w.field_i32(3, compressed)?;
        if page_type == 2 {
            w.field_struct(7, |w| {
                w.field_i32(1, num_values)?;
                w.field_i32(2, 0)
            })?;
        } else {
            w.field_struct(5, |w| {
                w.field_i32(1, num_values)?;
                w.field_i32(2, value_encoding)?;
                w.field_i32(3, 3)?;
                w.field_i32(4, 3)
            })?;
        }
        Ok(())
    }).map_err(thrift_error)?;
    Ok(writer.into_inner().len())
}
