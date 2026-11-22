// Tests build small fixtures with literal casts, compare decoded floats
// exactly, and spell out long scenarios.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::float_cmp,
    clippy::match_wildcard_for_single_variants,
    clippy::too_many_lines
)]

use compact_parquet_thrift::parquet::{footer_range, scan_footer};
use compact_parquet_thrift::{SliceSink, WireType, Writer};

fn inspect(bytes: &[u8], expected_rows: i64, expected_columns: u64) {
    assert_eq!(&bytes[..4], b"PAR1");
    let tail: &[u8; 8] = bytes[bytes.len() - 8..].try_into().unwrap();
    let range = footer_range(bytes.len() as u64, tail).unwrap();
    let metadata = &bytes[range.start as usize..range.end as usize];
    let mut visited = 0;
    let summary = scan_footer(metadata, |column| {
        assert_eq!(column.row_group, 0);
        assert_eq!(u64::from(column.index), visited);
        assert!(column.compressed_size.unwrap() >= 0);
        assert!(column.data_page_offset.unwrap() >= 4);
        assert!(column.codec.is_some());
        visited += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(summary.num_rows, Some(expected_rows));
    assert_eq!(summary.row_groups, 1);
    assert_eq!(summary.columns, expected_columns);
    assert_eq!(visited, expected_columns);
}

#[test]
fn genuine_flat_and_list_parquet_file() {
    inspect(
        include_bytes!("fixtures/list_field_values_file.parquet"),
        5,
        7,
    );
}

#[test]
fn genuine_nested_parquet_file() {
    inspect(
        include_bytes!("fixtures/list_nested_struct_file.parquet"),
        3,
        7,
    );
}

#[test]
fn trailer_bounds_are_validated() {
    assert!(footer_range(4, b"\x00\x00\x00\x00PAR1").is_err());
    assert!(footer_range(30, b"\x00\x00\x00\x00FAIL").is_err());
    assert!(footer_range(16, b"\xff\xff\xff\xffPAR1").is_err());
}

#[test]
fn page_header_exposes_v1_and_dictionary_encodings() {
    use compact_parquet_thrift::{SliceSink, Writer};
    let mut output = [0u8; 128];
    let mut w = Writer::new(SliceSink::new(&mut output));
    w.structure(|w| {
        w.field_i32(1, 0)?; // DATA_PAGE
        w.field_i32(2, 64)?;
        w.field_i32(3, 44)?;
        w.field_struct(5, |w| {
            w.field_i32(1, 15)?;
            w.field_i32(2, 8)?; // RLE_DICTIONARY
            w.field_i32(3, 3)?; // RLE
            w.field_i32(4, 3)?; // RLE
            Ok(())
        })
    })
    .unwrap();
    let written = w.into_inner();
    let (head, len) = compact_parquet_thrift::parquet::page_header(written.as_written()).unwrap();
    assert_eq!(len, written.as_written().len());
    assert_eq!(head.data_v1_num_values, Some(15));
    assert_eq!(head.data_v1_encoding, Some(8));
    assert_eq!(head.data_v1_definition_encoding, Some(3));
    assert_eq!(head.data_v1_repetition_encoding, Some(3));
}

#[test]
fn scan_footer_emits_512_columns_inline() {
    assert_scanned_row_group(512, false);
}

#[test]
fn scan_footer_emits_more_than_512_columns_with_row_group_rows() {
    assert_scanned_row_group(600, false);
}

#[test]
fn scan_footer_attaches_num_rows_written_before_columns() {
    assert_scanned_row_group(3, true);
}

#[test]
fn scan_footer_wide_row_group_with_num_rows_first() {
    assert_scanned_row_group(600, true);
}

fn assert_scanned_row_group(columns: u32, rows_before_columns: bool) {
    let metadata = write_row_group_metadata(columns, rows_before_columns);
    let mut visited = 0u64;
    let summary = scan_footer(&metadata, |column| {
        assert_eq!(column.row_group, 0);
        assert_eq!(u64::from(column.index), visited);
        assert_eq!(column.row_group_rows, Some(10));
        assert_eq!(column.data_page_offset, Some(4));
        visited += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(summary.num_rows, Some(10));
    assert_eq!(summary.row_groups, 1);
    assert_eq!(summary.columns, u64::from(columns));
    assert_eq!(visited, u64::from(columns));
}

fn write_row_group_metadata(columns: u32, rows_before_columns: bool) -> Vec<u8> {
    let mut output = vec![0u8; 128 * 1024];
    let written = {
        let mut w = Writer::new(SliceSink::new(&mut output));
        w.structure(|w| {
            w.field_i32(1, 1)?;
            w.field_i64(3, 10)?;
            w.field_header(4, WireType::List)?;
            w.list_header(WireType::Struct, 1)?;
            w.structure(|w| {
                if rows_before_columns {
                    w.field_i64(3, 10)?;
                }
                w.field_header(1, WireType::List)?;
                w.list_header(WireType::Struct, columns)?;
                for _ in 0..columns {
                    w.structure(|w| {
                        w.field_i64(2, 4)?;
                        w.field_struct(3, |w| {
                            w.field_i32(1, 1)?;
                            w.field_i32(4, 0)?;
                            w.field_i64(5, 10)?;
                            w.field_i64(6, 40)?;
                            w.field_i64(7, 40)?;
                            w.field_i64(9, 4)
                        })
                    })?;
                }
                if !rows_before_columns {
                    w.field_i64(3, 10)?;
                }
                Ok(())
            })
        })
        .unwrap();
        w.into_inner().len()
    };
    output.truncate(written);
    output
}

#[test]
fn dictionary_page_header_exposes_physical_value_encoding() {
    use compact_parquet_thrift::{SliceSink, Writer};
    let mut output = [0u8; 64];
    let mut w = Writer::new(SliceSink::new(&mut output));
    w.structure(|w| {
        w.field_i32(1, 2)?; // DICTIONARY_PAGE
        w.field_i32(2, 24)?;
        w.field_i32(3, 24)?;
        w.field_struct(7, |w| {
            w.field_i32(1, 3)?;
            w.field_i32(2, 0) // PLAIN
        })
    })
    .unwrap();
    let written = w.into_inner();
    let (head, _) = compact_parquet_thrift::parquet::page_header(written.as_written()).unwrap();
    assert_eq!(head.dictionary_num_values, Some(3));
    assert_eq!(head.dictionary_encoding, Some(0));
}
