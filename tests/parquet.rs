use compact_parquet_thrift::parquet::{footer_range, scan_footer};

fn inspect(bytes: &[u8], expected_rows: i64, expected_columns: u64) {
    assert_eq!(&bytes[..4], b"PAR1");
    let tail: &[u8; 8] = bytes[bytes.len() - 8..].try_into().unwrap();
    let range = footer_range(bytes.len() as u64, tail).unwrap();
    let metadata = &bytes[range.start as usize..range.end as usize];
    let mut visited = 0;
    let summary = scan_footer(metadata, |column| {
        assert_eq!(column.row_group, 0);
        assert_eq!(column.index as u64, visited);
        assert!(column.compressed_size.unwrap() >= 0);
        assert!(column.data_page_offset.unwrap() >= 4);
        assert!(column.codec.is_some());
        visited += 1;
        Ok(())
    }).unwrap();
    assert_eq!(summary.num_rows, Some(expected_rows));
    assert_eq!(summary.row_groups, 1);
    assert_eq!(summary.columns, expected_columns);
    assert_eq!(visited, expected_columns);
}

#[test]
fn genuine_flat_and_list_parquet_file() {
    inspect(include_bytes!("fixtures/list_field_values_file.parquet"), 5, 7);
}

#[test]
fn genuine_nested_parquet_file() {
    inspect(include_bytes!("fixtures/list_nested_struct_file.parquet"), 3, 7);
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
    }).unwrap();
    let written = w.into_inner();
    let (head, len) = compact_parquet_thrift::parquet::page_header(written.as_written()).unwrap();
    assert_eq!(len, written.as_written().len());
    assert_eq!(head.data_v1_num_values, Some(15));
    assert_eq!(head.data_v1_encoding, Some(8));
    assert_eq!(head.data_v1_definition_encoding, Some(3));
    assert_eq!(head.data_v1_repetition_encoding, Some(3));
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
    }).unwrap();
    let written = w.into_inner();
    let (head, _) = compact_parquet_thrift::parquet::page_header(written.as_written()).unwrap();
    assert_eq!(head.dictionary_num_values, Some(3));
    assert_eq!(head.dictionary_encoding, Some(0));
}
