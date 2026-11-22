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

use compact_parquet_thrift::pages::{
    ColumnSpec, DecodeWorkspace, Error, FrameEvent, Identity, Inflate, Location, PageAssembler,
    PageEvent, PageFramer, Physical, RawSnappy, Value,
};
use compact_parquet_thrift::values::DeltaByteParts;
use compact_parquet_thrift::{
    parquet::{page_header, PageHeader},
    SliceSink, Writer,
};

fn header_v1(
    count: i32,
    encoding: i32,
    compressed: usize,
    uncompressed: usize,
    crc: Option<u32>,
) -> Vec<u8> {
    let mut bytes = [0u8; 256];
    let mut writer = Writer::new(SliceSink::new(&mut bytes));
    writer
        .structure(|w| {
            w.field_i32(1, 0)?;
            w.field_i32(2, uncompressed as i32)?;
            w.field_i32(3, compressed as i32)?;
            if let Some(crc) = crc {
                w.field_i32(4, crc as i32)?;
            }
            w.field_struct(5, |w| {
                w.field_i32(1, count)?;
                w.field_i32(2, encoding)?;
                w.field_i32(3, 3)?;
                w.field_i32(4, 3)
            })
        })
        .unwrap();
    writer.into_inner().as_written().to_vec()
}

fn header_v2(
    count: i32,
    nulls: i32,
    encoding: i32,
    compressed: usize,
    uncompressed: usize,
    definition_length: usize,
    is_compressed: bool,
) -> Vec<u8> {
    let mut bytes = [0u8; 256];
    let mut writer = Writer::new(SliceSink::new(&mut bytes));
    writer
        .structure(|w| {
            w.field_i32(1, 3)?;
            w.field_i32(2, uncompressed as i32)?;
            w.field_i32(3, compressed as i32)?;
            w.field_struct(8, |w| {
                w.field_i32(1, count)?;
                w.field_i32(2, nulls)?;
                w.field_i32(3, count)?;
                w.field_i32(4, encoding)?;
                w.field_i32(5, definition_length as i32)?;
                w.field_i32(6, 0)?;
                w.field_bool(7, is_compressed)
            })
        })
        .unwrap();
    writer.into_inner().as_written().to_vec()
}

fn header_dictionary(count: i32, size: usize) -> Vec<u8> {
    let mut bytes = [0u8; 256];
    let mut writer = Writer::new(SliceSink::new(&mut bytes));
    writer
        .structure(|w| {
            w.field_i32(1, 2)?;
            w.field_i32(2, size as i32)?;
            w.field_i32(3, size as i32)?;
            w.field_struct(7, |w| {
                w.field_i32(1, count)?;
                w.field_i32(2, 0)
            })
        })
        .unwrap();
    writer.into_inner().as_written().to_vec()
}

fn header(bytes: &[u8]) -> PageHeader {
    let (value, used) = page_header(bytes).unwrap();
    assert_eq!(used, bytes.len());
    value
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in bytes {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (if crc & 1 == 1 { 0xedb8_8320 } else { 0 });
        }
    }
    !crc
}

fn workspace<'a>(
    present: &'a mut [u8],
    widths: &'a mut [u8],
    lengths: &'a mut [u32],
    parts: &'a mut [DeltaByteParts],
    previous: &'a mut [u8],
) -> DecodeWorkspace<'a> {
    DecodeWorkspace {
        present,
        widths,
        lengths,
        parts,
        previous,
    }
}

fn decode_i32(h: PageHeader, data: &[u8], spec: ColumnSpec, fragmented: usize) -> Vec<Option<i32>> {
    let mut present = [0u8; 64];
    let mut widths = [0u8; 4];
    let mut lengths = [0u32; 64];
    let mut parts = [DeltaByteParts::default(); 64];
    let mut previous = [0u8; 64];
    let mut codec = Identity::default();
    let mut page = PageAssembler::new(
        h,
        spec,
        workspace(
            &mut present,
            &mut widths,
            &mut lengths,
            &mut parts,
            &mut previous,
        ),
        &mut codec,
    )
    .unwrap();
    let mut result = Vec::new();
    let mut emit = |ev: PageEvent<'_>| match ev {
        PageEvent::Null(row) => {
            assert_eq!(row as usize, result.len());
            result.push(None);
        }
        PageEvent::Value(Location::Row(row), Value::I32(v)) => {
            assert_eq!(row as usize, result.len());
            result.push(Some(v));
        }
        _ => panic!("unexpected event: {ev:?}"),
    };
    for chunk in data.chunks(fragmented) {
        assert_eq!(page.feed(chunk, &mut emit).unwrap(), chunk.len());
    }
    page.finish(&mut emit).unwrap();
    result
}

#[test]
fn trailing_bytes_after_the_value_count_are_padding() {
    let mut payload = 7i32.to_le_bytes().to_vec();
    payload.extend_from_slice(&[0xff, 0x41, 0x00]);
    let h = header(&header_v1(1, 0, payload.len(), payload.len(), None));
    for fragment in 1..=payload.len() {
        assert_eq!(
            decode_i32(h, &payload, ColumnSpec::flat(Physical::Int32, 0), fragment),
            vec![Some(7)]
        );
    }
}

#[test]
fn parses_headers_and_payload_boundaries_for_arbitrary_chunks() {
    let h1 = header_v1(1, 0, 4, 4, None);
    let h2 = header_v1(2, 0, 8, 8, None);
    let mut joined = Vec::new();
    joined.extend(&h1);
    joined.extend([7, 0, 0, 0]);
    joined.extend(&h2);
    joined.extend([8, 0, 0, 0, 9, 0, 0, 0]);
    for chunk_size in 1..=joined.len() {
        let mut header_buffer = [0u8; 512];
        let mut framer = PageFramer::new(&mut header_buffer, 1024);
        let mut seen = Vec::new();
        for chunk in joined.chunks(chunk_size) {
            assert_eq!(
                framer
                    .feed(chunk, |event| {
                        match event {
                            FrameEvent::Start(h) => {
                                seen.push(format!("start:{}", h.data_v1_num_values.unwrap()));
                            }
                            FrameEvent::Body(b) => {
                                // Fragments may split at arbitrary boundaries.
                                for x in b {
                                    seen.push(format!("byte:{x}"));
                                }
                            }
                            FrameEvent::End => seen.push("end".into()),
                        }
                        Ok(())
                    })
                    .unwrap(),
                chunk.len()
            );
        }
        framer.finish().unwrap();
        let wanted = [
            "start:1", "byte:7", "byte:0", "byte:0", "byte:0", "end", "start:2", "byte:8",
            "byte:0", "byte:0", "byte:0", "byte:9", "byte:0", "byte:0", "byte:0", "end",
        ];
        assert_eq!(seen.iter().map(String::as_str).collect::<Vec<_>>(), wanted);
    }
}

#[test]
fn v1_optional_plain_with_trailing_nulls_and_crc() {
    // Levels [1,0,1,1,0,1,0] are encoded as five RLE runs; each run uses
    // one header byte and one value byte, preceded by the V1 length prefix.
    let levels = [2, 1, 2, 0, 4, 1, 2, 0, 2, 1, 2, 0];
    let mut data = Vec::new();
    data.extend((levels.len() as u32).to_le_bytes());
    data.extend(levels);
    for v in [10i32, 30, 40, 60] {
        data.extend(v.to_le_bytes());
    }
    let h = header(&header_v1(7, 0, data.len(), data.len(), Some(crc32(&data))));
    let spec = ColumnSpec::flat(Physical::Int32, 0).optional();
    for n in 1..=data.len() {
        assert_eq!(
            decode_i32(h, &data, spec, n),
            [Some(10), None, Some(30), Some(40), None, Some(60), None]
        );
    }
}

#[test]
fn v2_optional_levels_are_uncompressed_and_can_cross_chunks() {
    let levels = [2, 1, 2, 0, 2, 1, 4, 0]; // [1,0,1,0,0]
    let mut data = levels.to_vec();
    data.extend(17i32.to_le_bytes());
    data.extend(29i32.to_le_bytes());
    let h = header(&header_v2(
        5,
        3,
        0,
        data.len(),
        data.len(),
        levels.len(),
        false,
    ));
    // The file declares Snappy, but this V2 page explicitly has raw values.
    let spec = ColumnSpec::flat(Physical::Int32, 1).optional();
    for n in 1..=data.len() {
        assert_eq!(
            decode_i32(h, &data, spec, n),
            [Some(17), None, Some(29), None, None]
        );
    }
}

#[test]
fn delta_binary_value_decoder_is_connected_to_v1_pages() {
    let encoded = [0x80, 1, 4, 5, 2, 2, 0, 0, 0, 0];
    let h = header(&header_v1(5, 5, encoded.len(), encoded.len(), None));
    for chunk in 1..=encoded.len() {
        assert_eq!(
            decode_i32(h, &encoded, ColumnSpec::flat(Physical::Int32, 0), chunk),
            [Some(1), Some(2), Some(3), Some(4), Some(5)]
        );
    }
}

#[test]
fn v2_snappy_compressed_value_section_is_incremental() {
    let values = [15i32, 25];
    let mut plain = Vec::new();
    for value in values {
        plain.extend(value.to_le_bytes());
    }
    // Raw Snappy: uncompressed len 8; literal of 8 bytes.
    let mut compressed = vec![8, 28];
    compressed.extend(&plain);
    let h = header(&header_v2(2, 0, 0, compressed.len(), plain.len(), 0, true));
    for chunk_size in 1..=compressed.len() {
        let mut present = [0u8; 1];
        let mut widths = [0u8; 4];
        let mut lengths = [0u32; 1];
        let mut parts = [DeltaByteParts::default(); 1];
        let mut previous = [0u8; 32];
        let mut history = [0u8; 32];
        let mut output = [0u8; 3];
        let mut snappy = RawSnappy::new(&mut history, &mut output).unwrap();
        let mut page = PageAssembler::new(
            h,
            ColumnSpec::flat(Physical::Int32, 1),
            workspace(
                &mut present,
                &mut widths,
                &mut lengths,
                &mut parts,
                &mut previous,
            ),
            &mut snappy,
        )
        .unwrap();
        let mut result = Vec::new();
        let mut emit = |ev: PageEvent<'_>| match ev {
            PageEvent::Value(Location::Row(i), Value::I32(value)) => {
                assert_eq!(i as usize, result.len());
                result.push(value);
            }
            _ => panic!("unexpected event"),
        };
        for part in compressed.chunks(chunk_size) {
            assert_eq!(page.feed(part, &mut emit).unwrap(), part.len());
        }
        page.finish(&mut emit).unwrap();
        assert_eq!(result, values);
    }
}

#[test]
fn raw_snappy_handles_overlapping_copies_and_bounded_history() {
    let encoded = [8, 4, b'a', b'b', 0x16, 2, 0]; // "ab" + copy(offset=2,len=6)
    for n in 1..=encoded.len() {
        let mut history = [0u8; 2];
        let mut output = [0u8; 3];
        let mut codec = RawSnappy::new(&mut history, &mut output).unwrap();
        codec.reset(8).unwrap();
        let mut result: Vec<u8> = Vec::new();
        for chunk in encoded.chunks(n) {
            codec
                .push(chunk, &mut |b| {
                    result.extend(b);
                    Ok(())
                })
                .unwrap();
        }
        codec
            .finish(&mut |b| {
                result.extend(b);
                Ok(())
            })
            .unwrap();
        assert_eq!(result, b"abababab");
    }
    let mut history = [0u8; 1];
    let mut output = [0u8; 3];
    let mut codec = RawSnappy::new(&mut history, &mut output).unwrap();
    codec.reset(8).unwrap();
    assert!(matches!(
        codec.push(&encoded, &mut |_| Ok(())),
        Err(Error::SnappyHistoryTooSmall {
            offset: 2,
            capacity: 1
        })
    ));
}

#[test]
fn dictionary_pages_emit_encoded_values_then_data_pages_emit_ids() {
    let mut dictionary = Vec::new();
    for word in ["UK", "US"] {
        dictionary.extend((word.len() as u32).to_le_bytes());
        dictionary.extend(word.as_bytes());
    }
    let h = header(&header_dictionary(2, dictionary.len()));
    for chunk_size in 1..=dictionary.len() {
        let mut present = [0u8; 1];
        let mut widths = [0u8; 4];
        let mut lengths = [0u32; 2];
        let mut parts = [DeltaByteParts::default(); 2];
        let mut previous = [0u8; 8];
        let mut codec = Identity::default();
        let mut page = PageAssembler::new(
            h,
            ColumnSpec::flat(Physical::ByteArray, 0),
            workspace(
                &mut present,
                &mut widths,
                &mut lengths,
                &mut parts,
                &mut previous,
            ),
            &mut codec,
        )
        .unwrap();
        let mut words = Vec::<Vec<u8>>::new();
        let mut emit = |e: PageEvent<'_>| match e {
            PageEvent::Value(Location::Dictionary(i), Value::BytesStart(_)) => {
                assert_eq!(i as usize, words.len());
                words.push(Vec::new());
            }
            PageEvent::Value(Location::Dictionary(_), Value::BytesChunk(bytes)) => {
                words.last_mut().unwrap().extend(bytes);
            }
            PageEvent::Value(Location::Dictionary(_), Value::BytesEnd) => (),
            _ => panic!("unexpected event: {e:?}"),
        };
        for part in dictionary.chunks(chunk_size) {
            page.feed(part, &mut emit).unwrap();
        }
        page.finish(&mut emit).unwrap();
        assert_eq!(words, [b"UK".to_vec(), b"US".to_vec()]);
    }

    // Data-page dictionary IDs: width=1, three repetitions of dictionary ID 1.
    let ids = [1, 6, 1];
    let h = header(&header_v1(3, 8, ids.len(), ids.len(), None));
    let mut present = [0u8; 1];
    let mut widths = [0u8; 4];
    let mut lengths = [0u32; 1];
    let mut parts = [DeltaByteParts::default(); 1];
    let mut previous = [0u8; 1];
    let mut codec = Identity::default();
    let mut page = PageAssembler::new(
        h,
        ColumnSpec::flat(Physical::ByteArray, 0).with_dictionary(2),
        workspace(
            &mut present,
            &mut widths,
            &mut lengths,
            &mut parts,
            &mut previous,
        ),
        &mut codec,
    )
    .unwrap();
    let mut out = Vec::new();
    let mut emit = |ev: PageEvent<'_>| match ev {
        PageEvent::Value(Location::Row(_), Value::DictionaryId(id)) => out.push(id),
        _ => panic!("unexpected event"),
    };
    for part in ids.chunks(1) {
        page.feed(part, &mut emit).unwrap();
    }
    page.finish(&mut emit).unwrap();
    assert_eq!(out, [1, 1, 1]);
}

#[test]
fn delta_length_and_prefix_strings_stream_through_pages() {
    // DELTA_LENGTH_BYTE_ARRAY: lengths [2,3] then raw bytes "abxyz".
    let mut lengths = vec![0x80, 1, 4, 2, 4, 2, 0, 0, 0, 0];
    lengths.extend(b"abxyz");
    let h = header(&header_v1(2, 6, lengths.len(), lengths.len(), None));
    for n in 1..=lengths.len() {
        assert_eq!(
            decode_strings(h, &lengths, 6, n),
            [b"ab".to_vec(), b"xyz".to_vec()]
        );
    }
    // DELTA_BYTE_ARRAY: prefixes [0,3], suffix lengths [3,1], bytes "cart".
    let mut prefix = vec![0x80, 1, 4, 2, 0, 6, 0, 0, 0, 0];
    prefix.extend([0x80, 1, 4, 2, 6, 3, 0, 0, 0, 0]);
    prefix.extend(b"cart");
    let h = header(&header_v1(2, 7, prefix.len(), prefix.len(), None));
    for n in 1..=prefix.len() {
        assert_eq!(
            decode_strings(h, &prefix, 7, n),
            [b"car".to_vec(), b"cart".to_vec()]
        );
    }
}

fn decode_strings(h: PageHeader, bytes: &[u8], encoding: i32, chunk_size: usize) -> Vec<Vec<u8>> {
    let mut present = [0u8; 1];
    let mut widths = [0u8; 4];
    let mut lengths = [0u32; 8];
    let mut parts = [DeltaByteParts::default(); 8];
    let mut previous = [0u8; 64];
    let mut codec = Identity::default();
    let mut page = PageAssembler::new(
        h,
        ColumnSpec::flat(Physical::ByteArray, 0),
        workspace(
            &mut present,
            &mut widths,
            &mut lengths,
            &mut parts,
            &mut previous,
        ),
        &mut codec,
    )
    .unwrap();
    assert_eq!(h.data_v1_encoding, Some(encoding));
    let mut words = Vec::<Vec<u8>>::new();
    let mut emit = |event: PageEvent<'_>| match event {
        PageEvent::Value(Location::Row(i), Value::BytesStart(_)) => {
            assert_eq!(i as usize, words.len());
            words.push(Vec::new());
        }
        PageEvent::Value(Location::Row(_), Value::BytesChunk(bytes)) => {
            words.last_mut().unwrap().extend(bytes);
        }
        PageEvent::Value(Location::Row(_), Value::BytesEnd) => (),
        _ => panic!("unexpected event: {event:?}"),
    };
    for part in bytes.chunks(chunk_size) {
        page.feed(part, &mut emit).unwrap();
    }
    page.finish(&mut emit).unwrap();
    words
}

#[test]
fn rejects_bad_checksum_short_workspaces_and_truncation() {
    let data = 42i32.to_le_bytes();
    let h = header(&header_v1(1, 0, 4, 4, Some(crc32(&data) ^ 1)));
    let mut present = [0u8; 1];
    let mut widths = [0u8; 4];
    let mut lengths = [0u32; 1];
    let mut parts = [DeltaByteParts::default(); 1];
    let mut previous = [0u8; 1];
    let mut codec = Identity::default();
    let mut page = PageAssembler::new(
        h,
        ColumnSpec::flat(Physical::Int32, 0),
        workspace(
            &mut present,
            &mut widths,
            &mut lengths,
            &mut parts,
            &mut previous,
        ),
        &mut codec,
    )
    .unwrap();
    page.feed(&data, &mut |_: PageEvent<'_>| {}).unwrap();
    assert!(matches!(
        page.finish(&mut |_: PageEvent<'_>| {}),
        Err(Error::ChecksumMismatch { .. })
    ));

    let h = header(&header_v1(16, 0, 64, 64, None));
    let mut present = [0u8; 1];
    let mut widths = [];
    let mut lengths = [];
    let mut parts = [];
    let mut previous = [];
    let mut codec = Identity::default();
    let result = PageAssembler::new(
        h,
        ColumnSpec::flat(Physical::Int32, 0).optional(),
        workspace(
            &mut present,
            &mut widths,
            &mut lengths,
            &mut parts,
            &mut previous,
        ),
        &mut codec,
    );
    assert!(matches!(
        result,
        Err(Error::WorkspaceTooSmall {
            needed: 2,
            available: 1
        })
    ));

    let mut buf = [0u8; 128];
    let mut framer = PageFramer::new(&mut buf, 1024);
    framer
        .feed(&header_v1(1, 0, 4, 4, None), |_| Ok(()))
        .unwrap();
    assert!(matches!(framer.finish(), Err(Error::Truncated)));
}

#[test]
fn snappy_v1_decompresses_definition_levels_and_values_together() {
    let levels = [2, 1, 2, 0, 2, 1]; // present/null/present
    let mut body = Vec::new();
    body.extend((levels.len() as u32).to_le_bytes());
    body.extend(levels);
    body.extend(55i32.to_le_bytes());
    body.extend(66i32.to_le_bytes());
    // One raw Snappy literal for the entire V1 body.
    assert!(body.len() < 60);
    let mut compressed = vec![body.len() as u8, ((body.len() - 1) * 4) as u8];
    compressed.extend(&body);
    let h = header(&header_v1(
        3,
        0,
        compressed.len(),
        body.len(),
        Some(crc32(&compressed)),
    ));
    for n in 1..=compressed.len() {
        let mut present = [0u8; 1];
        let mut widths = [0u8; 4];
        let mut lengths = [0u32; 1];
        let mut parts = [DeltaByteParts::default(); 1];
        let mut previous = [0u8; 1];
        let mut history = [0u8; 32];
        let mut output = [0u8; 3];
        let mut codec = RawSnappy::new(&mut history, &mut output).unwrap();
        let mut page = PageAssembler::new(
            h,
            ColumnSpec::flat(Physical::Int32, 1).optional(),
            workspace(
                &mut present,
                &mut widths,
                &mut lengths,
                &mut parts,
                &mut previous,
            ),
            &mut codec,
        )
        .unwrap();
        let mut values = Vec::new();
        let mut emit = |e: PageEvent<'_>| match e {
            PageEvent::Null(_) => values.push(None),
            PageEvent::Value(Location::Row(_), Value::I32(v)) => values.push(Some(v)),
            _ => panic!("unexpected event"),
        };
        for chunk in compressed.chunks(n) {
            page.feed(chunk, &mut emit).unwrap();
        }
        page.finish(&mut emit).unwrap();
        assert_eq!(values, [Some(55), None, Some(66)]);
    }
}

#[test]
fn v2_rejects_wrong_null_count() {
    let encoded_levels = [2, 1, 2, 0]; // one null in two rows
    let mut payload = encoded_levels.to_vec();
    payload.extend(8i32.to_le_bytes());
    let header = header(&header_v2(
        2,
        0,
        0,
        payload.len(),
        payload.len(),
        encoded_levels.len(),
        false,
    ));
    let mut present = [0u8; 1];
    let mut widths = [0u8; 4];
    let mut lengths = [0u32; 1];
    let mut parts = [DeltaByteParts::default(); 1];
    let mut previous = [0u8; 1];
    let mut codec = Identity::default();
    let mut page = PageAssembler::new(
        header,
        ColumnSpec::flat(Physical::Int32, 0).optional(),
        workspace(
            &mut present,
            &mut widths,
            &mut lengths,
            &mut parts,
            &mut previous,
        ),
        &mut codec,
    )
    .unwrap();
    let err = page.feed(&payload, &mut |_: PageEvent<'_>| {}).unwrap_err();
    assert_eq!(err, Error::InvalidNullCount);
}

#[test]
fn genuine_parquet_file_flat_int64_dictionary_and_data_pages() {
    use compact_parquet_thrift::parquet::{footer_range, scan_footer};
    // A real Apache Parquet fixture (not synthetic Thrift headers).
    let file = include_bytes!("fixtures/list_nested_struct_file.parquet");
    let tail: &[u8; 8] = file[file.len() - 8..].try_into().unwrap();
    let range = footer_range(file.len() as u64, tail).unwrap();
    let mut offsets = None;
    scan_footer(&file[range.start as usize..range.end as usize], |chunk| {
        if chunk.flat_name == Some(b"col_flat" as &[u8]) {
            offsets = Some((
                chunk.dictionary_page_offset.unwrap() as usize,
                chunk.data_page_offset.unwrap() as usize,
            ));
        }
        Ok(())
    })
    .unwrap();
    let (dictionary_offset, data_offset) = offsets.expect("flat INT64 column");

    for chunk_size in [1, 2, 3, 7, 32] {
        let mut dictionary_entries = Vec::new();
        let dict_bytes = &file[dictionary_offset..];
        let (dict_header, dict_header_size) = page_header(dict_bytes).unwrap();
        let dict_payload = &dict_bytes
            [dict_header_size..dict_header_size + dict_header.compressed_size.unwrap() as usize];
        let mut present = [0u8; 1];
        let mut widths = [0u8; 4];
        let mut lengths = [];
        let mut parts = [];
        let mut previous = [];
        let mut history = [0u8; 32];
        let mut output = [0u8; 3];
        let mut snappy = RawSnappy::new(&mut history, &mut output).unwrap();
        let mut page = PageAssembler::new(
            dict_header,
            ColumnSpec::flat(Physical::Int64, 1),
            workspace(
                &mut present,
                &mut widths,
                &mut lengths,
                &mut parts,
                &mut previous,
            ),
            &mut snappy,
        )
        .unwrap();
        let mut emit = |event: PageEvent<'_>| match event {
            PageEvent::Value(Location::Dictionary(i), Value::I64(v)) => {
                assert_eq!(i as usize, dictionary_entries.len());
                dictionary_entries.push(v);
            }
            _ => panic!("unexpected dictionary event"),
        };
        for part in dict_payload.chunks(chunk_size) {
            page.feed(part, &mut emit).unwrap();
        }
        page.finish(&mut emit).unwrap();
        assert_eq!(dictionary_entries, [0, 1, 2]);

        let data_bytes = &file[data_offset..];
        let (data_header, data_header_size) = page_header(data_bytes).unwrap();
        let payload = &data_bytes
            [data_header_size..data_header_size + data_header.compressed_size.unwrap() as usize];
        let mut present = [0u8; 1];
        let mut widths = [0u8; 4];
        let mut lengths = [];
        let mut parts = [];
        let mut previous = [];
        let mut history = [0u8; 32];
        let mut output = [0u8; 3];
        let mut snappy = RawSnappy::new(&mut history, &mut output).unwrap();
        let mut page = PageAssembler::new(
            data_header,
            ColumnSpec::flat(Physical::Int64, 1)
                .optional()
                .with_dictionary(3),
            workspace(
                &mut present,
                &mut widths,
                &mut lengths,
                &mut parts,
                &mut previous,
            ),
            &mut snappy,
        )
        .unwrap();
        let mut ids = Vec::new();
        let mut emit = |event: PageEvent<'_>| match event {
            PageEvent::Value(Location::Row(i), Value::DictionaryId(id)) => {
                assert_eq!(i as usize, ids.len());
                ids.push(id);
            }
            _ => panic!("unexpected data event"),
        };
        for part in payload.chunks(chunk_size) {
            page.feed(part, &mut emit).unwrap();
        }
        page.finish(&mut emit).unwrap();
        assert_eq!(ids, [0, 1, 2]);
    }
}
