//! Round-trip the encoder through this crate's decoder, then through DuckDB.

use std::process::Command;

use compact_parquet_thrift::encode::{
    compress_snappy, delta_binary_bound, delta_byte_array_bound, delta_length_bound,
    encode_delta_bytes, encode_delta_i32, encode_delta_i64, encode_delta_length_bytes,
    encode_hybrid, scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY, UNCOMPRESSED,
};
use compact_parquet_thrift::pages::{
    ColumnSpec, DecodeWorkspace, Identity, Inflate, PageAssembler, PageEvent, Physical, RawSnappy,
    Value,
};
use compact_parquet_thrift::parquet::{footer_range, page_header, scan_footer};
use compact_parquet_thrift::values::{
    DeltaBinary, DeltaByteArrays, DeltaByteParts, DeltaLengthByteArrays, HybridRle,
};

#[test]
fn hybrid_rle_roundtrips_runs_literals_and_padding() {
    roundtrip_hybrid(1, &[1, 1, 1, 1, 1, 1, 1, 1, 0]);
    roundtrip_hybrid(3, &[0, 1, 2, 3, 4, 5, 6, 7, 1, 1, 1, 1, 1, 1, 1, 1, 1]);
    roundtrip_hybrid(8, &(0..20).collect::<Vec<_>>());
    roundtrip_hybrid(0, &[0, 0, 0, 0, 0]);
    roundtrip_hybrid(32, &[u32::MAX, 0, 7]);
    roundtrip_hybrid(1, &[]);
}

fn roundtrip_hybrid(width: u8, values: &[u32]) {
    let mut buf = vec![0u8; values.len() * 8 + 64];
    let n = encode_hybrid(width, values, &mut buf).unwrap();
    let sizes: Vec<usize> = if n == 0 { vec![1] } else { (1..=n).collect() };
    for size in sizes {
        let mut decoder = HybridRle::new(width, values.len() as u64).unwrap();
        let mut got = Vec::new();
        for chunk in buf[..n].chunks(size) {
            assert_eq!(decoder.feed(chunk, |v| got.push(v)).unwrap(), chunk.len());
        }
        decoder.finish().unwrap();
        assert_eq!(got, values, "width {width} chunk {size}");
    }
}

#[test]
fn snappy_literals_and_backreferences_roundtrip() {
    snappy_roundtrip(b"", 0);
    snappy_roundtrip(b"hello", 0);
    snappy_roundtrip(b"aaaaaaaaaa snappy snappy snappy", 256);
    snappy_roundtrip(&(0u8..200).collect::<Vec<_>>(), 128);
    snappy_roundtrip(b"abcabcabcabcabcabc", 64);
}

fn snappy_roundtrip(input: &[u8], table_len: usize) {
    let mut encoded = vec![0u8; input.len() * 2 + 64];
    let mut table = vec![0u32; table_len];
    let n = compress_snappy(input, &mut encoded, &mut table).unwrap();
    let mut history = vec![0u8; input.len().max(1)];
    let mut output = vec![0u8; 48];
    let mut snappy = RawSnappy::new(&mut history, &mut output).unwrap();
    snappy.reset(input.len()).unwrap();
    let mut got = Vec::new();
    let mut emit = |bytes: &[u8]| {
        got.extend_from_slice(bytes);
        Ok(())
    };
    snappy.push(&encoded[..n], &mut emit).unwrap();
    snappy.finish(&mut emit).unwrap();
    assert_eq!(got, input);
}

#[test]
fn file_roundtrips_through_the_page_reader_and_duckdb() {
    let rows = 4i64;
    let a_present = [0b0000_1101u8]; // 10, null, 3, 7
    let a_values = [10i64, 3, 7];
    let b_present = [0b0000_0111u8]; // aa, bb, aa, null
    let dictionary: [&[u8]; 2] = [b"aa", b"bb"];
    let b_ids = [0u32, 1, 0];
    let c_values = [1.5f64, 2.5, 3.0, 4.0];
    let d_values = [true, false, true, true];
    let e_values = [1i32, 2, 3, 4];
    let columns = [
        Column {
            name: "a",
            rows: 4,
            codec: SNAPPY,
            present: Some(&a_present),
            values: ColumnValues::I64(&a_values),
        },
        Column {
            name: "b",
            rows: 4,
            codec: SNAPPY,
            present: Some(&b_present),
            values: ColumnValues::DictBytes {
                dictionary: &dictionary,
                ids: &b_ids,
            },
        },
        Column {
            name: "c",
            rows: 4,
            codec: UNCOMPRESSED,
            present: None,
            values: ColumnValues::F64(&c_values),
        },
        Column {
            name: "d",
            rows: 4,
            codec: UNCOMPRESSED,
            present: None,
            values: ColumnValues::Bool(&d_values),
        },
        Column {
            name: "e",
            rows: 4,
            codec: SNAPPY,
            present: None,
            values: ColumnValues::I32(&e_values),
        },
    ];
    let mut places = [ChunkPlace::default(); 5];
    let mut scratch = vec![0u8; scratch_len(4, 64).unwrap()];
    let mut table = vec![0u32; 256];
    let mut file = vec![0u8; 16 * 1024];
    let n = write_file(
        &mut file,
        rows,
        &columns,
        &mut places,
        &mut scratch,
        &mut table,
    )
    .unwrap();
    let file = &file[..n];

    assert_eq!(&file[..4], b"PAR1");
    assert_eq!(&file[n - 4..], b"PAR1");
    let tail: [u8; 8] = file[n - 8..].try_into().unwrap();
    let range = footer_range(n as u64, &tail).unwrap();
    let mut seen = Vec::new();
    let summary = scan_footer(&file[range.start as usize..range.end as usize], |chunk| {
        seen.push((
            chunk.flat_name.unwrap().to_vec(),
            chunk.num_values.unwrap(),
            chunk.codec.unwrap(),
            chunk.data_page_offset.unwrap(),
            chunk.dictionary_page_offset,
            chunk.compressed_size.unwrap(),
        ));
        Ok(())
    })
    .unwrap();
    assert_eq!(summary.num_rows, Some(4));
    assert_eq!(summary.row_groups, 1);
    assert_eq!(seen.len(), 5);
    for (place, meta) in places.iter().zip(&seen) {
        assert_eq!(meta.3, place.data_page_offset as i64);
        assert_eq!(meta.4, place.dictionary_page_offset.map(|v| v as i64));
        assert_eq!(meta.5, place.compressed_size as i64);
        assert_eq!(meta.1, 4);
    }

    let decoded_a = decode(file, &places[0], Physical::Int64, SNAPPY, true);
    assert_eq!(
        decoded_a,
        vec![
            Some(Owned::I64(10)),
            None,
            Some(Owned::I64(3)),
            Some(Owned::I64(7))
        ]
    );
    let decoded_b = decode(file, &places[1], Physical::ByteArray, SNAPPY, true);
    assert_eq!(
        decoded_b,
        vec![
            Some(Owned::Bytes(b"aa".to_vec())),
            Some(Owned::Bytes(b"bb".to_vec())),
            Some(Owned::Bytes(b"aa".to_vec())),
            None,
        ]
    );
    let decoded_c = decode(file, &places[2], Physical::Double, UNCOMPRESSED, false);
    assert_eq!(
        decoded_c,
        c_values
            .iter()
            .copied()
            .map(|v| Some(Owned::F64(v)))
            .collect::<Vec<_>>()
    );
    let decoded_d = decode(file, &places[3], Physical::Boolean, UNCOMPRESSED, false);
    assert_eq!(
        decoded_d,
        d_values
            .iter()
            .copied()
            .map(|v| Some(Owned::Bool(v)))
            .collect::<Vec<_>>()
    );
    let decoded_e = decode(file, &places[4], Physical::Int32, SNAPPY, false);
    assert_eq!(
        decoded_e,
        e_values
            .iter()
            .copied()
            .map(|v| Some(Owned::I32(v)))
            .collect::<Vec<_>>()
    );

    let duckdb = "/Users/addmaple/.duckdb/cli/latest/duckdb";
    if !std::path::Path::new(duckdb).exists() {
        return;
    }
    let path = std::env::temp_dir().join("compact-parquet-thrift-encode.parquet");
    std::fs::write(&path, file).unwrap();
    let sql = format!(
        "SELECT count(*)::VARCHAR, coalesce(sum(a), 0)::VARCHAR, \
         (count(*) FILTER (WHERE a IS NULL))::VARCHAR, \
         coalesce(sum(length(b)), 0)::VARCHAR, \
         (count(*) FILTER (WHERE b IS NULL))::VARCHAR, \
         sum(c)::VARCHAR, coalesce(sum(CAST(d AS INTEGER)), 0)::VARCHAR, \
         coalesce(sum(e), 0)::VARCHAR \
         FROM read_parquet('{}')",
        path.display()
    );
    let output = Command::new(duckdb)
        .args(["-csv", "-noheader", "-c", &sql])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let line = String::from_utf8_lossy(&output.stdout);
    assert_eq!(line.trim(), "4,20,1,6,1,11.0,3,10");
}

/// Integer and boolean chunks carry signed min/max (plain little-endian) and a null
/// count; byte arrays and all-null chunks carry only the null count.
#[test]
fn footer_carries_chunk_statistics() {
    let i32_present = [0b0000_1011u8]; // -5, 9, null, 2
    let i32_values = [-5i32, 9, 2];
    let dictionary = [70i64, -30, 12];
    let ids = [0u32, 2, 0, 1];
    let flags = [false, false, true, false];
    let words: [&[u8]; 4] = [b"b", b"a", b"c", b"a"];
    let nothing: [i64; 0] = [];
    let all_null = [0u8];
    let columns = [
        Column {
            name: "i32",
            rows: 4,
            codec: SNAPPY,
            present: Some(&i32_present),
            values: ColumnValues::I32(&i32_values),
        },
        Column {
            name: "dict_i64",
            rows: 4,
            codec: UNCOMPRESSED,
            present: None,
            values: ColumnValues::DictI64 {
                dictionary: &dictionary,
                ids: &ids,
            },
        },
        Column {
            name: "bool",
            rows: 4,
            codec: UNCOMPRESSED,
            present: None,
            values: ColumnValues::Bool(&flags),
        },
        Column {
            name: "bytes",
            rows: 4,
            codec: UNCOMPRESSED,
            present: None,
            values: ColumnValues::Bytes(&words),
        },
        Column {
            name: "nulls",
            rows: 4,
            codec: UNCOMPRESSED,
            present: Some(&all_null),
            values: ColumnValues::I64(&nothing),
        },
    ];
    let mut places = [ChunkPlace::default(); 5];
    let mut scratch = vec![0u8; scratch_len(4, 64).unwrap()];
    let mut table = vec![0u32; 256];
    let mut file = vec![0u8; 16 * 1024];
    let n = write_file(
        &mut file,
        4,
        &columns,
        &mut places,
        &mut scratch,
        &mut table,
    )
    .unwrap();
    let file = &file[..n];
    let tail: [u8; 8] = file[n - 8..].try_into().unwrap();
    let range = footer_range(n as u64, &tail).unwrap();
    let mut seen = Vec::new();
    scan_footer(&file[range.start as usize..range.end as usize], |chunk| {
        seen.push((
            chunk.min_value.map(<[u8]>::to_vec),
            chunk.max_value.map(<[u8]>::to_vec),
            chunk.null_count,
        ));
        Ok(())
    })
    .unwrap();
    let le32 = |v: i32| Some(v.to_le_bytes().to_vec());
    let le64 = |v: i64| Some(v.to_le_bytes().to_vec());
    assert_eq!(
        seen,
        vec![
            (le32(-5), le32(9), Some(1)),
            (le64(-30), le64(70), Some(0)),
            (Some(vec![0]), Some(vec![1]), Some(0)),
            (None, None, Some(0)),
            (None, None, Some(4)),
        ]
    );

    let duckdb = "/Users/addmaple/.duckdb/cli/latest/duckdb";
    if !std::path::Path::new(duckdb).exists() {
        return;
    }
    let path = std::env::temp_dir().join("compact-parquet-thrift-stats.parquet");
    std::fs::write(&path, file).unwrap();
    let sql = format!(
        "SELECT string_agg(concat_ws(':', stats_min_value, stats_max_value, stats_null_count), ',' \
         ORDER BY column_id) FROM parquet_metadata('{}')",
        path.display()
    );
    let output = Command::new(duckdb)
        .args(["-csv", "-noheader", "-c", &sql])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout)
            .trim()
            .trim_matches('"'),
        "-5:9:1,-30:70:0,false:true:0,0,4"
    );
}

#[derive(Clone, Debug, PartialEq)]
enum Owned {
    I32(i32),
    I64(i64),
    F64(f64),
    Bool(bool),
    Bytes(Vec<u8>),
}

fn decode(
    file: &[u8],
    place: &ChunkPlace,
    physical: Physical,
    codec: i32,
    optional: bool,
) -> Vec<Option<Owned>> {
    let chunk =
        &file[place.offset as usize..place.offset as usize + place.compressed_size as usize];
    let mut at = 0;
    let mut dictionary: Vec<Owned> = Vec::new();
    let mut rows = Vec::new();
    while at < chunk.len() {
        let (header, header_len) = page_header(&chunk[at..]).unwrap();
        let compressed = header.compressed_size.unwrap() as usize;
        let body = &chunk[at + header_len..at + header_len + compressed];
        let is_dict = header.page_type == Some(2);
        let mut spec = ColumnSpec::flat(physical, codec);
        if optional && !is_dict {
            spec = spec.optional();
        }
        if !is_dict && !dictionary.is_empty() {
            spec = spec.with_dictionary(dictionary.len() as u32);
        }
        spec.max_page_bytes = 1024 * 1024;
        let count = header
            .data_v1_num_values
            .or(header.dictionary_num_values)
            .unwrap() as usize;
        let mut present = vec![0u8; count.div_ceil(8).max(1)];
        let mut widths = [0u8; 32];
        let mut lengths = vec![0u32; count];
        let mut parts = vec![DeltaByteParts::default(); count];
        let mut previous = vec![0u8; 4096];
        let workspace = DecodeWorkspace {
            present: &mut present,
            widths: &mut widths,
            lengths: &mut lengths,
            parts: &mut parts,
            previous: &mut previous,
        };
        let unc = header.uncompressed_size.unwrap().max(0) as usize;
        let mut history = vec![0u8; unc.max(1)];
        let mut output = vec![0u8; 64];
        let mut page_values = Vec::new();
        let mut building: Option<(u32, Vec<u8>)> = None;
        let mut sink = |event: PageEvent<'_>| match event {
            PageEvent::Null(_) => page_values.push(None),
            PageEvent::Value(_, Value::I32(v)) => page_values.push(Some(Owned::I32(v))),
            PageEvent::Value(_, Value::I64(v)) => page_values.push(Some(Owned::I64(v))),
            PageEvent::Value(_, Value::F64(v)) => page_values.push(Some(Owned::F64(v))),
            PageEvent::Value(_, Value::Boolean(v)) => page_values.push(Some(Owned::Bool(v))),
            PageEvent::Value(_, Value::BytesStart(len)) => building = Some((len, Vec::new())),
            PageEvent::Value(_, Value::BytesChunk(chunk)) => {
                building.as_mut().unwrap().1.extend_from_slice(chunk)
            }
            PageEvent::Value(_, Value::BytesEnd) => {
                let (len, bytes) = building.take().unwrap();
                assert_eq!(bytes.len(), len as usize);
                page_values.push(Some(Owned::Bytes(bytes)));
            }
            PageEvent::Value(_, Value::DictionaryId(id)) => {
                page_values.push(Some(dictionary[id as usize].clone()));
            }
            other => panic!("unexpected {other:?}"),
        };
        if codec == 0 {
            let mut identity = Identity::default();
            let mut page = PageAssembler::new(header, spec, workspace, &mut identity).unwrap();
            assert_eq!(page.feed(body, &mut sink).unwrap(), body.len());
            page.finish(&mut sink).unwrap();
        } else {
            let mut snappy = RawSnappy::new(&mut history, &mut output).unwrap();
            let mut page = PageAssembler::new(header, spec, workspace, &mut snappy).unwrap();
            assert_eq!(page.feed(body, &mut sink).unwrap(), body.len());
            page.finish(&mut sink).unwrap();
        }
        if is_dict {
            dictionary = page_values
                .into_iter()
                .map(|v| v.expect("dictionary null"))
                .collect();
        } else {
            rows.extend(page_values);
        }
        at += header_len + compressed;
    }
    rows
}

#[test]
fn delta_binary_roundtrips_block_edges_and_extremes() {
    for n in [0, 1, 2, 31, 32, 33, 127, 128, 129, 130, 256, 257] {
        let values: Vec<i32> = (0..n as i32)
            .map(|i| i.wrapping_mul(3).wrapping_sub(10))
            .collect();
        roundtrip_i32(&values);
        let wide: Vec<i64> = (0..n as i64)
            .map(|i| i.wrapping_mul(1_000_000_003))
            .collect();
        roundtrip_i64(&wide);
    }
    roundtrip_i32(&[0, i32::MAX, -1, i32::MIN, 1]);
    roundtrip_i32(&vec![7; 300]);
    roundtrip_i64(&[i64::MIN, i64::MAX, 0, -1, 1]);
    roundtrip_i64(&vec![-5; 129]);
}

fn roundtrip_i32(values: &[i32]) {
    let bound = delta_binary_bound(values.len(), 4).unwrap();
    let mut buf = vec![0u8; bound];
    let n = encode_delta_i32(values, &mut buf).unwrap();
    assert!(n <= bound);
    let mut got = Vec::new();
    let mut widths = [0u8; 4];
    let mut decoder = DeltaBinary::<i32>::new(values.len() as u64, &mut widths);
    let mut at = 0;
    while at < n {
        let used = decoder.feed(&buf[at..at + 1], |v| got.push(v)).unwrap();
        assert_eq!(used, 1);
        at += used;
    }
    decoder.finish().unwrap();
    assert_eq!(got, values);
}

fn roundtrip_i64(values: &[i64]) {
    let bound = delta_binary_bound(values.len(), 8).unwrap();
    let mut buf = vec![0u8; bound];
    let n = encode_delta_i64(values, &mut buf).unwrap();
    assert!(n <= bound);
    let mut got = Vec::new();
    let mut widths = [0u8; 4];
    let mut decoder = DeltaBinary::<i64>::new(values.len() as u64, &mut widths);
    let mut at = 0;
    while at < n {
        let used = decoder.feed(&buf[at..at + 1], |v| got.push(v)).unwrap();
        assert_eq!(used, 1);
        at += used;
    }
    decoder.finish().unwrap();
    assert_eq!(got, values);
}

#[test]
fn delta_byte_arrays_roundtrip_prefixes_and_empty_values() {
    roundtrip_length(&[]);
    roundtrip_length(&[b""]);
    roundtrip_length(&[b"a", b"a", b"abc", b""]);
    roundtrip_prefixed(&[]);
    roundtrip_prefixed(&[b""]);
    roundtrip_prefixed(&[b"alpha", b"alpine", b"alp", b"beta", b"beta", b""]);
    let owned: Vec<Vec<u8>> = (0..140)
        .map(|i| format!("row-{i:04}-{}", i % 5).into_bytes())
        .collect();
    let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
    roundtrip_length(&refs);
    roundtrip_prefixed(&refs);
}

fn roundtrip_length(values: &[&[u8]]) {
    let total: usize = values.iter().map(|v| v.len()).sum();
    let bound = delta_length_bound(values.len(), total).unwrap();
    let mut buf = vec![0u8; bound];
    let n = encode_delta_length_bytes(values, &mut buf).unwrap();
    assert!(n <= bound);
    let got = decode_length(&buf[..n], values.len());
    assert_eq!(got, values);
}

fn roundtrip_prefixed(values: &[&[u8]]) {
    let total: usize = values.iter().map(|v| v.len()).sum();
    let bound = delta_byte_array_bound(values.len(), total).unwrap();
    let mut buf = vec![0u8; bound];
    let n = encode_delta_bytes(values, &mut buf).unwrap();
    assert!(n <= bound);
    let got = decode_prefixed(&buf[..n], values.len());
    assert_eq!(got, values);
}

fn decode_length(input: &[u8], count: usize) -> Vec<Vec<u8>> {
    let mut lengths = vec![0u32; count];
    let mut widths = [0u8; 4];
    let mut decoder =
        DeltaLengthByteArrays::new(count, &mut lengths, &mut widths, 1_000_000).unwrap();
    let mut got = Vec::new();
    let mut building: Option<Vec<u8>> = None;
    let mut at = 0;
    while at < input.len() || !decoder.is_complete() {
        let end = (at + 1).min(input.len());
        let used = decoder
            .feed(&input[at..end], |event| {
                collect_bytes(event, &mut got, &mut building)
            })
            .unwrap();
        if used == 0 && end == at {
            decoder.finish().unwrap();
            break;
        }
        at += used;
    }
    decoder.finish().unwrap();
    got
}

fn decode_prefixed(input: &[u8], count: usize) -> Vec<Vec<u8>> {
    let mut parts = vec![DeltaByteParts::default(); count];
    let mut widths = [0u8; 4];
    let mut previous = vec![0u8; 1_000_000];
    let mut decoder =
        DeltaByteArrays::new(count, &mut parts, &mut widths, &mut previous, 1_000_000).unwrap();
    let mut got = Vec::new();
    let mut building: Option<Vec<u8>> = None;
    let mut at = 0;
    while at < input.len() || !decoder.is_complete() {
        let end = (at + 1).min(input.len());
        let used = decoder
            .feed(&input[at..end], |event| {
                collect_bytes(event, &mut got, &mut building)
            })
            .unwrap();
        if used == 0 && end == at {
            decoder.finish().unwrap();
            break;
        }
        at += used;
    }
    decoder.finish().unwrap();
    got
}

fn collect_bytes(
    event: compact_parquet_thrift::values::ByteArrayEvent<'_>,
    got: &mut Vec<Vec<u8>>,
    building: &mut Option<Vec<u8>>,
) {
    use compact_parquet_thrift::values::ByteArrayEvent as E;
    match event {
        E::Start(len) => {
            assert!(building.is_none());
            building.replace(Vec::with_capacity(len as usize));
        }
        E::Chunk(chunk) => building.as_mut().unwrap().extend_from_slice(chunk),
        E::End => got.push(building.take().unwrap()),
    }
}

#[test]
fn delta_file_roundtrips_through_the_page_reader_and_duckdb() {
    let rows = 130usize;
    let mut present = vec![0u8; rows.div_ceil(8)];
    let mut a_values = Vec::new();
    let mut a_sum = 0i64;
    let mut a_nulls = 0i64;
    for i in 0..rows {
        if i % 7 == 0 {
            a_nulls += 1;
        } else {
            present[i >> 3] |= 1 << (i & 7);
            let value = if i % 2 == 0 {
                -(i as i32)
            } else {
                i as i32 * 3
            };
            a_sum += i64::from(value);
            a_values.push(value);
        }
    }
    let b_values: Vec<i64> = (0..rows as i64)
        .map(|i| if i % 2 == 0 { -i } else { i * 1000 })
        .collect();
    let b_sum: i64 = b_values.iter().sum();
    let owned: Vec<String> = (0..rows).map(|i| format!("row-{i:04}-{}", i % 5)).collect();
    let bytes: Vec<&[u8]> = owned.iter().map(|s| s.as_bytes()).collect();
    let byte_sum: i64 = bytes.iter().map(|s| s.len() as i64).sum();

    let value_bytes = delta_binary_bound(rows, 8)
        .unwrap()
        .max(delta_byte_array_bound(rows, byte_sum as usize).unwrap())
        .max(delta_length_bound(rows, byte_sum as usize).unwrap());
    let mut scratch = vec![0u8; scratch_len(rows, value_bytes).unwrap()];
    let mut table = vec![0u32; 256];
    let mut file = vec![0u8; 64 * 1024];
    let mut places = [ChunkPlace::default(); 4];
    let columns = [
        Column {
            name: "a",
            rows,
            codec: SNAPPY,
            present: Some(&present),
            values: ColumnValues::DeltaI32(&a_values),
        },
        Column {
            name: "b",
            rows,
            codec: UNCOMPRESSED,
            present: None,
            values: ColumnValues::DeltaI64(&b_values),
        },
        Column {
            name: "c",
            rows,
            codec: SNAPPY,
            present: None,
            values: ColumnValues::DeltaBytes(&bytes),
        },
        Column {
            name: "d",
            rows,
            codec: UNCOMPRESSED,
            present: None,
            values: ColumnValues::DeltaLengthBytes(&bytes),
        },
    ];
    let n = write_file(
        &mut file,
        rows as i64,
        &columns,
        &mut places,
        &mut scratch,
        &mut table,
    )
    .unwrap();
    let file = &file[..n];

    let decoded_a = decode(file, &places[0], Physical::Int32, SNAPPY, true);
    let mut expect_a = Vec::new();
    let mut cursor = 0;
    for i in 0..rows {
        if i % 7 == 0 {
            expect_a.push(None);
        } else {
            expect_a.push(Some(Owned::I32(a_values[cursor])));
            cursor += 1;
        }
    }
    assert_eq!(decoded_a, expect_a);
    let decoded_b = decode(file, &places[1], Physical::Int64, UNCOMPRESSED, false);
    assert_eq!(
        decoded_b,
        b_values
            .iter()
            .copied()
            .map(|v| Some(Owned::I64(v)))
            .collect::<Vec<_>>()
    );
    let decoded_c = decode(file, &places[2], Physical::ByteArray, SNAPPY, false);
    assert_eq!(
        decoded_c,
        bytes
            .iter()
            .map(|v| Some(Owned::Bytes(v.to_vec())))
            .collect::<Vec<_>>()
    );
    let decoded_d = decode(file, &places[3], Physical::ByteArray, UNCOMPRESSED, false);
    assert_eq!(
        decoded_d,
        bytes
            .iter()
            .map(|v| Some(Owned::Bytes(v.to_vec())))
            .collect::<Vec<_>>()
    );

    let duckdb = "/Users/addmaple/.duckdb/cli/latest/duckdb";
    if !std::path::Path::new(duckdb).exists() {
        return;
    }
    let path = std::env::temp_dir().join("compact-parquet-thrift-delta.parquet");
    std::fs::write(&path, file).unwrap();
    let sql = format!(
        "SELECT count(*)::VARCHAR, coalesce(sum(a), 0)::VARCHAR, \
         (count(*) FILTER (WHERE a IS NULL))::VARCHAR, \
         coalesce(sum(b), 0)::VARCHAR, \
         coalesce(sum(length(c)), 0)::VARCHAR, \
         coalesce(sum(length(d)), 0)::VARCHAR \
         FROM read_parquet('{}')",
        path.display()
    );
    let output = Command::new(duckdb)
        .args(["-csv", "-noheader", "-c", &sql])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let line = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        line.trim(),
        format!("{rows},{a_sum},{a_nulls},{b_sum},{byte_sum},{byte_sum}")
    );
}
