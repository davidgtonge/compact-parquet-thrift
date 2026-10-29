use compact_parquet_thrift::values::{
    ByteArrayEvent as E, ByteArrays, DictionaryIds, Error, FixedByteArrays, HybridRle,
    Int96, Levels, PlainBool, PlainValues,
};

#[test]
fn plain_i32_i64_float_double_and_int96_are_chunk_independent() {
    let values = [-12i32, 0, i32::MAX, i32::MIN, 101];
    let mut encoded = Vec::new();
    for v in values { encoded.extend_from_slice(&v.to_le_bytes()); }
    for chunk_size in 1..=encoded.len() {
        let mut d = PlainValues::<i32>::new(values.len() as u64);
        let mut decoded = Vec::new();
        for bytes in encoded.chunks(chunk_size) {
            assert_eq!(d.feed(bytes, |v| decoded.push(v)).unwrap(), bytes.len());
        }
        d.finish().unwrap();
        assert_eq!(decoded, values);
    }

    let mut d = PlainValues::<i64>::new(2);
    let mut got = Vec::new();
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&i64::MIN.to_le_bytes());
    encoded.extend_from_slice(&i64::MAX.to_le_bytes());
    for bytes in encoded.chunks(3) { d.feed(bytes, |x| got.push(x)).unwrap(); }
    assert_eq!(got, [i64::MIN, i64::MAX]);
    d.finish().unwrap();

    let mut d = PlainValues::<f32>::new(3);
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&1.25f32.to_le_bytes());
    encoded.extend_from_slice(&(-0.0f32).to_le_bytes());
    encoded.extend_from_slice(&f32::INFINITY.to_le_bytes());
    let mut got = Vec::new();
    for bytes in encoded.chunks(1) { d.feed(bytes, |v| got.push(v.to_bits())).unwrap(); }
    assert_eq!(got, [1.25f32.to_bits(), (-0.0f32).to_bits(), f32::INFINITY.to_bits()]);
    d.finish().unwrap();

    let mut d = PlainValues::<f64>::new(1);
    let mut got = Vec::new();
    for bytes in 12.25f64.to_le_bytes().chunks(2) {
        d.feed(bytes, |v| got.push(v)).unwrap();
    }
    assert_eq!(got, [12.25]);
    d.finish().unwrap();

    let mut d = PlainValues::<Int96>::new(1);
    let mut got = Vec::new();
    let input: [u8; 12] = core::array::from_fn(|i| i as u8);
    for bytes in input.chunks(5) { d.feed(bytes, |v| got.push(v)).unwrap(); }
    assert_eq!(got, [Int96(input)]);
    d.finish().unwrap();
}

#[test]
fn plain_stops_at_count_and_rejects_incomplete_value() {
    let mut d = PlainValues::<i32>::new(1);
    assert_eq!(d.feed(&[1, 0, 0, 0, 99], |_| {}).unwrap(), 4);
    d.finish().unwrap();
    let mut d = PlainValues::<i32>::new(1);
    assert_eq!(d.feed(&[1, 2], |_| {}).unwrap(), 2);
    assert_eq!(d.finish(), Err(Error::Truncated));
}

#[test]
fn plain_bool_uses_lsb_first_and_discards_padding() {
    let mut d = PlainBool::new(10);
    let mut result = Vec::new();
    assert_eq!(d.feed(&[0b1010_0110], |v| result.push(v)).unwrap(), 1);
    assert_eq!(d.feed(&[0b1111_1111, 0], |v| result.push(v)).unwrap(), 1);
    assert_eq!(result, [false, true, true, false, false, true, false, true, true, true]);
    d.finish().unwrap();
}

#[test]
fn plain_bool_limited_feed_resumes_inside_the_byte() {
    let mut d = PlainBool::new(4);
    let mut result = Vec::new();
    assert_eq!(d.feed_limited(&[0b0000_1101], 1, |v| result.push(v)).unwrap(), 1);
    assert!(!d.is_complete());
    assert_eq!(d.feed_limited(&[], 1, |v| result.push(v)).unwrap(), 0);
    assert_eq!(d.feed_limited(&[], 8, |v| result.push(v)).unwrap(), 0);
    assert_eq!(result, [true, false, true, true]);
    d.finish().unwrap();
}

#[test]
fn byte_arrays_stream_borrowed_fragments_and_empty_values() {
    let encoded = [
        2, 0, 0, 0, b'U', b'K',
        0, 0, 0, 0,
        5, 0, 0, 0, b'h', b'e', b'l', b'l', b'o',
    ];
    for size in 1..=encoded.len() {
        let mut d = ByteArrays::new(3, 100);
        let mut strings = Vec::<Vec<u8>>::new();
        let mut active = false;
        for bytes in encoded.chunks(size) {
            assert_eq!(d.feed(bytes, |event| match event {
                E::Start(_) => { assert!(!active); active = true; strings.push(Vec::new()); },
                E::Chunk(data) => { assert!(active); strings.last_mut().unwrap().extend(data); },
                E::End => { assert!(active); active = false; },
            }).unwrap(), bytes.len());
        }
        d.finish().unwrap();
        assert!(!active);
        assert_eq!(strings, [b"UK".to_vec(), b"".to_vec(), b"hello".to_vec()]);
    }
}

#[test]
fn byte_arrays_reject_huge_lengths_without_allocating() {
    let mut d = ByteArrays::new(1, 64);
    assert_eq!(d.feed(&1000u32.to_le_bytes(), |_| {}),
        Err(Error::ValueTooLarge { length: 1000, limit: 64 }));
    let mut d = ByteArrays::new(1, 64);
    d.feed(&[5, 0, 0, 0, b'a'], |_| {}).unwrap();
    assert_eq!(d.finish(), Err(Error::Truncated));
}

#[test]
fn fixed_byte_arrays_stream_value_parts() {
    let mut d = FixedByteArrays::new(2, 3).unwrap();
    let mut parts = Vec::new();
    let mut current: Vec<u8> = Vec::new();
    for b in b"abcdef".chunks(2) {
        assert_eq!(d.feed(b, |event| match event {
            E::Start(n) => { assert_eq!(n, 3); current.clear(); },
            E::Chunk(p) => current.extend(p),
            E::End => parts.push(current.clone()),
        }).unwrap(), b.len());
    }
    assert_eq!(parts, [b"abc".to_vec(), b"def".to_vec()]);
    d.finish().unwrap();
    assert!(matches!(FixedByteArrays::new(1, 0), Err(Error::InvalidFixedLength)));
}

fn decode_hybrid(bit_width: u8, bytes: &[u8], count: usize, chunk_size: usize) -> Vec<u32> {
    let mut d = HybridRle::new(bit_width, count as u64).unwrap();
    let mut values = Vec::new();
    for chunk in bytes.chunks(chunk_size) {
        assert_eq!(d.feed(chunk, |v| values.push(v)).unwrap(), chunk.len());
    }
    d.finish().unwrap();
    values
}

#[test]
fn hybrid_mixed_runs_and_split_varints() {
    // RLE run: 5 copies of 3; bitpacked run: [0,1,2,3,0,1,2,3]
    let input = [0x0a, 0x03, 0x03, 0xe4, 0xe4];
    let expected = [3,3,3,3,3, 0,1,2,3,0,1,2,3];
    for size in 1..=input.len() {
        assert_eq!(decode_hybrid(2, &input, 13, size), expected);
    }
    // 130 repetitions -> ULEB header 260 = 0x84,0x02.
    let input = [0x84, 0x02, 0x01];
    assert_eq!(decode_hybrid(1, &input, 130, 1), vec![1; 130]);
}

#[test]
fn hybrid_rle_feed_limited_resumes_mid_repeat_run() {
    let input = [0x0a, 0x03, 0x03]; // 5 repeats of value 3
    let mut d = HybridRle::new(2, 5).unwrap();
    let mut first = Vec::new();
    assert_eq!(
        d.feed_limited(&input, 2, |v| first.push(v)).unwrap(),
        2
    );
    assert_eq!(first, [3, 3]);
    let mut second = Vec::new();
    assert_eq!(d.feed_limited(&input[2..], 3, |v| second.push(v)).unwrap(), 0);
    assert_eq!(second, [3, 3, 3]);
    d.finish().unwrap();
}

#[test]
fn hybrid_rle_feed_limited_resumes_mid_packed_group() {
    // One packed run (header 0x11 = 1 group), 4-bit width, 8 values in group; cap stops after 3.
    let group = pack_bit_group(4, &[0, 1, 2, 3, 4, 5, 6, 7]);
    let mut input = vec![0x03];
    input.extend_from_slice(&group);
    let mut d = HybridRle::new(4, 8).unwrap();
    let mut first = Vec::new();
    assert_eq!(
        d.feed_limited(&input, 3, |v| first.push(v)).unwrap(),
        1 + group.len()
    );
    assert_eq!(first, [0, 1, 2]);
    let mut second = Vec::new();
    assert_eq!(
        d.feed_limited(&[], 5, |v| second.push(v)).unwrap(),
        0
    );
    assert_eq!(second, [3, 4, 5, 6, 7]);
    d.finish().unwrap();
}

fn pack_bit_group(bits: u8, values: &[u32; 8]) -> Vec<u8> {
    let mut result = vec![0u8; bits as usize];
    for (i, &value) in values.iter().enumerate() {
        for bit in 0..bits {
            if (value >> bit) & 1 != 0 {
                let pos = i * bits as usize + bit as usize;
                result[pos / 8] |= 1 << (pos % 8);
            }
        }
    }
    result
}

#[test]
fn hybrid_bitpacking_across_all_widths_with_padding() {
    for bits in [0, 1, 2, 3, 7, 8, 9, 16, 24, 31, 32] {
        let mask = if bits == 32 { u32::MAX } else { (1u32 << bits) - 1 };
        let values = core::array::from_fn::<_, 8, _>(|i| {
            (u32::MAX.rotate_left(i as u32) ^ (i as u32 * 17)) & mask
        });
        let mut encoded = vec![3u8]; // 1 bit-packed group (8 values)
        encoded.extend(pack_bit_group(bits, &values));
        for size in 1..=encoded.len().max(1) {
            assert_eq!(decode_hybrid(bits, &encoded, 8, size), values);
            assert_eq!(decode_hybrid(bits, &encoded, 5, size), values[..5]);
        }
    }
}

#[test]
fn hybrid_zero_width_repeated_and_bitpacked() {
    assert_eq!(decode_hybrid(0, &[18], 9, 1), [0; 9]);
    assert_eq!(decode_hybrid(0, &[3], 8, 1), [0; 8]);
}

#[test]
fn hybrid_rejects_invalid_counts_widths_and_values() {
    assert!(matches!(HybridRle::new(33, 1), Err(Error::InvalidBitWidth(33))));
    let mut d = HybridRle::new(1, 1).unwrap();
    assert_eq!(d.feed(&[0], |_| {}), Err(Error::InvalidRun));
    // A run longer than the logical count is padding, but the payload
    // bytes still have to arrive. A bare header is truncated.
    let mut d = HybridRle::new(1, 1).unwrap();
    assert_eq!(d.feed(&[4], |_| {}), Ok(1));
    assert_eq!(d.finish(), Err(Error::Truncated));
    let mut d = HybridRle::new(1, 1).unwrap();
    assert_eq!(d.feed(&[5], |_| {}), Ok(1));
    assert_eq!(d.finish(), Err(Error::Truncated));
    let mut d = HybridRle::new(2, 1).unwrap().with_max_value(1);
    assert_eq!(d.feed(&[2, 3], |_| {}),
        Err(Error::ValueOutOfRange { value: 3, max: 1 }));
    let mut d = HybridRle::new(1, 130).unwrap();
    assert_eq!(d.feed(&[0x84], |_| {}).unwrap(), 1);
    assert_eq!(d.finish(), Err(Error::Truncated));
    let mut d = HybridRle::new(1, 1).unwrap();
    assert_eq!(d.feed(&[0x80; 10], |_| {}), Err(Error::InvalidVarint));
}

#[test]
fn dictionary_ids_validate_bounds_and_do_not_materialize_strings() {
    // width=2; 5 repeated IDs of 2, followed by 8 bit-packed IDs
    let encoded = [2, 10, 2, 3, 0xe4, 0xe4];
    for chunk_size in 1..=encoded.len() {
        let mut d = DictionaryIds::new(13, 4).unwrap();
        let mut ids = Vec::new();
        for bytes in encoded.chunks(chunk_size) {
            assert_eq!(d.feed(bytes, |id| ids.push(id)).unwrap(), bytes.len());
        }
        assert_eq!(ids, [2,2,2,2,2,0,1,2,3,0,1,2,3]);
        d.finish().unwrap();
    }
    let mut invalid = DictionaryIds::new(1, 3).unwrap();
    assert_eq!(invalid.feed(&[2, 2, 3], |_| {}),
        Err(Error::ValueOutOfRange { value: 3, max: 2 }));
    assert!(matches!(DictionaryIds::new(1, 0), Err(Error::EmptyDictionary)));
}

#[test]
fn v1_v2_levels_are_length_framed_and_stop_before_values() {
    // levels are [1,1,1,1,1], encoded as one 5-value RLE run (0x0A, 0x01)
    let v1 = [2, 0, 0, 0, 0x0a, 1, 0xaa]; // trailing page value 0xaa
    let v2 = [0x0a, 1, 0xaa];
    for n in 1..=6 {
        let mut d = Levels::v1(1, 5, 64).unwrap();
        let mut vals = Vec::new();
        let mut at = 0;
        while at < 6 {
            let end = (at + n).min(v1.len());
            let offered = end - at;
            let used = d.feed(&v1[at..end], |v| vals.push(v)).unwrap();
            at += used;
            if used < offered { break; }
        }
        d.finish().unwrap();
        assert_eq!(at, 6);
        assert_eq!(vals, [1; 5]);

        let mut d = Levels::v2(1, 5, 2).unwrap();
        let mut vals = Vec::new();
        for chunk in v2[..2].chunks(n) {
            assert_eq!(d.feed(chunk, |v| vals.push(v)).unwrap(), chunk.len());
        }
        d.finish().unwrap();
        assert_eq!(vals, [1; 5]);
        assert_eq!(d.feed(&v2[2..], |_| {}).unwrap(), 0);
    }
}

#[test]
fn levels_reject_invalid_sizes_and_truncated_frames() {
    let mut d = Levels::v1(1, 5, 64).unwrap();
    assert_eq!(d.feed(&[200,0,0,0], |_| {}),
        Err(Error::FrameTooLarge { length: 200, limit: 64 }));
    let mut d = Levels::v2(1, 5, 1).unwrap();
    assert_eq!(d.feed(&[0x0a, 1], |_| {}).unwrap(), 1);
    assert_eq!(d.finish(), Err(Error::Truncated));
    let mut d = Levels::v2(1, 5, 3).unwrap();
    assert_eq!(d.feed(&[0x0a, 1, 9], |_| {}).unwrap(), 3);
    d.finish().unwrap();
}

#[test]
fn definition_levels_allow_frame_padding_and_incremental_feed() {
    let mut payload = vec![0x0a, 0x01];
    payload.extend([0u8; 5]);
    let mut framed = (payload.len() as u32).to_le_bytes().to_vec();
    framed.extend(payload);
    let mut d = Levels::v1(1, 5, 64).unwrap();
    let mut vals = Vec::new();
    for chunk in framed.chunks(3) {
        d.feed(chunk, |v| vals.push(v)).unwrap();
    }
    d.finish().unwrap();
    assert_eq!(vals, [1; 5]);
}

#[test]
fn rle_boolean_uses_length_prefix_for_both_page_versions() {
    use compact_parquet_thrift::values::RleBool;
    // 3 true values followed by 4 false values.
    let encoded = [4, 0, 0, 0, 6, 1, 8, 0];
    for size in 1..=encoded.len() {
        let mut d = RleBool::new(7, 64).unwrap();
        let mut got = Vec::new();
        for bytes in encoded.chunks(size) {
            assert_eq!(d.feed(bytes, |b| got.push(b)).unwrap(), bytes.len());
        }
        assert_eq!(got, [true,true,true,false,false,false,false]);
        d.finish().unwrap();
    }
}

#[test]
fn rle_boolean_feed_limited_emits_one_value_at_a_time() {
    use compact_parquet_thrift::values::RleBool;
    let encoded = [4, 0, 0, 0, 6, 1, 8, 0];
    let mut d = RleBool::new(7, 64).unwrap();
    let mut got = Vec::new();
    let mut rest = &encoded[..];
    let mut after_first = None;
    while got.len() < 7 {
        let mut one = None;
        let used = d.feed_limited(rest, 1, |b| {
            assert!(one.is_none());
            one = Some(b);
        }).unwrap();
        assert!(used <= rest.len());
        if used == 0 && one.is_none() {
            panic!("decoder stalled after {got:?}");
        }
        rest = &rest[used..];
        if let Some(value) = one {
            if got.is_empty() {
                after_first = Some(rest.len());
            }
            got.push(value);
        }
    }
    assert_eq!(got, [true, true, true, false, false, false, false]);
    assert_eq!(after_first, Some(2), "the second run must stay unconsumed");
    assert!(rest.is_empty());
    d.finish().unwrap();
}

#[test]
fn rle_three_bit_packing_matches_apache_spec_example() {
    // The Parquet spec specifies [0,1,2,3,4,5,6,7] in 3-bit LSB packing
    // as 0x88, 0xC6, 0xFA.
    for size in 1..=4 {
        assert_eq!(decode_hybrid(3, &[0x03, 0x88, 0xc6, 0xfa], 8, size),
            [0,1,2,3,4,5,6,7]);
    }
}

#[test]
fn oversized_bit_packed_run_discards_extra_groups() {
    // Header 9 = 4 groups. Only 9 logical values are real. The last two
    // groups are a flushed literal tail and must be consumed, not emitted.
    let input = [9, 0b0101_0101, 0b0000_0001, 0xff, 0xaa];
    let expected = [1, 0, 1, 0, 1, 0, 1, 0, 1];
    for size in 1..=input.len() {
        assert_eq!(decode_hybrid(1, &input, 9, size), expected);
    }
}

#[test]
fn partial_final_packed_group_is_fully_consumed() {
    // Two 8-value groups, with only 9 logical values and 7 padding values.
    let input = [5, 0b0101_0101, 0b1111_1111];
    for size in 1..=3 {
        assert_eq!(decode_hybrid(1, &input, 9, size),
            [true, false, true, false, true, false, true, false, true]
                .map(u32::from));
    }
}

#[test]
fn byte_array_payload_is_borrowed_from_the_input_chunk() {
    let input = [3,0,0,0, b'a', b'b', b'c'];
    let mut d = ByteArrays::new(1, 10);
    let expected = input[4..].as_ptr();
    d.feed(&input, |event| {
        if let E::Chunk(part) = event { assert_eq!(part.as_ptr(), expected); }
    }).unwrap();
    d.finish().unwrap();
}

#[test]
fn decoder_state_has_bounded_size() {
    use core::mem::size_of;
    assert!(size_of::<PlainValues<f64>>() <= 64);
    assert!(size_of::<ByteArrays>() <= 64);
    assert!(size_of::<HybridRle>() <= 128);
    assert!(size_of::<DictionaryIds>() <= 160);
}
