use compact_parquet_thrift::values::{
    ByteArrayEvent as E, DeltaBinary, DeltaByteArrays, DeltaByteParts,
    DeltaLengthByteArrays, Error,
};

// Independent reference *test encoder*. Production crate contains only the
// zero-allocation streaming decoders, not a page encoder.
fn uleb(mut n: u64, out: &mut Vec<u8>) {
    while n >= 128 {
        out.push((n as u8 & 0x7f) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}
fn zigzag(n: i64) -> u64 { ((n as u64) << 1) ^ ((n >> 63) as u64) }

fn encode_delta(first: i64, deltas: &[i64], bits: u8) -> Vec<u8> {
    let mut out = Vec::new();
    uleb(128, &mut out);
    uleb(4, &mut out);
    uleb(deltas.len() as u64 + 1, &mut out);
    uleb(zigzag(first), &mut out);
    for block in deltas.chunks(128) {
        let min = *block.iter().min().unwrap();
        uleb(zigzag(min), &mut out);
        let mut widths = [255u8; 4]; // unused widths may be arbitrary
        for (i, mini) in block.chunks(32).enumerate() {
            let max = mini.iter().map(|&x| {
                if bits == 32 {
                    (x as i32).wrapping_sub(min as i32) as u32 as u64
                } else {
                    x.wrapping_sub(min) as u64
                }
            }).max().unwrap_or(0);
            widths[i] = (64 - max.leading_zeros()) as u8;
        }
        out.extend_from_slice(&widths);
        for (i, mini) in block.chunks(32).enumerate() {
            let width = usize::from(widths[i]);
            let mut packed = vec![0u8; 32 * width / 8];
            for j in 0..32 {
                // Deliberately fill the trailing mini-block padding with 1s:
                // the reader must consume it but not emit those fake values.
                let v = if j < mini.len() {
                    if bits == 32 {
                        (mini[j] as i32).wrapping_sub(min as i32) as u32 as u64
                    } else {
                        mini[j].wrapping_sub(min) as u64
                    }
                } else { u64::MAX };
                for b in 0..width {
                    if (v >> b) & 1 != 0 {
                        let bit = j * width + b;
                        packed[bit / 8] |= 1 << (bit % 8);
                    }
                }
            }
            out.extend_from_slice(&packed);
        }
    }
    out
}

fn i64_stream(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        let mut output = Vec::new();
        uleb(128, &mut output);
        uleb(4, &mut output);
        uleb(0, &mut output);
        uleb(0, &mut output); // first-value slot remains in the header
        return output;
    }
    let deltas: Vec<_> = values.windows(2).map(|pair| pair[1].wrapping_sub(pair[0])).collect();
    encode_delta(values[0], &deltas, 64)
}

fn i32_stream(values: &[i32]) -> Vec<u8> {
    if values.is_empty() { return i64_stream(&[]); }
    let deltas: Vec<_> = values.windows(2)
        .map(|pair| pair[1].wrapping_sub(pair[0]) as i64).collect();
    encode_delta(values[0] as i64, &deltas, 32)
}

fn encode_lengths(values: &[&[u8]]) -> Vec<u8> {
    let lengths: Vec<_> = values.iter().map(|v| v.len() as i32).collect();
    let mut result = i32_stream(&lengths);
    for value in values { result.extend_from_slice(value); }
    result
}

fn encode_prefixes(values: &[&[u8]]) -> Vec<u8> {
    let mut prefixes = Vec::new();
    let mut suffixes = Vec::new();
    let mut suffix_data = Vec::new();
    let mut previous = &[][..];
    for &value in values {
        let prefix = value.iter().zip(previous.iter())
            .take_while(|(a,b)| a == b).count();
        prefixes.push(prefix as i32);
        suffixes.push((value.len() - prefix) as i32);
        suffix_data.extend_from_slice(&value[prefix..]);
        previous = value;
    }
    let mut encoded = i32_stream(&prefixes);
    encoded.extend(i32_stream(&suffixes));
    encoded.extend(suffix_data);
    encoded
}

fn append(event: E<'_>, parts: &mut Vec<Vec<u8>>, current: &mut bool) {
    match event {
        E::Start(_) => {
            assert!(!*current);
            *current = true;
            parts.push(Vec::new());
        },
        E::Chunk(bytes) => {
            assert!(*current);
            parts.last_mut().unwrap().extend_from_slice(bytes);
        },
        E::End => {
            assert!(*current);
            *current = false;
        },
    }
}

#[test]
fn reference_simple_integer_encoding() {
    // [1,2,3,4,5]: one min-delta = 1 and zero-width mini-blocks.
    let bytes = [0x80, 0x01, 4, 5, 2, 2, 0, 0, 0, 0, 0x55];
    for piece in 1..=bytes.len() {
        let mut widths = [0u8; 4];
        let mut d = DeltaBinary::<i32>::new(5, &mut widths);
        let mut out = Vec::new();
        let mut at = 0;
        while at < bytes.len() && !d.is_complete() {
            let end = (at + piece).min(bytes.len());
            let used = d.feed(&bytes[at..end], |x| out.push(x)).unwrap();
            at += used;
            assert!(used > 0);
        }
        assert_eq!(at, 10);
        d.finish().unwrap();
        assert_eq!(out, [1,2,3,4,5]);
    }
}

#[test]
fn multi_block_integer_streams_all_chunk_sizes_and_padding() {
    let mut vals = Vec::new();
    let mut previous = -1030i64;
    vals.push(previous);
    for i in 0..300i64 {
        previous = previous.wrapping_add(if i < 75 { -3 } else if i < 150 { 10 } else {
            (i * 31 % 251) - 120
        });
        vals.push(previous);
    }
    let bytes = i64_stream(&vals);
    for n in [1,2,3,4,5,7,8,11,19,31,64,129,10000] {
        let mut widths = [0u8; 4];
        let mut d = DeltaBinary::<i64>::new(vals.len() as u64, &mut widths);
        let mut decoded = Vec::new();
        for part in bytes.chunks(n) {
            assert_eq!(d.feed(part, |v| decoded.push(v)).unwrap(), part.len());
        }
        d.finish().unwrap();
        assert_eq!(decoded, vals);
    }
}

#[test]
fn i32_wrap_and_i64_full_width() {
    let original = [i32::MAX - 1, i32::MAX, i32::MIN, i32::MIN + 1, 0, i32::MAX];
    let encoded = i32_stream(&original);
    for size in 1..=encoded.len() {
        let mut widths = [0u8; 4];
        let mut d = DeltaBinary::<i32>::new(original.len() as u64, &mut widths);
        let mut out = Vec::new();
        for part in encoded.chunks(size) { d.feed(part, |x| out.push(x)).unwrap(); }
        d.finish().unwrap();
        assert_eq!(out, original);
    }

    let original = [i64::MIN, i64::MAX, 0, -1, i64::MAX];
    let encoded = i64_stream(&original);
    let mut widths = [0u8; 4];
    let mut d = DeltaBinary::<i64>::new(original.len() as u64, &mut widths);
    let mut out = Vec::new();
    for part in encoded.chunks(1) { d.feed(part, |x| out.push(x)).unwrap(); }
    d.finish().unwrap();
    assert_eq!(out.as_slice(), &original);
}

#[test]
fn empty_and_single_value_streams() {
    let cases: [&[i64]; 2] = [&[], &[123]];
    for original in cases {
        let mut widths = [0u8; 4];
        let mut d = DeltaBinary::<i64>::new(original.len() as u64, &mut widths);
        let mut out = Vec::new();
        let encoded = i64_stream(original);
        for part in encoded.chunks(1) { d.feed(part, |x| out.push(x)).unwrap(); }
        d.finish().unwrap();
        assert_eq!(out.as_slice(), original);
    }
}

#[test]
fn decoder_reports_truncated_invalid_headers_and_small_workspace() {
    let mut widths = [0u8; 3];
    let mut d = DeltaBinary::<i64>::new(2, &mut widths);
    assert_eq!(d.feed(&[0x80,1,4], |_| {}),
        Err(Error::DeltaWorkspaceTooSmall { needed: 4, available: 3 }));

    let mut widths = [0u8; 4];
    let mut d = DeltaBinary::<i64>::new(2, &mut widths);
    assert_eq!(d.feed(&[8], |_| {}), Err(Error::InvalidDeltaHeader));

    let mut widths = [0u8; 4];
    let mut d = DeltaBinary::<i64>::new(2, &mut widths);
    assert_eq!(d.feed(&[0x80,1,4,3], |_| {}), Err(Error::LengthMismatch));

    let mut widths = [0u8; 4];
    let mut d = DeltaBinary::<i32>::new(2, &mut widths);
    assert_eq!(d.feed(&[0x80,1,4,2,0,0,33,0,0,0], |_| {}),
        Err(Error::InvalidBitWidth(33)));

    let original = [13i64, 1, 2];
    let bytes = i64_stream(&original);
    let mut widths = [0u8; 4];
    let mut d = DeltaBinary::<i64>::new(3, &mut widths);
    d.feed(&bytes[..bytes.len()-1], |_| {}).unwrap();
    assert_eq!(d.finish(), Err(Error::Truncated));

    let mut widths = [0u8; 4];
    let mut d = DeltaBinary::<i64>::new(0, &mut widths);
    assert_eq!(d.feed(&[0x80; 11], |_| {}), Err(Error::InvalidVarint));
}

#[test]
fn delta_length_byte_array_streams_values_with_empty_strings() {
    let strings: &[&[u8]] = &[b"hello", b"world", b"", b"foobar", b"ABCDEF"];
    let encoded = encode_lengths(strings);
    for size in 1..=encoded.len() {
        let mut lengths = [0u32; 5];
        let mut widths = [0u8; 4];
        let mut d = DeltaLengthByteArrays::new(5, &mut lengths, &mut widths, 100).unwrap();
        let mut decoded = Vec::new();
        let mut active = false;
        for part in encoded.chunks(size) {
            let n = d.feed(part, |e| append(e, &mut decoded, &mut active)).unwrap();
            assert_eq!(n, part.len());
        }
        d.finish().unwrap();
        assert!(!active);
        let got: Vec<&[u8]> = decoded.iter().map(Vec::as_slice).collect();
        assert_eq!(got.as_slice(), strings);
    }
}

#[test]
fn delta_length_rejects_invalid_lengths_and_workspace() {
    let mut lengths = [0u32; 1];
    let mut widths = [0u8; 4];
    assert!(matches!(DeltaLengthByteArrays::new(2, &mut lengths, &mut widths, 100),
        Err(Error::DeltaWorkspaceTooSmall { .. })));

    let bad = i32_stream(&[-2]);
    let mut lengths = [0u32; 1];
    let mut widths = [0u8; 4];
    let mut d = DeltaLengthByteArrays::new(1, &mut lengths, &mut widths, 100).unwrap();
    assert_eq!(d.feed(&bad, |_| {}), Err(Error::InvalidDeltaLength(-2)));

    let bad = i32_stream(&[101]);
    let mut lengths = [0u32; 1];
    let mut widths = [0u8; 4];
    let mut d = DeltaLengthByteArrays::new(1, &mut lengths, &mut widths, 100).unwrap();
    assert_eq!(d.feed(&bad, |_| {}),
        Err(Error::ValueTooLarge { length: 101, limit: 100 }));
}

#[test]
fn delta_byte_array_prefix_reconstruction_and_duplicates() {
    let strings: &[&[u8]] = &[
        b"axis", b"axle", b"babble", b"babyhood", b"babyhood", b"", b"p", b"pearl",
    ];
    let encoded = encode_prefixes(strings);
    for size in 1..=encoded.len() {
        let mut parts = [DeltaByteParts::default(); 8];
        let mut widths = [0u8; 4];
        let mut prev = [0u8; 64];
        let mut d = DeltaByteArrays::new(8, &mut parts, &mut widths, &mut prev, 64).unwrap();
        let mut decoded = Vec::new();
        let mut active = false;
        for part in encoded.chunks(size) {
            let used = d.feed(part, |e| append(e, &mut decoded, &mut active)).unwrap();
            assert_eq!(used, part.len());
        }
        d.finish().unwrap();
        assert!(!active);
        let got: Vec<&[u8]> = decoded.iter().map(Vec::as_slice).collect();
        assert_eq!(got.as_slice(), strings);
    }
}

#[test]
fn delta_byte_array_fixed_length_and_errors() {
    let input: &[&[u8]] = &[b"aaaa", b"aaab", b"aaac", b"zzzz"];
    let encoded = encode_prefixes(input);
    let mut parts = [DeltaByteParts::default(); 4];
    let mut widths = [0u8; 4];
    let mut previous = [0u8; 4];
    let mut d = DeltaByteArrays::new(4, &mut parts, &mut widths, &mut previous, 4)
        .unwrap().with_fixed_length(4).unwrap();
    let mut decoded = Vec::new();
    let mut active = false;
    for part in encoded.chunks(1) {
        d.feed(part, |e| append(e, &mut decoded, &mut active)).unwrap();
    }
    let got: Vec<&[u8]> = decoded.iter().map(Vec::as_slice).collect();
    assert_eq!(got.as_slice(), input);
    d.finish().unwrap();

    // First prefix must be zero unless a non-standard cross-page seed is used.
    let mut bad = i32_stream(&[1]);
    bad.extend(i32_stream(&[1]));
    bad.push(b'x');
    let mut parts = [DeltaByteParts::default(); 1];
    let mut widths = [0u8; 4];
    let mut previous = [0u8; 4];
    let mut d = DeltaByteArrays::new(1, &mut parts, &mut widths, &mut previous, 4).unwrap();
    assert_eq!(d.feed(&bad, |_| {}), Err(Error::InvalidPrefix {
        prefix: 1, previous_length: 0,
    }));

    let encoded = encode_prefixes(&[b"hello"]);
    let mut parts = [DeltaByteParts::default(); 1];
    let mut widths = [0u8; 4];
    let mut previous = [0u8; 3];
    let mut d = DeltaByteArrays::new(1, &mut parts, &mut widths, &mut previous, 8).unwrap();
    assert_eq!(d.feed(&encoded, |_| {}), Err(Error::OutputBufferTooSmall {
        needed: 5, available: 3,
    }));
}

#[test]
fn delta_string_decoders_leave_trailing_sections_unconsumed() {
    let mut length_data = encode_lengths(&[b"hi", b"bye"]);
    let encoded_len = length_data.len();
    length_data.extend_from_slice(&[0xaa, 0xbb]);
    let mut lengths = [0u32; 2];
    let mut widths = [0u8; 4];
    let mut d = DeltaLengthByteArrays::new(2, &mut lengths, &mut widths, 100).unwrap();
    let mut got = Vec::new();
    let mut current = false;
    assert_eq!(d.feed(&length_data, |e| append(e, &mut got, &mut current)).unwrap(),
        encoded_len);
    d.finish().unwrap();
    assert_eq!(got, [b"hi".to_vec(), b"bye".to_vec()]);

    let mut prefix_data = encode_prefixes(&[b"abc", b"abd"]);
    let encoded_len = prefix_data.len();
    prefix_data.extend_from_slice(&[0xaa, 0xbb]);
    let mut parts = [DeltaByteParts::default(); 2];
    let mut widths = [0u8; 4];
    let mut previous = [0u8; 10];
    let mut d = DeltaByteArrays::new(2, &mut parts, &mut widths, &mut previous, 10).unwrap();
    let mut got = Vec::new();
    let mut current = false;
    assert_eq!(d.feed(&prefix_data, |e| append(e, &mut got, &mut current)).unwrap(),
        encoded_len);
    d.finish().unwrap();
    assert_eq!(got, [b"abc".to_vec(), b"abd".to_vec()]);
}

#[test]
fn delta_binary_reset_reuses_same_width_scratch() {
    let mut widths = [0u8; 4];
    let mut d = DeltaBinary::<i32>::new(3, &mut widths);
    let first = i32_stream(&[10, 11, 12]);
    let second = i32_stream(&[-4, -5, -6, -7]);
    let mut got = Vec::new();
    d.feed(&first, |v| got.push(v)).unwrap();
    d.finish().unwrap();
    assert_eq!(got, [10, 11, 12]);
    d.reset(4);
    got.clear();
    for part in second.chunks(1) { d.feed(part, |v| got.push(v)).unwrap(); }
    d.finish().unwrap();
    assert_eq!(got, [-4,-5,-6,-7]);
}
