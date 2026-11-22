//! Untrusted bytes must produce errors, never panics or hangs.
//!
//! A deterministic generator feeds random and bit-flipped inputs to every
//! entry point that parses file contents. Outcomes are ignored; reaching the
//! end of each loop is the assertion.

use compact_parquet_thrift::encode::{
    scratch_len, write_file, ChunkPlace, Column, ColumnValues, SNAPPY, UNCOMPRESSED,
};
use compact_parquet_thrift::pages::{
    ColumnSpec, DecodeWorkspace, FrameEvent, Identity, PageAssembler, PageEvent, PageFramer,
    Physical, RawSnappy,
};
use compact_parquet_thrift::parquet::{
    footer_range, page_header, scan_footer, DataPageV2, PageHeader,
};
use compact_parquet_thrift::values::{
    ByteArrays, DeltaBinary, DeltaByteArrays, DeltaByteParts, DeltaLengthByteArrays, DictionaryIds,
    FixedByteArrays, HybridRle, Levels, PlainBool, PlainValues, RleBool,
};
use compact_parquet_thrift::{Reader, WireType};

/// xorshift64*: small, deterministic and good enough to spread bytes.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % n as u64).unwrap()
    }

    /// A value in `1..=n`.
    fn small_u32(&mut self, n: usize) -> u32 {
        u32::try_from(1 + self.below(n)).unwrap()
    }

    fn bytes(&mut self, max_len: usize) -> Vec<u8> {
        let len = self.below(max_len + 1);
        (0..len).map(|_| self.next().to_le_bytes()[0]).collect()
    }

    /// Flip a few bits, overwrite a byte, or truncate.
    fn mutate(&mut self, input: &[u8]) -> Vec<u8> {
        let mut out = input.to_vec();
        if out.is_empty() {
            return out;
        }
        for _ in 0..=self.below(4) {
            let at = self.below(out.len());
            match self.below(3) {
                0 => out[at] ^= 1 << self.below(8),
                1 => out[at] = self.next().to_le_bytes()[0],
                _ => out.truncate(at),
            }
            if out.is_empty() {
                break;
            }
        }
        out
    }
}

const CASES: usize = 3000;

#[test]
fn thrift_reader_rejects_garbage() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let reference = include_bytes!("fixtures/compact_reference.bin");
    for _ in 0..CASES {
        let input = if rng.below(2) == 0 {
            rng.bytes(64)
        } else {
            rng.mutate(reference)
        };
        let _ = Reader::new(&input).skip(WireType::Struct);
        let _ = Reader::with_max_depth(&input, 4).raw_value(WireType::List);
        let _ = Reader::new(&input).map_header();
        let _ = page_header(&input);
    }
}

#[test]
fn footer_scan_rejects_mutated_files() {
    let mut rng = Rng(0x0123_4567_89ab_cdef);
    let files: [&[u8]; 2] = [
        include_bytes!("fixtures/list_field_values_file.parquet"),
        include_bytes!("fixtures/list_nested_struct_file.parquet"),
    ];
    for file in files {
        let tail: [u8; 8] = file[file.len() - 8..].try_into().unwrap();
        let range = footer_range(file.len() as u64, &tail).unwrap();
        let footer =
            &file[usize::try_from(range.start).unwrap()..usize::try_from(range.end).unwrap()];
        for _ in 0..CASES {
            let mutated = rng.mutate(footer);
            let _ = scan_footer(&mutated, |_| Ok(()));
            let tail: [u8; 8] = rng.mutate(&tail).try_into().unwrap_or(tail);
            let _ = footer_range(rng.next() % 4096, &tail);
        }
    }
}

#[test]
fn value_decoders_reject_garbage() {
    let mut rng = Rng(0xdead_beef_cafe_f00d);
    for _ in 0..CASES {
        let input = rng.bytes(96);
        let count = rng.below(300) as u64;
        let width = u8::try_from(rng.below(40)).unwrap();
        let chunk = 1 + rng.below(8);
        let feed = |f: &mut dyn FnMut(&[u8]) -> usize| {
            for part in input.chunks(chunk) {
                let mut at = 0;
                // Bounded retries: a decoder that never consumes must not loop forever.
                for _ in 0..part.len() + 2 {
                    if at >= part.len() {
                        break;
                    }
                    at += f(&part[at..]);
                }
            }
        };

        if let Ok(mut d) = HybridRle::new(width, count) {
            feed(&mut |b| d.feed(b, |_| {}).unwrap_or(usize::MAX / 2));
        }
        if let Ok(mut d) = DictionaryIds::new(count, rng.small_u32(10)) {
            feed(&mut |b| d.feed(b, |_| {}).unwrap_or(usize::MAX / 2));
        }
        if let Ok(mut d) = Levels::v1(1, count, 64) {
            feed(&mut |b| d.feed(b, |_| {}).unwrap_or(usize::MAX / 2));
        }
        if let Ok(mut d) = Levels::v2(3, count, 16) {
            feed(&mut |b| d.feed(b, |_| {}).unwrap_or(usize::MAX / 2));
        }
        if let Ok(mut d) = RleBool::new(count, 64) {
            feed(&mut |b| d.feed(b, |_| {}).unwrap_or(usize::MAX / 2));
        }
        let mut d = PlainBool::new(count);
        feed(&mut |b| d.feed(b, |_| {}).unwrap_or(usize::MAX / 2));
        let mut d = PlainValues::<i64>::new(count);
        feed(&mut |b| d.feed(b, |_| {}).unwrap_or(usize::MAX / 2));
        let mut d = ByteArrays::new(count, 32);
        feed(&mut |b| d.feed(b, |_| {}).unwrap_or(usize::MAX / 2));
        if let Ok(mut d) = FixedByteArrays::new(count, u32::from(width)) {
            feed(&mut |b| d.feed(b, |_| {}).unwrap_or(usize::MAX / 2));
        }

        let mut widths = [0u8; 4];
        let mut d = DeltaBinary::<i32>::new(count, &mut widths);
        feed(&mut |b| d.feed(b, |_| {}).unwrap_or(usize::MAX / 2));
        let mut widths = [0u8; 4];
        let mut d = DeltaBinary::<i64>::new(count, &mut widths);
        feed(&mut |b| d.feed(b, |_| {}).unwrap_or(usize::MAX / 2));

        let n = rng.below(40);
        let mut widths = [0u8; 4];
        let mut lengths = [0u32; 40];
        if let Ok(mut d) = DeltaLengthByteArrays::new(n, &mut lengths, &mut widths, 64) {
            feed(&mut |b| d.feed(b, |_| {}).unwrap_or(usize::MAX / 2));
        }
        let mut widths = [0u8; 4];
        let mut parts = [DeltaByteParts::default(); 40];
        let mut previous = [0u8; 16];
        if let Ok(mut d) = DeltaByteArrays::new(n, &mut parts, &mut widths, &mut previous, 64) {
            feed(&mut |b| d.feed(b, |_| {}).unwrap_or(usize::MAX / 2));
        }
    }
}

const PHYSICALS: [Physical; 8] = [
    Physical::Boolean,
    Physical::Int32,
    Physical::Int64,
    Physical::Int96,
    Physical::Float,
    Physical::Double,
    Physical::ByteArray,
    Physical::FixedByteArray(3),
];

fn random_header(rng: &mut Rng, body_len: usize) -> PageHeader {
    let small = |rng: &mut Rng| i32::try_from(rng.below(40)).unwrap() - 2;
    let size = i32::try_from(body_len).unwrap();
    let mut header = PageHeader {
        page_type: Some([0, 2, 3][rng.below(3)]),
        uncompressed_size: Some(if rng.below(2) == 0 { size } else { small(rng) }),
        compressed_size: Some(size),
        data_v1_num_values: Some(small(rng)),
        data_v1_encoding: Some([0, 2, 3, 5, 6, 7, 8][rng.below(7)]),
        data_v1_definition_encoding: Some(3),
        dictionary_num_values: Some(small(rng)),
        dictionary_encoding: Some(0),
        ..PageHeader::default()
    };
    if header.page_type == Some(3) {
        header.data_v2 = Some(DataPageV2 {
            num_values: header.data_v1_num_values,
            num_nulls: Some(small(rng).max(0)),
            num_rows: header.data_v1_num_values,
            encoding: header.data_v1_encoding,
            definition_levels_bytes: Some(small(rng).min(size)),
            repetition_levels_bytes: Some(0),
            is_compressed: rng.below(2) == 0,
        });
    }
    header
}

#[test]
fn page_assembler_rejects_garbage() {
    let mut rng = Rng(0x5151_7272_9393_b4b4);
    let mut events = |_: PageEvent<'_>| {};
    for _ in 0..CASES {
        let body = rng.bytes(80);
        let header = random_header(&mut rng, body.len());
        let codec = i32::try_from(rng.below(2)).unwrap();
        let mut spec = ColumnSpec::flat(PHYSICALS[rng.below(PHYSICALS.len())], codec)
            .with_dictionary(rng.small_u32(8));
        if rng.below(2) == 0 {
            spec = spec.optional();
        }
        spec.max_value_bytes = 16;
        let mut present = [0u8; 8];
        let mut widths = [0u8; 4];
        let mut lengths = [0u32; 40];
        let mut parts = [DeltaByteParts::default(); 40];
        let mut previous = [0u8; 16];
        let workspace = DecodeWorkspace {
            present: &mut present,
            widths: &mut widths,
            lengths: &mut lengths,
            parts: &mut parts,
            previous: &mut previous,
        };
        let chunk = 1 + rng.below(16);
        if codec == UNCOMPRESSED {
            let mut identity = Identity::default();
            if let Ok(mut page) = PageAssembler::new(header, spec, workspace, &mut identity) {
                let ok = body
                    .chunks(chunk)
                    .all(|part| page.feed(part, &mut events).is_ok());
                if ok {
                    let _ = page.finish(&mut events);
                }
            }
        } else {
            let mut history = [0u8; 32];
            let mut output = [0u8; 7];
            let mut snappy = RawSnappy::new(&mut history, &mut output).unwrap();
            if let Ok(mut page) = PageAssembler::new(header, spec, workspace, &mut snappy) {
                let ok = body
                    .chunks(chunk)
                    .all(|part| page.feed(part, &mut events).is_ok());
                if ok {
                    let _ = page.finish(&mut events);
                }
            }
        }
    }
}

#[test]
fn framer_and_decoder_reject_mutated_pages() {
    let rows = 64usize;
    let ints: Vec<i32> = (0..64).map(|i| i * 37 - 900).collect();
    let words_owned: Vec<String> = (0..rows).map(|i| format!("w{}", i % 9)).collect();
    let words: Vec<&[u8]> = words_owned.iter().map(String::as_bytes).collect();
    let present: Vec<u8> = vec![0b1011_0111; rows / 8];
    let kept = (0..rows)
        .filter(|&r| present[r / 8] & (1 << (r % 8)) != 0)
        .count();
    let ids: Vec<u32> = (0..u32::try_from(kept).unwrap()).map(|i| i % 3).collect();
    let mut rng = Rng(0x7777_1111_3333_5555);
    for codec in [UNCOMPRESSED, SNAPPY] {
        let columns = [
            (
                Physical::Int32,
                None,
                Column {
                    name: "a",
                    rows,
                    codec,
                    present: None,
                    values: ColumnValues::DeltaI32(&ints),
                },
            ),
            (
                Physical::ByteArray,
                None,
                Column {
                    name: "b",
                    rows,
                    codec,
                    present: None,
                    values: ColumnValues::DeltaBytes(&words),
                },
            ),
            (
                Physical::Int64,
                Some(3),
                Column {
                    name: "c",
                    rows,
                    codec,
                    present: Some(&present),
                    values: ColumnValues::DictI64 {
                        dictionary: &[5, 6, 7],
                        ids: &ids,
                    },
                },
            ),
        ];
        for (physical, dictionary, column) in columns {
            let mut out = vec![0u8; 1 << 14];
            let mut scratch = vec![0u8; scratch_len(rows, 4096).unwrap()];
            let mut table = [0u32; 256];
            let mut places = [ChunkPlace::default()];
            write_file(
                &mut out,
                i64::try_from(rows).unwrap(),
                &[column],
                &mut places,
                &mut scratch,
                &mut table,
            )
            .unwrap();
            let start = usize::try_from(places[0].offset).unwrap();
            let end = start + usize::try_from(places[0].compressed_size).unwrap();
            let chunk = &out[start..end];
            let mut spec = ColumnSpec::flat(physical, codec);
            if column.present.is_some() {
                spec = spec.optional();
            }
            if let Some(len) = dictionary {
                spec = spec.with_dictionary(len);
            }
            for _ in 0..CASES / 3 {
                decode_chunk(&rng.mutate(chunk), spec, 1 + rng.below(32));
            }
        }
    }
}

/// Frame and decode every page of a column chunk, stopping at the first error.
fn decode_chunk(chunk: &[u8], spec: ColumnSpec, fragment: usize) {
    let mut header_buf = [0u8; 128];
    let mut framer = PageFramer::new(&mut header_buf, 1 << 12);
    let mut framed_pages: Vec<(PageHeader, Vec<u8>)> = Vec::new();
    let all_framed = chunk.chunks(fragment).all(|part| {
        framer
            .feed(part, |event| {
                match event {
                    FrameEvent::Start(header) => framed_pages.push((header, Vec::new())),
                    FrameEvent::Body(bytes) => {
                        if let Some((_, body)) = framed_pages.last_mut() {
                            body.extend_from_slice(bytes);
                        }
                    }
                    FrameEvent::End => {}
                }
                Ok(())
            })
            .is_ok()
    });
    if !all_framed {
        return;
    }
    let mut events = |_: PageEvent<'_>| {};
    for (header, body) in framed_pages {
        let mut present = [0u8; 16];
        let mut widths = [0u8; 4];
        let mut lengths = [0u32; 128];
        let mut parts = [DeltaByteParts::default(); 128];
        let mut previous = [0u8; 64];
        let workspace = DecodeWorkspace {
            present: &mut present,
            widths: &mut widths,
            lengths: &mut lengths,
            parts: &mut parts,
            previous: &mut previous,
        };
        let mut history = [0u8; 1024];
        let mut output = [0u8; 64];
        let mut snappy = RawSnappy::new(&mut history, &mut output).unwrap();
        let mut identity = Identity::default();
        let result = if spec.codec == SNAPPY {
            PageAssembler::new(header, spec, workspace, &mut snappy).and_then(|mut page| {
                body.chunks(fragment)
                    .try_for_each(|part| page.feed(part, &mut events).map(drop))?;
                page.finish(&mut events)
            })
        } else {
            PageAssembler::new(header, spec, workspace, &mut identity).and_then(|mut page| {
                body.chunks(fragment)
                    .try_for_each(|part| page.feed(part, &mut events).map(drop))?;
                page.finish(&mut events)
            })
        };
        if result.is_err() {
            return;
        }
    }
}

/// A row group that repeats its column list must not overrun the inline
/// column buffer (the lists together exceed it; each alone fits).
#[test]
fn footer_with_repeated_column_lists_is_bounded() {
    use compact_parquet_thrift::{SliceSink, Writer};

    let mut bytes = vec![0u8; 1 << 16];
    let mut writer = Writer::new(SliceSink::new(&mut bytes));
    writer
        .structure(|w| {
            w.field_header(4, WireType::List)?;
            w.list_header(WireType::Struct, 1)?;
            w.structure(|w| {
                for _ in 0..2 {
                    w.field_header(1, WireType::List)?;
                    w.list_header(WireType::Struct, 300)?;
                    for _ in 0..300 {
                        w.structure(|w| w.field_i64(2, 4))?;
                    }
                }
                w.field_i64(3, 10)
            })
        })
        .unwrap();
    let footer = writer.into_inner().as_written().to_vec();
    let mut seen = 0;
    let summary = scan_footer(&footer, |info| {
        assert_eq!(info.row_group_rows, Some(10));
        seen += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!((summary.columns, seen), (300, 300));
}
