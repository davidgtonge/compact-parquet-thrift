//! Read the local wcol Parquet files and check them against their own
//! metadata, then against DuckDB when the CLI is available.
//!
//! The files are gitignored, so this test does nothing unless that directory
//! exists. Run it with the files in place:
//!
//! `cargo test --test wcol_files -- --nocapture`

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;

use compact_parquet_thrift::pages::{
    ColumnSpec, DecodeWorkspace, Identity, Location, PageAssembler, PageEvent, PageSink, Physical,
    RawSnappy, Value,
};
use compact_parquet_thrift::parquet::{footer_range, scan_footer, ColumnChunkInfo, PageHeader};
use compact_parquet_thrift::values::{DeltaByteParts, Int96};
use compact_parquet_thrift::{Reader, WireType};

const DUCKDB: &str = "/Users/addmaple/.duckdb/cli/latest/duckdb";

#[derive(Clone, Debug)]
struct Leaf {
    name: String,
    physical: Option<i32>,
    type_length: Option<i32>,
    max_def: u8,
    max_rep: u8,
}

#[derive(Clone, Debug)]
enum Metric {
    /// Wrapping-free sum of integer or boolean values. Nulls are skipped.
    Sum(i128),
    /// Sum of decoded byte lengths. Nulls are skipped.
    Bytes(i128),
    /// XOR of little-endian bits, for float/double.
    Bits(u64),
}

#[derive(Clone, Debug)]
struct Agg {
    rows: u64,
    nulls: u64,
    metric: Metric,
}

fn main_files() -> Vec<PathBuf> {
    let dir = Path::new("/Users/addmaple/sites/dtonge/wcol/data");
    if !dir.is_dir() {
        return Vec::new();
    }
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .expect("read wcol data dir")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("parquet"))
        .collect();
    files.sort();
    if let Ok(filter) = std::env::var("WCOL_PARQUET_FILTER") {
        files.retain(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains(&filter))
        });
    }
    files
}

#[test]
fn reads_local_wcol_parquet_files() {
    let files = main_files();
    if files.is_empty() {
        eprintln!("skipping: wcol/data has no parquet files");
        return;
    }
    let mut failures = Vec::new();
    for path in &files {
        eprintln!("reading {}", path.file_name().unwrap().to_string_lossy());
        match read_file(path) {
            Ok(report) => {
                println!(
                    "{}\tcolumns={}\tdecoded={}\tskipped={}\trows={:?}",
                    path.file_name().unwrap().to_string_lossy(),
                    report.columns,
                    report.decoded,
                    report.skipped,
                    report.rows,
                );
                for line in &report.notes {
                    println!("  {line}");
                }
            }
            Err(error) => {
                let name = path.file_name().unwrap().to_string_lossy();
                eprintln!("FAIL {name}: {error}");
                failures.push(format!("{name}: {error}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

struct Report {
    columns: usize,
    decoded: usize,
    skipped: usize,
    rows: Option<i64>,
    notes: Vec<String>,
}

fn read_file(path: &Path) -> Result<Report, String> {
    let mut file = File::open(path).map_err(|e| e.to_string())?;
    let size = file.metadata().map_err(|e| e.to_string())?.len();
    let mut trailer = [0u8; 8];
    file.seek(SeekFrom::End(-8)).map_err(|e| e.to_string())?;
    file.read_exact(&mut trailer).map_err(|e| e.to_string())?;
    let range = footer_range(size, &trailer).map_err(|e| format!("trailer: {e:?}"))?;
    let mut footer = vec![0u8; (range.end - range.start) as usize];
    file.seek(SeekFrom::Start(range.start))
        .map_err(|e| e.to_string())?;
    file.read_exact(&mut footer).map_err(|e| e.to_string())?;

    let leaves = schema_leaves(&footer)?;
    let mut columns = Vec::new();
    let summary = scan_footer(&footer, |column| {
        columns.push(OwnedColumn::from(&column));
        Ok(())
    })
    .map_err(|e| format!("footer: {e:?}"))?;

    if columns.len() % leaves.len() != 0 {
        return Err(format!(
            "{} column chunks is not a multiple of {} schema leaves",
            columns.len(),
            leaves.len()
        ));
    }

    let mut decoded = 0;
    let mut skipped = 0;
    let mut totals = vec![None; leaves.len()];
    let mut skip_reason = vec![None; leaves.len()];
    for (index, column) in columns.iter().enumerate() {
        let leaf_index = index % leaves.len();
        let leaf = &leaves[leaf_index];
        let label = format!("rg{} {}", column.row_group, leaf.name);
        match classify(leaf, column) {
            Class::Skip(reason) => {
                skipped += 1;
                if skip_reason[leaf_index].is_none() {
                    skip_reason[leaf_index] = Some(reason);
                }
            }
            Class::Read(physical) => {
                let agg = decode_column(&mut file, column, leaf, physical)
                    .map_err(|e| format!("{label}: {e}"))?;
                let expected = column
                    .num_values
                    .ok_or_else(|| format!("{label}: no num_values"))?;
                let expected =
                    u64::try_from(expected).map_err(|_| format!("{label}: negative num_values"))?;
                if agg.rows != expected {
                    return Err(format!(
                        "{label}: decoded {} logical values, metadata says {expected}",
                        agg.rows
                    ));
                }
                decoded += 1;
                totals[leaf_index] = Some(match totals[leaf_index].take() {
                    Some(mut total) => {
                        merge_agg(&mut total, &agg);
                        total
                    }
                    None => agg,
                });
            }
        }
    }
    let comparable: Vec<Compared> = leaves
        .iter()
        .enumerate()
        .map(|(i, leaf)| Compared {
            name: leaf.name.clone(),
            agg: totals[i].clone(),
            skip: skip_reason[i].clone(),
        })
        .collect();

    let mut notes = Vec::new();
    let skip_reasons: Vec<_> = comparable.iter().filter_map(|c| c.skip.clone()).collect();
    if !skip_reasons.is_empty() {
        let mut counts = std::collections::BTreeMap::<&str, usize>::new();
        for reason in &skip_reasons {
            *counts.entry(reason.as_str()).or_default() += 1;
        }
        let text: Vec<_> = counts.iter().map(|(k, n)| format!("{n} {k}")).collect();
        notes.push(format!("skipped first row group: {}", text.join(", ")));
    }
    if Path::new(DUCKDB).exists() && !comparable.is_empty() {
        check_duckdb(path, &comparable).map_err(|e| format!("duckdb: {e}"))?;
        notes.push("duckdb aggregates matched".to_string());
    }

    Ok(Report {
        columns: columns.len(),
        decoded,
        skipped,
        rows: summary.num_rows,
        notes,
    })
}

struct Compared {
    name: String,
    agg: Option<Agg>,
    skip: Option<String>,
}

enum Class {
    Skip(String),
    Read(Physical),
}

fn classify(leaf: &Leaf, column: &OwnedColumn) -> Class {
    if leaf.max_rep != 0 || leaf.max_def > 1 {
        return Class::Skip(format!("nested def={} rep={}", leaf.max_def, leaf.max_rep));
    }
    let codec = column.codec.unwrap_or(-1);
    if codec != 0 && codec != 1 {
        return Class::Skip(format!("codec {codec}"));
    }
    let physical = match leaf.physical {
        Some(0) => Physical::Boolean,
        Some(1) => Physical::Int32,
        Some(2) => Physical::Int64,
        Some(3) => Physical::Int96,
        Some(4) => Physical::Float,
        Some(5) => Physical::Double,
        Some(6) => Physical::ByteArray,
        Some(7) => {
            let len = leaf.type_length.unwrap_or(-1);
            if len <= 0 {
                return Class::Skip("fixed-length missing".into());
            }
            Physical::FixedByteArray(len as u32)
        }
        other => return Class::Skip(format!("physical {other:?}")),
    };
    if column.compressed_size.unwrap_or(0) < 0 || column.data_page_offset.unwrap_or(-1) < 0 {
        return Class::Skip("bad offsets".into());
    }
    Class::Read(physical)
}

#[derive(Clone)]
struct OwnedColumn {
    row_group: u32,
    codec: Option<i32>,
    num_values: Option<i64>,
    data_page_offset: Option<i64>,
    dictionary_page_offset: Option<i64>,
    compressed_size: Option<i64>,
}

impl From<&ColumnChunkInfo<'_>> for OwnedColumn {
    fn from(column: &ColumnChunkInfo<'_>) -> Self {
        Self {
            row_group: column.row_group,
            codec: column.codec,
            num_values: column.num_values,
            data_page_offset: column.data_page_offset,
            dictionary_page_offset: column.dictionary_page_offset,
            compressed_size: column.compressed_size,
        }
    }
}

fn merge_agg(total: &mut Agg, next: &Agg) {
    total.rows += next.rows;
    total.nulls += next.nulls;
    match (&mut total.metric, &next.metric) {
        (Metric::Sum(a), Metric::Sum(b)) => *a += *b,
        (Metric::Bytes(a), Metric::Bytes(b)) => *a += *b,
        (Metric::Bits(a), Metric::Bits(b)) => *a ^= *b,
        _ => panic!("mixed metrics while merging row groups"),
    }
}

fn decode_column(
    file: &mut File,
    column: &OwnedColumn,
    leaf: &Leaf,
    physical: Physical,
) -> Result<Agg, String> {
    let start = match (column.dictionary_page_offset, column.data_page_offset) {
        (Some(dict), Some(data)) => dict.min(data),
        (Some(dict), None) => dict,
        (None, Some(data)) => data,
        (None, None) => return Err("column has no page offset".into()),
    };
    let len = column.compressed_size.unwrap() as usize;
    let mut bytes = vec![0u8; len];
    file.seek(SeekFrom::Start(start as u64))
        .map_err(|e| e.to_string())?;
    file.read_exact(&mut bytes).map_err(|e| e.to_string())?;

    let mut at = 0;
    let mut dict: Vec<Decoded> = Vec::new();
    let mut rows = 0u64;
    let mut nulls = 0u64;
    let mut metric = match physical {
        Physical::ByteArray | Physical::FixedByteArray(_) => Metric::Bytes(0),
        Physical::Float | Physical::Double | Physical::Int96 => Metric::Bits(0),
        Physical::Boolean | Physical::Int32 | Physical::Int64 => Metric::Sum(0),
    };
    let mut saw_page = false;
    while at < bytes.len() {
        let (header, header_len) = compact_parquet_thrift::parquet::page_header(&bytes[at..])
            .map_err(|e| format!("page header at {at}: {e:?}"))?;
        let compressed = usize::try_from(header.compressed_size.ok_or("missing compressed size")?)
            .map_err(|_| "negative compressed size".to_string())?;
        let body_at = at + header_len;
        let body_end = body_at
            .checked_add(compressed)
            .ok_or("page size overflow")?;
        if body_end > bytes.len() {
            return Err(format!(
                "page body {body_at}..{body_end} exceeds column chunk {}",
                bytes.len()
            ));
        }
        let body = &bytes[body_at..body_end];
        let page_rows = decode_page(
            header,
            body,
            leaf,
            physical,
            column.codec.unwrap(),
            &dict,
            &mut metric,
            &mut nulls,
        )
        .map_err(|e| format!("page at {at}: {e}"))?;
        if header.page_type == Some(2) {
            // Dictionary page: its values are the dictionary, not logical rows.
            // decode_page recorded them by replacing `dict` through the return.
            dict = page_rows.dictionary;
        } else {
            rows += page_rows.rows;
        }
        saw_page = true;
        at = body_end;
    }
    if !saw_page && column.num_values.unwrap_or(0) != 0 {
        return Err("column chunk has no pages".into());
    }
    Ok(Agg {
        rows,
        nulls,
        metric,
    })
}

struct PageOut {
    rows: u64,
    dictionary: Vec<Decoded>,
}

#[derive(Clone, Debug)]
enum Decoded {
    Bool(bool),
    I64(i64),
    Bits(u64),
    Bytes(Vec<u8>),
}

fn decode_page(
    header: PageHeader,
    body: &[u8],
    leaf: &Leaf,
    physical: Physical,
    codec: i32,
    dict: &[Decoded],
    metric: &mut Metric,
    nulls: &mut u64,
) -> Result<PageOut, String> {
    let mut spec = ColumnSpec::flat(physical, codec);
    if leaf.max_def == 1 {
        spec = spec.optional();
    }
    spec.max_page_bytes = 256 * 1024 * 1024;
    spec.max_page_values = 64 * 1024 * 1024;
    spec.max_value_bytes = 32 * 1024 * 1024;
    if header.page_type != Some(2) && !dict.is_empty() {
        let len = u32::try_from(dict.len()).map_err(|_| "dictionary longer than u32")?;
        spec = spec.with_dictionary(len);
    }

    let count = page_value_count(&header)?;
    let encoding = header
        .data_v1_encoding
        .or(header.data_v2.and_then(|page| page.encoding))
        .or(header.dictionary_encoding)
        .unwrap_or(0);
    let mut present = vec![0u8; count.div_ceil(8)];
    let mut widths = vec![0u8; 256];
    let mut lengths = Vec::new();
    let mut parts = Vec::new();
    let mut previous = Vec::new();
    if encoding == 6 {
        lengths.resize(count, 0);
    }
    if encoding == 7 {
        parts.resize(count, DeltaByteParts::default());
        previous.resize(1024 * 1024, 0);
    }
    let uncompressed = usize::try_from(header.uncompressed_size.unwrap_or(0)).unwrap_or(0);
    let mut history = vec![0u8; uncompressed.max(1)];
    let mut output = vec![0u8; 64 * 1024];
    let workspace = DecodeWorkspace {
        present: &mut present,
        widths: &mut widths,
        lengths: &mut lengths,
        parts: &mut parts,
        previous: &mut previous,
    };

    let mut rows = 0u64;
    let mut dictionary = Vec::new();
    let mut building: Option<Build> = None;
    let is_dict = header.page_type == Some(2);
    let mut sink = |event: PageEvent<'_>| {
        absorb(
            event,
            is_dict,
            dict,
            metric,
            nulls,
            &mut rows,
            &mut dictionary,
            &mut building,
        );
    };

    if codec == 0 {
        let mut identity = Identity::default();
        drive(&header, body, spec, workspace, &mut identity, &mut sink)?;
    } else {
        let mut snappy = RawSnappy::new(&mut history, &mut output).map_err(|e| format!("{e:?}"))?;
        drive(&header, body, spec, workspace, &mut snappy, &mut sink)?;
    }
    if building.is_some() {
        return Err("byte array missing end".into());
    }
    Ok(PageOut { rows, dictionary })
}

fn drive<C, F>(
    header: &PageHeader,
    body: &[u8],
    spec: ColumnSpec,
    workspace: DecodeWorkspace<'_>,
    codec: &mut C,
    sink: &mut F,
) -> Result<(), String>
where
    C: compact_parquet_thrift::pages::Inflate,
    F: for<'a> PageSink<'a>,
{
    let mut page =
        PageAssembler::new(*header, spec, workspace, codec).map_err(|e| format!("{e:?}"))?;
    let used = page.feed(body, sink).map_err(|e| format!("{e:?}"))?;
    if used != body.len() {
        return Err(format!("page consumed {used} of {} body bytes", body.len()));
    }
    page.finish(sink).map_err(|e| format!("{e:?}"))?;
    Ok(())
}

struct Build {
    length: u32,
    seen: usize,
    bytes: Option<Vec<u8>>,
}

fn absorb(
    event: PageEvent<'_>,
    is_dict: bool,
    dict: &[Decoded],
    metric: &mut Metric,
    nulls: &mut u64,
    rows: &mut u64,
    dictionary: &mut Vec<Decoded>,
    building: &mut Option<Build>,
) {
    let store = |value: Decoded,
                 is_dict: bool,
                 dictionary: &mut Vec<Decoded>,
                 metric: &mut Metric,
                 rows: &mut u64| {
        if is_dict {
            dictionary.push(value);
        } else {
            add_metric(metric, &value);
            *rows += 1;
        }
    };
    match event {
        PageEvent::Null(row) => {
            assert!(!is_dict, "dictionary page emitted a null");
            assert_eq!(row as u64, *rows, "null row index");
            *nulls += 1;
            *rows += 1;
        }
        PageEvent::Value(location, value) => {
            let row = match location {
                Location::Row(row) | Location::Dictionary(row) => row,
            };
            match value {
                Value::BytesStart(length) => {
                    assert!(building.is_none(), "nested byte array");
                    if !is_dict {
                        assert_eq!(row as u64, *rows, "byte row index");
                    }
                    *building = Some(Build {
                        length,
                        seen: 0,
                        bytes: is_dict.then(Vec::new),
                    });
                }
                Value::BytesChunk(chunk) => {
                    let build = building.as_mut().expect("chunk without start");
                    build.seen += chunk.len();
                    if let Some(bytes) = &mut build.bytes {
                        bytes.extend_from_slice(chunk);
                    }
                }
                Value::BytesEnd => {
                    let build = building.take().expect("end without start");
                    assert_eq!(build.seen, build.length as usize, "byte array length");
                    if is_dict {
                        store(
                            Decoded::Bytes(build.bytes.unwrap_or_default()),
                            true,
                            dictionary,
                            metric,
                            rows,
                        );
                    } else if let Metric::Bytes(sum) = metric {
                        *sum += i128::from(build.length);
                        *rows += 1;
                    } else {
                        panic!("byte array in a non-byte column");
                    }
                }
                Value::DictionaryId(id) => {
                    assert!(!is_dict, "dictionary page emitted an id");
                    assert_eq!(row as u64, *rows, "dictionary-id row index");
                    let value = dict.get(id as usize).unwrap_or_else(|| {
                        panic!("dictionary id {id} out of range {}", dict.len())
                    });
                    add_metric(metric, value);
                    *rows += 1;
                }
                Value::Boolean(v) => store(Decoded::Bool(v), is_dict, dictionary, metric, rows),
                Value::I32(v) => store(
                    Decoded::I64(i64::from(v)),
                    is_dict,
                    dictionary,
                    metric,
                    rows,
                ),
                Value::I64(v) => store(Decoded::I64(v), is_dict, dictionary, metric, rows),
                Value::F32(v) => store(
                    Decoded::Bits(u64::from(v.to_bits())),
                    is_dict,
                    dictionary,
                    metric,
                    rows,
                ),
                Value::F64(v) => store(
                    Decoded::Bits(v.to_bits()),
                    is_dict,
                    dictionary,
                    metric,
                    rows,
                ),
                Value::Int96(Int96(bytes)) => {
                    let mut bits = 0u64;
                    for (i, b) in bytes.iter().enumerate() {
                        bits ^= u64::from(*b) << ((i % 8) * 8);
                    }
                    store(Decoded::Bits(bits), is_dict, dictionary, metric, rows);
                }
            }
        }
    }
}

fn add_metric(metric: &mut Metric, value: &Decoded) {
    match (metric, value) {
        (Metric::Sum(sum), Decoded::I64(v)) => *sum += i128::from(*v),
        (Metric::Sum(sum), Decoded::Bool(v)) => *sum += i128::from(*v),
        (Metric::Bytes(sum), Decoded::Bytes(bytes)) => *sum += i128::from(bytes.len() as u64),
        (Metric::Bits(bits), Decoded::Bits(v)) => *bits ^= *v,
        (metric, value) => panic!("metric {metric:?} does not accept {value:?}"),
    }
}

fn page_value_count(header: &PageHeader) -> Result<usize, String> {
    let count = match header.page_type {
        Some(0) => header.data_v1_num_values,
        Some(2) => header.dictionary_num_values,
        Some(3) => header.data_v2.and_then(|v| v.num_values),
        other => return Err(format!("unsupported page type {other:?}")),
    };
    usize::try_from(count.ok_or("page is missing its value count")?)
        .map_err(|_| "negative page count".into())
}

fn schema_leaves(footer: &[u8]) -> Result<Vec<Leaf>, String> {
    let mut reader = Reader::new(footer);
    let mut root = reader.structure().map_err(|e| format!("schema: {e:?}"))?;
    let mut elements = Vec::new();
    while let Some(field) = root.next_field().map_err(|e| format!("schema: {e:?}"))? {
        if field.id != 2 {
            root.skip(field.kind)
                .map_err(|e| format!("schema skip: {e:?}"))?;
            continue;
        }
        field.expect(WireType::List).map_err(|e| format!("{e:?}"))?;
        let list = root.list_header().map_err(|e| format!("{e:?}"))?;
        if list.element != WireType::Struct {
            return Err("schema list is not structs".into());
        }
        for _ in 0..list.len {
            elements.push(read_schema_element(&mut root)?);
        }
    }
    if elements.is_empty() {
        return Err("parquet file has no schema".into());
    }
    let mut leaves = Vec::new();
    let mut index = 0;
    walk_schema(&elements, &mut index, 0, 0, true, &mut leaves)?;
    if index != elements.len() {
        return Err(format!(
            "schema walk consumed {index} of {} elements",
            elements.len()
        ));
    }
    Ok(leaves)
}

struct SchemaElement {
    name: String,
    physical: Option<i32>,
    type_length: Option<i32>,
    repetition: Option<i32>,
    num_children: Option<i32>,
}

fn read_schema_element(reader: &mut Reader<'_>) -> Result<SchemaElement, String> {
    let mut element = reader.structure().map_err(|e| format!("{e:?}"))?;
    let mut out = SchemaElement {
        name: String::new(),
        physical: None,
        type_length: None,
        repetition: None,
        num_children: None,
    };
    while let Some(field) = element.next_field().map_err(|e| format!("{e:?}"))? {
        match field.id {
            1 => {
                field.expect(WireType::I32).map_err(|e| format!("{e:?}"))?;
                out.physical = Some(element.i32().map_err(|e| format!("{e:?}"))?);
            }
            2 => {
                field.expect(WireType::I32).map_err(|e| format!("{e:?}"))?;
                out.type_length = Some(element.i32().map_err(|e| format!("{e:?}"))?);
            }
            3 => {
                field.expect(WireType::I32).map_err(|e| format!("{e:?}"))?;
                out.repetition = Some(element.i32().map_err(|e| format!("{e:?}"))?);
            }
            4 => {
                field
                    .expect(WireType::Binary)
                    .map_err(|e| format!("{e:?}"))?;
                let name = element.binary().map_err(|e| format!("{e:?}"))?;
                out.name =
                    String::from_utf8(name.to_vec()).map_err(|_| "schema name is not utf-8")?;
            }
            5 => {
                field.expect(WireType::I32).map_err(|e| format!("{e:?}"))?;
                out.num_children = Some(element.i32().map_err(|e| format!("{e:?}"))?);
            }
            _ => element.skip(field.kind).map_err(|e| format!("{e:?}"))?,
        }
    }
    Ok(out)
}

fn walk_schema(
    elements: &[SchemaElement],
    index: &mut usize,
    def: u8,
    rep: u8,
    is_root: bool,
    leaves: &mut Vec<Leaf>,
) -> Result<(), String> {
    let element = elements.get(*index).ok_or("schema ended early")?;
    *index += 1;
    let mut def = def;
    let mut rep = rep;
    if !is_root {
        match element.repetition {
            Some(1) => def = def.checked_add(1).ok_or("definition level overflow")?,
            Some(2) => {
                def = def.checked_add(1).ok_or("definition level overflow")?;
                rep = rep.checked_add(1).ok_or("repetition level overflow")?;
            }
            Some(0) | None => {}
            Some(other) => return Err(format!("repetition type {other}")),
        }
    }
    let children = element.num_children.unwrap_or(0);
    if children == 0 {
        leaves.push(Leaf {
            name: element.name.clone(),
            physical: element.physical,
            type_length: element.type_length,
            max_def: def,
            max_rep: rep,
        });
        return Ok(());
    }
    for _ in 0..children {
        walk_schema(elements, index, def, rep, false, leaves)?;
    }
    Ok(())
}

fn check_duckdb(path: &Path, columns: &[Compared]) -> Result<(), String> {
    let types = duckdb_types(path)?;
    let mut selects = Vec::new();
    let mut expected = Vec::new();
    for (i, column) in columns.iter().enumerate() {
        let Some(agg) = &column.agg else { continue };
        let quoted = quote(&column.name);
        let nulls = format!("n{i}");
        selects.push(format!(
            "count(*) FILTER (WHERE {quoted} IS NULL)::VARCHAR AS \"{nulls}\""
        ));
        expected.push((nulls, agg.nulls.to_string()));
        let kind = types.get(&column.name).map(String::as_str).unwrap_or("");
        match &agg.metric {
            Metric::Sum(sum) if is_integer_type(kind) => {
                let alias = format!("s{i}");
                selects.push(format!(
                    "coalesce(sum(CAST({quoted} AS HUGEINT)), 0)::VARCHAR AS \"{alias}\""
                ));
                expected.push((alias, sum.to_string()));
            }
            Metric::Bytes(sum) => {
                let expr = byte_length_expr(kind, &quoted);
                if expr.is_empty() {
                    continue;
                }
                let alias = format!("b{i}");
                selects.push(format!("coalesce(sum({expr}), 0)::VARCHAR AS \"{alias}\""));
                expected.push((alias, sum.to_string()));
            }
            Metric::Sum(_) | Metric::Bits(_) => {}
        }
    }
    if selects.is_empty() {
        // Still check the file is readable and the row count matches the first column.
        let sql = format!(
            "SELECT count(*)::VARCHAR FROM read_parquet('{}')",
            path.display().to_string().replace('\'', "''")
        );
        let out = run_duckdb(&sql)?;
        let count: u64 = out
            .trim()
            .parse()
            .map_err(|_| format!("duckdb count: {out}"))?;
        if let Some(agg) = columns.iter().find_map(|c| c.agg.as_ref()) {
            if agg.rows != count {
                return Err(format!(
                    "row count {count}, first decoded column has {}",
                    agg.rows
                ));
            }
        }
        return Ok(());
    }
    let sql = format!(
        "SELECT {} FROM read_parquet('{}')",
        selects.join(", "),
        path.display().to_string().replace('\'', "''")
    );
    let output = run_duckdb(&sql)?;
    let mut got = std::collections::HashMap::<String, String>::new();
    for line in output.lines() {
        if let Some((key, value)) = line.split_once('\t') {
            got.insert(key.to_string(), value.trim().to_string());
        }
    }
    for (alias, want) in expected {
        let have = got
            .get(&alias)
            .ok_or_else(|| format!("duckdb omitted {alias}"))?;
        if have != &want {
            return Err(format!("{alias}: duckdb {have}, decoder {want}"));
        }
    }
    Ok(())
}

fn duckdb_types(path: &Path) -> Result<std::collections::HashMap<String, String>, String> {
    let sql = format!(
        "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('{}'))",
        path.display().to_string().replace('\'', "''")
    );
    let output = Command::new(DUCKDB)
        .args(["-csv", "-c", &sql])
        .output()
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut types = std::collections::HashMap::new();
    for line in text.lines().skip(1) {
        if line.is_empty() {
            continue;
        }
        let fields = parse_csv(line);
        if fields.len() != 2 {
            return Err(format!("describe row: {line}"));
        }
        types.insert(fields[0].clone(), fields[1].clone());
    }
    Ok(types)
}

fn is_integer_type(kind: &str) -> bool {
    matches!(
        kind,
        "BOOLEAN"
            | "TINYINT"
            | "SMALLINT"
            | "INTEGER"
            | "BIGINT"
            | "HUGEINT"
            | "UTINYINT"
            | "USMALLINT"
            | "UINTEGER"
            | "UBIGINT"
    )
}

fn byte_length_expr(kind: &str, quoted: &str) -> String {
    if kind == "BLOB" {
        format!("octet_length({quoted})")
    } else if kind == "VARCHAR" {
        format!("(bit_length({quoted}) // 8)")
    } else {
        String::new()
    }
}

fn parse_csv(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut chars = line.chars().peekable();
    let mut quoted = false;
    while let Some(ch) = chars.next() {
        if quoted {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    current.push('"');
                } else {
                    quoted = false;
                }
            } else {
                current.push(ch);
            }
        } else if ch == '"' && current.is_empty() {
            quoted = true;
        } else if ch == ',' {
            fields.push(std::mem::take(&mut current));
        } else {
            current.push(ch);
        }
    }
    fields.push(current);
    fields
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn run_duckdb(sql: &str) -> Result<String, String> {
    let output = Command::new(DUCKDB)
        .args(["-csv", "-c", sql])
        .output()
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).trim().to_string());
    }
    // csv mode still prints a header. Convert to alias\tvalue by transposing one row.
    let text = String::from_utf8_lossy(&output.stdout);
    let mut lines = text.lines();
    let header = lines.next().unwrap_or("");
    let row = lines.next().unwrap_or("");
    let mut paired = String::new();
    for (name, value) in header.split(',').zip(row.split(',')) {
        paired.push_str(name.trim_matches('"'));
        paired.push('\t');
        paired.push_str(value.trim_matches('"'));
        paired.push('\n');
    }
    Ok(paired)
}
