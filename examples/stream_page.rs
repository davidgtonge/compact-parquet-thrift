//! cargo run --example `stream_page`
//! A page header followed by nullable INT32 values, decoded in 2-byte chunks.

use compact_parquet_thrift::pages::{
    ColumnSpec, DecodeWorkspace, Identity, Location, PageAssembler, PageEvent, Physical, Value,
};
use compact_parquet_thrift::values::DeltaByteParts;
use compact_parquet_thrift::{parquet::page_header, SliceSink, Writer};

fn main() {
    // V1 definition levels [1, 0, 1] (two present values).
    let mut body = vec![6, 0, 0, 0, 2, 1, 2, 0, 2, 1];
    body.extend(10i32.to_le_bytes());
    body.extend(30i32.to_le_bytes());

    let mut bytes = [0u8; 128];
    let mut writer = Writer::new(SliceSink::new(&mut bytes));
    writer
        .structure(|w| {
            w.field_i32(1, 0)?; // DATA_PAGE
            let len = i32::try_from(body.len()).expect("small page");
            w.field_i32(2, len)?;
            w.field_i32(3, len)?;
            w.field_struct(5, |w| {
                w.field_i32(1, 3)?; // num_values (including NULLs)
                w.field_i32(2, 0)?; // PLAIN
                w.field_i32(3, 3)?; // definition levels: RLE
                w.field_i32(4, 3) // repetition levels: RLE
            })
        })
        .unwrap();
    let encoded_header = writer.into_inner();
    let (header, _) = page_header(encoded_header.as_written()).unwrap();

    let mut present = [0u8; 1];
    let mut widths = [0u8; 4];
    let mut lengths: [u32; 0] = [];
    let mut parts: [DeltaByteParts; 0] = [];
    let mut previous: [u8; 0] = [];
    let mut codec = Identity::default();
    let ws = DecodeWorkspace {
        present: &mut present,
        widths: &mut widths,
        lengths: &mut lengths,
        parts: &mut parts,
        previous: &mut previous,
    };
    let mut page = PageAssembler::new(
        header,
        ColumnSpec::flat(Physical::Int32, 0).optional(),
        ws,
        &mut codec,
    )
    .unwrap();

    let mut emit = |e: PageEvent<'_>| match e {
        PageEvent::Null(row) => println!("row {row}: null"),
        PageEvent::Value(Location::Row(row), Value::I32(value)) => {
            println!("row {row}: {value}");
        }
        PageEvent::Value(..) => unreachable!(),
    };
    for fragment in body.chunks(2) {
        assert_eq!(page.feed(fragment, &mut emit).unwrap(), fragment.len());
    }
    page.finish(&mut emit).unwrap();
}
