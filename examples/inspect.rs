//! cargo run --example inspect -- file.parquet
//! Only the 8-byte trailer and footer are loaded, not the entire Parquet file.

use compact_parquet_thrift::parquet::{footer_range, scan_footer};
use std::{
    env,
    fs::File,
    io::{Read, Seek, SeekFrom},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args().nth(1).expect("usage: inspect FILE.parquet");
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    let mut trailer = [0u8; 8];
    file.seek(SeekFrom::End(-8))?;
    file.read_exact(&mut trailer)?;
    let range = footer_range(size, &trailer).expect("invalid Parquet trailer");

    let mut footer = vec![0u8; (range.end - range.start) as usize];
    file.seek(SeekFrom::Start(range.start))?;
    file.read_exact(&mut footer)?;

    let summary = scan_footer(&footer, |col| {
        let name = col.flat_name
            .and_then(|s| std::str::from_utf8(s).ok())
            .unwrap_or("<nested>");
        println!(
            "rg={} col={} name={} codec={:?} data_offset={:?} dictionary_offset={:?} compressed_size={:?}",
            col.row_group, col.index, name, col.codec, col.data_page_offset,
            col.dictionary_page_offset, col.compressed_size,
        );
        Ok(())
    }).expect("invalid Thrift footer");

    println!(
        "rows={:?} row_groups={} columns={}",
        summary.num_rows, summary.row_groups, summary.columns
    );
    Ok(())
}
