//! Allocation-free Thrift Compact codec and bounded-memory Parquet streaming.
//!
//! `no_std`, no `unsafe`, no dependencies. Every layer borrows its input and
//! writes into caller-owned buffers, so it runs unchanged in WebAssembly with
//! a fixed memory budget.
//!
//! | Module | Purpose |
//! | --- | --- |
//! | crate root | Thrift Compact [`Reader`] and [`Writer`] |
//! | [`parquet`] | Footer location, footer scan and page header parsing |
//! | [`pages`] | Page framing, Snappy, CRC-32 and flat column decoding |
//! | [`values`] | Resumable PLAIN, RLE, dictionary and delta decoders |
//! | [`encode`] | Parquet file, page and footer encoder |
//!
//! Decoders accept input split at any byte boundary and report malformed or
//! oversized data as errors; they do not panic on untrusted input.
//!
//! # Example
//!
//! Write a Thrift struct `{ 1: i32 = 123, 2: bool = true }` and read it back:
//!
//! ```
//! use compact_parquet_thrift::{Reader, SliceSink, WireType, Writer};
//!
//! let mut output = [0u8; 32];
//! let mut writer = Writer::new(SliceSink::new(&mut output));
//! writer.structure(|w| {
//!     w.field_i32(1, 123)?;
//!     w.field_bool(2, true)
//! })?;
//! let written = writer.into_inner();
//!
//! let mut reader = Reader::new(written.as_written());
//! let mut fields = reader.structure()?;
//! let first = fields.next_field()?.ok_or(compact_parquet_thrift::Error::EndOfInput)?;
//! assert_eq!((first.id, first.kind), (1, WireType::I32));
//! assert_eq!(fields.i32()?, 123);
//! assert_eq!(fields.next_field()?.map(|f| f.bool_value()), Some(Ok(true)));
//! assert_eq!(fields.next_field()?, None);
//! # Ok::<(), compact_parquet_thrift::Error>(())
//! ```
//!
//! Reading a Parquet footer takes the last eight bytes of the object, then
//! the footer range they point at:
//!
//! ```
//! use compact_parquet_thrift::parquet::{footer_range, scan_footer};
//!
//! # fn read(range: core::ops::Range<u64>) -> Vec<u8> { vec![0x15, 0x02, 0x00] }
//! let trailer: [u8; 8] = [3, 0, 0, 0, b'P', b'A', b'R', b'1'];
//! let range = footer_range(100, &trailer)?;
//! let footer = read(range); // e.g. an HTTP range request
//! let summary = scan_footer(&footer, |column| {
//!     // offsets, sizes, codec and statistics of one column chunk
//!     let _ = (column.data_page_offset, column.compressed_size);
//!     Ok(())
//! })?;
//! assert_eq!(summary.version, Some(1));
//! # Ok::<(), compact_parquet_thrift::Error>(())
//! ```

#![no_std]
#![deny(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::todo,
    clippy::unimplemented,
    clippy::dbg_macro,
    clippy::print_stdout,
    clippy::print_stderr
)]

pub mod encode;
pub mod pages;
pub mod parquet;
pub mod values;

mod protocol;
mod reader;
mod varint;
mod writer;

pub use protocol::{Error, Result, WireType};
pub use reader::{Field, ListHeader, MapHeader, Reader, StructReader};
pub use writer::{Sink, SliceSink, Writer};
