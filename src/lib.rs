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

//! A small, allocation-free Thrift Compact Protocol codec.
//!
//! Built for Parquet's serialized footers and page headers. Reading borrows
//! from the input; writing uses a caller-supplied sink. No heap, std, unsafe,
//! generated Thrift structs, or third-party crates are required.
//!
//! The codec operates on *available byte slices*. Network / R2 streaming
//! belongs in an outer transport layer; see README.md for the distinction.

pub mod parquet;
mod protocol;
mod reader;
mod varint;
mod writer;

pub use protocol::{Error, Result, WireType};
pub use reader::{Field, ListHeader, MapHeader, Reader, StructReader};
pub use writer::{Sink, SliceSink, Writer};

pub mod values;

pub mod pages;

pub mod encode;
