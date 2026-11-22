//! Incrementally find Thrift page headers and bound compressed page bodies.
//! Header bytes live in a caller-owned buffer; body fragments borrow input.

use super::{Error, Result};
use crate::{
    parquet::{page_header, PageHeader},
    Error as ThriftError,
};

#[derive(Debug, Clone, Copy)]
pub enum FrameEvent<'a> {
    Start(PageHeader),
    Body(&'a [u8]),
    End,
}

#[derive(Debug, Clone, Copy)]
enum State {
    Header,
    Body(usize),
}

pub struct PageFramer<'a> {
    header: &'a mut [u8],
    filled: usize,
    state: State,
    max_payload: usize,
}

impl<'a> PageFramer<'a> {
    pub fn new(header: &'a mut [u8], max_payload: usize) -> Self {
        Self {
            header,
            filled: 0,
            state: State::Header,
            max_payload,
        }
    }

    /// Parses as many complete pages as the offered bytes contain. Header
    /// decoding retries on each byte; headers are small and bounded, whereas
    /// body fragments are passed through as whole borrowed slices.
    pub fn feed<'input>(
        &mut self,
        input: &'input [u8],
        mut emit: impl FnMut(FrameEvent<'input>) -> Result<()>,
    ) -> Result<usize> {
        let mut at = 0;
        while at < input.len() {
            match self.state {
                State::Header => {
                    if self.filled == self.header.len() {
                        return Err(Error::HeaderTooLarge);
                    }
                    self.header[self.filled] = input[at];
                    at += 1;
                    self.filled += 1;
                    match page_header(&self.header[..self.filled]) {
                        Ok((header, used)) => {
                            if used != self.filled {
                                return Err(Error::InvalidHeader);
                            }
                            let payload = usize::try_from(
                                header.compressed_size.ok_or(Error::InvalidHeader)?,
                            )
                            .map_err(|_| Error::InvalidPageSize)?;
                            if payload > self.max_payload {
                                return Err(Error::InvalidPageSize);
                            }
                            emit(FrameEvent::Start(header))?;
                            self.filled = 0;
                            if payload == 0 {
                                emit(FrameEvent::End)?;
                            } else {
                                self.state = State::Body(payload);
                            }
                        }
                        Err(ThriftError::EndOfInput) => (),
                        Err(e) => return Err(Error::Thrift(e)),
                    }
                }
                State::Body(left) => {
                    let n = left.min(input.len() - at);
                    emit(FrameEvent::Body(&input[at..at + n]))?;
                    at += n;
                    if n == left {
                        self.state = State::Header;
                        emit(FrameEvent::End)?;
                    } else {
                        self.state = State::Body(left - n);
                    }
                }
            }
        }
        Ok(at)
    }

    pub fn finish(&self) -> Result<()> {
        match self.state {
            State::Header if self.filled == 0 => Ok(()),
            _ => Err(Error::Truncated),
        }
    }
}
