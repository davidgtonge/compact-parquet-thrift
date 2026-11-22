//! Incrementally find Thrift page headers and bound compressed page bodies.
//! Header bytes live in a caller-owned buffer; body fragments borrow input.

use super::{Error, Result};
use crate::{
    parquet::{page_header, PageHeader},
    Error as ThriftError,
};

/// One step of the page byte stream.
#[derive(Debug, Clone, Copy)]
pub enum FrameEvent<'a> {
    /// A complete page header; its body follows.
    Start(PageHeader),
    /// A fragment of the current page's compressed body.
    Body(&'a [u8]),
    /// The current page's body is complete.
    End,
}

#[derive(Debug, Clone, Copy)]
enum State {
    Header,
    Body(usize),
}

/// Splits a column chunk's byte stream into page headers and bodies.
pub struct PageFramer<'a> {
    header: &'a mut [u8],
    filled: usize,
    state: State,
    max_payload: usize,
}

impl<'a> PageFramer<'a> {
    /// A framer buffering headers in `header` and rejecting bodies larger
    /// than `max_payload`.
    #[must_use]
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
    ///
    /// # Errors
    ///
    /// [`Error::HeaderTooLarge`] if a header outgrows the buffer,
    /// [`Error::InvalidHeader`] or [`Error::InvalidPageSize`] for a bad
    /// header, a Thrift error, or an error returned by `emit`.
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

    /// Check that the stream ended on a page boundary.
    ///
    /// # Errors
    ///
    /// [`Error::Truncated`] inside a header or body.
    pub fn finish(&self) -> Result<()> {
        match self.state {
            State::Header if self.filled == 0 => Ok(()),
            _ => Err(Error::Truncated),
        }
    }
}
