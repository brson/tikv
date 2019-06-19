// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

pub mod bytes;
pub mod number;

use crate::lossy_clone_io_error;
use std::io::{self, ErrorKind};

pub type BytesSlice<'a> = &'a [u8];

#[inline]
pub fn read_slice<'a>(data: &mut BytesSlice<'a>, size: usize) -> Result<BytesSlice<'a>> {
    if data.len() >= size {
        let buf = &data[0..size];
        *data = &data[size..];
        Ok(buf)
    } else {
        Err(Error::unexpected_eof())
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        KeyLength {description("bad format key(length)")}
        KeyPadding {description("bad format key(padding)")}
        KeyNotFound {description("key not found")}
    }
}

impl Error {
    pub fn lossy_clone(&self) -> Error {
        match *self {
            Error::KeyLength => Error::KeyLength,
            Error::KeyPadding => Error::KeyPadding,
            Error::KeyNotFound => Error::KeyNotFound,
            Error::Io(ref e) => Error::Io(lossy_clone_io_error(e)),
        }
    }
}

impl Error {
    pub fn unexpected_eof() -> Error {
        Error::Io(io::Error::new(ErrorKind::UnexpectedEof, "eof"))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
