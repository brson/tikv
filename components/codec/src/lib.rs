// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg_attr(test, feature(test))]

#[cfg(all(unix))]
extern crate jemallocator;
#[macro_use]
extern crate quick_error;
#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate test;

extern crate panic_hook;

mod buffer;
mod error;

pub mod prelude {
    pub use super::buffer::{BufferReader, BufferWriter};
}

pub use self::buffer::{BufferReader, BufferWriter};
pub use self::error::{Error, Result};

// Currently, only crates that link to TiKV use jemalloc, our production
// allocator. This crate has a test, `test_vec_reallocate`, that is testing
// allocator behavior, so we also link to jemalloc.
#[cfg(all(unix, not(fuzzing)))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
