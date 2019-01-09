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

//! This crate controls the global allocator used by TiKV.
//!
//! As of now TiKV always turns on jemalloc on Unix, though libraries generally
//! shouldn't be opinionated about their allocators like this. It's easier to do
//! this in one place than to have all our bins turn it on themselves.
//!
//! Writing `extern crate tikv_alloc;` will link it to jemalloc when
//! appropriate. The tikv library itself links to `tikv_alloc` to
//! ensure 
//!
//! With some exception, _every binary and project in the TiKV workspace
//! should link (perhaps transitively) to tikv_alloc_.` This is to ensure
//! that tests and benchmarks run with the production allocator. In other
//! words, binaries and projects that don't link to `tikv` should link
//! to `tikv_alloc`.
//!
//! At present all Unixes use jemalloc, and other systems don't.
//! TODO
//! cfg `fuzzing` is defined by `run_libfuzzer` in `fuzz/cli.rs` and is passed
//! to rustc directly with `--cfg`; in other words it's not controlled through a
//! crate feature.
//!
//! Where jemalloc-specific code needs conditional compilation the correct
//! attribute is `#[cfg(all(unix, not(fuzzing)))]`. Any such code is a good
//! candidate to move into this crate though.

#[cfg(all(unix, not(fuzzing)))]
extern crate jemallocator;
extern crate libc;

#[cfg(all(unix, not(fuzzing)))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub use self::impl_::*;

// The implementation of this crate when jemalloc is turned on
#[cfg(all(unix, not(fuzzing)))]
mod impl_ {
    use jemallocator::ffi::malloc_stats_print;
    use libc::{self, c_char, c_void};
    use std::{ptr, slice};

    pub fn dump_stats() -> String {
        let mut buf = Vec::with_capacity(1024);
        unsafe {
            malloc_stats_print(
                write_cb,
                &mut buf as *mut Vec<u8> as *mut c_void,
                ptr::null(),
            )
        }
        String::from_utf8_lossy(&buf).into_owned()
    }

    extern "C" fn write_cb(printer: *mut c_void, msg: *const c_char) {
        unsafe {
            let buf = &mut *(printer as *mut Vec<u8>);
            let len = libc::strlen(msg);
            let bytes = slice::from_raw_parts(msg as *const u8, len);
            buf.extend_from_slice(bytes);
        }
    }
}

#[cfg(not(all(unix, not(fuzzing))))]
mod impl_ {
    pub fn dump_stats() -> String { String::empty() }
}

// TODO FIXME test me, write better tests
mod test {

    #[cfg(test)]
    mod tests {
        #[test]
        fn test_dump_stats() {
            // just dump the data, ensure it doesn't core.
            super::dump_stats();
        }
    }
}

