#![feature(test)]
#![feature(mem_take)]

#[macro_use]
extern crate bitflags;
#[macro_use] #[cfg(test)]
extern crate derive_more;
#[macro_use]
extern crate fail;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog_global;
#[cfg(test)]
extern crate test;
#[macro_use]
extern crate tikv_util;

pub mod coprocessor;
pub mod errors;
pub mod store;

pub use errors::{Error, Result};
