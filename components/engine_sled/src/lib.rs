// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(unused)]

mod cf_handle;
pub use crate::cf_handle::*;
mod cf_names;
pub use crate::cf_names::*;
mod cf_options;
pub use crate::cf_options::*;
mod compact;
pub use crate::compact::*;
mod db_options;
pub use crate::db_options::*;
mod db_vector;
pub use crate::db_vector::*;
mod engine;
pub use crate::engine::*;
mod import;
pub use import::*;
mod misc;
pub use crate::misc::*;
mod snapshot;
pub use crate::snapshot::*;
mod sst;
pub use crate::sst::*;
mod table_properties;
pub use crate::table_properties::*;
mod write_batch;
pub use crate::write_batch::*;
pub mod range_properties;
pub use crate::range_properties::*;
pub mod mvcc_properties;
pub use crate::mvcc_properties::*;

mod raft_engine;

// FIXME this is a temporary reexport so that engine_test can construct sled engines
pub use sled as raw;

/// Convert from sled::Result to engine_traits::Result
pub trait EngineResult<T> {
    fn engine_result(self) -> engine_traits::Result<T>;
}

impl<T> EngineResult<T> for sled::Result<T> {
    fn engine_result(self) -> engine_traits::Result<T> {
        self.map_err(EngineError::engine_error)
    }
}

/// Convert from sled::Error to engine_traits::Error
pub trait EngineError {
    fn engine_error(self) -> engine_traits::Error;
}

impl EngineError for sled::Error {
    fn engine_error(self) -> engine_traits::Error {
        let error_string = self.to_string();
        match self {
            sled::Error::CollectionNotFound(v) => {
                let name = String::from_utf8_lossy(&v).to_string();
                engine_traits::Error::CFName(name)
            }
            sled::Error::Unsupported(_) => {
                engine_traits::Error::Engine(error_string)
            }
            sled::Error::ReportableBug(_) => {
                engine_traits::Error::Engine(error_string)
            }
            sled::Error::Io(e) => {
                engine_traits::Error::Io(e)
            }
            sled::Error::Corruption {..} => {
                engine_traits::Error::Engine(error_string)
            }
        }
    }
}


