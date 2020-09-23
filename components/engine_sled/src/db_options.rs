// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SledEngine;
use engine_traits::Result;
use engine_traits::{DBOptions, DBOptionsExt, TitanDBOptions};

impl DBOptionsExt for SledEngine {
    type DBOptions = SledDBOptions;

    fn get_db_options(&self) -> Self::DBOptions {
        panic!()
    }
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()> {
        panic!()
    }
}

pub struct SledDBOptions;

impl DBOptions for SledDBOptions {
    type TitanDBOptions = SledTitanDBOptions;

    fn new() -> Self {
        panic!()
    }

    fn get_max_background_jobs(&self) -> i32 {
        panic!()
    }

    fn get_rate_bytes_per_sec(&self) -> Option<i64> {
        panic!()
    }

    fn set_rate_bytes_per_sec(&mut self, rate_bytes_per_sec: i64) -> Result<()> {
        panic!()
    }

    fn set_titandb_options(&mut self, opts: &Self::TitanDBOptions) {
        panic!()
    }
}

pub struct SledTitanDBOptions;

impl TitanDBOptions for SledTitanDBOptions {
    fn new() -> Self {
        panic!()
    }
    fn set_min_blob_size(&mut self, size: u64) {
        panic!()
    }
}
