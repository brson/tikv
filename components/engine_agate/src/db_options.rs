// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::AgateEngine;
use engine_traits::Result;
use engine_traits::{DBOptions, DBOptionsExt, TitanDBOptions};

impl DBOptionsExt for AgateEngine {
    type DBOptions = AgateDBOptions;

    fn get_db_options(&self) -> Self::DBOptions {
        panic!()
    }
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()> {
        panic!()
    }
}

pub struct AgateDBOptions;

impl DBOptions for AgateDBOptions {
    type TitanDBOptions = AgateTitanDBOptions;

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

    fn get_rate_limiter_auto_tuned(&self) -> Option<bool> {
        panic!()
    }

    fn set_rate_limiter_auto_tuned(&mut self, rate_limiter_auto_tuned: bool) -> Result<()> {
        panic!()
    }

    fn set_titandb_options(&mut self, opts: &Self::TitanDBOptions) {
        panic!()
    }
}

pub struct AgateTitanDBOptions;

impl TitanDBOptions for AgateTitanDBOptions {
    fn new() -> Self {
        panic!()
    }
    fn set_min_blob_size(&mut self, size: u64) {
        panic!()
    }
}
