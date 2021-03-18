// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::AgateEngine;
use engine_traits::CFNamesExt;

impl CFNamesExt for AgateEngine {
    fn cf_names(&self) -> Vec<&str> {
        panic!()
    }
}
