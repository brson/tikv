// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SledEngine;
use engine_traits::CFNamesExt;

impl CFNamesExt for SledEngine {
    fn cf_names(&self) -> Vec<&str> {
        self.inner().cf_map.keys().map(|k| &**k).collect()
    }
}
