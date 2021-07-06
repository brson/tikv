// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SimpleEngine;
use engine_traits::CFNamesExt;

impl CFNamesExt for SimpleEngine {
    fn cf_names(&self) -> Vec<&str> {
        self.tree_names.iter().map(AsRef::as_ref).collect()
    }
}
