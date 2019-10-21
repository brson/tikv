// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{TablePropertiesExt, TablePropertiesCollection};
use engine_traits::Result;
use crate::db::Rocks;

impl TablePropertiesExt for Rocks {
    type TablePropertiesCollection = RocksTablePropertiesCollection;

    fn get_range_properties_cf(&self, _cfname: &str, _start_key: &[u8], _end_key: &[u8]) -> Result<RocksTablePropertiesCollection> {
        panic!()
    }
}

pub struct RocksTablePropertiesCollection {
}

impl TablePropertiesCollection for RocksTablePropertiesCollection {
}
