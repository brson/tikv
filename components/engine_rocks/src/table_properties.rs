// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine::rocks::{self, Range};
use engine_traits::{TablePropertiesExt, TablePropertiesCollection};
use engine_traits::Result;
use crate::db::Rocks;

impl TablePropertiesExt for Rocks {
    type TablePropertiesCollection = RocksTablePropertiesCollection;

    fn get_range_properties_cf(&self, cfname: &str, start_key: &[u8], end_key: &[u8]) -> Result<RocksTablePropertiesCollection> {
        let db = self.as_inner();
        let cf = rocks::util::get_cf_handle(db, cfname)?;
        let range = Range::new(start_key, end_key);
        db.get_properties_of_tables_in_range(cf, &[range])
            .map_err(|e| e.into())
    }
}

pub struct RocksTablePropertiesCollection {
}

impl TablePropertiesCollection for RocksTablePropertiesCollection {
}
