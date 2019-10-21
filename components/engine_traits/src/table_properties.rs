// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;

pub trait TablePropertiesExt {
    type TablePropertiesCollection: TablePropertiesCollection;

    fn get_range_properties_cf(&self, cfname: &str, start_key: &[u8], end_key: &[u8]) -> Result<Self::TablePropertiesCollection>;
}

pub trait TablePropertiesCollection {
}

