// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::AgateEngine;
use engine_traits::{Result, TtlProperties, TtlPropertiesExt};

impl TtlPropertiesExt for AgateEngine {
    fn get_range_ttl_properties_cf(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(String, TtlProperties)>> {
        panic!()
    }
}
