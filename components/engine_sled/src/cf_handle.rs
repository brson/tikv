// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cf_options::SledColumnFamilyOptions;
use crate::engine::SledEngine;
use engine_traits::{CFHandle, CFHandleExt, Result};

impl CFHandleExt for SledEngine {
    type CFHandle = SledCFHandle;
    type ColumnFamilyOptions = SledColumnFamilyOptions;

    fn cf_handle(&self, name: &str) -> Result<&Self::CFHandle> {
        panic!()
    }
    fn get_options_cf(&self, cf: &Self::CFHandle) -> Self::ColumnFamilyOptions {
        panic!()
    }
    fn set_options_cf(&self, cf: &Self::CFHandle, options: &[(&str, &str)]) -> Result<()> {
        panic!()
    }
}

pub struct SledCFHandle;

impl CFHandle for SledCFHandle {}
