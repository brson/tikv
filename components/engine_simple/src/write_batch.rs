// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SimpleEngine;
use engine_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions};

use std::cell::RefCell;
use futures::executor::block_on;
use crate::error::ResultExt;
use engine_traits::CF_DEFAULT;

impl WriteBatchExt for SimpleEngine {
    type WriteBatch = SimpleWriteBatch;
    type WriteBatchVec = SimpleWriteBatch;

    const WRITE_BATCH_MAX_KEYS: usize = 1;

    fn support_write_batch_vec(&self) -> bool {
        panic!()
    }

    fn write_batch(&self) -> Self::WriteBatch {
        let batch = self.db.write_batch();
        SimpleWriteBatch(RefCell::new(Some(batch)))
    }
    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        panic!()
    }
}

pub struct SimpleWriteBatch(RefCell<Option<blocksy2::WriteBatch>>);

impl WriteBatch<SimpleEngine> for SimpleWriteBatch {
    fn with_capacity(_: &SimpleEngine, _: usize) -> Self {
        panic!()
    }

    fn write_opt(&self, _: &WriteOptions) -> Result<()> {
        let batch = self.0.borrow_mut().take().expect("batch");
        block_on(batch.commit()).engine_result()
    }

    fn write(&self) -> Result<()> {
        self.write_opt(&WriteOptions::default())
    }
}

impl Mutable for SimpleWriteBatch {
    fn data_size(&self) -> usize {
        panic!()
    }
    fn count(&self) -> usize {
        panic!()
    }
    fn is_empty(&self) -> bool {
        panic!()
    }
    fn should_write_to_engine(&self) -> bool {
        panic!()
    }

    fn clear(&mut self) {
        panic!()
    }
    fn set_save_point(&mut self) {
        panic!()
    }
    fn pop_save_point(&mut self) -> Result<()> {
        panic!()
    }
    fn rollback_to_save_point(&mut self) -> Result<()> {
        panic!()
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, value)
    }
    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let batch = self.0.borrow();
        let batch = batch.as_ref().expect("batch");
        let tree = batch.tree(cf);
        tree.write(key, value);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }
    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        let batch = self.0.borrow();
        let batch = batch.as_ref().expect("batch");
        let tree = batch.tree(cf);
        tree.delete(key);
        Ok(())
    }
    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        panic!()
    }
}
