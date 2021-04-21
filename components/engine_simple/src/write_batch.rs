// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SimpleEngine;
use engine_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions};

use crate::error::ResultExt;
use engine_traits::CF_DEFAULT;
use futures::executor::block_on;
use std::mem;

impl WriteBatchExt for SimpleEngine {
    type WriteBatch = SimpleWriteBatch;
    type WriteBatchVec = SimpleWriteBatch;

    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn support_write_batch_vec(&self) -> bool {
        panic!()
    }

    fn write_batch(&self) -> Self::WriteBatch {
        SimpleWriteBatch::new(self.db.clone())
    }
    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        SimpleWriteBatch::new(self.db.clone())
    }
}

pub struct SimpleWriteBatch {
    db: blocksy3::Db,
    inner: Option<blocksy3::WriteBatch>,
    save_points: Vec<usize>,
}

impl Drop for SimpleWriteBatch {
    fn drop(&mut self) {
        block_on(async {
            let batch = mem::replace(&mut self.inner, None).expect("batch");
            batch.close().await;
        })
    }
}

impl SimpleWriteBatch {
    fn new(db: blocksy3::Db) -> SimpleWriteBatch {
        block_on(async {
            // FIXME: Need to return Result here
            let batch = db.write_batch().await.engine_result().expect("write batch");;

            SimpleWriteBatch {
                db: db,
                inner: Some(batch),
                save_points: vec![0],
            }
        })
    }
}

impl WriteBatch<SimpleEngine> for SimpleWriteBatch {
    fn with_capacity(engine: &SimpleEngine, _: usize) -> Self {
        SimpleWriteBatch::new(engine.db.clone())
    }

    fn write_opt(&self, _: &WriteOptions) -> Result<()> {
        block_on(async {
            let batch = self.inner.as_ref().expect("batch");
            batch.commit().await.engine_result()?;
            Ok(())
        })
    }

    fn data_size(&self) -> usize {
        // This engine doesn't store write batches in memory
        // the way rocksdb does, but it does keep a command history
        // in memory until the batch is closed.
        //
        // Here we're just loosely estimating the byte size of that
        // history.

        const BOGUS_COMMAND_SIZE: usize = 32;
        self.count().checked_mul(BOGUS_COMMAND_SIZE).expect("overflow")
    }
    fn count(&self) -> usize {
        self.save_points.iter().sum()
    }
    fn is_empty(&self) -> bool {
        self.count() == 0
    }
    fn should_write_to_engine(&self) -> bool {
        self.count() > SimpleEngine::WRITE_BATCH_MAX_KEYS
    }

    fn clear(&mut self) {
        // FIXME: return Result
        let new_batch = SimpleWriteBatch::new(self.db.clone());
        let old_batch = mem::replace(self, new_batch);
        drop(old_batch);
    }
    fn set_save_point(&mut self) {
        block_on(async {
            let batch = self.inner.as_ref().expect("batch");
            // FIXME return Result
            batch.push_save_point().await.engine_result().expect("set save point");
            self.save_points.push(0);
        })
    }
    fn pop_save_point(&mut self) -> Result<()> {
        block_on(async {
            assert!(!self.save_points.is_empty());
            if self.save_points.len() == 1 {
                return Err(blocksy3::anyhow!("save point pop with no save point")).engine_result();
            }
            let batch = self.inner.as_ref().expect("batch");
            batch.pop_save_point().await.engine_result()?;
            // Add command count to previous save point
            let command_count = self.save_points.pop().expect("pop save points");
            let last_save_point = self.save_points.last_mut().expect("save point");
            *last_save_point = last_save_point.checked_add(command_count).expect("overflow");
            Ok(())
        })
    }
    fn rollback_to_save_point(&mut self) -> Result<()> {
        block_on(async {
            assert!(!self.save_points.is_empty());
            if self.save_points.len() == 1 {
                return Err(blocksy3::anyhow!("save point rollback with no save point")).engine_result();
            }
            let batch = self.inner.as_ref().expect("batch");
            batch.rollback_save_point().await.engine_result()?;
            self.save_points.pop().expect("pop save points");
            Ok(())
        })
    }
}

impl Mutable for SimpleWriteBatch {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, value)
    }
    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        block_on(async {
            let batch = self.inner.as_ref().expect("batch");
            let tree = batch.tree(cf);
            tree.write(key, value).await.engine_result()?;
            let last_save_point = self.save_points.last_mut().expect("save point");
            *last_save_point = last_save_point.checked_add(1).expect("overflow");
            Ok(())
        })
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }
    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        block_on(async {
            let batch = self.inner.as_ref().expect("batch");
            let tree = batch.tree(cf);
            tree.delete(key).await.engine_result()?;
            let last_save_point = self.save_points.last_mut().expect("save point");
            *last_save_point = last_save_point.checked_add(1).expect("overflow");
            Ok(())
        })
    }
    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.delete_range_cf(CF_DEFAULT, begin_key, end_key)
    }
    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        block_on(async {
            let batch = self.inner.as_ref().expect("batch");
            let tree = batch.tree(cf);
            tree.delete_range(begin_key, end_key).await.engine_result()?;
            let last_save_point = self.save_points.last_mut().expect("save point");
            *last_save_point = last_save_point.checked_add(1).expect("overflow");
            Ok(())
        })
    }
}
