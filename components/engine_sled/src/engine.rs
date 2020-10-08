// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::db_vector::SledDBVector;
use crate::snapshot::SledSnapshot;
use crate::write_batch::SledWriteBatch;
use engine_traits::{
    IterOptions, Iterable, Iterator, KvEngine, Peekable, ReadOptions, Result, SeekKey, SyncMutable,
    WriteOptions,
};

use crate::EngineResult;

#[derive(Clone, Debug)]
pub struct SledEngine(sled::Db);

impl SledEngine {
    pub fn from_raw(db: sled::Db) -> SledEngine {
        SledEngine(db)
    }
}

impl KvEngine for SledEngine {
    type Snapshot = SledSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        panic!()
    }
    fn sync(&self) -> Result<()> {
        panic!()
    }
    fn bad_downcast<T: 'static>(&self) -> &T {
        panic!()
    }
}

impl Peekable for SledEngine {
    type DBVector = SledDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DBVector>> {
        Ok(self.0.get(key).engine_result()?.map(SledDBVector::from_raw))
    }
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DBVector>> {
        panic!()
    }
}

impl SyncMutable for SledEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.0.insert(key, value).engine_result()?;
        Ok(())
    }
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        panic!()
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        panic!()
    }
}

impl Iterable for SledEngine {
    type Iterator = SledEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
}

pub struct SledEngineIterator;

impl Iterator for SledEngineIterator {
    fn seek(&mut self, key: SeekKey) -> Result<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, key: SeekKey) -> Result<bool> {
        panic!()
    }

    fn prev(&mut self) -> Result<bool> {
        panic!()
    }
    fn next(&mut self) -> Result<bool> {
        panic!()
    }

    fn key(&self) -> &[u8] {
        panic!()
    }
    fn value(&self) -> &[u8] {
        panic!()
    }

    fn valid(&self) -> Result<bool> {
        panic!()
    }
}
