// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::db_vector::SimpleDBVector;
use crate::engine::SimpleEngine;
use engine_traits::{
    IterOptions, Iterable, Iterator, Peekable, ReadOptions, Result, SeekKey, Snapshot,
};
use std::ops::Deref;

#[derive(Clone, Debug)]
pub struct SimpleSnapshot;

impl Snapshot for SimpleSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        panic!()
    }
}

impl Peekable for SimpleSnapshot {
    type DBVector = SimpleDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DBVector>> {
        panic!()
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

impl Iterable for SimpleSnapshot {
    type Iterator = SimpleSnapshotIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
}

pub struct SimpleSnapshotIterator;

impl Iterator for SimpleSnapshotIterator {
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