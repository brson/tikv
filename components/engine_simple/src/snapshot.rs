// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::db_vector::SimpleDBVector;
use crate::engine::SimpleEngine;
use engine_traits::{
    IterOptions, Iterable, Iterator, Peekable, ReadOptions, Result, SeekKey, Snapshot,
};
use std::ops::Deref;

use crate::error::ResultExt;
use engine_traits::CF_DEFAULT;
use futures::executor::block_on;

#[derive(Clone, Debug)]
pub struct SimpleSnapshot {
    view: blocksy2::ReadView,
    tree_names: Vec<String>,
}

impl SimpleSnapshot {
    pub (crate) fn from_inner(view: blocksy2::ReadView, tree_names: Vec<String>) -> SimpleSnapshot {
        SimpleSnapshot {
            view, tree_names
        }
    }
}

impl Snapshot for SimpleSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        self.tree_names.iter().map(AsRef::as_ref).collect()
    }
}

impl Peekable for SimpleSnapshot {
    type DBVector = SimpleDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DBVector>> {
        self.get_value_cf_opt(opts, CF_DEFAULT, key)
    }
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DBVector>> {
        let tree = self.view.tree(cf);
        let value = tree.read(key);
        let value = block_on(value).engine_result()?;
        Ok(value.map(SimpleDBVector::from_inner))
    }
}

impl Iterable for SimpleSnapshot {
    type Iterator = SimpleSnapshotIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        self.iterator_cf_opt(CF_DEFAULT, opts)
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let tree = self.view.tree(cf);
        let cursor = tree.cursor();
        Ok(SimpleSnapshotIterator(cursor))
    }
}

pub struct SimpleSnapshotIterator(blocksy2::Cursor);

impl Iterator for SimpleSnapshotIterator {
    fn seek(&mut self, key: SeekKey) -> Result<bool> {
        match key {
            SeekKey::Start => {
                block_on(self.0.seek_first()).engine_result()?;
                Ok(self.0.valid())
            }
            SeekKey::End => {
                block_on(self.0.seek_last()).engine_result()?;
                Ok(self.0.valid())
            }
            SeekKey::Key(k) => {
                block_on(self.0.seek_key(k)).engine_result()?;
                Ok(self.0.valid())
            }
        }
    }

    fn seek_for_prev(&mut self, key: SeekKey) -> Result<bool> {
        match key {
            SeekKey::Start => {
                block_on(self.0.seek_first()).engine_result()?;
                Ok(self.0.valid())
            }
            SeekKey::End => {
                block_on(self.0.seek_last()).engine_result()?;
                Ok(self.0.valid())
            }
            SeekKey::Key(k) => {
                block_on(self.0.seek_key_rev(k)).engine_result()?;
                Ok(self.0.valid())
            }
        }
    }

    fn prev(&mut self) -> Result<bool> {
        block_on(self.0.prev()).engine_result()?;
        Ok(self.0.valid())
    }
    fn next(&mut self) -> Result<bool> {
        block_on(self.0.next()).engine_result()?;
        Ok(self.0.valid())
    }

    fn key(&self) -> &[u8] {
        self.0.key_value().0
    }
    fn value(&self) -> &[u8] {
        self.0.key_value().1
    }

    fn valid(&self) -> Result<bool> {
        Ok(self.0.valid())
    }
}
