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
    view: blocksy3::ReadView,
    tree_names: Vec<String>,
}

impl SimpleSnapshot {
    pub (crate) fn from_inner(view: blocksy3::ReadView, tree_names: Vec<String>) -> SimpleSnapshot {
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
        block_on(async {
            let tree = self.view.tree(cf);
            let value = tree.read(key).await.engine_result()?;
            Ok(value.map(SimpleDBVector::from_inner))
        })
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
        Ok(SimpleSnapshotIterator {
            cursor,
            kv: None,
        })
    }
}

pub struct SimpleSnapshotIterator {
    cursor: blocksy3::Cursor,
    kv: Option<(Vec<u8>, Vec<u8>)>,
}

impl Iterator for SimpleSnapshotIterator {
    fn seek(&mut self, key: SeekKey) -> Result<bool> {
        match key {
            SeekKey::Start => {
                self.cursor.seek_first();
                self.update_kv()
            }
            SeekKey::End => {
                self.cursor.seek_last();
                self.update_kv()
            }
            SeekKey::Key(k) => {
                self.cursor.seek_key(k);
                self.update_kv()
            }
        }
    }
    fn seek_for_prev(&mut self, key: SeekKey) -> Result<bool> {
        match key {
            SeekKey::Start => {
                self.cursor.seek_first();
                self.update_kv()
            }
            SeekKey::End => {
                self.cursor.seek_last();
                self.update_kv()
            }
            SeekKey::Key(k) => {
                self.cursor.seek_key_rev(k);
                self.update_kv()
            }
        }
    }

    fn prev(&mut self) -> Result<bool> {
        if !self.valid()? {
            return Err(blocksy3::anyhow!("prev on invalid iterator")).engine_result();
        }
        self.cursor.prev();
        self.update_kv()
    }
    fn next(&mut self) -> Result<bool> {
        if !self.valid()? {
            return Err(blocksy3::anyhow!("next on invalid iterator")).engine_result();
        }
        self.cursor.next();
        self.update_kv()
    }

    fn key(&self) -> &[u8] {
        match self.kv {
            Some((ref key, _)) => key,
            None => panic!("invalid iterator"),
        }
    }
    fn value(&self) -> &[u8] {
        match self.kv {
            Some((_, ref value)) => value,
            None => panic!("invalid iterator"),
        }
    }

    fn valid(&self) -> Result<bool> {
        Ok(self.cursor.valid())
    }
}

impl SimpleSnapshotIterator {
    fn update_kv(&mut self) -> Result<bool> {
        block_on(async {
            if self.cursor.valid() {
                let key = self.cursor.key();
                let value = self.cursor.value().await.engine_result()?;
                self.kv = Some((key, value));
                Ok(true)
            } else {
                self.kv = None;
                Ok(false)
            }
        })
    }
}
