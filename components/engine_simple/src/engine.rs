// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::db_vector::SimpleDBVector;
use crate::snapshot::SimpleSnapshot;
use crate::write_batch::SimpleWriteBatch;
use engine_traits::{
    IterOptions, Iterable, Iterator, KvEngine, Peekable, ReadOptions, Result, SeekKey, SyncMutable,
    WriteOptions,
};

use crate::error::ResultExt;
use futures::executor::block_on;
use engine_traits::CF_DEFAULT;

#[derive(Clone, Debug)]
pub struct SimpleEngine {
    pub (crate) db: blocksy3::Db,
    pub (crate) tree_names: Vec<String>,
    pub (crate) data_dir: String,
}

impl SimpleEngine {
    pub fn open(config: blocksy3::DbConfig) -> Result<SimpleEngine> {
        block_on(async {
            let tree_names = config.trees.clone();
            let data_dir = config.dir.as_ref().ok_or_else(|| {
                blocksy3::anyhow!("data directory required to create engine_simple")
            }).engine_result()?;
            let data_dir = data_dir.to_str().expect("utf8-paths").to_owned();
            blocksy3::Db::open(config).await
                 .engine_result()
                 .map(|db| SimpleEngine {
                     db, tree_names, data_dir,
                 })
        })
    }
}

impl KvEngine for SimpleEngine {
    type Snapshot = SimpleSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        let view = self.db.read_view();
        SimpleSnapshot::from_inner(view, self.tree_names.clone())
    }
    fn sync(&self) -> Result<()> {
        block_on(async {
            self.db.sync().await.engine_result()
        })
    }
}

impl Peekable for SimpleEngine {
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
            let view = self.db.read_view();
            let tree = view.tree(cf);
            let value = tree.read(key).await.engine_result()?;
            Ok(value.map(SimpleDBVector::from_inner))
        })
    }
}

impl SyncMutable for SimpleEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, value)
    }
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        block_on(async {
            let batch = self.db.write_batch().await.engine_result()?;
            let tree = batch.tree(cf);
            tree.write(key, value).await.engine_result()?;
            drop(tree);
            batch.commit().await.engine_result()?;
            Ok(())
        })
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }
    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        block_on(async {
            let batch = self.db.write_batch().await.engine_result()?;
            let tree = batch.tree(cf);
            tree.delete(key).await.engine_result()?;
            drop(tree);
            batch.commit().await.engine_result()?;
            Ok(())
        })
    }
    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.delete_range_cf(CF_DEFAULT, begin_key, end_key)
    }
    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        block_on(async {
            let batch = self.db.write_batch().await.engine_result()?;
            let tree = batch.tree(cf);
            tree.delete_range(begin_key, end_key).await.engine_result()?;
            drop(tree);
            batch.commit().await.engine_result()?;
            Ok(())
        })
    }
}

impl Iterable for SimpleEngine {
    type Iterator = SimpleEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        self.iterator_cf_opt(CF_DEFAULT, opts)
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let view = self.db.read_view();
        let tree = view.tree(cf);
        let cursor = tree.cursor();
        Ok(SimpleEngineIterator {
            cursor,
            kv: None,
        })
    }
}

pub struct SimpleEngineIterator {
    cursor: blocksy3::Cursor,
    kv: Option<(Vec<u8>, Vec<u8>)>,
}

impl Iterator for SimpleEngineIterator {
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

impl SimpleEngineIterator {
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
