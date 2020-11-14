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
    pub (crate) db: blocksy2::Db,
    pub (crate) tree_names: Vec<String>,
}

impl SimpleEngine {
    pub fn open(config: blocksy2::DbConfig) -> Result<SimpleEngine> {
        let tree_names = config.trees.clone();
        block_on(blocksy2::Db::open(config))
            .engine_result()
            .map(|db| SimpleEngine {
                db, tree_names
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
        block_on(self.db.sync()).engine_result()
    }
    fn bad_downcast<T: 'static>(&self) -> &T {
        panic!()
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
        let view = self.db.read_view();
        let tree = view.tree(cf);
        let value = tree.read(key);
        let value = block_on(value).engine_result()?;
        Ok(value.map(SimpleDBVector::from_inner))
    }
}

impl SyncMutable for SimpleEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, value)
    }
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let batch = self.db.write_batch();
        batch.tree(cf).write(key, value);
        block_on(batch.commit()).engine_result()
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }
    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        let  batch = self.db.write_batch();
        batch.tree(cf).delete(key);
        block_on(batch.commit()).engine_result()
    }
    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        panic!()
    }

    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        if end_key < begin_key {
            panic!("end key less than begin key in delete_range_cf");
        }

        let batch = self.db.write_batch();
        let batch_tree = batch.tree(cf);
        let view = self.db.read_view();
        let view_tree = view.tree(cf);
        let mut cursor = view_tree.cursor();

        block_on(cursor.seek_key(begin_key)).engine_result()?;

        while cursor.valid() {
            if cursor.key_value().0 < end_key {
                batch_tree.delete(&cursor.key_value().0);
            }
            block_on(cursor.next()).engine_result()?;
        }

        drop(batch_tree);
        block_on(batch.commit()).engine_result()?;

        Ok(())
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
        Ok(SimpleEngineIterator(cursor))
    }
}

pub struct SimpleEngineIterator(blocksy2::Cursor);

impl Iterator for SimpleEngineIterator {
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
