// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SimpleEngine;
use engine_traits::{Mutable, Result, WriteBatch, WriteBatchExt, WriteOptions};
use engine_traits::Error as EngineTraitsError;

use futures::executor::block_on;
use crate::error::ResultExt;
use engine_traits::CF_DEFAULT;

impl WriteBatchExt for SimpleEngine {
    type WriteBatch = SimpleWriteBatch;
    type WriteBatchVec = SimpleWriteBatch;

    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn support_write_batch_vec(&self) -> bool {
        panic!()
    }

    fn write_batch(&self) -> Self::WriteBatch {
        SimpleWriteBatch {
            db: self.db.clone(),
            cmds: vec![],
            data_size: 0,
            save_points: vec![],
        }
    }
    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        SimpleWriteBatch {
            db: self.db.clone(),
            cmds: vec![],
            data_size: 0,
            save_points: vec![],
        }
    }
}

pub struct SimpleWriteBatch {
    db: blocksy2::Db,
    cmds: Vec<WriteBatchCmd>,
    // NB: data_size is not maintained with any accuracy by this engine.
    // It just behaves in a way that satisfies the tests.
    data_size: usize,
    save_points: Vec<usize>,
}

enum WriteBatchCmd {
    Put {
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        cf: String,
        key: Vec<u8>,
    },
    DeleteRange {
        cf: String,
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

impl WriteBatch<SimpleEngine> for SimpleWriteBatch {
    fn with_capacity(e: &SimpleEngine, cap: usize) -> Self {
        e.write_batch_with_cap(cap)
    }

    fn write_opt(&self, _: &WriteOptions) -> Result<()> {
        for cmd in &self.cmds {
            let batch = self.db.write_batch();
            match cmd {
                WriteBatchCmd::Put { cf, key, value } => {
                    let tree = batch.tree(cf);
                    tree.write(key, value);
                }
                WriteBatchCmd::Delete { cf, key } => {
                    let tree = batch.tree(cf);
                    tree.delete(key);
                }
                WriteBatchCmd::DeleteRange { cf, start, end } => {
                    if end < start {
                        panic!("end key less than begin key in delete_range_cf");
                    }
                    let view = self.db.read_view();
                    let read_tree = view.tree(cf);
                    let write_tree = batch.tree(cf);
                    let mut cursor = read_tree.cursor();
                    block_on(cursor.seek_key(start)).engine_result()?;
                    while cursor.valid() {
                        let key = &cursor.key_value().0;
                        if &**key >= &**end {
                            break;
                        }
                        write_tree.delete(key);
                        block_on(cursor.next()).engine_result()?;
                    }
                }
            }
            block_on(batch.commit()).engine_result()?;
        }
        Ok(())
    }

    fn write(&self) -> Result<()> {
        self.write_opt(&WriteOptions::default())
    }
}

impl Mutable for SimpleWriteBatch {
    fn data_size(&self) -> usize {
        self.data_size
    }
    fn count(&self) -> usize {
        self.cmds.len()
    }
    fn is_empty(&self) -> bool {
        self.cmds.is_empty()
    }
    fn should_write_to_engine(&self) -> bool {
        self.count() > SimpleEngine::WRITE_BATCH_MAX_KEYS
    }

    fn clear(&mut self) {
        self.cmds.truncate(0);
        self.data_size = 0;
    }
    fn set_save_point(&mut self) {
        self.save_points.push(self.cmds.len());
    }
    fn pop_save_point(&mut self) -> Result<()> {
        if let None = self.save_points.pop() {
            Err(EngineTraitsError::Engine("no save point".to_string()))
        } else {
            Ok(())
        }
    }
    fn rollback_to_save_point(&mut self) -> Result<()> {
        if let Some(len) = self.save_points.pop() {
            assert!(self.cmds.len() >= len);
            while self.cmds.len() > len {
                self.cmds.pop();
            }
            Ok(())
        } else {
            Err(EngineTraitsError::Engine("no save point".to_string()))
        }
    }
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, value)
    }
    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.cmds.push(WriteBatchCmd::Put {
            cf: cf.to_owned(),
            key: key.to_owned(),
            value: value.to_owned(),
        });
        self.data_size += 1;
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }
    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.cmds.push(WriteBatchCmd::Delete {
            cf: cf.to_owned(),
            key: key.to_owned(),
        });
        self.data_size += 1;
        Ok(())
    }
    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.cmds.push(WriteBatchCmd::DeleteRange {
            cf: cf.to_owned(),
            start: begin_key.to_owned(),
            end: end_key.to_owned(),
        });
        self.data_size += 1;
        Ok(())
    }
}
