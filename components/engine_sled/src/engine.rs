// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::db_vector::SledDBVector;
use crate::snapshot::SledSnapshot;
use crate::write_batch::SledWriteBatch;
use engine_traits::{
    IterOptions, Iterable, Iterator, KvEngine, Peekable, ReadOptions, Result, SeekKey, SyncMutable,
    WriteOptions,
};
use engine_traits::CF_DEFAULT;

use std::str;
use std::sync::Arc;
use std::collections::BTreeMap;
use crate::EngineResult;

use tikv_util::box_try;

#[derive(Clone, Debug)]
pub struct SledEngine(Arc<SledEngineInner>);

impl SledEngine {
    pub fn from_raw(db: sled::Db) -> Result<SledEngine> {
        let mut cf_map = BTreeMap::new();
        for name in db.tree_names() {
            let utf8_name = box_try!(str::from_utf8(&*name)).to_string();
            // Ignore sled's default tree, we use our own named "default"
            if utf8_name == "__sled__default" {
                continue;
            }
            let tree = db.open_tree(name).engine_result()?;
            cf_map.insert(utf8_name, tree);
        }
        Ok(SledEngine(Arc::new(SledEngineInner {
            db, cf_map
        })))
    }

    pub (crate) fn inner(&self) -> &SledEngineInner {
        &self.0
    }

    pub (crate) fn cf_tree(&self, cf: &str) -> Result<&sled::Tree> {
        self.inner().cf_map.get(cf)
            .ok_or_else(|| engine_traits::Error::CFName(cf.to_string()))
    }
}

#[derive(Debug)]
pub (crate) struct SledEngineInner {
    pub db: sled::Db,
    // To satisfy the CFNamesExt interface we need to keep in-memory iterable pointers
    // to string CF names. This serves that purpose as well as caching handles to
    // to sled Tree objects.
    pub cf_map: BTreeMap<String, sled::Tree>,
}

impl KvEngine for SledEngine {
    type Snapshot = SledSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        panic!()
    }
    fn sync(&self) -> Result<()> {
        for tree_name in self.inner().db.tree_names() {
            let tree = self.inner().db.open_tree(tree_name).engine_result()?;
            tree.flush().engine_result()?;
        }
        Ok(())
    }
    fn bad_downcast<T: 'static>(&self) -> &T {
        panic!()
    }
}

impl Peekable for SledEngine {
    type DBVector = SledDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Self::DBVector>> {
        self.get_value_cf_opt(opts, CF_DEFAULT, key)
    }
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DBVector>> {
        Ok(self.cf_tree(cf)?.get(key).engine_result()?.map(SledDBVector::from_raw))
    }
}

impl SyncMutable for SledEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf(CF_DEFAULT, key, value)
    }
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.cf_tree(cf)?.insert(key, value).engine_result()?;
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_cf(CF_DEFAULT, key)
    }
    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        self.cf_tree(cf)?.remove(key).engine_result()?;
        Ok(())
    }
    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        panic!()
    }
}

impl Iterable for SledEngine {
    type Iterator = SledEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        self.iterator_cf_opt(CF_DEFAULT, opts)
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let tree = self.cf_tree(cf)?.clone();
        Ok(SledEngineIterator::from_tree(tree))
    }
}

pub struct SledEngineIterator(SledEngineIteratorInner);

enum SledEngineIteratorInner {
    Uninit {
        tree: sled::Tree,
    },
    Forward {
        tree: sled::Tree,
        iter: sled::Iter,
        curr: (sled::IVec, sled::IVec),
    },
    Reverse {
        tree: sled::Tree,
        iter: std::iter::Rev<sled::Iter>,
        curr: (sled::IVec, sled::IVec),
    },
    Placeholder,
}

impl SledEngineIterator {
    fn from_tree(mut tree: sled::Tree) -> SledEngineIterator {
        SledEngineIterator(SledEngineIteratorInner::Uninit {
            tree
        })
    }
}

impl Iterator for SledEngineIterator {
    fn seek(&mut self, key: SeekKey) -> Result<bool> {
        let state = std::mem::replace(&mut self.0, SledEngineIteratorInner::Placeholder);
        match (state, key) {
            (SledEngineIteratorInner::Uninit { tree }, SeekKey::Start) => {
                let mut iter = tree.iter();
                let curr = iter.next();
                match curr {
                    Some(curr) => {
                        let curr = curr.engine_result()?;
                        self.0 = SledEngineIteratorInner::Forward {
                            tree, iter, curr
                        };
                        Ok(true)
                    }
                    None => {
                        Ok(false)
                    }
                }
            }
            (SledEngineIteratorInner::Uninit { tree }, SeekKey::End) => {
                let mut iter = tree.iter().rev();
                let curr = iter.next();
                match curr {
                    Some(curr) => {
                        let curr = curr.engine_result()?;
                        self.0 = SledEngineIteratorInner::Reverse {
                            tree, iter, curr
                        };
                        Ok(true)
                    }
                    None => {
                        Ok(false)
                    }
                }
            }
            (SledEngineIteratorInner::Uninit { tree }, SeekKey::Key(key)) => {
                let mut iter = tree.range(key..);
                let curr = iter.next();
                match curr {
                    Some(curr) => {
                        let curr = curr.engine_result()?;
                        self.0 = SledEngineIteratorInner::Forward {
                            tree, iter, curr
                        };
                        Ok(true)
                    }
                    None => {
                        Ok(false)
                    }
                }
            }
            _ => {
                Ok(false)
            }
        }
    }

    fn seek_for_prev(&mut self, key: SeekKey) -> Result<bool> {
        Ok(false)
    }

    fn prev(&mut self) -> Result<bool> {
        let state = std::mem::replace(&mut self.0, SledEngineIteratorInner::Placeholder);
        match state {
            SledEngineIteratorInner::Uninit { .. } => {
                panic!("invalid iterator");
            }
            SledEngineIteratorInner::Forward { tree, curr, .. } => {
                let key = curr.0;
                let mut iter = tree.range(..key).rev();
                let curr = iter.next();
                match curr {
                    Some(curr) => {
                        let curr = curr.engine_result()?;
                        self.0 = SledEngineIteratorInner::Reverse {
                            tree, iter, curr
                        };
                        Ok(true)
                    }
                    None => {
                        Ok(false)
                    }
                }
            }
            SledEngineIteratorInner::Reverse { tree, mut iter, .. } => {
                let curr = iter.next();
                match curr {
                    Some(curr) => {
                        let curr = curr.engine_result()?;
                        self.0 = SledEngineIteratorInner::Reverse {
                            tree, iter, curr
                        };
                        Ok(true)
                    }
                    None => {
                        self.0 = SledEngineIteratorInner::Uninit {
                            tree
                        };
                        Ok(false)
                    }
                }
            }
            SledEngineIteratorInner::Placeholder => {
                panic!();
            }
        }
    }

    fn next(&mut self) -> Result<bool> {
        let state = std::mem::replace(&mut self.0, SledEngineIteratorInner::Placeholder);
        match state {
            SledEngineIteratorInner::Uninit { .. } => {
                panic!("invalid iterator");
            }
            SledEngineIteratorInner::Forward { tree, mut iter, .. } => {
                let curr = iter.next();
                match curr {
                    Some(curr) => {
                        let curr = curr.engine_result()?;
                        self.0 = SledEngineIteratorInner::Forward {
                            tree, iter, curr
                        };
                        Ok(true)
                    }
                    None => {
                        self.0 = SledEngineIteratorInner::Uninit {
                            tree
                        };
                        Ok(false)
                    }
                }
            }
            SledEngineIteratorInner::Reverse { tree, iter, curr } => {
                let key = curr.0;
                let mut iter = tree.range(key.clone()..);
                let curr = iter.next();
                match curr {
                    Some(curr) => {
                        let curr = curr.engine_result()?;
                        if curr.0 == key {
                            let curr = iter.next();
                            match curr {
                                Some(curr) => {
                                    let curr = curr.engine_result()?;
                                    self.0 = SledEngineIteratorInner::Forward {
                                        tree, iter, curr
                                    };
                                    Ok(true)
                                }
                                None => {
                                    Ok(false)
                                }
                            }
                        } else {
                            panic!(); // FIXME add test first
                            self.0 = SledEngineIteratorInner::Forward {
                                tree, iter, curr
                            };
                            Ok(true)
                        }
                    }
                    None => {
                        Ok(false)
                    }
                }
            }
            SledEngineIteratorInner::Placeholder => {
                panic!();
            }
        }
    }

    fn key(&self) -> &[u8] {
        match &self.0 {
            SledEngineIteratorInner::Uninit { .. } => {
                panic!("invalid iterator");
            }
            SledEngineIteratorInner::Forward { curr, .. } => {
                &curr.0
            }
            SledEngineIteratorInner::Reverse { curr, .. } => {
                &curr.0
            }
            SledEngineIteratorInner::Placeholder => {
                panic!();
            }
        }
    }

    fn value(&self) -> &[u8] {
        match &self.0 {
            SledEngineIteratorInner::Uninit { .. } => {
                panic!("invalid iterator");
            }
            SledEngineIteratorInner::Forward { curr, .. } => {
                &curr.1
            }
            SledEngineIteratorInner::Reverse { curr, .. } => {
                &curr.1
            }
            SledEngineIteratorInner::Placeholder => {
                panic!();
            }
        }
    }

    fn valid(&self) -> Result<bool> {
        match self.0 {
            SledEngineIteratorInner::Uninit { .. } => Ok(false),
            SledEngineIteratorInner::Forward { .. } => Ok(true),
            SledEngineIteratorInner::Reverse { .. } => Ok(true),
            SledEngineIteratorInner::Placeholder => panic!(),
        }
    }
}
