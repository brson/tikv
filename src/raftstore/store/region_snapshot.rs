// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use engine::rocks::{DBIterator, DBVector, SeekKey, TablePropertiesCollection, DB};
use engine::{
    self, Error as EngineError, IterOption, Peekable, Result as EngineResult, Snapshot,
    SyncSnapshot,
};
use kvproto::metapb::Region;
use std::sync::Arc;

use crate::raftstore::store::keys::DATA_PREFIX_KEY;
use crate::raftstore::store::{keys, util, PeerStorage};
use crate::raftstore::Result;
use tikv_util::keybuilder::KeyBuilder;
use tikv_util::metrics::CRITICAL_ERROR;
use tikv_util::{panic_when_unexpected_key_or_data, set_panic_mark};

/// Snapshot of a region.
///
/// Only data within a region can be accessed.
#[derive(Debug)]
pub struct RegionSnapshot {
    snap: SyncSnapshot,
    region: Arc<Region>,
}

impl RegionSnapshot {
    pub fn new(ps: &PeerStorage) -> RegionSnapshot {
        RegionSnapshot::from_snapshot(ps.raw_snapshot().into_sync(), ps.region().clone())
    }

    pub fn from_raw(db: Arc<DB>, region: Region) -> RegionSnapshot {
        RegionSnapshot::from_snapshot(Snapshot::new(db).into_sync(), region)
    }

    pub fn from_snapshot(snap: SyncSnapshot, region: Region) -> RegionSnapshot {
        RegionSnapshot {
            snap,
            region: Arc::new(region),
        }
    }

    pub fn get_region(&self) -> &Region {
        &self.region
    }

    pub fn iter(&self, iter_opt: IterOption) -> RegionIterator {
        RegionIterator::new(&self.snap, Arc::clone(&self.region), iter_opt)
    }

    pub fn iter_cf(&self, cf: &str, iter_opt: IterOption) -> Result<RegionIterator> {
        Ok(RegionIterator::new_cf(
            &self.snap,
            Arc::clone(&self.region),
            iter_opt,
            cf,
        ))
    }

    // scan scans database using an iterator in range [start_key, end_key), calls function f for
    // each iteration, if f returns false, terminates this scan.
    pub fn scan<F>(&self, start_key: &[u8], end_key: &[u8], fill_cache: bool, f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_key, DATA_PREFIX_KEY.len(), 0);
        let end = KeyBuilder::from_slice(end_key, DATA_PREFIX_KEY.len(), 0);
        let iter_opt = IterOption::new(Some(start), Some(end), fill_cache);
        self.scan_impl(self.iter(iter_opt), start_key, f)
    }

    // like `scan`, only on a specific column family.
    pub fn scan_cf<F>(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_key, DATA_PREFIX_KEY.len(), 0);
        let end = KeyBuilder::from_slice(end_key, DATA_PREFIX_KEY.len(), 0);
        let iter_opt = IterOption::new(Some(start), Some(end), fill_cache);
        self.scan_impl(self.iter_cf(cf, iter_opt)?, start_key, f)
    }

    fn scan_impl<F>(&self, mut it: RegionIterator, start_key: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        if !it.seek(start_key)? {
            return Ok(());
        }
        while it.valid() {
            let r = f(it.key(), it.value())?;

            if !r || !it.next() {
                break;
            }
        }

        it.status()
    }

    pub fn get_properties_cf(&self, cf: &str) -> Result<TablePropertiesCollection> {
        let start = keys::enc_start_key(&self.region);
        let end = keys::enc_end_key(&self.region);
        let prop = engine::util::get_range_properties_cf(&self.snap.get_db(), cf, &start, &end)?;
        Ok(prop)
    }

    pub fn get_start_key(&self) -> &[u8] {
        self.region.get_start_key()
    }

    pub fn get_end_key(&self) -> &[u8] {
        self.region.get_end_key()
    }
}

impl Clone for RegionSnapshot {
    fn clone(&self) -> Self {
        RegionSnapshot {
            snap: self.snap.clone(),
            region: Arc::clone(&self.region),
        }
    }
}

impl Peekable for RegionSnapshot {
    fn get_value(&self, key: &[u8]) -> EngineResult<Option<DBVector>> {
        engine::util::check_key_in_range(
            key,
            self.region.get_id(),
            self.region.get_start_key(),
            self.region.get_end_key(),
        )?;
        let data_key = keys::data_key(key);
        self.snap.get_value(&data_key)
    }

    fn get_value_cf(&self, cf: &str, key: &[u8]) -> EngineResult<Option<DBVector>> {
        engine::util::check_key_in_range(
            key,
            self.region.get_id(),
            self.region.get_start_key(),
            self.region.get_end_key(),
        )?;
        let data_key = keys::data_key(key);
        self.snap.get_value_cf(cf, &data_key)
    }
}

/// `RegionIterator` wrap a rocksdb iterator and only allow it to
/// iterate in the region. It behaves as if underlying
/// db only contains one region.
pub struct RegionIterator {
    iter: DBIterator<Arc<DB>>,
    valid: bool,
    region: Arc<Region>,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
}

fn update_lower_bound(iter_opt: &mut IterOption, region: &Region) {
    let region_start_key = keys::enc_start_key(region);
    if iter_opt.lower_bound().is_some() && !iter_opt.lower_bound().as_ref().unwrap().is_empty() {
        iter_opt.set_lower_bound_prefix(keys::DATA_PREFIX_KEY);
        if region_start_key.as_slice() > *iter_opt.lower_bound().as_ref().unwrap() {
            iter_opt.set_vec_lower_bound(region_start_key);
        }
    } else {
        iter_opt.set_vec_lower_bound(region_start_key);
    }
}

fn update_upper_bound(iter_opt: &mut IterOption, region: &Region) {
    let region_end_key = keys::enc_end_key(region);
    if iter_opt.upper_bound().is_some() && !iter_opt.upper_bound().as_ref().unwrap().is_empty() {
        iter_opt.set_upper_bound_prefix(keys::DATA_PREFIX_KEY);
        if region_end_key.as_slice() < *iter_opt.upper_bound().as_ref().unwrap() {
            iter_opt.set_vec_upper_bound(region_end_key);
        }
    } else {
        iter_opt.set_vec_upper_bound(region_end_key);
    }
}

// we use engine::rocks's style iterator, doesn't need to impl std iterator.
impl RegionIterator {
    pub fn new(snap: &Snapshot, region: Arc<Region>, mut iter_opt: IterOption) -> RegionIterator {
        update_lower_bound(&mut iter_opt, &region);
        update_upper_bound(&mut iter_opt, &region);
        let start_key = iter_opt.lower_bound().unwrap().to_vec();
        let end_key = iter_opt.upper_bound().unwrap().to_vec();
        let iter = snap.db_iterator(iter_opt);
        RegionIterator {
            iter,
            valid: false,
            start_key,
            end_key,
            region,
        }
    }

    pub fn new_cf(
        snap: &Snapshot,
        region: Arc<Region>,
        mut iter_opt: IterOption,
        cf: &str,
    ) -> RegionIterator {
        update_lower_bound(&mut iter_opt, &region);
        update_upper_bound(&mut iter_opt, &region);
        let start_key = iter_opt.lower_bound().unwrap().to_vec();
        let end_key = iter_opt.upper_bound().unwrap().to_vec();
        let iter = snap.db_iterator_cf(cf, iter_opt).unwrap();
        RegionIterator {
            iter,
            valid: false,
            start_key,
            end_key,
            region,
        }
    }

    pub fn seek_to_first(&mut self) -> bool {
        self.valid = self.iter.seek(self.start_key.as_slice().into());

        self.update_valid(true)
    }

    #[inline]
    fn update_valid(&mut self, forward: bool) -> bool {
        if self.valid {
            let key = self.iter.key();
            self.valid = if forward {
                key < self.end_key.as_slice()
            } else {
                key >= self.start_key.as_slice()
            };
        }
        self.valid
    }

    pub fn seek_to_last(&mut self) -> bool {
        if !self.iter.seek(self.end_key.as_slice().into()) && !self.iter.seek(SeekKey::End) {
            self.valid = false;
            return self.valid;
        }

        while self.iter.key() >= self.end_key.as_slice() && self.iter.prev() {}

        self.valid = self.iter.valid();
        self.update_valid(false)
    }

    pub fn seek(&mut self, key: &[u8]) -> Result<bool> {
        fail_point!("region_snapshot_seek", |_| {
            return Err(box_err!("region seek error"));
        });

        self.should_seekable(key)?;
        let key = keys::data_key(key);
        if key == self.end_key {
            self.valid = false;
        } else {
            self.valid = self.iter.seek(key.as_slice().into());
        }

        Ok(self.update_valid(true))
    }

    pub fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.should_seekable(key)?;
        let key = keys::data_key(key);
        self.valid = self.iter.seek_for_prev(key.as_slice().into());
        if self.valid && self.iter.key() == self.end_key.as_slice() {
            self.valid = self.iter.prev();
        }
        Ok(self.update_valid(false))
    }

    pub fn prev(&mut self) -> bool {
        if !self.valid {
            return false;
        }
        self.valid = self.iter.prev();

        self.update_valid(false)
    }

    pub fn next(&mut self) -> bool {
        if !self.valid {
            return false;
        }
        self.valid = self.iter.next();

        self.update_valid(true)
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        assert!(self.valid);
        keys::origin_key(self.iter.key())
    }

    #[inline]
    pub fn value(&self) -> &[u8] {
        assert!(self.valid);
        self.iter.value()
    }

    #[inline]
    pub fn valid(&self) -> bool {
        self.valid
    }

    #[inline]
    pub fn status(&self) -> Result<()> {
        self.iter
            .status()
            .map_err(|e| EngineError::RocksDb(e))
            .map_err(From::from)
    }

    #[inline]
    pub fn should_seekable(&self, key: &[u8]) -> Result<()> {
        if let Err(e) = util::check_key_in_region_inclusive(key, &self.region) {
            CRITICAL_ERROR
                .with_label_values(&["key not in region"])
                .inc();
            if panic_when_unexpected_key_or_data() {
                set_panic_mark();
                panic!("key exceed bound: {:?}", e);
            } else {
                return Err(e);
            }
        }
        Ok(())
    }
}
