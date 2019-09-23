// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Bound::Excluded;

use engine::rocks::DB;
use engine::util;
use engine::{CF_DEFAULT, CF_WRITE};
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;

use crate::raftstore::store::keys;
use tikv_util::config::ReadableSize;

use super::super::error::Result;
use super::super::properties::RangeProperties;
use super::super::{Coprocessor, KeyEntry, ObserverContext, SplitCheckObserver, SplitChecker};
use super::size::get_region_approximate_size_cf;
use super::Host;

pub const BUCKET_NUMBER_LIMIT: usize = 1024;
const BUCKET_SIZE_LIMIT_MB: u64 = 512;

pub struct Checker {
    buckets: Vec<Vec<u8>>,
    cur_bucket_size: u64,
    each_bucket_size: u64,
    policy: CheckPolicy,
}

impl Checker {
    fn new(each_bucket_size: u64, policy: CheckPolicy) -> Checker {
        Checker {
            each_bucket_size,
            cur_bucket_size: 0,
            buckets: vec![],
            policy,
        }
    }
}

impl SplitChecker for Checker {
    fn on_kv(&mut self, _: &mut ObserverContext<'_>, entry: &KeyEntry) -> bool {
        if self.buckets.is_empty() || self.cur_bucket_size >= self.each_bucket_size {
            self.buckets.push(entry.key().to_vec());
            self.cur_bucket_size = 0;
        }
        self.cur_bucket_size += entry.entry_size() as u64;
        false
    }

    fn split_keys(&mut self) -> Vec<Vec<u8>> {
        let mid = self.buckets.len() / 2;
        if mid == 0 {
            vec![]
        } else {
            let data_key = self.buckets.swap_remove(mid);
            let key = keys::origin_key(&data_key).to_vec();
            vec![key]
        }
    }

    fn approximate_split_keys(&mut self, region: &Region, engine: &DB) -> Result<Vec<Vec<u8>>> {
        let ks = box_try!(get_region_approximate_middle(engine, region)
            .map(|keys| keys.map_or(vec![], |key| vec![key])));

        Ok(ks)
    }

    fn policy(&self) -> CheckPolicy {
        self.policy
    }
}

pub struct HalfCheckObserver {
    half_split_bucket_size: u64,
}

impl HalfCheckObserver {
    pub fn new(region_size_limit: u64) -> HalfCheckObserver {
        let mut half_split_bucket_size = region_size_limit / BUCKET_NUMBER_LIMIT as u64;
        let bucket_size_limit = ReadableSize::mb(BUCKET_SIZE_LIMIT_MB).0;
        if half_split_bucket_size == 0 {
            half_split_bucket_size = 1;
        } else if half_split_bucket_size > bucket_size_limit {
            half_split_bucket_size = bucket_size_limit;
        }
        HalfCheckObserver {
            half_split_bucket_size,
        }
    }
}

impl Coprocessor for HalfCheckObserver {}

impl SplitCheckObserver for HalfCheckObserver {
    fn add_checker(
        &self,
        _: &mut ObserverContext<'_>,
        host: &mut Host,
        _: &DB,
        policy: CheckPolicy,
    ) {
        if host.auto_split() {
            return;
        }
        host.add_checker(Box::new(Checker::new(self.half_split_bucket_size, policy)))
    }
}

/// Get region approximate middle key based on default and write cf size.
pub fn get_region_approximate_middle(db: &DB, region: &Region) -> Result<Option<Vec<u8>>> {
    let get_cf_size = |cf: &str| get_region_approximate_size_cf(db, cf, &region);

    let default_cf_size = box_try!(get_cf_size(CF_DEFAULT));
    let write_cf_size = box_try!(get_cf_size(CF_WRITE));

    let middle_by_cf = if default_cf_size >= write_cf_size {
        CF_DEFAULT
    } else {
        CF_WRITE
    };

    get_region_approximate_middle_cf(db, middle_by_cf, region)
}

/// Get the approximate middle key of the region. If we suppose the region
/// is stored on disk as a plain file, "middle key" means the key whose
/// position is in the middle of the file.
///
/// The returned key maybe is timestamped if transaction KV is used,
/// and must start with "z".
pub fn get_region_approximate_middle_cf(
    db: &DB,
    cfname: &str,
    region: &Region,
) -> Result<Option<Vec<u8>>> {
    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    let collection = box_try!(util::get_range_properties_cf(
        db, cfname, &start_key, &end_key
    ));

    let mut keys = Vec::new();
    for (_, v) in &*collection {
        let props = box_try!(RangeProperties::decode(v.user_collected_properties()));
        keys.extend(
            props
                .offsets
                .range::<[u8], _>((Excluded(start_key.as_slice()), Excluded(end_key.as_slice())))
                .map(|(k, _)| k.to_owned()),
        );
    }
    if keys.is_empty() {
        return Ok(None);
    }
    keys.sort();
    // Calculate the position by (len-1)/2. So it's the left one
    // of two middle positions if the number of keys is even.
    let middle = (keys.len() - 1) / 2;
    Ok(Some(keys.swap_remove(middle)))
}

