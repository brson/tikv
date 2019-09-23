// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::raftstore::store::{keys, CasualMessage, CasualRouter};
use engine::rocks::DB;
use engine::rocks::{self, Range};
use engine::util;
use engine::CF_WRITE;
use kvproto::{metapb::Region, pdpb::CheckPolicy};
use std::mem;
use std::sync::Mutex;

use super::super::error::Result;
use super::super::metrics::*;
use super::super::properties::{get_range_entries_and_versions, RangeProperties};
use super::super::{Coprocessor, KeyEntry, ObserverContext, SplitCheckObserver, SplitChecker};
use super::Host;

pub struct Checker {
    max_keys_count: u64,
    split_threshold: u64,
    current_count: u64,
    split_keys: Vec<Vec<u8>>,
    batch_split_limit: u64,
    policy: CheckPolicy,
}

impl Checker {
    pub fn new(
        max_keys_count: u64,
        split_threshold: u64,
        batch_split_limit: u64,
        policy: CheckPolicy,
    ) -> Checker {
        Checker {
            max_keys_count,
            split_threshold,
            current_count: 0,
            split_keys: Vec::with_capacity(1),
            batch_split_limit,
            policy,
        }
    }
}

impl SplitChecker for Checker {
    fn on_kv(&mut self, _: &mut ObserverContext<'_>, key: &KeyEntry) -> bool {
        if !key.is_commit_version() {
            return false;
        }
        self.current_count += 1;

        let mut over_limit = self.split_keys.len() as u64 >= self.batch_split_limit;
        if self.current_count > self.split_threshold && !over_limit {
            self.split_keys.push(keys::origin_key(key.key()).to_vec());
            // if for previous on_kv() self.current_count == self.split_threshold,
            // the split key would be pushed this time, but the entry for this time should not be ignored.
            self.current_count = 1;
            over_limit = self.split_keys.len() as u64 >= self.batch_split_limit;
        }

        // For a large region, scan over the range maybe cost too much time,
        // so limit the number of produced split_key for one batch.
        // Also need to scan over self.max_keys_count for last part.
        over_limit && self.current_count + self.split_threshold >= self.max_keys_count
    }

    fn split_keys(&mut self) -> Vec<Vec<u8>> {
        // make sure not to split when less than max_keys_count for last part
        if self.current_count + self.split_threshold < self.max_keys_count {
            self.split_keys.pop();
        }
        if !self.split_keys.is_empty() {
            mem::replace(&mut self.split_keys, vec![])
        } else {
            vec![]
        }
    }

    fn policy(&self) -> CheckPolicy {
        self.policy
    }
}

pub struct KeysCheckObserver<C> {
    region_max_keys: u64,
    split_keys: u64,
    batch_split_limit: u64,
    router: Mutex<C>,
}

impl<C: CasualRouter> KeysCheckObserver<C> {
    pub fn new(
        region_max_keys: u64,
        split_keys: u64,
        batch_split_limit: u64,
        router: C,
    ) -> KeysCheckObserver<C> {
        KeysCheckObserver {
            region_max_keys,
            split_keys,
            batch_split_limit,
            router: Mutex::new(router),
        }
    }
}

impl<C> Coprocessor for KeysCheckObserver<C> {}

impl<C: CasualRouter + Send> SplitCheckObserver for KeysCheckObserver<C> {
    fn add_checker(
        &self,
        ctx: &mut ObserverContext<'_>,
        host: &mut Host,
        engine: &DB,
        policy: CheckPolicy,
    ) {
        let region = ctx.region();
        let region_id = region.get_id();
        let region_keys = match get_region_approximate_keys(engine, region) {
            Ok(keys) => keys,
            Err(e) => {
                warn!(
                    "failed to get approximate keys";
                    "region_id" => region_id,
                    "err" => %e,
                );
                // Need to check keys.
                host.add_checker(Box::new(Checker::new(
                    self.region_max_keys,
                    self.split_keys,
                    self.batch_split_limit,
                    policy,
                )));
                return;
            }
        };

        let res = CasualMessage::RegionApproximateKeys { keys: region_keys };
        if let Err(e) = self.router.lock().unwrap().send(region_id, res) {
            warn!(
                "failed to send approximate region keys";
                "region_id" => region_id,
                "err" => %e,
            );
        }

        REGION_KEYS_HISTOGRAM.observe(region_keys as f64);
        if region_keys >= self.region_max_keys {
            info!(
                "approximate keys over threshold, need to do split check";
                "region_id" => region.get_id(),
                "keys" => region_keys,
                "threshold" => self.region_max_keys,
            );
            // Need to check keys.
            host.add_checker(Box::new(Checker::new(
                self.region_max_keys,
                self.split_keys,
                self.batch_split_limit,
                policy,
            )));
        } else {
            // Does not need to check keys.
            debug!(
                "approximate keys less than threshold, does not need to do split check";
                "region_id" => region.get_id(),
                "keys" => region_keys,
                "threshold" => self.region_max_keys,
            );
        }
    }
}

/// Get the approximate number of keys in the range.
pub fn get_region_approximate_keys(db: &DB, region: &Region) -> Result<u64> {
    // try to get from RangeProperties first.
    match get_region_approximate_keys_cf(db, CF_WRITE, region) {
        Ok(v) => {
            return Ok(v);
        }
        Err(e) => debug!(
            "failed to get keys from RangeProperties";
            "err" => ?e,
        ),
    }

    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let cf = box_try!(rocks::util::get_cf_handle(db, CF_WRITE));
    let (_, keys) = get_range_entries_and_versions(db, cf, &start, &end).unwrap_or_default();
    Ok(keys)
}

pub fn get_region_approximate_keys_cf(db: &DB, cfname: &str, region: &Region) -> Result<u64> {
    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    let cf = box_try!(rocks::util::get_cf_handle(db, cfname));
    let range = Range::new(&start_key, &end_key);
    let (mut keys, _) = db.get_approximate_memtable_stats_cf(cf, &range);

    let collection = box_try!(util::get_range_properties_cf(
        db, cfname, &start_key, &end_key
    ));
    for (_, v) in &*collection {
        let props = box_try!(RangeProperties::decode(v.user_collected_properties()));
        keys += props.get_approximate_keys_in_range(&start_key, &end_key);
    }
    Ok(keys)
}

