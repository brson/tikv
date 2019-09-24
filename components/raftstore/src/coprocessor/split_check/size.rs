// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::Bound::Excluded;
use std::mem;
use std::sync::Mutex;

use engine::rocks;
use engine::rocks::DB;
use engine::LARGE_CFS;
use engine::{util, Range};
use engine::{CF_DEFAULT, CF_WRITE};
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;

use keys;
use crate::store::msg::{CasualMessage};
use crate::store::transport::{CasualRouter};

use super::super::error::Result;
use super::super::metrics::*;
use storage_types::properties::RangeProperties;
use super::super::model::{Coprocessor, KeyEntry, ObserverContext, SplitCheckObserver, SplitChecker};
use super::Host;

pub struct Checker {
    max_size: u64,
    split_size: u64,
    current_size: u64,
    split_keys: Vec<Vec<u8>>,
    batch_split_limit: u64,
    policy: CheckPolicy,
}

impl Checker {
    pub fn new(
        max_size: u64,
        split_size: u64,
        batch_split_limit: u64,
        policy: CheckPolicy,
    ) -> Checker {
        Checker {
            max_size,
            split_size,
            current_size: 0,
            split_keys: Vec::with_capacity(1),
            batch_split_limit,
            policy,
        }
    }
}

impl SplitChecker for Checker {
    fn on_kv(&mut self, _: &mut ObserverContext<'_>, entry: &KeyEntry) -> bool {
        let size = entry.entry_size() as u64;
        self.current_size += size;

        let mut over_limit = self.split_keys.len() as u64 >= self.batch_split_limit;
        if self.current_size > self.split_size && !over_limit {
            self.split_keys.push(keys::origin_key(entry.key()).to_vec());
            // if for previous on_kv() self.current_size == self.split_size,
            // the split key would be pushed this time, but the entry size for this time should not be ignored.
            self.current_size = if self.current_size - size == self.split_size {
                size
            } else {
                0
            };
            over_limit = self.split_keys.len() as u64 >= self.batch_split_limit;
        }

        // For a large region, scan over the range maybe cost too much time,
        // so limit the number of produced split_key for one batch.
        // Also need to scan over self.max_size for last part.
        over_limit && self.current_size + self.split_size >= self.max_size
    }

    fn split_keys(&mut self) -> Vec<Vec<u8>> {
        // make sure not to split when less than max_size for last part
        if self.current_size + self.split_size < self.max_size {
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

    fn approximate_split_keys(&mut self, region: &Region, engine: &DB) -> Result<Vec<Vec<u8>>> {
        Ok(box_try!(get_approximate_split_keys(
            engine,
            region,
            self.split_size,
            self.max_size,
            self.batch_split_limit,
        )))
    }
}

pub struct SizeCheckObserver<C> {
    region_max_size: u64,
    split_size: u64,
    split_limit: u64,
    router: Mutex<C>,
}

impl<C: CasualRouter> SizeCheckObserver<C> {
    pub fn new(
        region_max_size: u64,
        split_size: u64,
        split_limit: u64,
        router: C,
    ) -> SizeCheckObserver<C> {
        SizeCheckObserver {
            region_max_size,
            split_size,
            split_limit,
            router: Mutex::new(router),
        }
    }
}

impl<C> Coprocessor for SizeCheckObserver<C> {}

impl<C: CasualRouter + Send> SplitCheckObserver for SizeCheckObserver<C> {
    fn add_checker(
        &self,
        ctx: &mut ObserverContext<'_>,
        host: &mut Host,
        engine: &DB,
        mut policy: CheckPolicy,
    ) {
        let region = ctx.region();
        let region_id = region.get_id();
        let region_size = match get_region_approximate_size(engine, &region) {
            Ok(size) => size,
            Err(e) => {
                warn!(
                    "failed to get approximate stat";
                    "region_id" => region_id,
                    "err" => %e,
                );
                // Need to check size.
                host.add_checker(Box::new(Checker::new(
                    self.region_max_size,
                    self.split_size,
                    self.split_limit,
                    policy,
                )));
                return;
            }
        };

        // send it to raftstore to update region approximate size
        let res = CasualMessage::RegionApproximateSize { size: region_size };
        if let Err(e) = self.router.lock().unwrap().send(region_id, res) {
            warn!(
                "failed to send approximate region size";
                "region_id" => region_id,
                "err" => %e,
            );
        }

        REGION_SIZE_HISTOGRAM.observe(region_size as f64);
        if region_size >= self.region_max_size {
            info!(
                "approximate size over threshold, need to do split check";
                "region_id" => region.get_id(),
                "size" => region_size,
                "threshold" => self.region_max_size,
            );
            // when meet large region use approximate way to produce split keys
            if region_size >= self.region_max_size * self.split_limit * 2 {
                policy = CheckPolicy::Approximate
            }
            // Need to check size.
            host.add_checker(Box::new(Checker::new(
                self.region_max_size,
                self.split_size,
                self.split_limit,
                policy,
            )));
        } else {
            // Does not need to check size.
            debug!(
                "approximate size less than threshold, does not need to do split check";
                "region_id" => region.get_id(),
                "size" => region_size,
                "threshold" => self.region_max_size,
            );
        }
    }
}

/// Get the approximate size of the range.
pub fn get_region_approximate_size(db: &DB, region: &Region) -> Result<u64> {
    let mut size = 0;
    for cfname in LARGE_CFS {
        size += get_region_approximate_size_cf(db, cfname, &region)?
    }
    Ok(size)
}

pub fn get_region_approximate_size_cf(db: &DB, cfname: &str, region: &Region) -> Result<u64> {
    let cf = box_try!(rocks::util::get_cf_handle(db, cfname));
    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    let range = Range::new(&start_key, &end_key);
    let (_, mut size) = db.get_approximate_memtable_stats_cf(cf, &range);

    let collection = box_try!(util::get_range_properties_cf(
        db, cfname, &start_key, &end_key
    ));
    for (_, v) in &*collection {
        let props = box_try!(RangeProperties::decode(v.user_collected_properties()));
        size += props.get_approximate_size_in_range(&start_key, &end_key);
    }
    Ok(size)
}

/// Get region approximate split keys based on default and write cf.
pub fn get_approximate_split_keys(
    db: &DB,
    region: &Region,
    split_size: u64,
    max_size: u64,
    batch_split_limit: u64,
) -> Result<Vec<Vec<u8>>> {
    let get_cf_size = |cf: &str| get_region_approximate_size_cf(db, cf, &region);

    let default_cf_size = box_try!(get_cf_size(CF_DEFAULT));
    let write_cf_size = box_try!(get_cf_size(CF_WRITE));
    if default_cf_size + write_cf_size == 0 {
        return Err(box_err!("default cf and write cf is empty"));
    }

    // assume the size of keys is uniform distribution in both cfs.
    let (cf, cf_split_size) = if default_cf_size >= write_cf_size {
        (
            CF_DEFAULT,
            split_size * default_cf_size / (default_cf_size + write_cf_size),
        )
    } else {
        (
            CF_WRITE,
            split_size * write_cf_size / (default_cf_size + write_cf_size),
        )
    };

    get_approximate_split_keys_cf(db, cf, &region, cf_split_size, max_size, batch_split_limit)
}

fn get_approximate_split_keys_cf(
    db: &DB,
    cfname: &str,
    region: &Region,
    split_size: u64,
    max_size: u64,
    batch_split_limit: u64,
) -> Result<Vec<Vec<u8>>> {
    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let collection = box_try!(util::get_range_properties_cf(db, cfname, &start, &end));

    let mut keys = vec![];
    let mut total_size = 0;
    for (_, v) in &*collection {
        let props = box_try!(RangeProperties::decode(v.user_collected_properties()));
        total_size += props.get_approximate_size_in_range(&start, &end);
        keys.extend(
            props
                .offsets
                .range::<[u8], _>((Excluded(start.as_slice()), Excluded(end.as_slice())))
                .map(|(k, _)| k.to_owned()),
        );
    }
    if keys.len() == 1 {
        return Ok(vec![]);
    }
    if keys.is_empty() || total_size == 0 || split_size == 0 {
        return Err(box_err!(
            "unexpected key len {} or total_size {} or split size {}, len of collection {}, cf {}, start {}, end {}",
            keys.len(),
            total_size,
            split_size,
            collection.len(),
            cfname,
            hex::encode_upper(&start),
            hex::encode_upper(&end)
        ));
    }
    keys.sort();

    // use total size of this range and the number of keys in this range to
    // calculate the average distance between two keys, and we produce a
    // split_key every `split_size / distance` keys.
    let len = keys.len();
    let distance = total_size as f64 / len as f64;
    let n = (split_size as f64 / distance).ceil() as usize;
    if n == 0 {
        return Err(box_err!(
            "unexpected n == 0, total_size: {}, split_size: {}, len: {}, distance: {}",
            total_size,
            split_size,
            keys.len(),
            distance
        ));
    }

    // cause first element of the iterator will always be returned by step_by(),
    // so the first key returned may not the desired split key. Note that, the
    // start key of region is not included, so we we drop first n - 1 keys.
    //
    // For example, the split size is `3 * distance`. And the numbers stand for the
    // key in `RangeProperties`, `^` stands for produced split key.
    //
    // skip:
    // start___1___2___3___4___5___6___7....
    //                 ^           ^
    //
    // not skip:
    // start___1___2___3___4___5___6___7....
    //         ^           ^           ^
    let mut split_keys = keys
        .into_iter()
        .skip(n - 1)
        .step_by(n)
        .collect::<Vec<Vec<u8>>>();

    if split_keys.len() as u64 > batch_split_limit {
        split_keys.truncate(batch_split_limit as usize);
    } else {
        // make sure not to split when less than max_size for last part
        let rest = (len % n) as u64;
        if rest * distance as u64 + split_size < max_size {
            split_keys.pop();
        }
    }
    Ok(split_keys)
}
