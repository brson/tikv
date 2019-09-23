// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::iter;
use std::sync::mpsc;
use std::sync::Arc;

use engine::rocks;
use engine::rocks::util::{new_engine_opt, CFOptions};
use engine::rocks::Writable;
use engine::rocks::{ColumnFamilyOptions, DBOptions};
use engine::{ALL_CFS, CF_DEFAULT, LARGE_CFS};
use kvproto::metapb::Peer;
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;
use tempfile::Builder;

use tikv::raftstore::coprocessor::properties::{
    RangePropertiesCollectorFactory, SizePropertiesCollectorFactory,
};
use tikv::raftstore::store::{keys, SplitCheckRunner, SplitCheckTask};
use tikv::storage::Key;
use tikv_util::config::ReadableSize;
use tikv_util::escape;
use tikv_util::worker::Runnable;

use super::test_split_check_size::must_split_at;
use tikv::raftstore::coprocessor::split_check::half::*;
use tikv::raftstore::coprocessor::{Config, CoprocessorHost};

#[test]
fn test_split_check() {
    let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
    let path_str = path.path().to_str().unwrap();
    let db_opts = DBOptions::new();
    let cfs_opts = ALL_CFS
        .iter()
        .map(|cf| {
            let mut cf_opts = ColumnFamilyOptions::new();
            let f = Box::new(SizePropertiesCollectorFactory::default());
            cf_opts.add_table_properties_collector_factory("tikv.size-collector", f);
            CFOptions::new(cf, cf_opts)
        })
        .collect();
    let engine = Arc::new(new_engine_opt(path_str, db_opts, cfs_opts).unwrap());

    let mut region = Region::default();
    region.set_id(1);
    region.mut_peers().push(Peer::default());
    region.mut_region_epoch().set_version(2);
    region.mut_region_epoch().set_conf_ver(5);

    let (tx, rx) = mpsc::sync_channel(100);
    let mut cfg = Config::default();
    cfg.region_max_size = ReadableSize(BUCKET_NUMBER_LIMIT as u64);
    let mut runnable = SplitCheckRunner::new(
        Arc::clone(&engine),
        tx.clone(),
        Arc::new(CoprocessorHost::new(cfg, tx.clone())),
    );

    // so split key will be z0005
    let cf_handle = engine.cf_handle(CF_DEFAULT).unwrap();
    for i in 0..11 {
        let k = format!("{:04}", i).into_bytes();
        let k = keys::data_key(Key::from_raw(&k).as_encoded());
        engine.put_cf(cf_handle, &k, &k).unwrap();
        // Flush for every key so that we can know the exact middle key.
        engine.flush_cf(cf_handle, true).unwrap();
    }
    runnable.run(SplitCheckTask::new(
        region.clone(),
        false,
        CheckPolicy::Scan,
    ));
    let split_key = Key::from_raw(b"0005");
    must_split_at(&rx, &region, vec![split_key.clone().into_encoded()]);
    runnable.run(SplitCheckTask::new(
        region.clone(),
        false,
        CheckPolicy::Approximate,
    ));
    must_split_at(&rx, &region, vec![split_key.into_encoded()]);
}

#[test]
fn test_get_region_approximate_middle_cf() {
    let tmp = Builder::new()
        .prefix("test_raftstore_util")
        .tempdir()
        .unwrap();
    let path = tmp.path().to_str().unwrap();

    let db_opts = DBOptions::new();
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_level_zero_file_num_compaction_trigger(10);
    let f = Box::new(RangePropertiesCollectorFactory::default());
    cf_opts.add_table_properties_collector_factory("tikv.size-collector", f);
    let cfs_opts = LARGE_CFS
        .iter()
        .map(|cf| CFOptions::new(cf, cf_opts.clone()))
        .collect();
    let engine = rocks::util::new_engine_opt(path, db_opts, cfs_opts).unwrap();

    let cf_handle = engine.cf_handle(CF_DEFAULT).unwrap();
    let mut big_value = Vec::with_capacity(256);
    big_value.extend(iter::repeat(b'v').take(256));
    for i in 0..100 {
        let k = format!("key_{:03}", i).into_bytes();
        let k = keys::data_key(Key::from_raw(&k).as_encoded());
        engine.put_cf(cf_handle, &k, &big_value).unwrap();
        // Flush for every key so that we can know the exact middle key.
        engine.flush_cf(cf_handle, true).unwrap();
    }

    let mut region = Region::default();
    region.mut_peers().push(Peer::default());
    let middle_key = get_region_approximate_middle_cf(&engine, CF_DEFAULT, &region)
        .unwrap()
        .unwrap();

    let middle_key = Key::from_encoded_slice(keys::origin_key(&middle_key))
        .into_raw()
        .unwrap();
    assert_eq!(escape(&middle_key), "key_049");
}
