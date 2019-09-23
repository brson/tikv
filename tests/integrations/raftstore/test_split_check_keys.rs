// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::test_split_check_size::must_split_at;
use tikv::raftstore::coprocessor::properties::{
    MvccPropertiesCollectorFactory, RangePropertiesCollectorFactory,
};
use tikv::raftstore::coprocessor::{Config, CoprocessorHost};
use tikv::raftstore::store::{keys, CasualMessage, SplitCheckRunner, SplitCheckTask};
use tikv::storage::mvcc::{Write, WriteType};
use tikv::storage::Key;
use engine::rocks;
use engine::rocks::util::{new_engine_opt, CFOptions};
use engine::rocks::{ColumnFamilyOptions, DBOptions, Writable};
use engine::DB;
use engine::{ALL_CFS, CF_DEFAULT, CF_WRITE, LARGE_CFS};
use kvproto::metapb::{Peer, Region};
use kvproto::pdpb::CheckPolicy;
use std::cmp;
use std::sync::{mpsc, Arc};
use std::u64;
use tempfile::Builder;
use tikv_util::worker::Runnable;

use tikv::raftstore::coprocessor::split_check::keys::*;

fn put_data(engine: &DB, mut start_idx: u64, end_idx: u64, fill_short_value: bool) {
    let write_cf = engine.cf_handle(CF_WRITE).unwrap();
    let default_cf = engine.cf_handle(CF_DEFAULT).unwrap();
    let write_value = if fill_short_value {
        Write::new(WriteType::Put, 0, Some(b"shortvalue".to_vec()))
    } else {
        Write::new(WriteType::Put, 0, None)
    }
    .to_bytes();

    while start_idx < end_idx {
        let batch_idx = cmp::min(start_idx + 20, end_idx);
        for i in start_idx..batch_idx {
            let key = keys::data_key(
                Key::from_raw(format!("{:04}", i).as_bytes())
                    .append_ts(2)
                    .as_encoded(),
            );
            engine.put_cf(write_cf, &key, &write_value).unwrap();
            engine.put_cf(default_cf, &key, &[0; 1024]).unwrap();
        }
        // Flush to generate SST files, so that properties can be utilized.
        engine.flush_cf(write_cf, true).unwrap();
        engine.flush_cf(default_cf, true).unwrap();
        start_idx = batch_idx;
    }
}

#[test]
fn test_split_check() {
    let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
    let path_str = path.path().to_str().unwrap();
    let db_opts = DBOptions::new();
    let mut cf_opts = ColumnFamilyOptions::new();
    let f = Box::new(RangePropertiesCollectorFactory::default());
    cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);

    let cfs_opts = ALL_CFS
        .iter()
        .map(|cf| CFOptions::new(cf, cf_opts.clone()))
        .collect();
    let engine = Arc::new(new_engine_opt(path_str, db_opts, cfs_opts).unwrap());

    let mut region = Region::default();
    region.set_id(1);
    region.set_start_key(vec![]);
    region.set_end_key(vec![]);
    region.mut_peers().push(Peer::default());
    region.mut_region_epoch().set_version(2);
    region.mut_region_epoch().set_conf_ver(5);

    let (tx, rx) = mpsc::sync_channel(100);
    let mut cfg = Config::default();
    cfg.region_max_keys = 100;
    cfg.region_split_keys = 80;
    cfg.batch_split_limit = 5;

    let mut runnable = SplitCheckRunner::new(
        Arc::clone(&engine),
        tx.clone(),
        Arc::new(CoprocessorHost::new(cfg, tx.clone())),
    );

    // so split key will be z0080
    put_data(&engine, 0, 90, false);
    runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::Scan));
    // keys has not reached the max_keys 100 yet.
    match rx.try_recv() {
        Ok((region_id, CasualMessage::RegionApproximateSize { .. }))
            | Ok((region_id, CasualMessage::RegionApproximateKeys { .. })) => {
                assert_eq!(region_id, region.get_id());
            }
        others => panic!("expect recv empty, but got {:?}", others),
    }

    put_data(&engine, 90, 160, true);
    runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::Scan));
    must_split_at(
        &rx,
        &region,
        vec![Key::from_raw(b"0080").append_ts(2).into_encoded()],
    );

    put_data(&engine, 160, 300, false);
    runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::Scan));
    must_split_at(
        &rx,
        &region,
        vec![
            Key::from_raw(b"0080").append_ts(2).into_encoded(),
            Key::from_raw(b"0160").append_ts(2).into_encoded(),
            Key::from_raw(b"0240").append_ts(2).into_encoded(),
        ],
    );

    put_data(&engine, 300, 500, false);
    runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::Scan));
    must_split_at(
        &rx,
        &region,
        vec![
            Key::from_raw(b"0080").append_ts(2).into_encoded(),
            Key::from_raw(b"0160").append_ts(2).into_encoded(),
            Key::from_raw(b"0240").append_ts(2).into_encoded(),
            Key::from_raw(b"0320").append_ts(2).into_encoded(),
            Key::from_raw(b"0400").append_ts(2).into_encoded(),
        ],
    );

    drop(rx);
    // It should be safe even the result can't be sent back.
    runnable.run(SplitCheckTask::new(region, true, CheckPolicy::Scan));
}

#[test]
fn test_region_approximate_keys() {
    let path = Builder::new()
        .prefix("_test_region_approximate_keys")
        .tempdir()
        .unwrap();
    let path_str = path.path().to_str().unwrap();
    let db_opts = DBOptions::new();
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_level_zero_file_num_compaction_trigger(10);
    let f = Box::new(RangePropertiesCollectorFactory::default());
    cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
    let cfs_opts = LARGE_CFS
        .iter()
        .map(|cf| CFOptions::new(cf, cf_opts.clone()))
        .collect();
    let db = rocks::util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

    let cases = [("a", 1024), ("b", 2048), ("c", 4096)];
    for &(key, vlen) in &cases {
        let key = keys::data_key(Key::from_raw(key.as_bytes()).append_ts(2).as_encoded());
        let write_v = Write::new(WriteType::Put, 0, None).to_bytes();
        let write_cf = db.cf_handle(CF_WRITE).unwrap();
        db.put_cf(write_cf, &key, &write_v).unwrap();
        db.flush_cf(write_cf, true).unwrap();

        let default_v = vec![0; vlen as usize];
        let default_cf = db.cf_handle(CF_DEFAULT).unwrap();
        db.put_cf(default_cf, &key, &default_v).unwrap();
        db.flush_cf(default_cf, true).unwrap();
    }

    let mut region = Region::default();
    region.mut_peers().push(Peer::default());
    let range_keys = get_region_approximate_keys(&db, &region).unwrap();
    assert_eq!(range_keys, cases.len() as u64);
}

#[test]
fn test_region_approximate_keys_sub_region() {
    let path = Builder::new()
        .prefix("_test_region_approximate_keys_sub_region")
        .tempdir()
        .unwrap();
    let path_str = path.path().to_str().unwrap();
    let db_opts = DBOptions::new();
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_level_zero_file_num_compaction_trigger(10);
    let f = Box::new(MvccPropertiesCollectorFactory::default());
    cf_opts.add_table_properties_collector_factory("tikv.mvcc-properties-collector", f);
    let f = Box::new(RangePropertiesCollectorFactory::default());
    cf_opts.add_table_properties_collector_factory("tikv.range-properties-collector", f);
    let cfs_opts = LARGE_CFS
        .iter()
        .map(|cf| CFOptions::new(cf, cf_opts.clone()))
        .collect();
    let db = rocks::util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

    let write_cf = db.cf_handle(CF_WRITE).unwrap();
    let default_cf = db.cf_handle(CF_DEFAULT).unwrap();
    // size >= 4194304 will insert a new point in range properties
    // 3 points will be inserted into range properties
    let cases = [("a", 4194304), ("b", 4194304), ("c", 4194304)];
    for &(key, vlen) in &cases {
        let key = keys::data_key(Key::from_raw(key.as_bytes()).append_ts(2).as_encoded());
        let write_v = Write::new(WriteType::Put, 0, None).to_bytes();
        db.put_cf(write_cf, &key, &write_v).unwrap();

        let default_v = vec![0; vlen as usize];
        db.put_cf(default_cf, &key, &default_v).unwrap();
    }
    // only flush once, so that mvcc properties will insert one point only
    db.flush_cf(write_cf, true).unwrap();
    db.flush_cf(default_cf, true).unwrap();

    // range properties get 0, mvcc properties get 3
    let mut region = Region::default();
    region.set_id(1);
    region.set_start_key(b"b1".to_vec());
    region.set_end_key(b"b2".to_vec());
    region.mut_peers().push(Peer::default());
    let range_keys = get_region_approximate_keys(&db, &region).unwrap();
    assert_eq!(range_keys, 0);

    // range properties get 1, mvcc properties get 3
    region.set_start_key(b"a".to_vec());
    region.set_end_key(b"c".to_vec());
    let range_keys = get_region_approximate_keys(&db, &region).unwrap();
    assert_eq!(range_keys, 1);
}
