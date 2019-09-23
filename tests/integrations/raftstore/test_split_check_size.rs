// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tikv::raftstore::coprocessor::split_check::size::{
    Checker, get_approximate_split_keys,
    get_region_approximate_size,
    get_region_approximate_size_cf,
};
use tikv::raftstore::coprocessor::properties::RangePropertiesCollectorFactory;
use tikv::raftstore::coprocessor::{Config, CoprocessorHost, ObserverContext, SplitChecker};
use tikv::raftstore::store::{
    keys, CasualMessage, KeyEntry, SplitCheckRunner, SplitCheckTask,
};
use tikv::storage::Key;
use engine::rocks;
use engine::rocks::util::{new_engine_opt, CFOptions};
use engine::rocks::{ColumnFamilyOptions, DBOptions, Writable};
use engine::{ALL_CFS, CF_DEFAULT, CF_WRITE, LARGE_CFS};
use kvproto::metapb::Peer;
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;
use std::sync::mpsc;
use std::sync::Arc;
use std::{iter, u64};
use tempfile::Builder;
use tikv_util::config::ReadableSize;
use tikv_util::worker::Runnable;

pub fn must_split_at(
    rx: &mpsc::Receiver<(u64, CasualMessage)>,
    exp_region: &Region,
    exp_split_keys: Vec<Vec<u8>>,
) {
    loop {
        match rx.try_recv() {
            Ok((region_id, CasualMessage::RegionApproximateSize { .. }))
                | Ok((region_id, CasualMessage::RegionApproximateKeys { .. })) => {
                    assert_eq!(region_id, exp_region.get_id());
                }
            Ok((
                region_id,
                CasualMessage::SplitRegion {
                    region_epoch,
                    split_keys,
                    ..
                },
            )) => {
                assert_eq!(region_id, exp_region.get_id());
                assert_eq!(&region_epoch, exp_region.get_region_epoch());
                assert_eq!(split_keys, exp_split_keys);
                break;
            }
            others => panic!("expect split check result, but got {:?}", others),
        }
    }
}

#[test]
fn test_split_check() {
    let path = Builder::new().prefix("test-raftstore").tempdir().unwrap();
    let path_str = path.path().to_str().unwrap();
    let db_opts = DBOptions::new();
    let mut cf_opts = ColumnFamilyOptions::new();
    let f = Box::new(RangePropertiesCollectorFactory::default());
    cf_opts.add_table_properties_collector_factory("tikv.range-collector", f);

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
    cfg.region_max_size = ReadableSize(100);
    cfg.region_split_size = ReadableSize(60);
    cfg.batch_split_limit = 5;

    let mut runnable = SplitCheckRunner::new(
        Arc::clone(&engine),
        tx.clone(),
        Arc::new(CoprocessorHost::new(cfg, tx.clone())),
    );

    // so split key will be [z0006]
    for i in 0..7 {
        let s = keys::data_key(format!("{:04}", i).as_bytes());
        engine.put(&s, &s).unwrap();
    }

    runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::Scan));
    // size has not reached the max_size 100 yet.
    match rx.try_recv() {
        Ok((region_id, CasualMessage::RegionApproximateSize { .. })) => {
            assert_eq!(region_id, region.get_id());
        }
        others => panic!("expect recv empty, but got {:?}", others),
    }

    for i in 7..11 {
        let s = keys::data_key(format!("{:04}", i).as_bytes());
        engine.put(&s, &s).unwrap();
    }

    // Approximate size of memtable is inaccurate for small data,
    // we flush it to SST so we can use the size properties instead.
    engine.flush(true).unwrap();

    runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::Scan));
    must_split_at(&rx, &region, vec![b"0006".to_vec()]);

    // so split keys will be [z0006, z0012]
    for i in 11..19 {
        let s = keys::data_key(format!("{:04}", i).as_bytes());
        engine.put(&s, &s).unwrap();
    }
    engine.flush(true).unwrap();
    runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::Scan));
    must_split_at(&rx, &region, vec![b"0006".to_vec(), b"0012".to_vec()]);

    // for test batch_split_limit
    // so split kets will be [z0006, z0012, z0018, z0024, z0030]
    for i in 19..51 {
        let s = keys::data_key(format!("{:04}", i).as_bytes());
        engine.put(&s, &s).unwrap();
    }
    engine.flush(true).unwrap();
    runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::Scan));
    must_split_at(
        &rx,
        &region,
        vec![
            b"0006".to_vec(),
            b"0012".to_vec(),
            b"0018".to_vec(),
            b"0024".to_vec(),
            b"0030".to_vec(),
        ],
    );

    drop(rx);
    // It should be safe even the result can't be sent back.
    runnable.run(SplitCheckTask::new(region, true, CheckPolicy::Scan));
}

#[test]
fn test_checker_with_same_max_and_split_size() {
    let mut checker = Checker::new(24, 24, 1, CheckPolicy::Scan);
    let region = Region::default();
    let mut ctx = ObserverContext::new(&region);
    loop {
        let data = KeyEntry::new(b"zxxxx".to_vec(), 0, 4, CF_WRITE);
        if checker.on_kv(&mut ctx, &data) {
            break;
        }
    }

    assert!(!checker.split_keys().is_empty());
}

#[test]
fn test_checker_with_max_twice_bigger_than_split_size() {
    let mut checker = Checker::new(20, 10, 1, CheckPolicy::Scan);
    let region = Region::default();
    let mut ctx = ObserverContext::new(&region);
    for _ in 0..2 {
        let data = KeyEntry::new(b"zxxxx".to_vec(), 0, 5, CF_WRITE);
        if checker.on_kv(&mut ctx, &data) {
            break;
        }
    }

    assert!(!checker.split_keys().is_empty());
}

fn make_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Region {
    let mut peer = Peer::default();
    peer.set_id(id);
    peer.set_store_id(id);
    let mut region = Region::default();
    region.set_id(id);
    region.set_start_key(start_key);
    region.set_end_key(end_key);
    region.mut_peers().push(peer);
    region
}

#[test]
fn test_get_approximate_split_keys_error() {
    let tmp = Builder::new()
        .prefix("test_raftstore_util")
        .tempdir()
        .unwrap();
    let path = tmp.path().to_str().unwrap();

    let db_opts = DBOptions::new();
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_level_zero_file_num_compaction_trigger(10);

    let cfs_opts = LARGE_CFS
        .iter()
        .map(|cf| CFOptions::new(cf, cf_opts.clone()))
        .collect();
    let engine = rocks::util::new_engine_opt(path, db_opts, cfs_opts).unwrap();

    let region = make_region(1, vec![], vec![]);
    assert_eq!(
        get_approximate_split_keys(&engine, &region, 3, 5, 1).is_err(),
        true
    );

    let cf_handle = engine.cf_handle(CF_DEFAULT).unwrap();
    let mut big_value = Vec::with_capacity(256);
    big_value.extend(iter::repeat(b'v').take(256));
    for i in 0..100 {
        let k = format!("key_{:03}", i).into_bytes();
        let k = keys::data_key(Key::from_raw(&k).as_encoded());
        engine.put_cf(cf_handle, &k, &big_value).unwrap();
        engine.flush_cf(cf_handle, true).unwrap();
    }
    assert_eq!(
        get_approximate_split_keys(&engine, &region, 3, 5, 1).is_err(),
        true
    );
}

#[test]
fn test_get_approximate_split_keys() {
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

    // total size for one key and value
    const ENTRY_SIZE: u64 = 256 + 9;

    for i in 0..4 {
        let k = format!("key_{:03}", i).into_bytes();
        let k = keys::data_key(Key::from_raw(&k).as_encoded());
        engine.put_cf(cf_handle, &k, &big_value).unwrap();
        // Flush for every key so that we can know the exact middle key.
        engine.flush_cf(cf_handle, true).unwrap();
    }
    let region = make_region(1, vec![], vec![]);
    let split_keys =
        get_approximate_split_keys(&engine, &region, 3 * ENTRY_SIZE, 5 * ENTRY_SIZE, 1)
        .unwrap()
        .into_iter()
        .map(|k| {
            Key::from_encoded_slice(keys::origin_key(&k))
                .into_raw()
                .unwrap()
        })
        .collect::<Vec<Vec<u8>>>();

    assert_eq!(split_keys.is_empty(), true);

    for i in 4..5 {
        let k = format!("key_{:03}", i).into_bytes();
        let k = keys::data_key(Key::from_raw(&k).as_encoded());
        engine.put_cf(cf_handle, &k, &big_value).unwrap();
        // Flush for every key so that we can know the exact middle key.
        engine.flush_cf(cf_handle, true).unwrap();
    }
    let split_keys =
        get_approximate_split_keys(&engine, &region, 3 * ENTRY_SIZE, 5 * ENTRY_SIZE, 5)
        .unwrap()
        .into_iter()
        .map(|k| {
            Key::from_encoded_slice(keys::origin_key(&k))
                .into_raw()
                .unwrap()
        })
        .collect::<Vec<Vec<u8>>>();

    assert_eq!(split_keys, vec![b"key_002".to_vec()]);

    for i in 5..10 {
        let k = format!("key_{:03}", i).into_bytes();
        let k = keys::data_key(Key::from_raw(&k).as_encoded());
        engine.put_cf(cf_handle, &k, &big_value).unwrap();
        // Flush for every key so that we can know the exact middle key.
        engine.flush_cf(cf_handle, true).unwrap();
    }
    let split_keys =
        get_approximate_split_keys(&engine, &region, 3 * ENTRY_SIZE, 5 * ENTRY_SIZE, 5)
        .unwrap()
        .into_iter()
        .map(|k| {
            Key::from_encoded_slice(keys::origin_key(&k))
                .into_raw()
                .unwrap()
        })
        .collect::<Vec<Vec<u8>>>();

    assert_eq!(split_keys, vec![b"key_002".to_vec(), b"key_005".to_vec()]);

    for i in 10..20 {
        let k = format!("key_{:03}", i).into_bytes();
        let k = keys::data_key(Key::from_raw(&k).as_encoded());
        engine.put_cf(cf_handle, &k, &big_value).unwrap();
        // Flush for every key so that we can know the exact middle key.
        engine.flush_cf(cf_handle, true).unwrap();
    }
    let split_keys =
        get_approximate_split_keys(&engine, &region, 3 * ENTRY_SIZE, 5 * ENTRY_SIZE, 5)
        .unwrap()
        .into_iter()
        .map(|k| {
            Key::from_encoded_slice(keys::origin_key(&k))
                .into_raw()
                .unwrap()
        })
        .collect::<Vec<Vec<u8>>>();

    assert_eq!(
        split_keys,
        vec![
            b"key_002".to_vec(),
            b"key_005".to_vec(),
            b"key_008".to_vec(),
            b"key_011".to_vec(),
            b"key_014".to_vec(),
        ]
    );
}

#[test]
fn test_region_approximate_size() {
    let path = Builder::new()
        .prefix("_test_raftstore_region_approximate_size")
        .tempdir()
        .unwrap();
    let path_str = path.path().to_str().unwrap();
    let db_opts = DBOptions::new();
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_level_zero_file_num_compaction_trigger(10);
    let f = Box::new(RangePropertiesCollectorFactory::default());
    cf_opts.add_table_properties_collector_factory("tikv.range-collector", f);
    let cfs_opts = LARGE_CFS
        .iter()
        .map(|cf| CFOptions::new(cf, cf_opts.clone()))
        .collect();
    let db = rocks::util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

    let cases = [("a", 1024), ("b", 2048), ("c", 4096)];
    let cf_size = 2 + 1024 + 2 + 2048 + 2 + 4096;
    for &(key, vlen) in &cases {
        for cfname in LARGE_CFS {
            let k1 = keys::data_key(key.as_bytes());
            let v1 = vec![0; vlen as usize];
            assert_eq!(k1.len(), 2);
            let cf = db.cf_handle(cfname).unwrap();
            db.put_cf(cf, &k1, &v1).unwrap();
            db.flush_cf(cf, true).unwrap();
        }
    }

    let region = make_region(1, vec![], vec![]);
    let size = get_region_approximate_size(&db, &region).unwrap();
    assert_eq!(size, cf_size * LARGE_CFS.len() as u64);
    for cfname in LARGE_CFS {
        let size = get_region_approximate_size_cf(&db, cfname, &region).unwrap();
        assert_eq!(size, cf_size);
    }
}

#[test]
fn test_region_maybe_inaccurate_approximate_size() {
    let path = Builder::new()
        .prefix("_test_raftstore_region_maybe_inaccurate_approximate_size")
        .tempdir()
        .unwrap();
    let path_str = path.path().to_str().unwrap();
    let db_opts = DBOptions::new();
    let mut cf_opts = ColumnFamilyOptions::new();
    cf_opts.set_disable_auto_compactions(true);
    let f = Box::new(RangePropertiesCollectorFactory::default());
    cf_opts.add_table_properties_collector_factory("tikv.range-collector", f);
    let cfs_opts = LARGE_CFS
        .iter()
        .map(|cf| CFOptions::new(cf, cf_opts.clone()))
        .collect();
    let db = rocks::util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

    let mut cf_size = 0;
    for i in 0..100 {
        let k1 = keys::data_key(format!("k1{}", i).as_bytes());
        let k2 = keys::data_key(format!("k9{}", i).as_bytes());
        let v = vec![0; 4096];
        cf_size += k1.len() + k2.len() + v.len() * 2;
        let cf = db.cf_handle("default").unwrap();
        db.put_cf(cf, &k1, &v).unwrap();
        db.put_cf(cf, &k2, &v).unwrap();
        db.flush_cf(cf, true).unwrap();
    }

    let region = make_region(1, vec![], vec![]);
    let size = get_region_approximate_size(&db, &region).unwrap();
    assert_eq!(size, cf_size as u64);

    let region = make_region(1, b"k2".to_vec(), b"k8".to_vec());
    let size = get_region_approximate_size(&db, &region).unwrap();
    assert_eq!(size, 0);
}
