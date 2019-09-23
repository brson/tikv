// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Write;
use std::sync::mpsc;
use std::sync::Arc;

use kvproto::metapb::Peer;
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;
use tempfile::Builder;

use tikv::raftstore::store::{CasualMessage, SplitCheckRunner, SplitCheckTask};
use tikv::storage::types::Key;
use engine::rocks::util::new_engine;
use engine::rocks::Writable;
use engine::ALL_CFS;
use engine::CF_WRITE;
use tidb_query::codec::table::{TABLE_PREFIX, TABLE_PREFIX_KEY_LEN};
use tikv_util::codec::number::NumberEncoder;
use tikv_util::config::ReadableSize;
use tikv_util::worker::Runnable;

use tikv::raftstore::coprocessor::split_check::table::*;
use tikv::raftstore::coprocessor::{Config, CoprocessorHost};

/// Composes table record and index prefix: `t[table_id]`.
// Port from TiDB
fn gen_table_prefix(table_id: i64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(TABLE_PREFIX_KEY_LEN);
    buf.write_all(TABLE_PREFIX).unwrap();
    buf.encode_i64(table_id).unwrap();
    buf
}

#[test]
fn test_last_key_of_region() {
    let path = Builder::new()
        .prefix("test_last_key_of_region")
        .tempdir()
        .unwrap();
    let engine =
        Arc::new(new_engine(path.path().to_str().unwrap(), None, ALL_CFS, None).unwrap());
    let write_cf = engine.cf_handle(CF_WRITE).unwrap();

    let mut region = Region::default();
    region.set_id(1);
    region.mut_peers().push(Peer::default());

    // arbitrary padding.
    let padding = b"_r00000005";
    // Put keys, t1_xxx, t2_xxx
    let mut data_keys = vec![];
    for i in 1..3 {
        let mut key = gen_table_prefix(i);
        key.extend_from_slice(padding);
        let k = keys::data_key(Key::from_raw(&key).as_encoded());
        engine.put_cf(write_cf, &k, &k).unwrap();
        data_keys.push(k)
    }

    type Case = (Option<i64>, Option<i64>, Option<Vec<u8>>);
    let mut check_cases = |cases: Vec<Case>| {
        for (start_id, end_id, want) in cases {
            region.set_start_key(
                start_id
                    .map(|id| Key::from_raw(&gen_table_prefix(id)).into_encoded())
                    .unwrap_or_else(Vec::new),
            );
            region.set_end_key(
                end_id
                    .map(|id| Key::from_raw(&gen_table_prefix(id)).into_encoded())
                    .unwrap_or_else(Vec::new),
            );
            assert_eq!(last_key_of_region(&engine, &region).unwrap(), want);
        }
    };

    check_cases(vec![
        // ["", "") => t2_xx
        (None, None, data_keys.get(1).cloned()),
        // ["", "t1") => None
        (None, Some(1), None),
        // ["t1", "") => t2_xx
        (Some(1), None, data_keys.get(1).cloned()),
        // ["t1", "t2") => t1_xx
        (Some(1), Some(2), data_keys.get(0).cloned()),
    ]);
}

#[test]
fn test_table_check_observer() {
    let path = Builder::new()
        .prefix("test_table_check_observer")
        .tempdir()
        .unwrap();
    let engine =
        Arc::new(new_engine(path.path().to_str().unwrap(), None, ALL_CFS, None).unwrap());
    let write_cf = engine.cf_handle(CF_WRITE).unwrap();

    let mut region = Region::default();
    region.set_id(1);
    region.mut_peers().push(Peer::default());
    region.mut_region_epoch().set_version(2);
    region.mut_region_epoch().set_conf_ver(5);

    let (tx, rx) = mpsc::sync_channel(100);
    let (stx, _rx) = mpsc::sync_channel(100);

    let mut cfg = Config::default();
    // Enable table split.
    cfg.split_region_on_table = true;

    // Try to "disable" size split.
    cfg.region_max_size = ReadableSize::gb(2);
    cfg.region_split_size = ReadableSize::gb(1);
    // Try to "disable" keys split
    cfg.region_max_keys = 2000000000;
    cfg.region_split_keys = 1000000000;
    // Try to ignore the ApproximateRegionSize
    let coprocessor = CoprocessorHost::new(cfg, stx);
    let mut runnable =
        SplitCheckRunner::new(Arc::clone(&engine), tx.clone(), Arc::new(coprocessor));

    type Case = (Option<Vec<u8>>, Option<Vec<u8>>, Option<i64>);
    let mut check_cases = |cases: Vec<Case>| {
        for (encoded_start_key, encoded_end_key, table_id) in cases {
            region.set_start_key(encoded_start_key.unwrap_or_else(Vec::new));
            region.set_end_key(encoded_end_key.unwrap_or_else(Vec::new));
            runnable.run(SplitCheckTask::new(region.clone(), true, CheckPolicy::Scan));

            if let Some(id) = table_id {
                let key = Key::from_raw(&gen_table_prefix(id));
                match rx.try_recv() {
                    Ok((_, CasualMessage::SplitRegion { split_keys, .. })) => {
                        assert_eq!(split_keys, vec![key.into_encoded()]);
                    }
                    others => panic!("expect {:?}, but got {:?}", key, others),
                }
            } else {
                match rx.try_recv() {
                    Err(mpsc::TryRecvError::Empty) => (),
                    others => panic!("expect empty, but got {:?}", others),
                }
            }
        }
    };

    let gen_encoded_table_prefix = |table_id| {
        let key = Key::from_raw(&gen_table_prefix(table_id));
        key.into_encoded()
    };

    // arbitrary padding.
    let padding = b"_r00000005";

    // Put some tables
    // t1_xx, t3_xx
    for i in 1..4 {
        if i % 2 == 0 {
            // leave some space.
            continue;
        }

        let mut key = gen_table_prefix(i);
        key.extend_from_slice(padding);
        let s = keys::data_key(Key::from_raw(&key).as_encoded());
        engine.put_cf(write_cf, &s, &s).unwrap();
    }

    check_cases(vec![
        // ["", "") => t1
        (None, None, Some(1)),
        // ["t1", "") => t3
        (Some(gen_encoded_table_prefix(1)), None, Some(3)),
        // ["t1", "t5") => t3
        (
            Some(gen_encoded_table_prefix(1)),
            Some(gen_encoded_table_prefix(5)),
            Some(3),
        ),
        // ["t2", "t4") => t3
        (
            Some(gen_encoded_table_prefix(2)),
            Some(gen_encoded_table_prefix(4)),
            Some(3),
        ),
    ]);

    // Put some data to t3
    for i in 1..4 {
        let mut key = gen_table_prefix(3);
        key.extend_from_slice(format!("{:?}{}", padding, i).as_bytes());
        let s = keys::data_key(Key::from_raw(&key).as_encoded());
        engine.put_cf(write_cf, &s, &s).unwrap();
    }

    check_cases(vec![
        // ["t1", "") => t3
        (Some(gen_encoded_table_prefix(1)), None, Some(3)),
        // ["t3", "") => skip
        (Some(gen_encoded_table_prefix(3)), None, None),
        // ["t3", "t5") => skip
        (
            Some(gen_encoded_table_prefix(3)),
            Some(gen_encoded_table_prefix(5)),
            None,
        ),
    ]);

    // Put some data before t and after t.
    for i in 0..3 {
        // m is less than t and is the prefix of meta keys.
        let key = format!("m{:?}{}", padding, i);
        let s = keys::data_key(Key::from_raw(key.as_bytes()).as_encoded());
        engine.put_cf(write_cf, &s, &s).unwrap();
        let key = format!("u{:?}{}", padding, i);
        let s = keys::data_key(Key::from_raw(key.as_bytes()).as_encoded());
        engine.put_cf(write_cf, &s, &s).unwrap();
    }

    check_cases(vec![
        // ["", "") => t1
        (None, None, Some(1)),
        // ["", "t1"] => skip
        (None, Some(gen_encoded_table_prefix(1)), None),
        // ["", "t3"] => t1
        (None, Some(gen_encoded_table_prefix(3)), Some(1)),
        // ["", "s"] => skip
        (None, Some(b"s".to_vec()), None),
        // ["u", ""] => skip
        (Some(b"u".to_vec()), None, None),
        // ["t3", ""] => None
        (Some(gen_encoded_table_prefix(3)), None, None),
        // ["t1", ""] => t3
        (Some(gen_encoded_table_prefix(1)), None, Some(3)),
    ]);
}
