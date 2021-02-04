// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Tests for the `engine_traits` crate
//!
//! These are basic tests that can be used to verify the conformance of
//! engines that implement the traits in the `engine_traits` crate.
//!
//! All engine instances are constructed through the `engine_test` crate,
//! so individual engines can be tested by setting that crate's feature flags.
//!
//! e.g. to test the `engine_sled` crate
//!
//! ```no_test
//! cargo test -p engine_traits_tests --no-default-features --features=protobuf-codec,test-engines-sled
//! ```

#![cfg(test)]


fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("tikv-engine-traits-tests")
        .tempdir()
        .unwrap()
}

struct TempDirEnginePair {
    // NB engine must drop before tempdir
    engine: engine_test::kv::KvTestEngine,
    #[allow(unused)]
    tempdir: tempfile::TempDir,
}

fn default_engine() -> TempDirEnginePair {
    use engine_traits::CF_DEFAULT;
    use engine_test::kv::KvTestEngine;
    use engine_test::ctor::EngineConstructorExt;

    let dir = tempdir();
    let path = dir.path().to_str().unwrap();
    let engine = KvTestEngine::new_engine(path, None, &[CF_DEFAULT], None).unwrap();
    TempDirEnginePair {
        engine, tempdir: dir,
    }
}

fn engine_cfs(cfs: &[&str]) -> TempDirEnginePair {
    use engine_test::kv::KvTestEngine;
    use engine_test::ctor::EngineConstructorExt;

    let dir = tempdir();
    let path = dir.path().to_str().unwrap();
    let engine = KvTestEngine::new_engine(path, None, cfs, None).unwrap();
    TempDirEnginePair {
        engine, tempdir: dir,
    }
}


mod ctor {
    //! Constructor tests

    use super::tempdir;

    use engine_traits::ALL_CFS;
    use engine_test::kv::KvTestEngine;
    use engine_test::ctor::{EngineConstructorExt, DBOptions, CFOptions, ColumnFamilyOptions};

    #[test]
    fn new_engine_basic() {
        let dir = tempdir();
        let path = dir.path().to_str().unwrap();
        let _db = KvTestEngine::new_engine(path, None, ALL_CFS, None).unwrap();
    }

    #[test]
    fn new_engine_opt_basic() {
        let dir = tempdir();
        let path = dir.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let cf_opts = ALL_CFS.iter().map(|cf| {
            CFOptions::new(cf, ColumnFamilyOptions::new())
        }).collect();
        let _db = KvTestEngine::new_engine_opt(path, db_opts, cf_opts).unwrap();
    }
}

mod basic_read_write {
    //! Reading and writing

    use super::{default_engine, engine_cfs};
    use engine_traits::{Peekable, SyncMutable};
    use engine_traits::{CF_WRITE, ALL_CFS, CF_DEFAULT};

    #[test]
    fn get_value_none() {
        let db = default_engine();
        let value = db.engine.get_value(b"foo").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn put_get() {
        let db = default_engine();
        db.engine.put(b"foo", b"bar").unwrap();
        let value = db.engine.get_value(b"foo").unwrap();
        let value = value.expect("value");
        assert_eq!(b"bar", &*value);
    }

    #[test]
    fn get_value_cf_none() {
        let db = engine_cfs(&[CF_WRITE]);
        let value = db.engine.get_value_cf(CF_WRITE, b"foo").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn put_get_cf() {
        let db = engine_cfs(&[CF_WRITE]);
        db.engine.put_cf(CF_WRITE, b"foo", b"bar").unwrap();
        let value = db.engine.get_value_cf(CF_WRITE, b"foo").unwrap();
        let value = value.expect("value");
        assert_eq!(b"bar", &*value);
    }

    // Store using put; load using get_cf(CF_DEFAULT)
    #[test]
    fn non_cf_methods_are_default_cf() {
        let db = engine_cfs(ALL_CFS);
        // Use the non-cf put function
        db.engine.put(b"foo", b"bar").unwrap();
        // Retreive with the cf get function
        let value = db.engine.get_value_cf(CF_DEFAULT, b"foo").unwrap();
        let value = value.expect("value");
        assert_eq!(b"bar", &*value);
    }

    #[test]
    fn non_cf_methods_implicit_default_cf() {
        let db = engine_cfs(&[CF_WRITE]);
        db.engine.put(b"foo", b"bar").unwrap();
        let value = db.engine.get_value(b"foo").unwrap();
        let value = value.expect("value");
        assert_eq!(b"bar", &*value);
        // CF_DEFAULT always exists
        let value = db.engine.get_value_cf(CF_DEFAULT, b"foo").unwrap();
        let value = value.expect("value");
        assert_eq!(b"bar", &*value);
    }

    #[test]
    fn delete_none() {
        let db = default_engine();
        let res = db.engine.delete(b"foo");
        assert!(res.is_ok());
    }

    #[test]
    fn delete_cf_none() {
        let db = engine_cfs(ALL_CFS);
        let res = db.engine.delete_cf(CF_WRITE, b"foo");
        assert!(res.is_ok());
    }

    #[test]
    fn delete() {
        let db = default_engine();
        db.engine.put(b"foo", b"bar").unwrap();
        let value = db.engine.get_value(b"foo").unwrap();
        assert!(value.is_some());
        db.engine.delete(b"foo").unwrap();
        let value = db.engine.get_value(b"foo").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn delete_cf() {
        let db = engine_cfs(ALL_CFS);
        db.engine.put_cf(CF_WRITE, b"foo", b"bar").unwrap();
        let value = db.engine.get_value_cf(CF_WRITE, b"foo").unwrap();
        assert!(value.is_some());
        db.engine.delete_cf(CF_WRITE, b"foo").unwrap();
        let value = db.engine.get_value_cf(CF_WRITE, b"foo").unwrap();
        assert!(value.is_none());
    }
}

mod cf_names {
    use super::{default_engine, engine_cfs};
    use engine_traits::{KvEngine, CFNamesExt, Snapshot};
    use engine_traits::{CF_DEFAULT, ALL_CFS, CF_WRITE};

    #[test]
    fn default_names() {
        let db = default_engine();
        let names = db.engine.cf_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], CF_DEFAULT);
    }

    #[test]
    fn cf_names() {
        let db = engine_cfs(ALL_CFS);
        let names = db.engine.cf_names();
        assert_eq!(names.len(), ALL_CFS.len());
        for cf in ALL_CFS {
            assert!(names.contains(cf));
        }
    }

    #[test]
    fn implicit_default_cf() {
        let db = engine_cfs(&[CF_WRITE]);
        let names = db.engine.cf_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&CF_DEFAULT));
    }

    #[test]
    fn default_names_snapshot() {
        let db = default_engine();
        let snapshot = db.engine.snapshot();
        let names = snapshot.cf_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], CF_DEFAULT);
    }

    #[test]
    fn cf_names_snapshot() {
        let db = engine_cfs(ALL_CFS);
        let snapshot = db.engine.snapshot();
        let names = snapshot.cf_names();
        assert_eq!(names.len(), ALL_CFS.len());
        for cf in ALL_CFS {
            assert!(names.contains(cf));
        }
    }

    #[test]
    fn implicit_default_cf_snapshot() {
        let db = engine_cfs(&[CF_WRITE]);
        let snapshot = db.engine.snapshot();
        let names = snapshot.cf_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&CF_DEFAULT));
    }
}

mod iterator {
    use super::{default_engine};
    use engine_traits::{Iterable, Iterator, KvEngine};
    use engine_traits::SeekKey;
    use std::panic::{self, AssertUnwindSafe};

    fn iter_empty<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        let mut iter = i(e);

        assert_eq!(iter.valid().unwrap(), false);

        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = iter.prev();
        })).is_err());
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = iter.next();
        })).is_err());
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.key();
        })).is_err());
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.value();
        })).is_err());

        assert_eq!(iter.seek(SeekKey::Start).unwrap(), false);
        assert_eq!(iter.seek(SeekKey::End).unwrap(), false);
        assert_eq!(iter.seek(SeekKey::Key(b"foo")).unwrap(), false);
        assert_eq!(iter.seek_for_prev(SeekKey::Start).unwrap(), false);
        assert_eq!(iter.seek_for_prev(SeekKey::End).unwrap(), false);
        assert_eq!(iter.seek_for_prev(SeekKey::Key(b"foo")).unwrap(), false);
    }

    #[test]
    fn iter_empty_engine() {
        let db = default_engine();
        iter_empty(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn iter_empty_snapshot() {
        let db = default_engine();
        iter_empty(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn iter_forward<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(!iter.valid().unwrap());

        assert!(iter.seek(SeekKey::Start).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.next().unwrap(), false);

        assert!(!iter.valid().unwrap());

        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.key();
        })).is_err());
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.value();
        })).is_err());
    }

    #[test]
    fn iter_forward_engine() {
        let db = default_engine();
        iter_forward(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn iter_forward_snapshot() {
        let db = default_engine();
        iter_forward(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn iter_reverse<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(!iter.valid().unwrap());

        assert!(iter.seek(SeekKey::End).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.prev().unwrap(), false);

        assert!(!iter.valid().unwrap());

        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.key();
        })).is_err());
        assert!(panic::catch_unwind(AssertUnwindSafe(|| {
            iter.value();
        })).is_err());
    }

    #[test]
    fn iter_reverse_engine() {
        let db = default_engine();
        iter_reverse(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn iter_reverse_snapshot() {
        let db = default_engine();
        iter_reverse(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn seek_to_key_then_forward<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(iter.seek(SeekKey::Key(b"b")).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.next().unwrap(), false);

        assert!(!iter.valid().unwrap());
    }

    #[test]
    fn seek_to_key_then_forward_engine() {
        let db = default_engine();
        seek_to_key_then_forward(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn seek_to_key_then_forward_snapshot() {
        let db = default_engine();
        seek_to_key_then_forward(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn seek_to_key_then_reverse<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(iter.seek(SeekKey::Key(b"b")).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.prev().unwrap(), false);

        assert!(!iter.valid().unwrap());
    }

    #[test]
    fn seek_to_key_then_reverse_engine() {
        let db = default_engine();
        seek_to_key_then_reverse(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn seek_to_key_then_reverse_snapshot() {
        let db = default_engine();
        seek_to_key_then_reverse(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn iter_forward_then_reverse<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(!iter.valid().unwrap());

        assert!(iter.seek(SeekKey::Start).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.prev().unwrap(), false);

        assert!(!iter.valid().unwrap());
    }

    #[test]
    fn iter_forward_then_reverse_engine() {
        let db = default_engine();
        iter_forward_then_reverse(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn iter_forward_then_reverse_snapshot() {
        let db = default_engine();
        iter_forward_then_reverse(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn iter_reverse_then_forward<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(!iter.valid().unwrap());

        assert!(iter.seek(SeekKey::End).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.prev().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"b");

        assert_eq!(iter.next().unwrap(), true);

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert_eq!(iter.next().unwrap(), false);

        assert!(!iter.valid().unwrap());
    }

    #[test]
    fn iter_reverse_then_forward_engine() {
        let db = default_engine();
        iter_reverse_then_forward(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn iter_reverse_then_forward_snapshot() {
        let db = default_engine();
        iter_reverse_then_forward(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    // When seek finds an exact key then seek_for_prev behaves just like seek
    fn seek_for_prev<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"a", b"a").unwrap();
        e.put(b"b", b"b").unwrap();
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(iter.seek_for_prev(SeekKey::Start).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"a");
        assert_eq!(iter.value(), b"a");

        assert!(iter.seek_for_prev(SeekKey::End).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");

        assert!(iter.seek_for_prev(SeekKey::Key(b"c")).unwrap());

        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");
        assert_eq!(iter.value(), b"c");
    }

    #[test]
    fn seek_for_prev_engine() {
        let db = default_engine();
        seek_for_prev(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn seek_for_prev_snapshot() {
        let db = default_engine();
        seek_for_prev(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    // When Seek::Key doesn't find an exact match,
    // it still might succeed, but its behavior differs
    // based on whether `seek` or `seek_for_prev` is called.
    fn seek_key_miss<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(!iter.valid().unwrap());

        assert!(iter.seek(SeekKey::Key(b"b")).unwrap());
        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");

        assert!(!iter.seek(SeekKey::Key(b"d")).unwrap());
        assert!(!iter.valid().unwrap());
    }

    #[test]
    fn seek_key_miss_engine() {
        let db = default_engine();
        seek_key_miss(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn seek_key_miss_snapshot() {
        let db = default_engine();
        seek_key_miss(&db.engine, |e| e.snapshot().iterator().unwrap());
    }

    fn seek_key_prev_miss<E, I, IF>(e: &E, i: IF)
    where E: KvEngine,
          I: Iterator,
          IF: Fn(&E) -> I,
    {
        e.put(b"c", b"c").unwrap();

        let mut iter = i(e);

        assert!(!iter.valid().unwrap());

        assert!(iter.seek_for_prev(SeekKey::Key(b"d")).unwrap());
        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");

        assert!(!iter.seek_for_prev(SeekKey::Key(b"b")).unwrap());
        assert!(!iter.valid().unwrap());
    }

    #[test]
    fn seek_key_prev_miss_engine() {
        let db = default_engine();
        seek_key_prev_miss(&db.engine, |e| e.iterator().unwrap());
    }

    #[test]
    fn seek_key_prev_miss_snapshot() {
        let db = default_engine();
        seek_key_prev_miss(&db.engine, |e| e.snapshot().iterator().unwrap());
    }
}

mod snapshot_basic {
    use super::{default_engine, engine_cfs};
    use engine_traits::{KvEngine, SyncMutable, Peekable};
    use engine_traits::{ALL_CFS, CF_WRITE};

    #[test]
    fn snapshot_get_value() {
        let db = default_engine();

        db.engine.put(b"a", b"aa").unwrap();

        let snap = db.engine.snapshot();

        let value = snap.get_value(b"a").unwrap();
        let value = value.unwrap();
        assert_eq!(value, b"aa");

        let value = snap.get_value(b"b").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn snapshot_get_value_after_put() {
        let db = default_engine();

        db.engine.put(b"a", b"aa").unwrap();

        let snap = db.engine.snapshot();

        db.engine.put(b"a", b"aaa").unwrap();

        let value = snap.get_value(b"a").unwrap();
        let value = value.unwrap();
        assert_eq!(value, b"aa");
    }

    #[test]
    fn snapshot_get_value_cf() {
        let db = engine_cfs(ALL_CFS);

        db.engine.put_cf(CF_WRITE, b"a", b"aa").unwrap();

        let snap = db.engine.snapshot();

        let value = snap.get_value_cf(CF_WRITE, b"a").unwrap();
        let value = value.unwrap();
        assert_eq!(value, b"aa");

        let value = snap.get_value_cf(CF_WRITE, b"b").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn snapshot_get_value_cf_after_put() {
        let db = engine_cfs(ALL_CFS);

        db.engine.put_cf(CF_WRITE, b"a", b"aa").unwrap();

        let snap = db.engine.snapshot();

        db.engine.put_cf(CF_WRITE, b"a", b"aaa").unwrap();

        let value = snap.get_value_cf(CF_WRITE, b"a").unwrap();
        let value = value.unwrap();
        assert_eq!(value, b"aa");
    }
}

mod read_consistency {
    //! Testing iterator and snapshot behavior in the presence of intermixed writes

    use super::{default_engine};
    use engine_traits::{KvEngine, SyncMutable, Peekable};
    use engine_traits::{Iterable, Iterator};

    #[test]
    fn snapshot_with_writes() {
        let db = default_engine();

        db.engine.put(b"a", b"aa").unwrap();

        let snapshot = db.engine.snapshot();

        assert_eq!(snapshot.get_value(b"a").unwrap().unwrap(), b"aa");

        db.engine.put(b"b", b"bb").unwrap();

        assert!(snapshot.get_value(b"b").unwrap().is_none());
        assert_eq!(db.engine.get_value(b"b").unwrap().unwrap(), b"bb");

        db.engine.delete(b"a").unwrap();

        assert_eq!(snapshot.get_value(b"a").unwrap().unwrap(), b"aa");
        assert!(db.engine.get_value(b"a").unwrap().is_none());
    }

    #[test]
    fn snapshot_iterator_with_writes() {
        let db = default_engine();

        db.engine.put(b"a", b"").unwrap();
        db.engine.put(b"c", b"").unwrap();

        let snapshot = db.engine.snapshot();
        let mut iter = snapshot.iterator().unwrap();

        assert!(iter.seek_to_first().unwrap());
        assert_eq!(iter.key(), b"a");

        db.engine.put(b"b", b"").unwrap();

        assert!(iter.next().unwrap());
        assert_eq!(iter.key(), b"c");
        assert!(db.engine.get_value(b"b").unwrap().is_some());

        db.engine.put(b"d", b"").unwrap();

        assert!(!iter.next().unwrap());
        assert!(db.engine.get_value(b"d").unwrap().is_some());

        db.engine.delete(b"a").unwrap();
        db.engine.delete(b"c").unwrap();

        iter.seek_to_first().unwrap();
        assert_eq!(iter.key(), b"a");
        assert!(iter.next().unwrap());
        assert_eq!(iter.key(), b"c");
        assert!(!iter.next().unwrap());

        assert!(db.engine.get_value(b"a").unwrap().is_none());
        assert!(db.engine.get_value(b"c").unwrap().is_none());
    }
}

mod misc {
    use super::{default_engine};
    use engine_traits::{KvEngine, SyncMutable, Peekable};

    #[test]
    fn sync_basic() {
        let db = default_engine();
        db.engine.put(b"foo", b"bar").unwrap();
        db.engine.sync().unwrap();
        let value = db.engine.get_value(b"foo").unwrap();
        let value = value.expect("value");
        assert_eq!(b"bar", &*value);
    }
}

