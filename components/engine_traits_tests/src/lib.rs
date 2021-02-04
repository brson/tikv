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

mod engine_iter {
    use super::{default_engine};
    use engine_traits::{Iterable, Iterator};
    use engine_traits::SeekKey;
    use engine_traits::SyncMutable;
    use std::panic::{self, AssertUnwindSafe};

    #[test]
    fn iter_empty() {
        let db = default_engine();
        let mut iter = db.engine.iterator().unwrap();

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
    fn iter_forward() {
        let db = default_engine();

        db.engine.put(b"a", b"a").unwrap();
        db.engine.put(b"b", b"b").unwrap();
        db.engine.put(b"c", b"c").unwrap();

        let mut iter = db.engine.iterator().unwrap();

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
    fn iter_reverse() {
        let db = default_engine();

        db.engine.put(b"a", b"a").unwrap();
        db.engine.put(b"b", b"b").unwrap();
        db.engine.put(b"c", b"c").unwrap();

        let mut iter = db.engine.iterator().unwrap();

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
    fn seek_to_key_then_forward() {
        let db = default_engine();

        db.engine.put(b"a", b"a").unwrap();
        db.engine.put(b"b", b"b").unwrap();
        db.engine.put(b"c", b"c").unwrap();

        let mut iter = db.engine.iterator().unwrap();

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
    fn seek_to_key_then_reverse() {
        let db = default_engine();

        db.engine.put(b"a", b"a").unwrap();
        db.engine.put(b"b", b"b").unwrap();
        db.engine.put(b"c", b"c").unwrap();

        let mut iter = db.engine.iterator().unwrap();

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
    fn iter_forward_then_reverse() {
        let db = default_engine();

        db.engine.put(b"a", b"a").unwrap();
        db.engine.put(b"b", b"b").unwrap();
        db.engine.put(b"c", b"c").unwrap();

        let mut iter = db.engine.iterator().unwrap();

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
    fn iter_reverse_then_forward() {
        let db = default_engine();

        db.engine.put(b"a", b"a").unwrap();
        db.engine.put(b"b", b"b").unwrap();
        db.engine.put(b"c", b"c").unwrap();

        let mut iter = db.engine.iterator().unwrap();

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

    // When seek finds an exact key then seek_for_prev behaves just like seek
    #[test]
    fn seek_for_prev() {
        let db = default_engine();

        db.engine.put(b"a", b"a").unwrap();
        db.engine.put(b"b", b"b").unwrap();
        db.engine.put(b"c", b"c").unwrap();

        let mut iter = db.engine.iterator().unwrap();

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

    // When Seek::Key doesn't find an exact match,
    // it still might succeed, but its behavior differs
    // based on whether `seek` or `seek_for_prev` is called.
    #[test]
    fn seek_key_miss() {
        let db = default_engine();

        db.engine.put(b"c", b"c").unwrap();

        let mut iter = db.engine.iterator().unwrap();

        assert!(!iter.valid().unwrap());

        assert!(iter.seek(SeekKey::Key(b"b")).unwrap());
        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");

        assert!(!iter.seek(SeekKey::Key(b"d")).unwrap());
        assert!(!iter.valid().unwrap());
    }

    #[test]
    fn seek_key_prev_miss() {
        let db = default_engine();

        db.engine.put(b"c", b"c").unwrap();

        let mut iter = db.engine.iterator().unwrap();

        assert!(!iter.valid().unwrap());

        assert!(iter.seek_for_prev(SeekKey::Key(b"d")).unwrap());
        assert!(iter.valid().unwrap());
        assert_eq!(iter.key(), b"c");

        assert!(!iter.seek_for_prev(SeekKey::Key(b"b")).unwrap());
        assert!(!iter.valid().unwrap());
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

mod snapshot {
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
