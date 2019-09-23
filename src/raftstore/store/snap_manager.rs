// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Reverse;
use std::fs;
use std::io::{self, ErrorKind};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::u64;

use kvproto::raft_serverpb::RaftSnapshotData;
use protobuf::Message;

use crate::raftstore::errors::Error as RaftStoreError;
use crate::raftstore::store::{RaftRouter, StoreMsg};
use crate::raftstore::Result as RaftStoreResult;
use engine::rocks::util::io_limiter::IOLimiter;
use tikv_util::collections::{HashMap, HashMapEntry as Entry};
use tikv_util::HandyRwLock;

use crate::raftstore::store::snap::*;

#[derive(PartialEq, Debug)]
pub enum SnapEntry {
    Generating = 1,
    Sending = 2,
    Receiving = 3,
    Applying = 4,
}

/// `SnapStats` is for snapshot statistics.
pub struct SnapStats {
    pub sending_count: usize,
    pub receiving_count: usize,
}

struct SnapManagerCore {
    base: String,
    registry: HashMap<SnapKey, Vec<SnapEntry>>,
    snap_size: Arc<AtomicU64>,
}

fn notify_stats(ch: Option<&RaftRouter>) {
    if let Some(ch) = ch {
        if let Err(e) = ch.send_control(StoreMsg::SnapshotStats) {
            error!(
                "failed to notify snapshot stats";
                "err" => ?e,
            )
        }
    }
}

/// `SnapManagerCore` trace all current processing snapshots.
#[derive(Clone)]
pub struct SnapManager {
    // directory to store snapfile.
    core: Arc<RwLock<SnapManagerCore>>,
    router: Option<RaftRouter>,
    limiter: Option<Arc<IOLimiter>>,
    max_total_size: u64,
}

impl SnapManager {
    pub fn new<T: Into<String>>(path: T, router: Option<RaftRouter>) -> SnapManager {
        SnapManagerBuilder::default().build(path, router)
    }

    pub fn init(&self) -> io::Result<()> {
        // Use write lock so only one thread initialize the directory at a time.
        let core = self.core.wl();
        let path = Path::new(&core.base);
        if !path.exists() {
            fs::create_dir_all(path)?;
            return Ok(());
        }
        if !path.is_dir() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("{} should be a directory", path.display()),
            ));
        }
        for f in fs::read_dir(path)? {
            let p = f?;
            if p.file_type()?.is_file() {
                if let Some(s) = p.file_name().to_str() {
                    if s.ends_with(TMP_FILE_SUFFIX) {
                        fs::remove_file(p.path())?;
                    } else if s.ends_with(SST_FILE_SUFFIX) {
                        let len = p.metadata()?.len();
                        core.snap_size.fetch_add(len, Ordering::SeqCst);
                    }
                }
            }
        }
        Ok(())
    }

    // Return all snapshots which is idle not being used.
    pub fn list_idle_snap(&self) -> io::Result<Vec<(SnapKey, bool)>> {
        let core = self.core.rl();
        let path = Path::new(&core.base);
        let read_dir = fs::read_dir(path)?;
        // Remove the duplicate snap keys.
        let mut v: Vec<_> = read_dir
            .filter_map(|p| {
                let p = match p {
                    Err(e) => {
                        error!(
                            "failed to list content of directory";
                            "directory" => %core.base,
                            "err" => ?e,
                        );
                        return None;
                    }
                    Ok(p) => p,
                };
                match p.file_type() {
                    Ok(t) if t.is_file() => {}
                    _ => return None,
                }
                let file_name = p.file_name();
                let name = match file_name.to_str() {
                    None => return None,
                    Some(n) => n,
                };
                let is_sending = name.starts_with(SNAP_GEN_PREFIX);
                let numbers: Vec<u64> = name.split('.').next().map_or_else(
                    || vec![],
                    |s| {
                        s.split('_')
                            .skip(1)
                            .filter_map(|s| s.parse().ok())
                            .collect()
                    },
                );
                if numbers.len() != 3 {
                    error!(
                        "failed to parse snapkey";
                        "snap_key" => %name,
                    );
                    return None;
                }
                let snap_key = SnapKey::new(numbers[0], numbers[1], numbers[2]);
                if core.registry.contains_key(&snap_key) {
                    // Skip those registered snapshot.
                    return None;
                }
                Some((snap_key, is_sending))
            })
            .collect();
        v.sort();
        v.dedup();
        Ok(v)
    }

    #[inline]
    pub fn has_registered(&self, key: &SnapKey) -> bool {
        self.core.rl().registry.contains_key(key)
    }

    pub fn get_snapshot_for_building(&self, key: &SnapKey) -> RaftStoreResult<Box<dyn Snapshot>> {
        let mut old_snaps = None;
        while self.get_total_snap_size() > self.max_total_snap_size() {
            if old_snaps.is_none() {
                let snaps = self.list_idle_snap()?;
                let mut key_and_snaps = Vec::with_capacity(snaps.len());
                for (key, is_sending) in snaps {
                    if !is_sending {
                        continue;
                    }
                    let snap = match self.get_snapshot_for_sending(&key) {
                        Ok(snap) => snap,
                        Err(_) => continue,
                    };
                    if let Ok(modified) = snap.meta().and_then(|m| m.modified()) {
                        key_and_snaps.push((key, snap, modified));
                    }
                }
                key_and_snaps.sort_by_key(|&(_, _, modified)| Reverse(modified));
                old_snaps = Some(key_and_snaps);
            }
            match old_snaps.as_mut().unwrap().pop() {
                Some((key, snap, _)) => self.delete_snapshot(&key, snap.as_ref(), false),
                None => return Err(RaftStoreError::Snapshot(Error::TooManySnapshots)),
            };
        }

        let (dir, snap_size) = {
            let core = self.core.rl();
            (core.base.clone(), Arc::clone(&core.snap_size))
        };
        let f = Snap::new_for_building(
            dir,
            key,
            snap_size,
            Box::new(self.clone()),
            self.limiter.clone(),
        )?;
        Ok(Box::new(f))
    }

    pub fn get_snapshot_for_sending(&self, key: &SnapKey) -> RaftStoreResult<Box<dyn Snapshot>> {
        let core = self.core.rl();
        let s = Snap::new_for_sending(
            &core.base,
            key,
            Arc::clone(&core.snap_size),
            Box::new(self.clone()),
        )?;
        Ok(Box::new(s))
    }

    pub fn get_snapshot_for_receiving(
        &self,
        key: &SnapKey,
        data: &[u8],
    ) -> RaftStoreResult<Box<dyn Snapshot>> {
        let core = self.core.rl();
        let mut snapshot_data = RaftSnapshotData::default();
        snapshot_data.merge_from_bytes(data)?;
        let f = Snap::new_for_receiving(
            &core.base,
            key,
            snapshot_data.take_meta(),
            Arc::clone(&core.snap_size),
            Box::new(self.clone()),
            self.limiter.clone(),
        )?;
        Ok(Box::new(f))
    }

    pub fn get_snapshot_for_applying(&self, key: &SnapKey) -> RaftStoreResult<Box<dyn Snapshot>> {
        let core = self.core.rl();
        let s = Snap::new_for_applying(
            &core.base,
            key,
            Arc::clone(&core.snap_size),
            Box::new(self.clone()),
        )?;
        if !s.exists() {
            return Err(RaftStoreError::Other(From::from(
                format!("snapshot of {:?} not exists.", key).to_string(),
            )));
        }
        Ok(Box::new(s))
    }

    /// Get the approximate size of snap file exists in snap directory.
    ///
    /// Return value is not guaranteed to be accurate.
    pub fn get_total_snap_size(&self) -> u64 {
        let core = self.core.rl();
        core.snap_size.load(Ordering::SeqCst)
    }

    pub fn max_total_snap_size(&self) -> u64 {
        self.max_total_size
    }

    pub fn register(&self, key: SnapKey, entry: SnapEntry) {
        debug!(
            "register snapshot";
            "key" => %key,
            "entry" => ?entry,
        );
        let mut core = self.core.wl();
        match core.registry.entry(key) {
            Entry::Occupied(mut e) => {
                if e.get().contains(&entry) {
                    warn!(
                        "snap key is registered more than once!";
                        "key" => %e.key(),
                    );
                    return;
                }
                e.get_mut().push(entry);
            }
            Entry::Vacant(e) => {
                e.insert(vec![entry]);
            }
        }

        notify_stats(self.router.as_ref());
    }

    pub fn deregister(&self, key: &SnapKey, entry: &SnapEntry) {
        debug!(
            "deregister snapshot";
            "key" => %key,
            "entry" => ?entry,
        );
        let mut need_clean = false;
        let mut handled = false;
        let mut core = self.core.wl();
        if let Some(e) = core.registry.get_mut(key) {
            let last_len = e.len();
            e.retain(|e| e != entry);
            need_clean = e.is_empty();
            handled = last_len > e.len();
        }
        if need_clean {
            core.registry.remove(key);
        }
        if handled {
            notify_stats(self.router.as_ref());
            return;
        }
        warn!(
            "stale deregister snapshot";
            "key" => %key,
            "entry" => ?entry,
        );
    }

    pub fn stats(&self) -> SnapStats {
        let core = self.core.rl();
        // send_count, generating_count, receiving_count, applying_count
        let (mut sending_cnt, mut receiving_cnt) = (0, 0);
        for v in core.registry.values() {
            let (mut is_sending, mut is_receiving) = (false, false);
            for s in v {
                match *s {
                    SnapEntry::Sending | SnapEntry::Generating => is_sending = true,
                    SnapEntry::Receiving | SnapEntry::Applying => is_receiving = true,
                }
            }
            if is_sending {
                sending_cnt += 1;
            }
            if is_receiving {
                receiving_cnt += 1;
            }
        }

        SnapStats {
            sending_count: sending_cnt,
            receiving_count: receiving_cnt,
        }
    }
}

impl SnapshotDeleter for SnapManager {
    fn delete_snapshot(&self, key: &SnapKey, snap: &dyn Snapshot, check_entry: bool) -> bool {
        let core = self.core.rl();
        if check_entry {
            if let Some(e) = core.registry.get(key) {
                if e.len() > 1 {
                    info!(
                        "skip to delete snapshot since it's registered more than once";
                        "snapshot" => %snap.path(),
                        "registered_entries" => ?e,
                    );
                    return false;
                }
            }
        } else if core.registry.contains_key(key) {
            info!(
                "skip to delete snapshot since it's registered";
                "snapshot" => %snap.path(),
            );
            return false;
        }
        snap.delete();
        true
    }
}

#[derive(Debug, Default)]
pub struct SnapManagerBuilder {
    max_write_bytes_per_sec: u64,
    max_total_size: u64,
}

impl SnapManagerBuilder {
    pub fn max_write_bytes_per_sec(&mut self, bytes: u64) -> &mut SnapManagerBuilder {
        self.max_write_bytes_per_sec = bytes;
        self
    }
    pub fn max_total_size(&mut self, bytes: u64) -> &mut SnapManagerBuilder {
        self.max_total_size = bytes;
        self
    }
    pub fn build<T: Into<String>>(&self, path: T, router: Option<RaftRouter>) -> SnapManager {
        let limiter = if self.max_write_bytes_per_sec > 0 {
            Some(Arc::new(IOLimiter::new(self.max_write_bytes_per_sec)))
        } else {
            None
        };
        let max_total_size = if self.max_total_size > 0 {
            self.max_total_size
        } else {
            u64::MAX
        };
        SnapManager {
            core: Arc::new(RwLock::new(SnapManagerCore {
                base: path.into(),
                registry: map![],
                snap_size: Arc::new(AtomicU64::new(0)),
            })),
            router,
            limiter,
            max_total_size,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::cmp;
    use std::fs::{File};
    use std::io::{self, Read, Write};
    use std::sync::atomic::{AtomicU64};
    use std::sync::Arc;

    use engine::{Snapshot as DbSnapshot};
    use kvproto::raft_serverpb::{
        RaftSnapshotData,
    };
    use protobuf::Message;
    use tempfile::Builder;

    use super::{
        Snap, SnapEntry, SnapKey, SnapManager, SnapManagerBuilder, Snapshot,
        SnapshotDeleter, SnapshotStatistics,
    };

    use super::super::snap::test_helpers::*;

    #[test]
    fn test_snap_mgr_create_dir() {
        // Ensure `mgr` creates the specified directory when it does not exist.
        let temp_dir = Builder::new()
            .prefix("test-snap-mgr-create-dir")
            .tempdir()
            .unwrap();
        let temp_path = temp_dir.path().join("snap1");
        let path = temp_path.to_str().unwrap().to_owned();
        assert!(!temp_path.exists());
        let mut mgr = SnapManager::new(path, None);
        mgr.init().unwrap();
        assert!(temp_path.exists());

        // Ensure `init()` will return an error if specified target is a file.
        let temp_path2 = temp_dir.path().join("snap2");
        let path2 = temp_path2.to_str().unwrap().to_owned();
        File::create(temp_path2).unwrap();
        mgr = SnapManager::new(path2, None);
        assert!(mgr.init().is_err());
    }

    #[test]
    fn test_snap_mgr_v2() {
        let temp_dir = Builder::new().prefix("test-snap-mgr-v2").tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap().to_owned();
        let mgr = SnapManager::new(path.clone(), None);
        mgr.init().unwrap();
        assert_eq!(mgr.get_total_snap_size(), 0);

        let db_dir = Builder::new()
            .prefix("test-snap-mgr-delete-temp-files-v2-db")
            .tempdir()
            .unwrap();
        let snapshot = DbSnapshot::new(open_test_db(&db_dir.path(), None, None).unwrap());
        let key1 = SnapKey::new(1, 1, 1);
        let size_track = Arc::new(AtomicU64::new(0));
        let deleter = Box::new(mgr.clone());
        let mut s1 =
            Snap::new_for_building(&path, &key1, Arc::clone(&size_track), deleter.clone(), None)
                .unwrap();
        let mut region = gen_test_region(1, 1, 1);
        let mut snap_data = RaftSnapshotData::default();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(
            &snapshot,
            &region,
            &mut snap_data,
            &mut stat,
            deleter.clone(),
        )
        .unwrap();
        let mut s =
            Snap::new_for_sending(&path, &key1, Arc::clone(&size_track), deleter.clone()).unwrap();
        let expected_size = s.total_size().unwrap();
        let mut s2 = Snap::new_for_receiving(
            &path,
            &key1,
            snap_data.get_meta().clone(),
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();
        let n = io::copy(&mut s, &mut s2).unwrap();
        assert_eq!(n, expected_size);
        s2.save().unwrap();

        let key2 = SnapKey::new(2, 1, 1);
        region.set_id(2);
        snap_data.set_region(region);
        let s3 =
            Snap::new_for_building(&path, &key2, Arc::clone(&size_track), deleter.clone(), None)
                .unwrap();
        let s4 = Snap::new_for_receiving(
            &path,
            &key2,
            snap_data.take_meta(),
            Arc::clone(&size_track),
            deleter.clone(),
            None,
        )
        .unwrap();

        assert!(s1.exists());
        assert!(s2.exists());
        assert!(!s3.exists());
        assert!(!s4.exists());

        let mgr = SnapManager::new(path, None);
        mgr.init().unwrap();
        assert_eq!(mgr.get_total_snap_size(), expected_size * 2);

        assert!(s1.exists());
        assert!(s2.exists());
        assert!(!s3.exists());
        assert!(!s4.exists());

        mgr.get_snapshot_for_sending(&key1).unwrap().delete();
        assert_eq!(mgr.get_total_snap_size(), expected_size);
        mgr.get_snapshot_for_applying(&key1).unwrap().delete();
        assert_eq!(mgr.get_total_snap_size(), 0);
    }

    fn check_registry_around_deregister(mgr: SnapManager, key: &SnapKey, entry: &SnapEntry) {
        let snap_keys = mgr.list_idle_snap().unwrap();
        assert!(snap_keys.is_empty());
        assert!(mgr.has_registered(key));
        mgr.deregister(key, entry);
        let mut snap_keys = mgr.list_idle_snap().unwrap();
        assert_eq!(snap_keys.len(), 1);
        let snap_key = snap_keys.pop().unwrap().0;
        assert_eq!(snap_key, *key);
        assert!(!mgr.has_registered(&snap_key));
    }

    #[test]
    fn test_snap_deletion_on_registry() {
        let src_temp_dir = Builder::new()
            .prefix("test-snap-deletion-on-registry-src")
            .tempdir()
            .unwrap();
        let src_path = src_temp_dir.path().to_str().unwrap().to_owned();
        let src_mgr = SnapManager::new(src_path.clone(), None);
        src_mgr.init().unwrap();

        let src_db_dir = Builder::new()
            .prefix("test-snap-deletion-on-registry-src-db")
            .tempdir()
            .unwrap();
        let db = open_test_db(&src_db_dir.path(), None, None).unwrap();
        let snapshot = DbSnapshot::new(db);

        let key = SnapKey::new(1, 1, 1);
        let region = gen_test_region(1, 1, 1);

        // Ensure the snapshot being built will not be deleted on GC.
        src_mgr.register(key.clone(), SnapEntry::Generating);
        let mut s1 = src_mgr.get_snapshot_for_building(&key).unwrap();
        let mut snap_data = RaftSnapshotData::default();
        snap_data.set_region(region.clone());
        let mut stat = SnapshotStatistics::new();
        s1.build(
            &snapshot,
            &region,
            &mut snap_data,
            &mut stat,
            Box::new(src_mgr.clone()),
        )
        .unwrap();
        let mut v = vec![];
        snap_data.write_to_vec(&mut v).unwrap();

        check_registry_around_deregister(src_mgr.clone(), &key, &SnapEntry::Generating);

        // Ensure the snapshot being sent will not be deleted on GC.
        src_mgr.register(key.clone(), SnapEntry::Sending);
        let mut s2 = src_mgr.get_snapshot_for_sending(&key).unwrap();
        let expected_size = s2.total_size().unwrap();

        let dst_temp_dir = Builder::new()
            .prefix("test-snap-deletion-on-registry-dst")
            .tempdir()
            .unwrap();
        let dst_path = dst_temp_dir.path().to_str().unwrap().to_owned();
        let dst_mgr = SnapManager::new(dst_path.clone(), None);
        dst_mgr.init().unwrap();

        // Ensure the snapshot being received will not be deleted on GC.
        dst_mgr.register(key.clone(), SnapEntry::Receiving);
        let mut s3 = dst_mgr.get_snapshot_for_receiving(&key, &v[..]).unwrap();
        let n = io::copy(&mut s2, &mut s3).unwrap();
        assert_eq!(n, expected_size);
        s3.save().unwrap();

        check_registry_around_deregister(src_mgr.clone(), &key, &SnapEntry::Sending);
        check_registry_around_deregister(dst_mgr.clone(), &key, &SnapEntry::Receiving);

        // Ensure the snapshot to be applied will not be deleted on GC.
        let mut snap_keys = dst_mgr.list_idle_snap().unwrap();
        assert_eq!(snap_keys.len(), 1);
        let snap_key = snap_keys.pop().unwrap().0;
        assert_eq!(snap_key, key);
        assert!(!dst_mgr.has_registered(&snap_key));
        dst_mgr.register(key.clone(), SnapEntry::Applying);
        let s4 = dst_mgr.get_snapshot_for_applying(&key).unwrap();
        let s5 = dst_mgr.get_snapshot_for_applying(&key).unwrap();
        dst_mgr.delete_snapshot(&key, s4.as_ref(), false);
        assert!(s5.exists());
    }

    #[test]
    fn test_snapshot_max_total_size() {
        let regions: Vec<u64> = (0..20).collect();
        let kv_path = Builder::new()
            .prefix("test-snapshot-max-total-size-db")
            .tempdir()
            .unwrap();
        let engine = get_test_db_for_regions(&kv_path, None, None, None, None, &regions).unwrap();

        let snapfiles_path = Builder::new()
            .prefix("test-snapshot-max-total-size-snapshots")
            .tempdir()
            .unwrap();
        let max_total_size = 10240;
        let snap_mgr = SnapManagerBuilder::default()
            .max_total_size(max_total_size)
            .build(snapfiles_path.path().to_str().unwrap(), None);
        let snapshot = DbSnapshot::new(engine.kv);

        // Add an oldest snapshot for receiving.
        let recv_key = SnapKey::new(100, 100, 100);
        let recv_head = {
            let mut stat = SnapshotStatistics::new();
            let mut snap_data = RaftSnapshotData::default();
            let mut s = snap_mgr.get_snapshot_for_building(&recv_key).unwrap();
            s.build(
                &snapshot,
                &gen_test_region(100, 1, 1),
                &mut snap_data,
                &mut stat,
                Box::new(snap_mgr.clone()),
            )
            .unwrap();
            snap_data.write_to_bytes().unwrap()
        };
        let recv_remain = {
            let mut data = Vec::with_capacity(1024);
            let mut s = snap_mgr.get_snapshot_for_sending(&recv_key).unwrap();
            s.read_to_end(&mut data).unwrap();
            assert!(snap_mgr.delete_snapshot(&recv_key, s.as_ref(), true));
            data
        };
        let mut s = snap_mgr
            .get_snapshot_for_receiving(&recv_key, &recv_head)
            .unwrap();
        s.write_all(&recv_remain).unwrap();
        s.save().unwrap();

        for (i, region_id) in regions.into_iter().enumerate() {
            let key = SnapKey::new(region_id, 1, 1);
            let region = gen_test_region(region_id, 1, 1);
            let mut s = snap_mgr.get_snapshot_for_building(&key).unwrap();
            let mut snap_data = RaftSnapshotData::default();
            let mut stat = SnapshotStatistics::new();
            s.build(
                &snapshot,
                &region,
                &mut snap_data,
                &mut stat,
                Box::new(snap_mgr.clone()),
            )
            .unwrap();

            // TODO: this size may change in different RocksDB version.
            let snap_size = 1438;
            let max_snap_count = (max_total_size + snap_size - 1) / snap_size;
            // The first snap_size is for region 100.
            // That snapshot won't be deleted because it's not for generating.
            assert_eq!(
                snap_mgr.get_total_snap_size(),
                snap_size * cmp::min(max_snap_count, (i + 2) as u64)
            );
        }
    }
}
