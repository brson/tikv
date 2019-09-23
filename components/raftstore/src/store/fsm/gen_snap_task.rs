// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine::rocks::{Snapshot};
use engine::Engines;
use raft::eraftpb::{Snapshot as RaftSnapshot};
use std::fmt::{self, Debug};
use std::sync::mpsc::SyncSender;
use crate::store::worker::region_task::Task as RegionTask;
use tikv_util::worker::Scheduler;
use crate::{Result};

pub struct GenSnapTask {
    pub region_id: u64,
    commit_index: u64,
    snap_notifier: SyncSender<RaftSnapshot>,
}

impl GenSnapTask {
    pub fn new(
        region_id: u64,
        commit_index: u64,
        snap_notifier: SyncSender<RaftSnapshot>,
    ) -> GenSnapTask {
        GenSnapTask {
            region_id,
            commit_index,
            snap_notifier,
        }
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn generate_and_schedule_snapshot(
        self,
        engines: &Engines,
        region_sched: &Scheduler<RegionTask>,
    ) -> Result<()> {
        let snapshot = RegionTask::Gen {
            region_id: self.region_id,
            notifier: self.snap_notifier,
            // This snapshot may be held for a long time, which may cause too many
            // open files in rocksdb.
            // TODO: figure out another way to do raft snapshot with short life rocksdb snapshots.
            raft_snap: Snapshot::new(engines.raft.clone()),
            kv_snap: Snapshot::new(engines.kv.clone()),
        };
        box_try!(region_sched.schedule(snapshot));
        Ok(())
    }
}

impl Debug for GenSnapTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenSnapTask")
            .field("region_id", &self.region_id)
            .field("commit_index", &self.commit_index)
            .finish()
    }
}
