// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::PerfContext;

use engine_traits::{IterStatsCountable, IterStatsCounter};
use crate::RocksEngineIterator;

// NB: IterStatsCountable only needs to be implemented for snapshot iterators,
// but this crate uses one iterator type for both engines and snapshots.
type RocksSnapshotIterator = RocksEngineIterator;

impl IterStatsCountable for RocksSnapshotIterator {
    type IterStatsCounter = RocksSnapshotIterStatsCounter;

    fn stats_counter(&self) -> RocksSnapshotIterStatsCounter {
        RocksSnapshotIterStatsCounter
    }
}

pub struct RocksSnapshotIterStatsCounter;

impl IterStatsCounter for RocksSnapshotIterStatsCounter {
    fn count(&self) -> usize {
        PerfContext::get().internal_delete_skipped_count() as usize
    }
}
