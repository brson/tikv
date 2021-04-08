// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{IterStatsCountable, IterStatsCounter};
use crate::snapshot::PanicSnapshotIterator;

impl IterStatsCountable for PanicSnapshotIterator {
    type IterStatsCounter = PanicSnapshotIterStatsCounter;

    fn stats_counter(&self) -> PanicSnapshotIterStatsCounter {
        panic!()
    }
}

pub struct PanicSnapshotIterStatsCounter;

impl IterStatsCounter for PanicSnapshotIterStatsCounter {
    fn count(&self) -> usize {
        panic!()
    }
}
