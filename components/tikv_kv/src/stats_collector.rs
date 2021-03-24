// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;

use crate::stats::CfStatistics;
use engine_rocks::PerfContext;

thread_local! {
    pub static TTL_TOMBSTONE : RefCell<usize> = RefCell::new(0);
}

pub enum StatsKind {
    Next,
    Prev,
    Seek,
    SeekForPrev,
}

pub struct StatsCollector<'a> {
    stats: &'a mut CfStatistics,
    kind: StatsKind,

    internal_tombstone: usize,
    ttl_tombstone: usize,
}

impl<'a> StatsCollector<'a> {
    pub fn new(kind: StatsKind, stats: &'a mut CfStatistics) -> Self {
        StatsCollector {
            stats,
            kind,
            internal_tombstone: PerfContext::get().internal_delete_skipped_count() as usize,
            ttl_tombstone: TTL_TOMBSTONE.with(|m| *m.borrow()),
        }
    }
}

impl Drop for StatsCollector<'_> {
    fn drop(&mut self) {
        self.stats.ttl_tombstone += TTL_TOMBSTONE.with(|m| *m.borrow()) - self.ttl_tombstone;
        let internal_tombstone =
            PerfContext::get().internal_delete_skipped_count() as usize - self.internal_tombstone;
        match self.kind {
            StatsKind::Next => {
                self.stats.next += 1;
                self.stats.next_tombstone += internal_tombstone;
            }
            StatsKind::Prev => {
                self.stats.prev += 1;
                self.stats.prev_tombstone += internal_tombstone;
            }
            StatsKind::Seek => {
                self.stats.seek += 1;
                self.stats.seek_tombstone += internal_tombstone;
            }
            StatsKind::SeekForPrev => {
                self.stats.seek_for_prev += 1;
                self.stats.seek_for_prev_tombstone += internal_tombstone;
            }
        }
    }
}
