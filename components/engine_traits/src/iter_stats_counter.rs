// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

/// See `IterStatsCounter` docs.
///
/// This needs to be implemented by `Snapshot` iterators.
pub trait IterStatsCountable {
    type IterStatsCounter: IterStatsCounter;

    fn stats_counter(&self) -> Self::IterStatsCounter;
}

/// It's not clear what the purpose of this is.
///
/// It is used by the tikv StatsCollector for some kind of internal accounting.
/// If you understand it, please update this documentation.
///
/// These currently need to be retrieved directly off an Iterator,
/// by `tikv_kv` `StatsCollector`.
///
/// This is an ugly API.
pub trait IterStatsCounter {
    fn count(&self) -> usize;
}
