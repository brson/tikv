// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{PerfContextExt, PerfLevel, PerfContext, PerfContextKind};

#[macro_export]
macro_rules! report_perf_context {
    ($e: expr, $ctx: expr, $metric: ident) => {
        if $ctx.perf_level != PerfLevel::Disable {
            if let Some(perf_context) = $e.get_perf_context($ctx.kind) {
                let pre_and_post_process = perf_context.write_pre_and_post_process_time();
                let write_thread_wait = perf_context.write_thread_wait_nanos();
                observe_perf_context_type!($ctx, perf_context, $metric, write_wal_time);
                observe_perf_context_type!($ctx, perf_context, $metric, write_memtable_time);
                observe_perf_context_type!($ctx, perf_context, $metric, db_mutex_lock_nanos);
                observe_perf_context_type!($ctx, $metric, pre_and_post_process);
                observe_perf_context_type!($ctx, $metric, write_thread_wait);
                observe_perf_context_type!(
                    $ctx,
                    perf_context,
                    $metric,
                    write_scheduling_flushes_compactions_time
                );
                observe_perf_context_type!($ctx, perf_context, $metric, db_condition_wait_nanos);
                observe_perf_context_type!($ctx, perf_context, $metric, write_delay_time);
            }
        }
    };
}

#[macro_export]
macro_rules! observe_perf_context_type {
    ($s:expr, $metric: expr, $v:ident) => {
        $metric.$v.observe((($v) - $s.$v) as f64 / 1_000_000_000.0);
        $s.$v = $v;
    };
    ($s:expr, $context: expr, $metric: expr, $v:ident) => {
        let $v = $context.$v();
        $metric.$v.observe((($v) - $s.$v) as f64 / 1_000_000_000.0);
        $s.$v = $v;
    };
}

pub struct PerfContextStatistics {
    pub perf_level: PerfLevel,
    pub kind: PerfContextKind,
    pub write_wal_time: u64,
    pub pre_and_post_process: u64,
    pub write_memtable_time: u64,
    pub write_thread_wait: u64,
    pub db_mutex_lock_nanos: u64,
    pub write_scheduling_flushes_compactions_time: u64,
    pub db_condition_wait_nanos: u64,
    pub write_delay_time: u64,
}

impl PerfContextStatistics {
    /// Create an instance which stores instant statistics values, retrieved at creation.
    pub fn new(perf_level: PerfLevel, kind: PerfContextKind) -> Self {
        PerfContextStatistics {
            perf_level,
            kind,
            write_wal_time: 0,
            pre_and_post_process: 0,
            write_thread_wait: 0,
            write_memtable_time: 0,
            db_mutex_lock_nanos: 0,
            write_scheduling_flushes_compactions_time: 0,
            db_condition_wait_nanos: 0,
            write_delay_time: 0,
        }
    }

    pub fn start(&mut self, engine: &impl PerfContextExt) {
        if self.perf_level == PerfLevel::Disable {
            return;
        }
        if let Some(mut ctx) = engine.get_perf_context(self.kind) {
            ctx.reset();
        }
        engine.set_perf_level(self.perf_level);
        self.write_wal_time = 0;
        self.pre_and_post_process = 0;
        self.db_mutex_lock_nanos = 0;
        self.write_thread_wait = 0;
        self.write_memtable_time = 0;
        self.write_scheduling_flushes_compactions_time = 0;
        self.db_condition_wait_nanos = 0;
        self.write_delay_time = 0;
    }

    pub fn report(&self, engine: &impl PerfContextExt) {
    }
}

