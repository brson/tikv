// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SimpleEngine;
use engine_traits::{PerfContext, PerfContextExt, PerfContextKind, PerfLevel};

impl PerfContextExt for SimpleEngine {
    type PerfContext = SimplePerfContext;

    fn get_perf_context(&self, level: PerfLevel, kind: PerfContextKind) -> Self::PerfContext {
        SimplePerfContext
    }
}

pub struct SimplePerfContext;

impl PerfContext for SimplePerfContext {
    fn start_observe(&mut self) {
        /* nop */
    }

    fn report_metrics(&mut self) {
        /* nop */
    }
}
