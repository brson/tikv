// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::AgateEngine;
use engine_traits::{PerfContext, PerfContextExt, PerfContextKind, PerfLevel};

impl PerfContextExt for AgateEngine {
    type PerfContext = AgatePerfContext;

    fn get_perf_context(&self, level: PerfLevel, kind: PerfContextKind) -> Self::PerfContext {
        panic!()
    }
}

pub struct AgatePerfContext;

impl PerfContext for AgatePerfContext {
    fn start_observe(&mut self) {
        panic!()
    }

    fn report_metrics(&mut self) {
        panic!()
    }
}
