// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::AgateEngine;
use engine_traits::{ImportExt, IngestExternalFileOptions, Result};
use std::path::Path;

impl ImportExt for AgateEngine {
    type IngestExternalFileOptions = AgateIngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf: &str,
        opts: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()> {
        panic!()
    }

    fn reset_global_seq<P: AsRef<Path>>(&self, cf: &str, path: P) -> Result<()> {
        panic!()
    }
}

pub struct AgateIngestExternalFileOptions;

impl IngestExternalFileOptions for AgateIngestExternalFileOptions {
    fn new() -> Self {
        panic!()
    }
    fn move_files(&mut self, f: bool) {
        panic!()
    }
}
