// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SimpleEngine;
use engine_traits::{ImportExt, IngestExternalFileOptions, Result};
use std::path::Path;

impl ImportExt for SimpleEngine {
    type IngestExternalFileOptions = SimpleIngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf: &str,
        opts: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()> {
        panic!()
    }

    fn validate_sst_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &str,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()> {
        panic!()
    }
}

pub struct SimpleIngestExternalFileOptions;

impl IngestExternalFileOptions for SimpleIngestExternalFileOptions {
    fn new() -> Self {
        panic!()
    }
    fn move_files(&mut self, f: bool) {
        panic!()
    }
}
