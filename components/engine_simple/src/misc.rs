// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SimpleEngine;
use engine_traits::{DeleteStrategy, MiscExt, Range, Result};

impl MiscExt for SimpleEngine {
    fn flush(&self, sync: bool) -> Result<()> {
        /* nop */
        Ok(())
    }

    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()> {
        /* nop */
        Ok(())
    }

    fn delete_ranges_cf(&self, cf: &str, strategy: DeleteStrategy, ranges: &[Range]) -> Result<()> {
        Ok(())
    }

    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range) -> Result<(u64, u64)> {
        panic!()
    }

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool> {
        Ok(false)
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        panic!()
    }

    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        panic!()
    }

    fn path(&self) -> &str {
        &*self.data_dir
    }

    fn sync_wal(&self) -> Result<()> {
        panic!()
    }

    fn exists(path: &str) -> bool {
        panic!()
    }

    fn dump_stats(&self) -> Result<String> {
        panic!()
    }

    fn get_latest_sequence_number(&self) -> u64 {
        0
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        None
    }

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>> {
        panic!()
    }

    fn get_range_entries_and_versions(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<(u64, u64)>> {
        panic!()
    }

    fn get_cf_num_files_at_level(&self, cf: &str, level: usize) -> Result<Option<u64>> {
        panic!()
    }
}
