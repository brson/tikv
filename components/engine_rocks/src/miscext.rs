use crate::db::Rocks;
use engine_traits::{CFHandleExt, EngineMiscExt, ALL_CFS};
use crate::cf_handle::RocksCFHandle;
use engine::rocks::util::engine_metrics::{
    ROCKSDB_CUR_SIZE_ALL_MEM_TABLES,
    ROCKSDB_TITANDB_LIVE_BLOB_FILE_SIZE, ROCKSDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE,
    ROCKSDB_TOTAL_SST_FILES_SIZE,
};

impl EngineMiscExt for Rocks {
    fn get_used_size(&self) -> u64 {
        let mut used_size: u64 = 0;
        for cf in ALL_CFS {
            let handle = self.cf_handle(cf).unwrap();
            used_size += get_engine_cf_used_size(self, handle);
        }
        used_size
    }
}

fn get_engine_cf_used_size(engine: &Rocks, handle: &RocksCFHandle) -> u64 {
    let engine = engine.as_inner();
    let handle = handle.as_inner();
    let mut cf_used_size = engine
        .get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE)
        .expect("rocksdb is too old, missing total-sst-files-size property");
    // For memtable
    if let Some(mem_table) = engine.get_property_int_cf(handle, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES) {
        cf_used_size += mem_table;
    }
    // For blob files
    if let Some(live_blob) = engine.get_property_int_cf(handle, ROCKSDB_TITANDB_LIVE_BLOB_FILE_SIZE)
    {
        cf_used_size += live_blob;
    }
    if let Some(obsolete_blob) =
        engine.get_property_int_cf(handle, ROCKSDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE)
    {
        cf_used_size += obsolete_blob;
    }

    cf_used_size
}
