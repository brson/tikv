// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub trait EngineMiscExt {
    /// Gets total used size of database, including:
    /// *  total size (bytes) of all SST files.
    /// *  total size (bytes) of active and unflushed immutable memtables.
    /// *  total size (bytes) of all blob files.
    ///
    fn get_used_size(&self) -> u64;
}
