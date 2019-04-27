#![feature(fnbox)]

#[macro_use]
extern crate quick_error;
#[macro_use(
    kv,
    slog_kv,
    slog_warn,
    slog_debug,
    slog_log,
    slog_record,
    slog_b,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[macro_use]
extern crate tikv_util;

pub mod compact_listener;
pub mod cop_table_consts;
pub mod cop_props;
pub mod flow_stats;
pub mod keys;
pub mod kv_region_info;
pub mod mvcc_lock;
pub mod mvcc_types;
pub mod mvcc_write;
pub mod pd_client;
pub mod pd_errors;
pub mod pd_task;
pub mod peer_storage;
pub mod raftstore_bootstrap;
pub mod raftstore_callback;
pub mod region_snapshot;
pub mod region_task;
pub mod storage_key;
pub mod store_info;
pub mod store_util;

pub const PD_INVALID_ID: u64 = 0;
