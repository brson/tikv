//! Conveniences for creating a TiKV server

use crate::config::TiKvConfig;
use crate::fatal;
use tikv_util::check_environment_variables;

const RESERVED_OPEN_FDS: u64 = 1000;

/// Various sanity-checks and logging before running a server.
///
/// Warnings are logged and fatal errors exit.
///
/// # Logs
///
/// The presense of these environment variables that affect the database
/// behavior is logged.
///
/// - `GRPC_POLL_STRATEGY`
/// - `http_proxy` and `https_proxy`
///
/// # Warnings
///
/// - if `net.core.somaxconn` < 32768
/// - if `net.ipv4.tcp_syncookies` is not 0
/// - if `vm.swappiness` is not 0
/// - if data directories are not on SSDs
/// - if the "TZ" environment variable is not set on unix
///
/// # Fatal errors
///
/// If the max open file descriptor limit is not high enough to support
/// the main database and the raft database.
///
/// # See also
///
/// See the `check_*` functions in `util::config`:
///
/// - [`tikv_util::config::check_max_open_fs`](../tikv_util/config/fn.check_max_open_fds.html
/// - [`tikv_util::config::check_kernel`](../tikv_util/config/fn.check_kernel.html
/// - [`tikv_util::config::check_data_dir`](../tikv_util/config/fn.check_data_dir.html
///
pub fn pre_start(cfg: &TiKvConfig) {
    // Before any startup, check system configuration and environment variables.
    check_system_config(&cfg);
    check_environment_variables();

    if cfg.panic_when_unexpected_key_or_data {
        info!("panic-when-unexpected-key-or-data is on");
        tikv_util::set_panic_when_unexpected_key_or_data(true);
    }
}

fn check_system_config(config: &TiKvConfig) {
    if let Err(e) = tikv_util::config::check_max_open_fds(
        RESERVED_OPEN_FDS + (config.rocksdb.max_open_files + config.raftdb.max_open_files) as u64,
    ) {
        fatal!("{}", e);
    }

    for e in tikv_util::config::check_kernel() {
        warn!(
            "check-kernel";
            "err" => %e
        );
    }

    // Check RocksDB data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.storage.data_dir) {
        warn!(
            "rocksdb check data dir";
            "err" => %e
        );
    }
    // Check raft data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.raft_store.raftdb_path) {
        warn!(
            "raft check data dir";
            "err" => %e
        );
    }
}
