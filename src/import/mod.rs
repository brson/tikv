// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! This mod contains components to support rapid data import with the project
//! `tidb-lightning`.
//!
//! It mainly exposes one service:
//!
//! The `ImportSSTService` is used to ingest the generated SST files into TiKV's
//! RocksDB instance. The ingesting process: `tidb-lightning` first uploads SST
//! files to the host where TiKV is located, and then calls the `Ingest` RPC.
//! After `ImportSSTService` receives the RPC, it sends a message to raftstore
//! thread to notify it of the ingesting operation.  This service is running
//! inside TiKV because it needs to interact with raftstore.

use import2::metrics;
use import2::service;
use import2::import_mode;
mod sst_service;

pub use import2::test_helpers;

pub use import2::Config;
pub use import2::{Error, Result};
pub use import2::SSTImporter;
pub use self::sst_service::ImportSSTService;
