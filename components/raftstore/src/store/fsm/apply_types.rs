// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use engine::rocks::Snapshot;
use kvproto::import_sstpb::SstMeta;
use kvproto::metapb::{Peer as PeerMeta, Region};
use kvproto::raft_cmdpb::CommitMergeRequest;
use kvproto::raft_serverpb::{
    MergeState, RaftApplyState, RaftTruncatedState,
};
use raft::eraftpb::ConfChange;

#[derive(Default, Debug)]
pub struct ChangePeer {
    pub conf_change: ConfChange,
    pub peer: PeerMeta,
    pub region: Region,
}

#[derive(Debug)]
pub struct Range {
    pub cf: String,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

impl Range {
    pub fn new(cf: String, start_key: Vec<u8>, end_key: Vec<u8>) -> Range {
        Range {
            cf,
            start_key,
            end_key,
        }
    }
}

/// A message that asks the delegate to apply to the given logs and then reply to
/// target mailbox.
#[derive(Default, Debug)]
pub struct CatchUpLogs {
    /// The target region to be notified when given logs are applied.
    pub target_region_id: u64,
    /// Merge request that contains logs to be applied.
    pub merge: CommitMergeRequest,
    /// A flag indicate that all source region's logs are applied.
    ///
    /// This is still necessary although we have a mailbox field already.
    /// Mailbox is used to notify target region, and trigger a round of polling.
    /// But due to the FIFO natural of channel, we need a flag to check if it's
    /// ready when polling.
    pub logs_up_to_date: Arc<AtomicU64>,
}

#[derive(Debug)]
pub enum ExecResult {
    ChangePeer(ChangePeer),
    CompactLog {
        state: RaftTruncatedState,
        first_index: u64,
    },
    SplitRegion {
        regions: Vec<Region>,
        derived: Region,
    },
    PrepareMerge {
        region: Region,
        state: MergeState,
    },
    CatchUpLogs(CatchUpLogs),
    CommitMerge {
        region: Region,
        source: Region,
    },
    RollbackMerge {
        region: Region,
        commit: u64,
    },
    ComputeHash {
        region: Region,
        index: u64,
        snap: Snapshot,
    },
    VerifyHash {
        index: u64,
        hash: Vec<u8>,
    },
    DeleteRange {
        ranges: Vec<Range>,
    },
    IngestSst {
        ssts: Vec<SstMeta>,
    },
}



#[derive(Default, Clone, Debug, PartialEq)]
pub struct ApplyMetrics {
    /// an inaccurate difference in region size since last reset.
    pub size_diff_hint: i64,
    /// delete keys' count since last reset.
    pub delete_keys_hint: u64,

    pub written_bytes: u64,
    pub written_keys: u64,
    pub lock_cf_written_bytes: u64,
}

#[derive(Debug)]
pub struct ApplyRes {
    pub region_id: u64,
    pub apply_state: RaftApplyState,
    pub applied_index_term: u64,
    pub exec_res: VecDeque<ExecResult>,
    pub metrics: ApplyMetrics,
}

#[derive(Debug)]
pub enum TaskRes {
    Apply(ApplyRes),
    Destroy {
        // ID of region that has been destroyed.
        region_id: u64,
        // ID of peer that has been destroyed.
        peer_id: u64,
    },
}
