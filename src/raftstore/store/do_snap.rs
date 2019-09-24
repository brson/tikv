// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine::rocks::{Snapshot as DbSnapshot};
use engine::CF_RAFT;
use engine::{Peekable};
use kvproto::raft_serverpb::{
    PeerState, RaftApplyState, RaftSnapshotData, RegionLocalState,
};
use raft::eraftpb::{Entry, Snapshot};
use crate::raftstore::store::util::conf_state_from_region;
use super::metrics::*;
use super::{SnapEntry, SnapKey, SnapManager, SnapshotStatistics};
use super::peer_storage::storage_error;
use protobuf::Message;

pub fn do_snapshot(
    mgr: SnapManager,
    raft_snap: DbSnapshot,
    kv_snap: DbSnapshot,
    region_id: u64,
) -> raft::Result<Snapshot> {
    debug!(
        "begin to generate a snapshot";
        "region_id" => region_id,
    );

    let apply_state: RaftApplyState =
        match kv_snap.get_msg_cf(CF_RAFT, &keys::apply_state_key(region_id))? {
            None => {
                return Err(storage_error(format!(
                    "could not load raft state of region {}",
                    region_id
                )));
            }
            Some(state) => state,
        };

    let idx = apply_state.get_applied_index();
    let term = if idx == apply_state.get_truncated_state().get_index() {
        apply_state.get_truncated_state().get_term()
    } else {
        match raft_snap.get_msg::<Entry>(&keys::raft_log_key(region_id, idx))? {
            None => {
                return Err(storage_error(format!(
                    "entry {} of {} not found.",
                    idx, region_id
                )));
            }
            Some(entry) => entry.get_term(),
        }
    };
    // Release raft engine snapshot to avoid too many open files.
    drop(raft_snap);

    let key = SnapKey::new(region_id, term, idx);

    mgr.register(key.clone(), SnapEntry::Generating);
    defer!(mgr.deregister(&key, &SnapEntry::Generating));

    let state: RegionLocalState = kv_snap
        .get_msg_cf(CF_RAFT, &keys::region_state_key(key.region_id))
        .and_then(|res| match res {
            None => Err(box_err!("could not find region info")),
            Some(state) => Ok(state),
        })?;

    if state.get_state() != PeerState::Normal {
        return Err(storage_error(format!(
            "snap job for {} seems stale, skip.",
            region_id
        )));
    }

    let mut snapshot = Snapshot::default();

    // Set snapshot metadata.
    snapshot.mut_metadata().set_index(key.idx);
    snapshot.mut_metadata().set_term(key.term);

    let conf_state = conf_state_from_region(state.get_region());
    snapshot.mut_metadata().set_conf_state(conf_state);

    let mut s = mgr.get_snapshot_for_building(&key)?;
    // Set snapshot data.
    let mut snap_data = RaftSnapshotData::default();
    snap_data.set_region(state.get_region().clone());
    let mut stat = SnapshotStatistics::new();
    s.build(
        &kv_snap,
        state.get_region(),
        &mut snap_data,
        &mut stat,
        Box::new(mgr.clone()),
    )?;
    let mut v = vec![];
    snap_data.write_to_vec(&mut v)?;
    snapshot.set_data(v);

    SNAPSHOT_KV_COUNT_HISTOGRAM.observe(stat.kv_count as f64);
    SNAPSHOT_SIZE_HISTOGRAM.observe(stat.size as f64);

    Ok(snapshot)
}
