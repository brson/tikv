// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use crate::store::{CasualMessage, PeerMsg, RaftCommand, RaftRouter, StoreMsg};
use crate::{DiscardReason, Error, Result};
use crossbeam::TrySendError;
use engine_rocks::RocksEngine;
use engine_traits::{KvEngine, Snapshot};
use kvproto::raft_serverpb::RaftMessage;
use std::sync::mpsc;

/// Transports messages between different Raft peers.
pub trait Transport: Send + Clone {
    fn send(&mut self, msg: RaftMessage) -> Result<()>;

    fn flush(&mut self);
}

/// Routes message to target region.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait CasualRouter<EK> where EK: KvEngine {
    fn send(&self, region_id: u64, msg: CasualMessage<EK, RocksEngine, EK::Snapshot>) -> Result<()>;
}

/// Routes proposal to target region.
pub trait ProposalRouter<S>
where
    S: Snapshot,
{
    fn send(&self, cmd: RaftCommand<S>) -> std::result::Result<(), TrySendError<RaftCommand<S>>>;
}

/// Routes message to store FSM.
///
/// Messages are not guaranteed to be delivered by this trait.
pub trait StoreRouter {
    fn send(&self, msg: StoreMsg) -> Result<()>;
}

impl<EK> CasualRouter<EK> for RaftRouter<EK, RocksEngine, EK::Snapshot> where EK: KvEngine {
    #[inline]
    fn send(&self, region_id: u64, msg: CasualMessage<EK, RocksEngine, EK::Snapshot>) -> Result<()> {
        match self.router.send(region_id, PeerMsg::CasualMessage(msg)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
            Err(TrySendError::Disconnected(_)) => Err(Error::RegionNotFound(region_id)),
        }
    }
}

impl<S: Snapshot> ProposalRouter<S> for RaftRouter<RocksEngine, RocksEngine, S> {
    #[inline]
    fn send(&self, cmd: RaftCommand<S>) -> std::result::Result<(), TrySendError<RaftCommand<S>>> {
        self.send_raft_command(cmd)
    }
}

impl<S: Snapshot> StoreRouter for RaftRouter<RocksEngine, RocksEngine, S> {
    #[inline]
    fn send(&self, msg: StoreMsg) -> Result<()> {
        match self.send_control(msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
            Err(TrySendError::Disconnected(_)) => {
                Err(Error::Transport(DiscardReason::Disconnected))
            }
        }
    }
}

impl<EK> CasualRouter<EK> for mpsc::SyncSender<(u64, CasualMessage<EK, RocksEngine, EK::Snapshot>)> where EK: KvEngine {
    fn send(&self, region_id: u64, msg: CasualMessage<EK, RocksEngine, EK::Snapshot>) -> Result<()> {
        match self.try_send((region_id, msg)) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Disconnected(_)) => {
                Err(Error::Transport(DiscardReason::Disconnected))
            }
            Err(mpsc::TrySendError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
        }
    }
}

impl<S: Snapshot> ProposalRouter<S> for mpsc::SyncSender<RaftCommand<S>> {
    fn send(
        &self,
        cmd: RaftCommand<S>,
    ) -> std::result::Result<(), TrySendError<RaftCommand<S>>> {
        match self.try_send(cmd) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Disconnected(cmd)) => Err(TrySendError::Disconnected(cmd)),
            Err(mpsc::TrySendError::Full(cmd)) => Err(TrySendError::Full(cmd)),
        }
    }
}

impl StoreRouter for mpsc::Sender<StoreMsg> {
    fn send(&self, msg: StoreMsg) -> Result<()> {
        match self.send(msg) {
            Ok(()) => Ok(()),
            Err(mpsc::SendError(_)) => Err(Error::Transport(DiscardReason::Disconnected)),
        }
    }
}
