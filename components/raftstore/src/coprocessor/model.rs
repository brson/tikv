use engine::{CfName, CF_WRITE};
use engine::rocks::DB;
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;
use kvproto::raft_cmdpb::{AdminRequest, AdminResponse, Request, Response};
use raft::StateRole;
use std::cmp::Ordering;
use super::error::Result;

#[derive(PartialEq, Eq)]
pub struct KeyEntry {
    key: Vec<u8>,
    pos: usize,
    value_size: usize,
    cf: CfName,
}

impl KeyEntry {
    pub fn new(key: Vec<u8>, pos: usize, value_size: usize, cf: CfName) -> KeyEntry {
        KeyEntry {
            key,
            pos,
            value_size,
            cf,
        }
    }

    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn is_commit_version(&self) -> bool {
        self.cf == CF_WRITE
    }

    pub fn entry_size(&self) -> usize {
        self.value_size + self.key.len()
    }

    #[inline]
    pub fn pos(&self) -> usize {
        self.pos
    }
}

impl PartialOrd for KeyEntry {
    fn partial_cmp(&self, rhs: &KeyEntry) -> Option<Ordering> {
        // BinaryHeap is max heap, so we have to reverse order to get a min heap.
        Some(self.key.cmp(&rhs.key).reverse())
    }
}

impl Ord for KeyEntry {
    fn cmp(&self, rhs: &KeyEntry) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

/// Coprocessor is used to provide a convenient way to inject code to
/// KV processing.
pub trait Coprocessor {
    fn start(&self) {}
    fn stop(&self) {}
}

/// Context of observer.
pub struct ObserverContext<'a> {
    region: &'a Region,
    /// Whether to bypass following observer hook.
    pub bypass: bool,
}

impl<'a> ObserverContext<'a> {
    pub fn new(region: &Region) -> ObserverContext<'_> {
        ObserverContext {
            region,
            bypass: false,
        }
    }

    pub fn region(&self) -> &Region {
        self.region
    }
}

pub trait AdminObserver: Coprocessor {
    /// Hook to call before proposing admin request.
    fn pre_propose_admin(&self, _: &mut ObserverContext<'_>, _: &mut AdminRequest) -> Result<()> {
        Ok(())
    }

    /// Hook to call before applying admin request.
    fn pre_apply_admin(&self, _: &mut ObserverContext<'_>, _: &AdminRequest) {}

    /// Hook to call after applying admin request.
    fn post_apply_admin(&self, _: &mut ObserverContext<'_>, _: &mut AdminResponse) {}
}

pub trait QueryObserver: Coprocessor {
    /// Hook to call before proposing write request.
    ///
    /// We don't propose read request, hence there is no hook for it yet.
    fn pre_propose_query(&self, _: &mut ObserverContext<'_>, _: &mut Vec<Request>) -> Result<()> {
        Ok(())
    }

    /// Hook to call before applying write request.
    fn pre_apply_query(&self, _: &mut ObserverContext<'_>, _: &[Request]) {}

    /// Hook to call after applying write request.
    fn post_apply_query(&self, _: &mut ObserverContext<'_>, _: &mut Vec<Response>) {}
}

/// SplitChecker is invoked during a split check scan, and decides to use
/// which keys to split a region.
pub trait SplitChecker {
    /// Hook to call for every kv scanned during split.
    ///
    /// Return true to abort scan early.
    fn on_kv(&mut self, _: &mut ObserverContext<'_>, _: &KeyEntry) -> bool {
        false
    }

    /// Get the desired split keys.
    fn split_keys(&mut self) -> Vec<Vec<u8>>;

    /// Get approximate split keys without scan.
    fn approximate_split_keys(&mut self, _: &Region, _: &DB) -> Result<Vec<Vec<u8>>> {
        Ok(vec![])
    }

    /// Get split policy.
    fn policy(&self) -> CheckPolicy;
}

pub trait SplitCheckObserver: Coprocessor {
    /// Add a checker for a split scan.
    fn add_checker(
        &self,
        _: &mut ObserverContext<'_>,
        _: &mut SplitCheckerHost,
        _: &DB,
        policy: CheckPolicy,
    );
}

pub trait RoleObserver: Coprocessor {
    /// Hook to call when role of a peer changes.
    ///
    /// Please note that, this hook is not called at realtime. There maybe a
    /// situation that the hook is not called yet, however the role of some peers
    /// have changed.
    fn on_role_change(&self, _: &mut ObserverContext<'_>, _: StateRole) {}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RegionChangeEvent {
    Create,
    Update,
    Destroy,
}

pub trait RegionChangeObserver: Coprocessor {
    /// Hook to call when a region changed on this TiKV
    fn on_region_changed(&self, _: &mut ObserverContext<'_>, _: RegionChangeEvent, _: StateRole) {}
}

#[derive(Default)]
pub struct SplitCheckerHost {
    checkers: Vec<Box<dyn SplitChecker>>,
    auto_split: bool,
}

impl SplitCheckerHost {
    pub fn new(auto_split: bool) -> SplitCheckerHost {
        SplitCheckerHost {
            auto_split,
            checkers: vec![],
        }
    }

    #[inline]
    pub fn auto_split(&self) -> bool {
        self.auto_split
    }

    #[inline]
    pub fn skip(&self) -> bool {
        self.checkers.is_empty()
    }

    pub fn policy(&self) -> CheckPolicy {
        for checker in &self.checkers {
            if checker.policy() == CheckPolicy::Approximate {
                return CheckPolicy::Approximate;
            }
        }
        CheckPolicy::Scan
    }

    /// Hook to call for every check during split.
    ///
    /// Return true means abort early.
    pub fn on_kv(&mut self, region: &Region, entry: &KeyEntry) -> bool {
        let mut ob_ctx = ObserverContext::new(region);
        for checker in &mut self.checkers {
            if checker.on_kv(&mut ob_ctx, entry) {
                return true;
            }
        }
        false
    }

    pub fn split_keys(&mut self) -> Vec<Vec<u8>> {
        for checker in &mut self.checkers {
            let keys = checker.split_keys();
            if !keys.is_empty() {
                return keys;
            }
        }
        vec![]
    }

    pub fn approximate_split_keys(&mut self, region: &Region, engine: &DB) -> Result<Vec<Vec<u8>>> {
        for checker in &mut self.checkers {
            let keys = box_try!(checker.approximate_split_keys(region, engine));
            if !keys.is_empty() {
                return Ok(keys);
            }
        }
        Ok(vec![])
    }

    #[inline]
    pub fn add_checker(&mut self, checker: Box<dyn SplitChecker>) {
        self.checkers.push(checker);
    }
}
