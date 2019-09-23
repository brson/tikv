use std::cmp::Ordering;

use engine::{CfName, CF_WRITE};

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
