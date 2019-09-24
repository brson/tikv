// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use engine::rocks::{SeekKey, DB};
use engine::CF_WRITE;
use engine::{IterOption, Iterable};
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;
use tidb_query::codec::table as table_codec;
use tikv_util::keybuilder::KeyBuilder;

use keys::{self, Key};

use super::super::model::{
    Coprocessor, KeyEntry, ObserverContext, SplitCheckObserver, SplitChecker,
};
use super::super::error::Result;
use super::Host;

#[derive(Default)]
pub struct Checker {
    first_encoded_table_prefix: Option<Vec<u8>>,
    split_key: Option<Vec<u8>>,
    policy: CheckPolicy,
}

impl SplitChecker for Checker {
    /// Feed keys in order to find the split key.
    /// If `current_data_key` does not belong to `status.first_encoded_table_prefix`.
    /// it returns the encoded table prefix of `current_data_key`.
    fn on_kv(&mut self, _: &mut ObserverContext<'_>, entry: &KeyEntry) -> bool {
        if self.split_key.is_some() {
            return true;
        }

        let current_encoded_key = keys::origin_key(entry.key());

        let split_key = if self.first_encoded_table_prefix.is_some() {
            if !is_same_table(
                self.first_encoded_table_prefix.as_ref().unwrap(),
                current_encoded_key,
            ) {
                // Different tables.
                Some(current_encoded_key)
            } else {
                None
            }
        } else if is_table_key(current_encoded_key) {
            // Now we meet the very first table key of this region.
            Some(current_encoded_key)
        } else {
            None
        };
        self.split_key = split_key.and_then(to_encoded_table_prefix);
        self.split_key.is_some()
    }

    fn split_keys(&mut self) -> Vec<Vec<u8>> {
        match self.split_key.take() {
            None => vec![],
            Some(key) => vec![key],
        }
    }

    fn policy(&self) -> CheckPolicy {
        self.policy
    }
}

#[derive(Default)]
pub struct TableCheckObserver;

impl Coprocessor for TableCheckObserver {}

impl SplitCheckObserver for TableCheckObserver {
    fn add_checker(
        &self,
        ctx: &mut ObserverContext<'_>,
        host: &mut Host,
        engine: &DB,
        policy: CheckPolicy,
    ) {
        let region = ctx.region();
        if is_same_table(region.get_start_key(), region.get_end_key()) {
            // Region is inside a table, skip for saving IO.
            return;
        }

        let end_key = match last_key_of_region(engine, region) {
            Ok(Some(end_key)) => end_key,
            Ok(None) => return,
            Err(err) => {
                warn!(
                    "failed to get region last key";
                    "region_id" => region.get_id(),
                    "err" => %err,
                );
                return;
            }
        };

        let encoded_start_key = region.get_start_key();
        let encoded_end_key = keys::origin_key(&end_key);

        if encoded_start_key.len() < table_codec::TABLE_PREFIX_KEY_LEN
            || encoded_end_key.len() < table_codec::TABLE_PREFIX_KEY_LEN
        {
            // For now, let us scan region if encoded_start_key or encoded_end_key
            // is less than TABLE_PREFIX_KEY_LEN.
            host.add_checker(Box::new(Checker {
                policy,
                ..Default::default()
            }));
            return;
        }

        let mut first_encoded_table_prefix = None;
        let mut split_key = None;
        // Table data starts with `TABLE_PREFIX`.
        // Find out the actual range of this region by comparing with `TABLE_PREFIX`.
        match (
            encoded_start_key[..table_codec::TABLE_PREFIX_LEN].cmp(table_codec::TABLE_PREFIX),
            encoded_end_key[..table_codec::TABLE_PREFIX_LEN].cmp(table_codec::TABLE_PREFIX),
        ) {
            // The range does not cover table data.
            (Ordering::Less, Ordering::Less) | (Ordering::Greater, Ordering::Greater) => return,

            // Following arms matches when the region contains table data.
            // Covers all table data.
            (Ordering::Less, Ordering::Greater) => {}
            // The later part contains table data.
            (Ordering::Less, Ordering::Equal) => {
                // It starts from non-table area to table area,
                // try to extract a split key from `encoded_end_key`, and save it in status.
                split_key = to_encoded_table_prefix(encoded_end_key);
            }
            // Region is in table area.
            (Ordering::Equal, Ordering::Equal) => {
                if is_same_table(encoded_start_key, encoded_end_key) {
                    // Same table.
                    return;
                } else {
                    // Different tables.
                    // Note that table id does not grow by 1, so have to use
                    // `encoded_end_key` to extract a table prefix.
                    // See more: https://github.com/pingcap/tidb/issues/4727
                    split_key = to_encoded_table_prefix(encoded_end_key);
                }
            }
            // The region starts from tabel area to non-table area.
            (Ordering::Equal, Ordering::Greater) => {
                // As the comment above, outside needs scan for finding a split key.
                first_encoded_table_prefix = to_encoded_table_prefix(encoded_start_key);
            }
            _ => panic!(
                "start_key {} and end_key {} out of order",
                hex::encode_upper(encoded_start_key),
                hex::encode_upper(encoded_end_key)
            ),
        }
        host.add_checker(Box::new(Checker {
            first_encoded_table_prefix,
            split_key,
            policy,
        }));
    }
}

pub fn last_key_of_region(db: &DB, region: &Region) -> Result<Option<Vec<u8>>> {
    let start_key = keys::enc_start_key(region);
    let end_key = keys::enc_end_key(region);
    let mut last_key = None;

    let iter_opt = IterOption::new(
        Some(KeyBuilder::from_vec(start_key, 0, 0)),
        Some(KeyBuilder::from_vec(end_key, 0, 0)),
        false,
    );
    let mut iter = box_try!(db.new_iterator_cf(CF_WRITE, iter_opt));

    // the last key
    if iter.seek(SeekKey::End) {
        let key = iter.key().to_vec();
        last_key = Some(key);
    } // else { No data in this CF }

    match last_key {
        Some(lk) => Ok(Some(lk)),
        None => Ok(None),
    }
}

fn to_encoded_table_prefix(encoded_key: &[u8]) -> Option<Vec<u8>> {
    if let Ok(raw_key) = Key::from_encoded_slice(encoded_key).to_raw() {
        table_codec::extract_table_prefix(&raw_key)
            .map(|k| Key::from_raw(k).into_encoded())
            .ok()
    } else {
        None
    }
}

// Encode a key like `t{i64}` will append some unnecessary bytes to the output,
// The first 10 bytes are enough to find out which table this key belongs to.
const ENCODED_TABLE_TABLE_PREFIX: usize = table_codec::TABLE_PREFIX_KEY_LEN + 1;

fn is_table_key(encoded_key: &[u8]) -> bool {
    encoded_key.starts_with(table_codec::TABLE_PREFIX)
        && encoded_key.len() >= ENCODED_TABLE_TABLE_PREFIX
}

fn is_same_table(left_key: &[u8], right_key: &[u8]) -> bool {
    is_table_key(left_key)
        && is_table_key(right_key)
        && left_key[..ENCODED_TABLE_TABLE_PREFIX] == right_key[..ENCODED_TABLE_TABLE_PREFIX]
}

