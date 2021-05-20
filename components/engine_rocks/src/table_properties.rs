// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use crate::util;
use engine_traits::DecodeProperties;
use engine_traits::Range;
use engine_traits::{Error, Result};
use engine_traits::{
    TableProperties, TablePropertiesCollectionIter, TablePropertiesKey, UserCollectedProperties,
};
use engine_traits::{TablePropertiesCollection};
use rocksdb::table_properties_rc as rc;
use std::ops::Deref;

impl RocksEngine {
    pub(crate) fn get_properties_of_tables_in_range(
        &self,
        cf: &str,
        ranges: &[Range],
    ) -> Result<RocksTablePropertiesCollection> {
        let cf = util::get_cf_handle(self.as_inner(), cf)?;
        // FIXME: extra allocation
        let ranges: Vec<_> = ranges.iter().map(util::range_to_rocks_range).collect();
        let raw = self
            .as_inner()
            .get_properties_of_tables_in_range_rc(cf, &ranges);
        let raw = raw.map_err(Error::Engine)?;
        Ok(RocksTablePropertiesCollection::from_raw(raw))
    }

    pub fn get_range_properties_cf(
        &self,
        cfname: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<RocksTablePropertiesCollection> {
        let range = Range::new(start_key, end_key);
        self.get_properties_of_tables_in_range(cfname, &[range])
    }
}

pub struct RocksTablePropertiesCollection(rc::TablePropertiesCollection);

impl RocksTablePropertiesCollection {
    fn from_raw(raw: rc::TablePropertiesCollection) -> RocksTablePropertiesCollection {
        RocksTablePropertiesCollection(raw)
    }
}

impl
    TablePropertiesCollection<
        RocksTablePropertiesCollectionIter,
        RocksTablePropertiesKey,
        RocksTableProperties,
        RocksUserCollectedProperties,
    > for RocksTablePropertiesCollection
{
    fn iter(&self) -> RocksTablePropertiesCollectionIter {
        RocksTablePropertiesCollectionIter(self.0.iter())
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct RocksTablePropertiesCollectionIter(rc::TablePropertiesCollectionIter);

impl
    TablePropertiesCollectionIter<
        RocksTablePropertiesKey,
        RocksTableProperties,
        RocksUserCollectedProperties,
    > for RocksTablePropertiesCollectionIter
{
}

impl Iterator for RocksTablePropertiesCollectionIter {
    type Item = (RocksTablePropertiesKey, RocksTableProperties);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(key, props)| (RocksTablePropertiesKey(key), RocksTableProperties(props)))
    }
}

pub struct RocksTablePropertiesKey(rc::TablePropertiesKey);

impl TablePropertiesKey for RocksTablePropertiesKey {}

impl Deref for RocksTablePropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.deref()
    }
}

pub struct RocksTableProperties(rc::TableProperties);

impl TableProperties<RocksUserCollectedProperties> for RocksTableProperties {
    fn num_entries(&self) -> u64 {
        self.0.num_entries()
    }

    fn user_collected_properties(&self) -> RocksUserCollectedProperties {
        RocksUserCollectedProperties(self.0.user_collected_properties())
    }
}

#[repr(transparent)]
pub struct RocksUserCollectedProperties(rc::UserCollectedProperties);

impl UserCollectedProperties for RocksUserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        self.0.get(index)
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl DecodeProperties for RocksUserCollectedProperties {
    fn decode(&self, k: &str) -> tikv_util::codec::Result<&[u8]> {
        self.get(k.as_bytes())
            .ok_or(tikv_util::codec::Error::KeyNotFound)
    }
}

#[repr(transparent)]
pub struct RocksUserCollectedPropertiesNoRc(rocksdb::UserCollectedProperties);
impl DecodeProperties for RocksUserCollectedPropertiesNoRc {
    fn decode(&self, k: &str) -> tikv_util::codec::Result<&[u8]> {
        self.0
            .get(k.as_bytes())
            .ok_or(tikv_util::codec::Error::KeyNotFound)
    }
}
