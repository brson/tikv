// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::AgateEngine;
use engine_traits::{
    DecodeProperties, Range, Result, TableProperties, TablePropertiesCollection,
    TablePropertiesCollectionIter, TablePropertiesExt, TablePropertiesKey, UserCollectedProperties,
};
use std::ops::Deref;

impl TablePropertiesExt for AgateEngine {
    type TablePropertiesCollection = AgateTablePropertiesCollection;
    type TablePropertiesCollectionIter = AgateTablePropertiesCollectionIter;
    type TablePropertiesKey = AgateTablePropertiesKey;
    type TableProperties = AgateTableProperties;
    type UserCollectedProperties = AgateUserCollectedProperties;

    fn get_properties_of_tables_in_range(
        &self,
        cf: &str,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection> {
        panic!()
    }
}

pub struct AgateTablePropertiesCollection;

impl
    TablePropertiesCollection<
        AgateTablePropertiesCollectionIter,
        AgateTablePropertiesKey,
        AgateTableProperties,
        AgateUserCollectedProperties,
    > for AgateTablePropertiesCollection
{
    fn iter(&self) -> AgateTablePropertiesCollectionIter {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

pub struct AgateTablePropertiesCollectionIter;

impl
    TablePropertiesCollectionIter<
        AgateTablePropertiesKey,
        AgateTableProperties,
        AgateUserCollectedProperties,
    > for AgateTablePropertiesCollectionIter
{
}

impl Iterator for AgateTablePropertiesCollectionIter {
    type Item = (AgateTablePropertiesKey, AgateTableProperties);

    fn next(&mut self) -> Option<Self::Item> {
        panic!()
    }
}

pub struct AgateTablePropertiesKey;

impl TablePropertiesKey for AgateTablePropertiesKey {}

impl Deref for AgateTablePropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        panic!()
    }
}

pub struct AgateTableProperties;

impl TableProperties<AgateUserCollectedProperties> for AgateTableProperties {
    fn num_entries(&self) -> u64 {
        panic!()
    }

    fn user_collected_properties(&self) -> AgateUserCollectedProperties {
        panic!()
    }
}

pub struct AgateUserCollectedProperties;

impl UserCollectedProperties for AgateUserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

impl DecodeProperties for AgateUserCollectedProperties {
    fn decode(&self, k: &str) -> tikv_util::codec::Result<&[u8]> {
        panic!()
    }
}
