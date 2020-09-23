// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SledEngine;
use engine_traits::{
    DecodeProperties, Range, Result, TableProperties, TablePropertiesCollection,
    TablePropertiesCollectionIter, TablePropertiesExt, TablePropertiesKey, UserCollectedProperties,
};
use std::ops::Deref;

impl TablePropertiesExt for SledEngine {
    type TablePropertiesCollection = SledTablePropertiesCollection;
    type TablePropertiesCollectionIter = SledTablePropertiesCollectionIter;
    type TablePropertiesKey = SledTablePropertiesKey;
    type TableProperties = SledTableProperties;
    type UserCollectedProperties = SledUserCollectedProperties;

    fn get_properties_of_tables_in_range(
        &self,
        cf: &Self::CFHandle,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection> {
        panic!()
    }
}

pub struct SledTablePropertiesCollection;

impl
    TablePropertiesCollection<
        SledTablePropertiesCollectionIter,
        SledTablePropertiesKey,
        SledTableProperties,
        SledUserCollectedProperties,
    > for SledTablePropertiesCollection
{
    fn iter(&self) -> SledTablePropertiesCollectionIter {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

pub struct SledTablePropertiesCollectionIter;

impl
    TablePropertiesCollectionIter<
        SledTablePropertiesKey,
        SledTableProperties,
        SledUserCollectedProperties,
    > for SledTablePropertiesCollectionIter
{
}

impl Iterator for SledTablePropertiesCollectionIter {
    type Item = (SledTablePropertiesKey, SledTableProperties);

    fn next(&mut self) -> Option<Self::Item> {
        panic!()
    }
}

pub struct SledTablePropertiesKey;

impl TablePropertiesKey for SledTablePropertiesKey {}

impl Deref for SledTablePropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        panic!()
    }
}

pub struct SledTableProperties;

impl TableProperties<SledUserCollectedProperties> for SledTableProperties {
    fn num_entries(&self) -> u64 {
        panic!()
    }

    fn user_collected_properties(&self) -> SledUserCollectedProperties {
        panic!()
    }
}

pub struct SledUserCollectedProperties;

impl UserCollectedProperties for SledUserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

impl DecodeProperties for SledUserCollectedProperties {
    fn decode(&self, k: &str) -> tikv_util::codec::Result<&[u8]> {
        panic!()
    }
}
