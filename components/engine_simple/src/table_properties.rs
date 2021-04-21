// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SimpleEngine;
use engine_traits::{
    DecodeProperties, Range, Result, TableProperties, TablePropertiesCollection,
    TablePropertiesCollectionIter, TablePropertiesExt, TablePropertiesKey, UserCollectedProperties,
};
use std::ops::Deref;

impl TablePropertiesExt for SimpleEngine {
    type TablePropertiesCollection = SimpleTablePropertiesCollection;
    type TablePropertiesCollectionIter = SimpleTablePropertiesCollectionIter;
    type TablePropertiesKey = SimpleTablePropertiesKey;
    type TableProperties = SimpleTableProperties;
    type UserCollectedProperties = SimpleUserCollectedProperties;

    fn get_properties_of_tables_in_range(
        &self,
        cf: &str,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection> {
        panic!()
    }
}

pub struct SimpleTablePropertiesCollection;

impl
    TablePropertiesCollection<
        SimpleTablePropertiesCollectionIter,
        SimpleTablePropertiesKey,
        SimpleTableProperties,
        SimpleUserCollectedProperties,
    > for SimpleTablePropertiesCollection
{
    fn iter(&self) -> SimpleTablePropertiesCollectionIter {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

pub struct SimpleTablePropertiesCollectionIter;

impl
    TablePropertiesCollectionIter<
        SimpleTablePropertiesKey,
        SimpleTableProperties,
        SimpleUserCollectedProperties,
    > for SimpleTablePropertiesCollectionIter
{
}

impl Iterator for SimpleTablePropertiesCollectionIter {
    type Item = (SimpleTablePropertiesKey, SimpleTableProperties);

    fn next(&mut self) -> Option<Self::Item> {
        panic!()
    }
}

pub struct SimpleTablePropertiesKey;

impl TablePropertiesKey for SimpleTablePropertiesKey {}

impl Deref for SimpleTablePropertiesKey {
    type Target = str;

    fn deref(&self) -> &str {
        panic!()
    }
}

pub struct SimpleTableProperties;

impl TableProperties<SimpleUserCollectedProperties> for SimpleTableProperties {
    fn num_entries(&self) -> u64 {
        panic!()
    }

    fn user_collected_properties(&self) -> SimpleUserCollectedProperties {
        panic!()
    }
}

pub struct SimpleUserCollectedProperties;

impl UserCollectedProperties for SimpleUserCollectedProperties {
    fn get(&self, index: &[u8]) -> Option<&[u8]> {
        panic!()
    }

    fn len(&self) -> usize {
        panic!()
    }
}

impl DecodeProperties for SimpleUserCollectedProperties {
    fn decode(&self, k: &str) -> tikv_util::codec::Result<&[u8]> {
        panic!()
    }
}
