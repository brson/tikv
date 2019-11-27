// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::CFHandleExt;
use crate::errors::{Result, Error};
use crate::range::Range;
use std::ops::Deref;
use std::iter::IntoIterator;

pub trait TablePropertiesExt: CFHandleExt {
    type TablePropertiesCollection: TablePropertiesCollection<Self::TablePropertiesCollectionView>;
    type TablePropertiesCollectionView: TablePropertiesCollectionView;

    fn get_properties_of_tables_in_range(
        &self,
        cf: &Self::CFHandle,
        ranges: &[Range],
    ) -> Result<Self::TablePropertiesCollection>;

    fn get_range_properties_cf(
        &self,
        cfname: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Self::TablePropertiesCollection> {
        let cf = self.cf_handle(cfname)
            .ok_or_else(|| Error::Engine(format!("cf {} not found", cfname)))?;
        let range = Range::new(start_key, end_key);
        Ok(self.get_properties_of_tables_in_range(cf, &[range])?)
    }
}

pub trait TablePropertiesCollection<V>
where Self: Deref<Target = V>
{
}

pub trait TablePropertiesCollectionView
{
    type TableProperties: TableProperties;
}

pub trait TablePropertiesCollectionIter {
}

pub trait TablePropertiesCollectionItem {
    type TableProperties: TableProperties;

    fn key(&self) -> &str;
    fn props(&self) -> &Self::TableProperties;
}

pub trait TableProperties { }

