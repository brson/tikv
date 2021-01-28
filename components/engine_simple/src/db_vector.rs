// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::DBVector;
use std::ops::Deref;

#[derive(Debug)]
pub struct SimpleDBVector(Vec<u8>);

impl SimpleDBVector {
    pub (crate) fn from_inner(inner: Vec<u8>) -> SimpleDBVector {
        SimpleDBVector(inner)
    }
}

impl DBVector for SimpleDBVector {}

impl Deref for SimpleDBVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl<'a> PartialEq<&'a [u8]> for SimpleDBVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}
