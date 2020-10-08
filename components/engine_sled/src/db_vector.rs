// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::DBVector;
use std::ops::Deref;

#[derive(Debug)]
pub struct SledDBVector(sled::IVec);

impl SledDBVector {
    pub (crate) fn from_raw(v: sled::IVec) -> SledDBVector {
        SledDBVector(v)
    }
}

impl DBVector for SledDBVector {}

impl Deref for SledDBVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.0.deref()
    }
}

impl<'a> PartialEq<&'a [u8]> for SledDBVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}
