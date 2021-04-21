// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

pub trait ResultExt<T> {
    fn engine_result(self) -> engine_traits::Result<T>;
}

impl<T> ResultExt<T> for blocksy3::Result<T> {
    fn engine_result(self) -> engine_traits::Result<T> {
        self.map_err(|e| engine_traits::Error::Engine(format!("{}", e)))
    }
}
