// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Result, SstExt, SstReader, SstWriterBuilder, SstWriter, ExternalSstFileInfo, Iterable, Iterator, IterOptions, CfName, SeekKey};
use crate::engine::PanicEngine;
use std::path::PathBuf;

impl SstExt for PanicEngine {
    type SstReader = PanicSstReader;
    type SstWriter = PanicSstWriter;
    type SstWriterBuilder = PanicSstWriterBuilder;
}

pub struct PanicSstReader;

impl SstReader for PanicSstReader {
    fn open(path: &str) -> Result<Self> { panic!() }
    fn verify_checksum(&self) -> Result<()> { panic!() }
    fn iter(&self) -> Self::Iterator { panic!() }
}

impl Iterable for PanicSstReader {
    type Iterator = PanicSstReaderIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> { panic!() }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> { panic!() }
}

pub struct PanicSstReaderIterator;

impl Iterator for PanicSstReaderIterator {
    fn seek(&mut self, key: SeekKey) -> bool { panic!() }
    fn seek_for_prev(&mut self, key: SeekKey) -> bool { panic!() }

    fn prev(&mut self) -> bool { panic!() }
    fn next(&mut self) -> bool { panic!() }

    fn key(&self) -> &[u8] { panic!() }
    fn value(&self) -> &[u8] { panic!() }

    fn valid(&self) -> bool { panic!() }
    fn status(&self) -> Result<()> { panic!() }
}

pub struct PanicSstWriter;

impl SstWriter for PanicSstWriter {
    type ExternalSstFileInfo = PanicExternalSstFileInfo;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> { panic!() }
    fn delete(&mut self, key: &[u8]) -> Result<()> { panic!() }
    fn file_size(&mut self) -> u64 { panic!() }
    fn finish(self) -> Result<Self::ExternalSstFileInfo> { panic!() }
    fn finish_into(self, buf: &mut Vec<u8>) -> Result<Self::ExternalSstFileInfo> { panic!() }
}

pub struct PanicSstWriterBuilder;

impl SstWriterBuilder<PanicEngine> for PanicSstWriterBuilder {
    fn new() -> Self { panic!() }
    fn set_db(self, db: &PanicEngine) -> Self { panic!() }
    fn set_cf(self, cf: CfName) -> Self { panic!() }
    fn set_in_memory(self, in_memory: bool) -> Self { panic!() }
    fn build(self, path: &str) -> Result<PanicSstWriter> { panic!() }
}

pub struct PanicExternalSstFileInfo;

impl ExternalSstFileInfo for PanicExternalSstFileInfo {
    fn new() -> Self { panic!() }
    fn file_path(&self) -> PathBuf { panic!() }
    fn smallest_key(&self) -> &[u8] { panic!() }
    fn largest_key(&self) -> &[u8] { panic!() }
    fn sequence_number(&self) -> u64 { panic!() }
    fn file_size(&self) -> u64 { panic!() }
    fn num_entries(&self) -> u64 { panic!() }
}
