// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SimpleEngine;
use engine_traits::{
    CfName, ExternalSstFileInfo, IterOptions, Iterable, Iterator, Result, SeekKey,
    SstCompressionType, SstExt, SstReader, SstWriter, SstWriterBuilder,
};
use std::path::PathBuf;

impl SstExt for SimpleEngine {
    type SstReader = SimpleSstReader;
    type SstWriter = SimpleSstWriter;
    type SstWriterBuilder = SimpleSstWriterBuilder;
}

pub struct SimpleSstReader;

impl SstReader for SimpleSstReader {
    fn open(path: &str) -> Result<Self> {
        panic!()
    }
    fn verify_checksum(&self) -> Result<()> {
        panic!()
    }
    fn iter(&self) -> Self::Iterator {
        panic!()
    }
}

impl Iterable for SimpleSstReader {
    type Iterator = SimpleSstReaderIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
}

pub struct SimpleSstReaderIterator;

impl Iterator for SimpleSstReaderIterator {
    fn seek(&mut self, key: SeekKey) -> Result<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, key: SeekKey) -> Result<bool> {
        panic!()
    }

    fn prev(&mut self) -> Result<bool> {
        panic!()
    }
    fn next(&mut self) -> Result<bool> {
        panic!()
    }

    fn key(&self) -> &[u8] {
        panic!()
    }
    fn value(&self) -> &[u8] {
        panic!()
    }

    fn valid(&self) -> Result<bool> {
        panic!()
    }
}

pub struct SimpleSstWriter;

impl SstWriter for SimpleSstWriter {
    type ExternalSstFileInfo = SimpleExternalSstFileInfo;
    type ExternalSstFileReader = SimpleExternalSstFileReader;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn file_size(&mut self) -> u64 {
        panic!()
    }
    fn finish(self) -> Result<Self::ExternalSstFileInfo> {
        panic!()
    }
    fn finish_read(self) -> Result<(Self::ExternalSstFileInfo, Self::ExternalSstFileReader)> {
        panic!()
    }
}

pub struct SimpleSstWriterBuilder;

impl SstWriterBuilder<SimpleEngine> for SimpleSstWriterBuilder {
    fn new() -> Self {
        panic!()
    }
    fn set_db(self, db: &SimpleEngine) -> Self {
        panic!()
    }
    fn set_cf(self, cf: &str) -> Self {
        panic!()
    }
    fn set_in_memory(self, in_memory: bool) -> Self {
        panic!()
    }
    fn set_compression_type(self, compression: Option<SstCompressionType>) -> Self {
        panic!()
    }
    fn set_compression_level(self, level: i32) -> Self {
        panic!()
    }

    fn build(self, path: &str) -> Result<SimpleSstWriter> {
        panic!()
    }
}

pub struct SimpleExternalSstFileInfo;

impl ExternalSstFileInfo for SimpleExternalSstFileInfo {
    fn new() -> Self {
        panic!()
    }
    fn file_path(&self) -> PathBuf {
        panic!()
    }
    fn smallest_key(&self) -> &[u8] {
        panic!()
    }
    fn largest_key(&self) -> &[u8] {
        panic!()
    }
    fn sequence_number(&self) -> u64 {
        panic!()
    }
    fn file_size(&self) -> u64 {
        panic!()
    }
    fn num_entries(&self) -> u64 {
        panic!()
    }
}

pub struct SimpleExternalSstFileReader;

impl std::io::Read for SimpleExternalSstFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        panic!()
    }
}
