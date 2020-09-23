// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SledEngine;
use engine_traits::{
    CfName, ExternalSstFileInfo, IterOptions, Iterable, Iterator, Result, SeekKey,
    SstCompressionType, SstExt, SstReader, SstWriter, SstWriterBuilder,
};
use std::path::PathBuf;

impl SstExt for SledEngine {
    type SstReader = SledSstReader;
    type SstWriter = SledSstWriter;
    type SstWriterBuilder = SledSstWriterBuilder;
}

pub struct SledSstReader;

impl SstReader for SledSstReader {
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

impl Iterable for SledSstReader {
    type Iterator = SledSstReaderIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
}

pub struct SledSstReaderIterator;

impl Iterator for SledSstReaderIterator {
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

pub struct SledSstWriter;

impl SstWriter for SledSstWriter {
    type ExternalSstFileInfo = SledExternalSstFileInfo;
    type ExternalSstFileReader = SledExternalSstFileReader;

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

pub struct SledSstWriterBuilder;

impl SstWriterBuilder<SledEngine> for SledSstWriterBuilder {
    fn new() -> Self {
        panic!()
    }
    fn set_db(self, db: &SledEngine) -> Self {
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

    fn build(self, path: &str) -> Result<SledSstWriter> {
        panic!()
    }
}

pub struct SledExternalSstFileInfo;

impl ExternalSstFileInfo for SledExternalSstFileInfo {
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

pub struct SledExternalSstFileReader;

impl std::io::Read for SledExternalSstFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        panic!()
    }
}
