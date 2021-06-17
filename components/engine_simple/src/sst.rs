// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::path::PathBuf;

use crate::error::ResultExt;
use crate::engine::SimpleEngine;
use engine_traits::{
    CfName, ExternalSstFileInfo, IterOptions, Iterable, Iterator, Result, SeekKey,
    SstCompressionType, SstExt, SstReader, SstWriter, SstWriterBuilder,
};

use blocksy3::raw::fs_thread::FsThread;
use blocksy3::raw::log::Log;
use blocksy3::raw::simple_log_file;
use blocksy3::raw::tree::{Tree, BatchWriter, Cursor};
use blocksy3::raw::types::{Key, Value, Batch, BatchCommit, Commit};
use futures::executor::block_on;

impl SstExt for SimpleEngine {
    type SstReader = SimpleSstReader;
    type SstWriter = SimpleSstWriter;
    type SstWriterBuilder = SimpleSstWriterBuilder;
}

pub struct SimpleSstReader {
    tree: Tree,
    fs_thread: Arc<FsThread>,
}

impl SstReader for SimpleSstReader {
    fn open(path: &str) -> Result<Self> {
        let fs_thread = Arc::new(FsThread::start().engine_result()?);
        let log = Log::new(simple_log_file::create(PathBuf::from(path), fs_thread.clone()));
        let tree = Tree::new(log);
        let mut replay = tree.init_replayer();
        block_on(replay.replay_commit(Batch(0), BatchCommit(0), Commit(0))).engine_result()?;
        replay.init_success();
        Ok(SimpleSstReader {
            tree, fs_thread,
        })
    }
    fn verify_checksum(&self) -> Result<()> {
        Ok(()) // todo fixme
    }
    fn iter(&self) -> Self::Iterator {
        SimpleSstReaderIterator {
            cursor: self.tree.cursor(Commit(1)),
            kv: None,
        }
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

pub struct SimpleSstReaderIterator {
    cursor: Cursor,
    kv: Option<(Key, Value)>,
}

impl SimpleSstReaderIterator {
    fn maybe_update_kv(&mut self) -> Result<()> {
        if self.cursor.valid() {
            let key = self.cursor.key();
            let value = block_on(self.cursor.value()).engine_result()?;
            self.kv = Some((key, value));
        } else {
            self.kv = None;
        }
        Ok(())
    }
}

impl Iterator for SimpleSstReaderIterator {
    fn seek(&mut self, key: SeekKey) -> Result<bool> {
        match key {
            SeekKey::Start => self.cursor.seek_first(),
            SeekKey::End => self.cursor.seek_last(),
            SeekKey::Key(key) => self.cursor.seek_key(Key::from_slice(key)),
        }

        self.maybe_update_kv()?;

        Ok(self.cursor.valid())
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
        self.kv.as_ref().expect("valid").0.0.as_ref()
    }
    fn value(&self) -> &[u8] {
        self.kv.as_ref().expect("valid").1.0.as_ref()
    }

    fn valid(&self) -> Result<bool> {
        Ok(self.cursor.valid())
    }
}

pub struct SimpleSstWriter {
    tree: Tree,
    batch_writer: BatchWriter,
    fs_thread: Arc<FsThread>,
    file_path: PathBuf,
}

impl SimpleSstWriter {
    fn new(path: &str) -> Result<SimpleSstWriter> {
        let fs_thread = Arc::new(FsThread::start().engine_result()?);
        let log = Log::new(simple_log_file::create(PathBuf::from(path), fs_thread.clone()));
        let tree = Tree::new(log);
        tree.skip_init();
        let batch_writer = tree.batch(Batch(0));
        block_on(batch_writer.open()).engine_result()?;

        let file_path = PathBuf::from(path);

        Ok(SimpleSstWriter {
            tree, batch_writer, fs_thread, file_path,
        })
    }
}

impl SstWriter for SimpleSstWriter {
    type ExternalSstFileInfo = SimpleExternalSstFileInfo;
    type ExternalSstFileReader = SimpleExternalSstFileReader;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        block_on(self.batch_writer.write(Key::from_slice(key), Value::from_slice(val))).engine_result()
    }
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn file_size(&mut self) -> u64 {
        panic!()
    }
    fn finish(self) -> Result<Self::ExternalSstFileInfo> {
        block_on(self.batch_writer.ready_commit(BatchCommit(0))).engine_result()?;
        self.batch_writer.commit_to_index(BatchCommit(0), Commit(0));
        block_on(self.batch_writer.close()).engine_result()?;
        block_on(self.tree.sync()).engine_result()?;

        Ok(SimpleExternalSstFileInfo {
            file_path: self.file_path.clone(),
        })
    }
    fn finish_read(self) -> Result<(Self::ExternalSstFileInfo, Self::ExternalSstFileReader)> {
        panic!()
    }
}

pub struct SimpleSstWriterBuilder;

impl SstWriterBuilder<SimpleEngine> for SimpleSstWriterBuilder {
    fn new() -> Self {
        SimpleSstWriterBuilder
    }
    fn set_db(self, db: &SimpleEngine) -> Self {
        self
    }
    fn set_cf(self, cf: &str) -> Self {
        self
    }
    fn set_in_memory(self, in_memory: bool) -> Self {
        self
    }
    fn set_compression_type(self, compression: Option<SstCompressionType>) -> Self {
        self
    }
    fn set_compression_level(self, level: i32) -> Self {
        self
    }

    fn build(self, path: &str) -> Result<SimpleSstWriter> {
        SimpleSstWriter::new(path)
    }
}

pub struct SimpleExternalSstFileInfo {
    file_path: PathBuf,
}

impl ExternalSstFileInfo for SimpleExternalSstFileInfo {
    fn new() -> Self {
        panic!()
    }
    fn file_path(&self) -> PathBuf {
        self.file_path.clone()
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
