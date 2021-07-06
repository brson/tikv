// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::path::PathBuf;

use crate::error::ResultExt;
use crate::engine::SimpleEngine;
use engine_traits::{
    CfName, ExternalSstFileInfo, IterOptions, Iterable, Iterator, Result, SeekKey,
    SstCompressionType, SstExt, SstReader, SstWriter, SstWriterBuilder,
    Error,
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

impl Iterator for SimpleSstReaderIterator {
    fn seek(&mut self, key: SeekKey) -> Result<bool> {
        match key {
            SeekKey::Start => self.cursor.seek_first(),
            SeekKey::End => self.cursor.seek_last(),
            SeekKey::Key(key) => self.cursor.seek_key(Key::from_slice(key)),
        }

        self.update_kv()
    }
    fn seek_for_prev(&mut self, key: SeekKey) -> Result<bool> {
        match key {
            SeekKey::Start => self.cursor.seek_first(),
            SeekKey::End => self.cursor.seek_last(),
            SeekKey::Key(key) => self.cursor.seek_key_rev(Key::from_slice(key)),
        }

        self.update_kv()
    }

    fn prev(&mut self) -> Result<bool> {
        if !self.valid()? {
            return Err(blocksy3::anyhow!("prev on invalid iterator")).engine_result();
        }
        self.cursor.prev();
        self.update_kv()
    }
    fn next(&mut self) -> Result<bool> {
        if !self.valid()? {
            return Err(blocksy3::anyhow!("next on invalid iterator")).engine_result();
        }
        self.cursor.next();
        self.update_kv()
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

impl SimpleSstReaderIterator {
    fn update_kv(&mut self) -> Result<bool> {
        block_on(async {
            if self.cursor.valid() {
                let key = self.cursor.key();
                let value = self.cursor.value().await.engine_result()?;
                self.kv = Some((key, value));
                Ok(true)
            } else {
                self.kv = None;
                Ok(false)
            }
        })
    }
}

pub struct SimpleSstWriter {
    tree: Tree,
    batch_writer: BatchWriter,
    fs_thread: Arc<FsThread>,
    file_path: PathBuf,
    smallest_key: Option<Vec<u8>>,
    largest_key: Option<Vec<u8>>,
    num_entries: u64,
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
            smallest_key: None,
            largest_key: None,
            num_entries: 0,
        })
    }
}

impl SstWriter for SimpleSstWriter {
    type ExternalSstFileInfo = SimpleExternalSstFileInfo;
    type ExternalSstFileReader = SimpleExternalSstFileReader;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        if let Some(largest_key) = self.largest_key.as_ref() {
            if key[..] <= largest_key[..] {
                return Err(Error::Engine("Keys must be added in order".to_string()));
            }
        }

        block_on(self.batch_writer.write(Key::from_slice(key), Value::from_slice(val))).engine_result()?;

        self.num_entries += 1;

        if let Some(curkey) = self.smallest_key.as_ref() {
            if key[..] < curkey[..] {
                self.smallest_key = Some(key.to_vec());
            }
        } else {
            self.smallest_key = Some(key.to_vec());
        }
        if let Some(curkey) = self.largest_key.as_ref() {
            if key[..] > curkey[..] {
                self.largest_key = Some(key.to_vec());
            }
        } else {
            self.largest_key = Some(key.to_vec());
        }

        Ok(())
    }
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if let Some(largest_key) = self.largest_key.as_ref() {
            if key[..] <= largest_key[..] {
                return Err(Error::Engine("Keys must be added in order".to_string()));
            }
        }

        block_on(self.batch_writer.delete(Key::from_slice(key))).engine_result()?;

        self.num_entries += 1;

        if let Some(curkey) = self.smallest_key.as_ref() {
            if key[..] < curkey[..] {
                self.smallest_key = Some(key.to_vec());
            }
        } else {
            self.smallest_key = Some(key.to_vec());
        }
        if let Some(curkey) = self.largest_key.as_ref() {
            if key[..] > curkey[..] {
                self.largest_key = Some(key.to_vec());
            }
        } else {
            self.largest_key = Some(key.to_vec());
        }

        Ok(())
    }
    fn file_size(&mut self) -> u64 {
        panic!()
    }
    fn finish(self) -> Result<Self::ExternalSstFileInfo> {
        if self.num_entries == 0 {
            return Err(Error::Engine("can't create sst with no entries".to_string()));
        }

        block_on(self.batch_writer.ready_commit(BatchCommit(0))).engine_result()?;
        self.batch_writer.commit_to_index(BatchCommit(0), Commit(0));
        block_on(self.batch_writer.close()).engine_result()?;
        block_on(self.tree.sync()).engine_result()?;

        let path = self.file_path.clone();
        let file_size = block_on(self.fs_thread.run(|ctx| -> blocksy3::anyhow::Result<u64> {
            let path = path;
            let file = ctx.open_append(&path)?;
            let meta = file.metadata()?;
            Ok(meta.len())
        })).engine_result()?;

        Ok(SimpleExternalSstFileInfo {
            file_path: self.file_path.clone(),
            smallest_key: self.smallest_key,
            largest_key: self.largest_key,
            num_entries: self.num_entries,
            file_size,
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
    smallest_key: Option<Vec<u8>>,
    largest_key: Option<Vec<u8>>,
    num_entries: u64,
    file_size: u64,
}

impl ExternalSstFileInfo for SimpleExternalSstFileInfo {
    fn new() -> Self {
        panic!()
    }
    fn file_path(&self) -> PathBuf {
        self.file_path.clone()
    }
    fn smallest_key(&self) -> &[u8] {
        self.smallest_key.as_ref().unwrap()
    }
    fn largest_key(&self) -> &[u8] {
        self.largest_key.as_ref().unwrap()
    }
    fn sequence_number(&self) -> u64 {
        panic!()
    }
    fn file_size(&self) -> u64 {
        self.file_size
    }
    fn num_entries(&self) -> u64 {
        self.num_entries
    }
}

pub struct SimpleExternalSstFileReader;

impl std::io::Read for SimpleExternalSstFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        panic!()
    }
}
