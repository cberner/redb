//! BenchDatabase adapters for the engines redb is compared against. Kept in this crate so that
//! their native dependencies stay out of redb-bench, whose benchmarks only exercise redb.

// Every benchmark including this module exercises only the subset of engines it compares against
#![allow(dead_code)]

use fjall::Readable as _;
use heed::{CompactionOption, EnvFlags, EnvInfo, FlagSetMode, PutFlags};
use redb_bench::*;
use rocksdb::{
    Direction, IteratorMode, OptimisticTransactionDB, OptimisticTransactionOptions, WriteOptions,
};
use rusqlite::{Connection, OptionalExtension, Statement, Transaction};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::{fs, mem};

// XXX: Awful hack because Rocksdb seems to have unbounded memory usage for bulk writes
const ROCKSDB_MAX_WRITES_PER_TXN: u64 = 100_000;

pub struct HeedBenchDatabase {
    env: Option<heed::Env>,
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
}

impl HeedBenchDatabase {
    pub fn new(env: heed::Env) -> Self {
        let mut tx = env.write_txn().unwrap();
        let db = env.create_database(&mut tx, None).unwrap();
        tx.commit().unwrap();
        Self { env: Some(env), db }
    }
}

impl BenchDatabase for HeedBenchDatabase {
    type C<'db>
        = HeedBenchDatabaseConnection<'db>
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "lmdb"
    }

    fn connect(&self) -> Self::C<'_> {
        HeedBenchDatabaseConnection {
            env: &self.env,
            db: self.db,
        }
    }

    fn compact(&mut self) -> bool {
        // We take the env to be able to compact and reopen it after compaction.
        let env = self.env.take().unwrap();
        let EnvInfo { map_size, .. } = env.info();
        let path = env.path().to_owned();
        let mut file2 = File::create_new(path.join("data2.mdb")).unwrap();
        env.copy_to_file(&mut file2, CompactionOption::Enabled)
            .unwrap();
        file2.sync_all().unwrap();
        drop(file2);

        // We close the env
        env.prepare_for_closing().wait();

        // We replace the previous data file with the new, compacted, one.
        fs::rename(path.join("data2.mdb"), path.join("data.mdb")).unwrap();

        // We reopen the env and the associated database
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(map_size)
                .open(path)
                .unwrap()
        };

        let tx = env.read_txn().unwrap();
        self.db = env.open_database(&tx, None).unwrap().unwrap();
        drop(tx);
        self.env = Some(env);

        true
    }
}

pub struct HeedBenchDatabaseConnection<'a> {
    env: &'a Option<heed::Env>,
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
}

impl BenchDatabaseConnection for HeedBenchDatabaseConnection<'_> {
    type W<'db>
        = HeedBenchWriteTransaction<'db>
    where
        Self: 'db;
    type R<'db>
        = HeedBenchReadTransaction<'db>
    where
        Self: 'db;

    fn set_sync(&mut self, sync: bool) -> bool {
        let env = self.env.as_ref().unwrap();
        if sync {
            unsafe {
                env.set_flags(EnvFlags::NO_SYNC, FlagSetMode::Disable)
                    .unwrap();
            }
        } else {
            unsafe {
                env.set_flags(EnvFlags::NO_SYNC, FlagSetMode::Enable)
                    .unwrap();
            }
        }
        true
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let env = self.env.as_ref().unwrap();
        let txn = env.write_txn().unwrap();
        Self::W {
            db: self.db,
            db_dir: self.env.as_ref().unwrap().path().to_path_buf(),
            txn,
        }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let env = self.env.as_ref().unwrap();
        let txn = env.read_txn().unwrap();
        Self::R { db: self.db, txn }
    }
}

pub struct HeedBenchWriteTransaction<'db> {
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    #[allow(dead_code)]
    db_dir: PathBuf,
    txn: heed::RwTxn<'db>,
}

impl<'db> BenchWriteTransaction for HeedBenchWriteTransaction<'db> {
    type W<'txn>
        = HeedBenchInserter<'txn, 'db>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        Self::W {
            db: self.db,
            txn: &mut self.txn,
        }
    }

    fn commit(self) -> Result<(), ()> {
        let result = self.txn.commit().map_err(|_| ());
        #[cfg(target_os = "macos")]
        {
            // Workaround for broken durability on MacOS in lmdb
            // See: https://github.com/cberner/redb/pull/928#issuecomment-2567032808
            for entry in fs::read_dir(self.db_dir).unwrap() {
                let entry = entry.unwrap();
                if entry.path().is_file() {
                    let file = File::open(entry.path()).unwrap();
                    file.sync_all().unwrap();
                }
            }
        }

        result
    }
}

pub struct HeedBenchInserter<'txn, 'db> {
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: &'txn mut heed::RwTxn<'db>,
}

pub struct HeedExtractIfIterator<'txn, F> {
    iter: heed::RwRange<'txn, heed::types::Bytes, heed::types::Bytes>,
    predicate: F,
}

impl<F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool> BenchIterator for HeedExtractIfIterator<'_, F> {
    type Output<'out>
        = Vec<u8>
    where
        Self: 'out;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        loop {
            let (key, value) = self.iter.next()?.unwrap();
            if (self.predicate)(key, value) {
                let entry = (key.to_vec(), value.to_vec());
                // safety: key and value were copied before deleting the current entry.
                unsafe { self.iter.del_current().unwrap() };
                return Some(entry);
            }
        }
    }
}

impl BenchInserter for HeedBenchInserter<'_, '_> {
    type Output<'out>
        = Vec<u8>
    where
        Self: 'out;
    type ExtractIfIterator<'out, F>
        = HeedExtractIfIterator<'out, F>
    where
        Self: 'out,
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.db.put(self.txn, key, value).map_err(|_| ())
    }

    fn insert_sorted<'i>(
        &mut self,
        pairs: impl Iterator<Item = (&'i [u8], &'i [u8])>,
    ) -> Result<(), ()> {
        // LMDB's append mode: writes at the cursor without searching the tree, and
        // requires each key to be greater than every key already in the table
        for (key, value) in pairs {
            self.db
                .put_with_flags(self.txn, PutFlags::APPEND, key, value)
                .map_err(|_| ())?;
        }
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.db.delete(self.txn, key).map(|_| ()).map_err(|_| ())
    }

    fn pop_first(&mut self) -> BenchPopResult<'_, Self> {
        let mut iter = self.db.iter_mut(self.txn).map_err(|_| ())?;
        let entry = iter
            .next()
            .transpose()
            .map_err(|_| ())?
            .map(|(key, value)| (key.to_vec(), value.to_vec()));
        if entry.is_some() {
            // safety: no references into the db are held across this call.
            unsafe { iter.del_current().map_err(|_| ())? };
        }
        Ok(entry)
    }

    fn pop_last(&mut self) -> BenchPopResult<'_, Self> {
        let mut iter = self.db.rev_iter_mut(self.txn).map_err(|_| ())?;
        let entry = iter
            .next()
            .transpose()
            .map_err(|_| ())?
            .map(|(key, value)| (key.to_vec(), value.to_vec()));
        if entry.is_some() {
            // safety: no references into the db are held across this call.
            unsafe { iter.del_current().map_err(|_| ())? };
        }
        Ok(entry)
    }

    fn retain<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, mut predicate: F) -> Result<u64, ()> {
        // Use heed's cursor-based mutable iterator with del_current so the benchmark reflects
        // LMDB's best effort, rather than a user-space iterate + delete loop.
        let mut iter = self.db.iter_mut(self.txn).map_err(|_| ())?;
        let mut removed = 0u64;
        loop {
            let keep = match iter.next() {
                Some(res) => {
                    let (k, v) = res.map_err(|_| ())?;
                    Some(predicate(k, v))
                }
                None => None,
            };
            match keep {
                None => break,
                Some(true) => {}
                Some(false) => {
                    // safety: no references into the db are held across this call.
                    unsafe { iter.del_current().map_err(|_| ())? };
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    fn extract_if<'a, F>(
        &'a mut self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        predicate: F,
    ) -> BenchExtractIfResult<'a, Self, F>
    where
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'a,
    {
        // Use heed's cursor-based mutable range iterator with del_current so the benchmark
        // reflects LMDB's best effort.
        let iter = self.db.range_mut(self.txn, &range).map_err(|_| ())?;
        Ok(HeedExtractIfIterator { iter, predicate })
    }
}

pub struct HeedBenchReadTransaction<'db> {
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: heed::RoTxn<'db, heed::WithTls>,
}

impl<'db> BenchReadTransaction for HeedBenchReadTransaction<'db> {
    type T<'txn>
        = HeedBenchReader<'txn, 'db>
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        Self::T {
            db: self.db,
            txn: &self.txn,
        }
    }
}

pub struct HeedBenchReader<'txn, 'db> {
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: &'txn heed::RoTxn<'db>,
}

impl BenchReader for HeedBenchReader<'_, '_> {
    type Output<'out>
        = &'out [u8]
    where
        Self: 'out;
    type Iterator<'out>
        = HeedBenchIterator<'out>
    where
        Self: 'out;

    fn get(&mut self, key: &[u8]) -> Option<&[u8]> {
        self.db.get(self.txn, key).unwrap()
    }

    fn range_from<'a>(&'a mut self, key: &'a [u8]) -> Self::Iterator<'a> {
        let range = (Bound::Included(key), Bound::Unbounded);
        let iter = self.db.range(self.txn, &range).unwrap();

        Self::Iterator { iter }
    }

    fn len(&mut self) -> u64 {
        self.db.stat(self.txn).unwrap().entries as u64
    }
}

pub struct HeedBenchIterator<'a> {
    iter: heed::RoRange<'a, heed::types::Bytes, heed::types::Bytes>,
}

impl BenchIterator for HeedBenchIterator<'_> {
    type Output<'out>
        = &'out [u8]
    where
        Self: 'out;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        self.iter.next().map(|x| x.unwrap())
    }
}

pub struct RocksdbBenchDatabase<'a> {
    db: &'a OptimisticTransactionDB,
}

impl<'a> RocksdbBenchDatabase<'a> {
    pub fn new(db: &'a OptimisticTransactionDB) -> Self {
        Self { db }
    }
}

impl BenchDatabase for RocksdbBenchDatabase<'_> {
    type C<'db>
        = RocksdbBenchDatabaseConnection<'db>
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "rocksdb"
    }

    fn connect(&self) -> Self::C<'_> {
        RocksdbBenchDatabaseConnection {
            db: self.db,
            sync: true,
        }
    }

    fn compact(&mut self) -> bool {
        self.db.compact_range::<&[u8], &[u8]>(None, None);
        true
    }
}

pub struct RocksdbBenchDatabaseConnection<'a> {
    db: &'a OptimisticTransactionDB,
    sync: bool,
}

impl BenchDatabaseConnection for RocksdbBenchDatabaseConnection<'_> {
    type W<'db>
        = RocksdbBenchWriteTransaction<'db>
    where
        Self: 'db;
    type R<'db>
        = RocksdbBenchReadTransaction<'db>
    where
        Self: 'db;

    fn set_sync(&mut self, sync: bool) -> bool {
        self.sync = sync;
        true
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let mut write_opt = WriteOptions::new();
        write_opt.set_sync(self.sync);
        let mut txn_opt = OptimisticTransactionOptions::new();
        txn_opt.set_snapshot(true);
        let txn = self.db.transaction_opt(&write_opt, &txn_opt);
        RocksdbBenchWriteTransaction {
            txn,
            db: self.db,
            db_dir: self.db.path().to_path_buf(),
            sync: self.sync,
        }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let snapshot = self.db.snapshot();
        RocksdbBenchReadTransaction { snapshot }
    }
}

pub struct RocksdbBenchWriteTransaction<'a> {
    txn: rocksdb::Transaction<'a, OptimisticTransactionDB>,
    db: &'a OptimisticTransactionDB,
    #[allow(dead_code)]
    db_dir: PathBuf,
    #[allow(dead_code)]
    sync: bool,
}

impl<'a> BenchWriteTransaction for RocksdbBenchWriteTransaction<'a> {
    type W<'txn>
        = RocksdbBenchInserter<'txn, 'a>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        RocksdbBenchInserter {
            txn: self,
            counter: 0,
        }
    }

    fn commit(self) -> Result<(), ()> {
        let result = self.txn.commit().map_err(|_| ());
        #[cfg(target_os = "macos")]
        if self.sync {
            // Workaround for broken durability on MacOS in rocksdb
            // See: https://github.com/cberner/redb/pull/928#issuecomment-2567032808
            for entry in fs::read_dir(self.db_dir).unwrap() {
                let entry = entry.unwrap();
                if entry.path().is_file() {
                    let file = File::open(entry.path()).unwrap();
                    file.sync_all().unwrap();
                }
            }
        }

        result
    }
}

pub struct RocksdbBenchInserter<'a, 'b> {
    txn: &'a mut RocksdbBenchWriteTransaction<'b>,
    counter: u64,
}

type RocksdbEntry = (Box<[u8]>, Box<[u8]>);

impl BenchInserter for RocksdbBenchInserter<'_, '_> {
    type Output<'out>
        = Box<[u8]>
    where
        Self: 'out;
    type ExtractIfIterator<'out, F>
        = VecBenchIterator<Box<[u8]>>
    where
        Self: 'out,
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.counter += 1;
        if self.counter == ROCKSDB_MAX_WRITES_PER_TXN {
            let txn = mem::replace(&mut self.txn.txn, self.txn.db.transaction());
            txn.commit().map_err(|_| ())?;
            self.counter = 0;
        }
        self.txn.txn.put(key, value).map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.counter += 1;
        if self.counter == ROCKSDB_MAX_WRITES_PER_TXN {
            let txn = mem::replace(&mut self.txn.txn, self.txn.db.transaction());
            txn.commit().map_err(|_| ())?;
            self.counter = 0;
        }
        self.txn.txn.delete(key).map_err(|_| ())
    }

    fn pop_first(&mut self) -> BenchPopResult<'_, Self> {
        let entry = self
            .txn
            .txn
            .iterator(IteratorMode::Start)
            .next()
            .transpose()
            .map_err(|_| ())?;
        if let Some((key, _)) = &entry {
            self.remove(key)?;
        }
        Ok(entry)
    }

    fn pop_last(&mut self) -> BenchPopResult<'_, Self> {
        let entry = self
            .txn
            .txn
            .iterator(IteratorMode::End)
            .next()
            .transpose()
            .map_err(|_| ())?;
        if let Some((key, _)) = &entry {
            self.remove(key)?;
        }
        Ok(entry)
    }

    fn retain<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, mut predicate: F) -> Result<u64, ()> {
        // RocksDB's Transaction API doesn't support cursor-based deletion, so we iterate,
        // collect the keys to remove, then delete them.
        let mut keys: Vec<Box<[u8]>> = Vec::new();
        {
            let iter = self.txn.txn.iterator(IteratorMode::Start);
            for entry in iter {
                let (k, v) = entry.map_err(|_| ())?;
                if !predicate(&k, &v) {
                    keys.push(k);
                }
            }
        }
        let count = keys.len() as u64;
        for key in &keys {
            self.remove(key)?;
        }
        Ok(count)
    }

    fn extract_if<'a, F>(
        &'a mut self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        mut predicate: F,
    ) -> BenchExtractIfResult<'a, Self, F>
    where
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'a,
    {
        // RocksDB's Transaction API has no cursor-based deletion. Position the iterator at the
        // start bound, walk it until the end bound is exceeded, evaluate the predicate, then
        // delete the collected entries.
        let mut entries: Vec<RocksdbEntry> = Vec::new();
        {
            let mode = match range.0 {
                Bound::Included(k) | Bound::Excluded(k) => {
                    IteratorMode::From(k, Direction::Forward)
                }
                Bound::Unbounded => IteratorMode::Start,
            };
            let iter = self.txn.txn.iterator(mode);
            for entry in iter {
                let (k, v) = entry.map_err(|_| ())?;
                let key_ref: &[u8] = &k;
                if let Bound::Excluded(start) = range.0
                    && key_ref == start
                {
                    continue;
                }
                let past_end = match range.1 {
                    Bound::Included(end) => key_ref > end,
                    Bound::Excluded(end) => key_ref >= end,
                    Bound::Unbounded => false,
                };
                if past_end {
                    break;
                }
                if predicate(&k, &v) {
                    entries.push((k, v));
                }
            }
        }
        for (key, _) in &entries {
            self.remove(key)?;
        }
        Ok(VecBenchIterator::new(entries))
    }
}

pub struct RocksdbBenchReadTransaction<'db> {
    snapshot: rocksdb::SnapshotWithThreadMode<'db, OptimisticTransactionDB>,
}

impl<'db> BenchReadTransaction for RocksdbBenchReadTransaction<'db> {
    type T<'txn>
        = RocksdbBenchReader<'db, 'txn>
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        RocksdbBenchReader {
            snapshot: &self.snapshot,
        }
    }
}

pub struct RocksdbBenchReader<'db, 'txn> {
    snapshot: &'txn rocksdb::SnapshotWithThreadMode<'db, OptimisticTransactionDB>,
}

impl BenchReader for RocksdbBenchReader<'_, '_> {
    type Output<'out>
        = Vec<u8>
    where
        Self: 'out;
    type Iterator<'out>
        = RocksdbBenchIterator<'out>
    where
        Self: 'out;

    fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.snapshot.get(key).unwrap()
    }

    fn range_from<'a>(&'a mut self, key: &'a [u8]) -> Self::Iterator<'a> {
        let iter = self
            .snapshot
            .iterator(IteratorMode::From(key, Direction::Forward));

        RocksdbBenchIterator { iter }
    }

    fn len(&mut self) -> u64 {
        self.snapshot.iterator(IteratorMode::Start).count() as u64
    }
}

pub struct RocksdbBenchIterator<'a> {
    iter: rocksdb::DBIteratorWithThreadMode<'a, OptimisticTransactionDB>,
}

impl BenchIterator for RocksdbBenchIterator<'_> {
    type Output<'out>
        = Box<[u8]>
    where
        Self: 'out;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        self.iter.next().map(|x| {
            let x = x.unwrap();
            (x.0, x.1)
        })
    }
}

pub struct FjallBenchDatabase<'a> {
    db: &'a mut fjall::SingleWriterTxDatabase,
}

impl<'a> FjallBenchDatabase<'a> {
    #[allow(dead_code)]
    pub fn new(db: &'a mut fjall::SingleWriterTxDatabase) -> Self {
        FjallBenchDatabase { db }
    }
}

impl BenchDatabase for FjallBenchDatabase<'_> {
    type C<'db>
        = FjallBenchDatabaseConnection<'db>
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "fjall"
    }

    fn connect(&self) -> Self::C<'_> {
        FjallBenchDatabaseConnection {
            db: self.db,
            sync: true,
        }
    }

    fn compact(&mut self) -> bool {
        true
    }
}

pub struct FjallBenchDatabaseConnection<'a> {
    db: &'a fjall::SingleWriterTxDatabase,
    sync: bool,
}

impl BenchDatabaseConnection for FjallBenchDatabaseConnection<'_> {
    type W<'db>
        = FjallBenchWriteTransaction<'db>
    where
        Self: 'db;
    type R<'db>
        = FjallBenchReadTransaction
    where
        Self: 'db;

    fn set_sync(&mut self, sync: bool) -> bool {
        self.sync = sync;
        true
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let part = self.db.keyspace("test", Default::default).unwrap();
        let txn = self.db.write_tx();
        FjallBenchWriteTransaction {
            txn,
            part,
            keyspace: self.db,
            sync: self.sync,
        }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let part = self.db.keyspace("test", Default::default).unwrap();
        let txn = self.db.read_tx();
        FjallBenchReadTransaction { txn, part }
    }
}

pub struct FjallBenchReadTransaction {
    part: fjall::SingleWriterTxKeyspace,
    txn: fjall::Snapshot,
}

impl BenchReadTransaction for FjallBenchReadTransaction {
    type T<'txn>
        = FjallBenchReader<'txn>
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        let FjallBenchReadTransaction { part, txn } = self;
        FjallBenchReader { part, txn }
    }
}

pub struct FjallBenchReader<'a> {
    part: &'a fjall::SingleWriterTxKeyspace,
    txn: &'a fjall::Snapshot,
}

impl BenchReader for FjallBenchReader<'_> {
    type Output<'out>
        = fjall::Slice
    where
        Self: 'out;
    type Iterator<'out>
        = FjallBenchIterator
    where
        Self: 'out;

    fn get<'a>(&'a mut self, key: &[u8]) -> Option<Self::Output<'a>> {
        self.txn.get(self.part, key).unwrap()
    }

    fn range_from<'a>(&'a mut self, key: &'a [u8]) -> Self::Iterator<'a> {
        let iter = self.txn.range(self.part, key..);
        FjallBenchIterator { iter }
    }

    fn len(&mut self) -> u64 {
        self.txn.len(self.part).unwrap().try_into().unwrap()
    }
}

pub struct FjallBenchIterator {
    iter: fjall::Iter,
}

impl BenchIterator for FjallBenchIterator {
    type Output<'a>
        = fjall::Slice
    where
        Self: 'a;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        self.iter.next().map(|item| item.into_inner().unwrap())
    }
}

pub struct FjallBenchWriteTransaction<'db> {
    keyspace: &'db fjall::SingleWriterTxDatabase,
    part: fjall::SingleWriterTxKeyspace,
    txn: fjall::SingleWriterWriteTx<'db>,
    sync: bool,
}

impl<'db> BenchWriteTransaction for FjallBenchWriteTransaction<'db> {
    type W<'txn>
        = FjallBenchInserter<'txn, 'db>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        let FjallBenchWriteTransaction {
            part,
            txn,
            keyspace: _,
            sync: _,
        } = self;
        FjallBenchInserter { part, txn }
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())?;
        let mode = if self.sync {
            fjall::PersistMode::SyncAll
        } else {
            fjall::PersistMode::Buffer
        };
        self.keyspace.persist(mode).map_err(|_| ())
    }
}

pub struct FjallBenchInserter<'txn, 'db> {
    part: &'txn fjall::SingleWriterTxKeyspace,
    txn: &'txn mut fjall::SingleWriterWriteTx<'db>,
}

impl BenchInserter for FjallBenchInserter<'_, '_> {
    type Output<'out>
        = fjall::Slice
    where
        Self: 'out;
    type ExtractIfIterator<'out, F>
        = VecBenchIterator<fjall::Slice>
    where
        Self: 'out,
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.txn.insert(self.part, key, value);
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.txn.remove(self.part, key);
        Ok(())
    }

    fn pop_first(&mut self) -> BenchPopResult<'_, Self> {
        let entry = self
            .txn
            .iter(self.part)
            .next()
            .map(fjall::Guard::into_inner)
            .transpose()
            .map_err(|_| ())?;
        if let Some((key, _)) = &entry {
            self.txn.remove(self.part, key.as_ref());
        }
        Ok(entry)
    }

    fn pop_last(&mut self) -> BenchPopResult<'_, Self> {
        let entry = self
            .txn
            .iter(self.part)
            .next_back()
            .map(fjall::Guard::into_inner)
            .transpose()
            .map_err(|_| ())?;
        if let Some((key, _)) = &entry {
            self.txn.remove(self.part, key.as_ref());
        }
        Ok(entry)
    }

    fn retain<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, mut predicate: F) -> Result<u64, ()> {
        let mut keys: Vec<fjall::Slice> = Vec::new();
        {
            let iter = self.txn.iter(self.part);
            for entry in iter {
                let (k, v) = entry.into_inner().map_err(|_| ())?;
                if !predicate(&k, &v) {
                    keys.push(k);
                }
            }
        }
        let count = keys.len() as u64;
        for key in keys {
            self.txn.remove(self.part, key);
        }
        Ok(count)
    }

    fn extract_if<'a, F>(
        &'a mut self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        mut predicate: F,
    ) -> BenchExtractIfResult<'a, Self, F>
    where
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'a,
    {
        // fjall's range iterator scans only the requested range; collect-then-delete since the
        // transaction API has no in-place cursor delete.
        let mut entries: Vec<(fjall::Slice, fjall::Slice)> = Vec::new();
        {
            let iter = self.txn.range::<&[u8], _>(self.part, range);
            for entry in iter {
                let (k, v) = entry.into_inner().map_err(|_| ())?;
                if predicate(&k, &v) {
                    entries.push((k, v));
                }
            }
        }
        for (key, _) in &entries {
            self.txn.remove(self.part, key.as_ref());
        }
        Ok(VecBenchIterator::new(entries))
    }
}

pub struct SqliteBenchDatabase {
    path: PathBuf,
}

impl SqliteBenchDatabase {
    pub fn new(path: &Path) -> Self {
        let conn = Connection::open(path).unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS kv (key BLOB PRIMARY KEY, value BLOB)",
            [],
        )
        .unwrap();
        Self {
            path: path.to_path_buf(),
        }
    }
}

impl BenchDatabase for SqliteBenchDatabase {
    type C<'db>
        = SqliteBenchDatabaseConnection
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "sqlite"
    }

    fn connect(&self) -> Self::C<'_> {
        let conn = Connection::open(&self.path).unwrap();
        SqliteBenchDatabaseConnection { conn }
    }

    fn compact(&mut self) -> bool {
        let conn = Connection::open(&self.path).unwrap();
        conn.execute("VACUUM", []).unwrap();
        true
    }
}

pub struct SqliteBenchDatabaseConnection {
    conn: Connection,
}

impl BenchDatabaseConnection for SqliteBenchDatabaseConnection {
    type W<'db>
        = SqliteBenchWriteTransaction<'db>
    where
        Self: 'db;
    type R<'db>
        = SqliteBenchReadTransaction<'db>
    where
        Self: 'db;

    fn set_sync(&mut self, sync: bool) -> bool {
        if sync {
            self.conn.execute("PRAGMA synchronous = FULL;", []).unwrap();
        } else {
            self.conn.execute("PRAGMA synchronous = OFF;", []).unwrap();
        }
        true
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let txn = self.conn.unchecked_transaction().unwrap();
        SqliteBenchWriteTransaction { txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        SqliteBenchReadTransaction { conn: &self.conn }
    }
}

pub struct SqliteBenchWriteTransaction<'db> {
    txn: Transaction<'db>,
}

impl<'db> BenchWriteTransaction for SqliteBenchWriteTransaction<'db> {
    type W<'txn>
        = SqliteBenchInserter<'txn, 'db>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        SqliteBenchInserter { txn: &self.txn }
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct SqliteBenchInserter<'txn, 'db> {
    txn: &'txn Transaction<'db>,
}

type SqlitePopResult = Result<Option<(Vec<u8>, Vec<u8>)>, ()>;

impl SqliteBenchInserter<'_, '_> {
    fn pop_ordered(&self, direction: &str) -> SqlitePopResult {
        let sql = format!("SELECT rowid, key, value FROM kv ORDER BY key {direction} LIMIT 1");
        let entry = self
            .txn
            .query_row(&sql, [], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .optional()
            .map_err(|_| ())?;
        if let Some((rowid, key, value)) = entry {
            self.txn
                .execute("DELETE FROM kv WHERE rowid = ?", [rowid])
                .map_err(|_| ())?;
            Ok(Some((key, value)))
        } else {
            Ok(None)
        }
    }
}

impl BenchInserter for SqliteBenchInserter<'_, '_> {
    type Output<'out>
        = Vec<u8>
    where
        Self: 'out;
    type ExtractIfIterator<'out, F>
        = VecBenchIterator<Vec<u8>>
    where
        Self: 'out,
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'out;

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.txn
            .execute(
                "INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)",
                [key, value],
            )
            .map(|_| ())
            .map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.txn
            .execute("DELETE FROM kv WHERE key = ?", [key])
            .map(|_| ())
            .map_err(|_| ())
    }

    fn pop_first(&mut self) -> BenchPopResult<'_, Self> {
        self.pop_ordered("ASC")
    }

    fn pop_last(&mut self) -> BenchPopResult<'_, Self> {
        self.pop_ordered("DESC")
    }

    fn retain<F: FnMut(&[u8], &[u8]) -> bool>(&mut self, mut predicate: F) -> Result<u64, ()> {
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        {
            let mut stmt = self
                .txn
                .prepare("SELECT key, value FROM kv ORDER BY key")
                .map_err(|_| ())?;
            let mut rows = stmt.query([]).map_err(|_| ())?;
            while let Some(row) = rows.next().map_err(|_| ())? {
                let k: Vec<u8> = row.get(0).map_err(|_| ())?;
                let v: Vec<u8> = row.get(1).map_err(|_| ())?;
                if !predicate(&k, &v) {
                    to_delete.push(k);
                }
            }
        }
        let count = to_delete.len() as u64;
        let mut delete_stmt = self
            .txn
            .prepare("DELETE FROM kv WHERE key = ?")
            .map_err(|_| ())?;
        for key in &to_delete {
            delete_stmt.execute([key]).map_err(|_| ())?;
        }
        Ok(count)
    }

    fn extract_if<'a, F>(
        &'a mut self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        mut predicate: F,
    ) -> BenchExtractIfResult<'a, Self, F>
    where
        F: for<'f> FnMut(&'f [u8], &'f [u8]) -> bool + 'a,
    {
        // Push the range bound down into SQL so SQLite scans only the relevant portion of the
        // index; predicate evaluation still happens in Rust.
        let mut conds: Vec<&str> = Vec::new();
        let mut params: Vec<&[u8]> = Vec::new();
        match range.0 {
            Bound::Included(s) => {
                conds.push("key >= ?");
                params.push(s);
            }
            Bound::Excluded(s) => {
                conds.push("key > ?");
                params.push(s);
            }
            Bound::Unbounded => {}
        }
        match range.1 {
            Bound::Included(e) => {
                conds.push("key <= ?");
                params.push(e);
            }
            Bound::Excluded(e) => {
                conds.push("key < ?");
                params.push(e);
            }
            Bound::Unbounded => {}
        }
        let mut sql = String::from("SELECT key, value FROM kv");
        if !conds.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conds.join(" AND "));
        }
        sql.push_str(" ORDER BY key");

        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        {
            let mut stmt = self.txn.prepare(&sql).map_err(|_| ())?;
            let mut rows = stmt
                .query(rusqlite::params_from_iter(params.iter()))
                .map_err(|_| ())?;
            while let Some(row) = rows.next().map_err(|_| ())? {
                let k: Vec<u8> = row.get(0).map_err(|_| ())?;
                let v: Vec<u8> = row.get(1).map_err(|_| ())?;
                if predicate(&k, &v) {
                    entries.push((k, v));
                }
            }
        }
        let mut delete_stmt = self
            .txn
            .prepare("DELETE FROM kv WHERE key = ?")
            .map_err(|_| ())?;
        for (key, _) in &entries {
            delete_stmt.execute([key]).map_err(|_| ())?;
        }
        Ok(VecBenchIterator::new(entries))
    }
}

pub struct SqliteBenchReadTransaction<'db> {
    conn: &'db Connection,
}

impl<'db> BenchReadTransaction for SqliteBenchReadTransaction<'db> {
    type T<'txn>
        = SqliteBenchReader<'db>
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        let get_stmt = self
            .conn
            .prepare("SELECT value FROM kv WHERE key = ?")
            .unwrap();
        let range_stmt = self
            .conn
            .prepare("SELECT key, value FROM kv WHERE key >= ? ORDER BY key")
            .unwrap();
        let len_stmt = self.conn.prepare("SELECT COUNT(*) FROM kv").unwrap();
        SqliteBenchReader {
            get_stmt,
            range_stmt,
            len_stmt,
        }
    }
}

pub struct SqliteBenchReader<'db> {
    get_stmt: Statement<'db>,
    range_stmt: Statement<'db>,
    len_stmt: Statement<'db>,
}

impl BenchReader for SqliteBenchReader<'_> {
    type Output<'out>
        = Vec<u8>
    where
        Self: 'out;
    type Iterator<'out>
        = SqliteBenchIterator<'out>
    where
        Self: 'out;

    fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_stmt.query_row([key], |row| row.get(0)).ok()
    }

    fn range_from<'a>(&'a mut self, key: &'a [u8]) -> Self::Iterator<'a> {
        let rows = self.range_stmt.query([key]).unwrap();
        SqliteBenchIterator { rows }
    }

    fn len(&mut self) -> u64 {
        self.len_stmt
            .query_row([], |row| row.get::<_, i64>(0))
            .unwrap() as u64
    }
}

pub struct SqliteBenchIterator<'stmt> {
    rows: rusqlite::Rows<'stmt>,
}

impl BenchIterator for SqliteBenchIterator<'_> {
    type Output<'out>
        = Vec<u8>
    where
        Self: 'out;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        let row = self.rows.next().unwrap()?;
        Some((
            row.get::<_, Vec<u8>>(0).unwrap(),
            row.get::<_, Vec<u8>>(1).unwrap(),
        ))
    }
}
