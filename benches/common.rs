use heed::{CompactionOption, EnvInfo};
use redb::{AccessGuard, ReadableTableMetadata, TableDefinition};
use rocksdb::{
    Direction, IteratorMode, OptimisticTransactionDB, OptimisticTransactionOptions, WriteOptions,
};
use sanakirja::btree::page_unsized;
use sanakirja::{Commit, RootDb};
use std::fs;
use std::fs::File;
use std::ops::Bound;
use std::path::Path;

#[allow(dead_code)]
const X: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

pub trait BenchDatabase {
    type W<'db>: BenchWriteTransaction
    where
        Self: 'db;
    type R<'db>: BenchReadTransaction
    where
        Self: 'db;

    fn db_type_name() -> &'static str;

    fn write_transaction(&self) -> Self::W<'_>;

    fn read_transaction(&self) -> Self::R<'_>;

    // Returns a boolean indicating whether compaction is supported
    fn compact(&mut self) -> bool {
        false
    }
}

pub trait BenchWriteTransaction {
    type W<'txn>: BenchInserter
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_>;

    #[allow(clippy::result_unit_err)]
    fn commit(self) -> Result<(), ()>;
}

pub trait BenchInserter {
    #[allow(clippy::result_unit_err)]
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()>;

    #[allow(clippy::result_unit_err)]
    fn remove(&mut self, key: &[u8]) -> Result<(), ()>;
}

pub trait BenchReadTransaction {
    type T<'txn>: BenchReader
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_>;
}

#[allow(clippy::len_without_is_empty)]
pub trait BenchReader {
    type Output<'out>: AsRef<[u8]> + 'out
    where
        Self: 'out;
    type Iterator<'out>: BenchIterator
    where
        Self: 'out;

    fn get<'a>(&'a self, key: &[u8]) -> Option<Self::Output<'a>>;

    fn range_from<'a>(&'a self, start: &'a [u8]) -> Self::Iterator<'a>;

    fn len(&self) -> u64;
}

pub trait BenchIterator {
    type Output<'out>: AsRef<[u8]> + 'out
    where
        Self: 'out;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)>;
}

pub struct RedbBenchDatabase<'a> {
    db: &'a mut redb::Database,
}

impl<'a> RedbBenchDatabase<'a> {
    #[allow(dead_code)]
    pub fn new(db: &'a mut redb::Database) -> Self {
        RedbBenchDatabase { db }
    }
}

impl<'a> BenchDatabase for RedbBenchDatabase<'a> {
    type W<'db> = RedbBenchWriteTransaction where Self: 'db;
    type R<'db> = RedbBenchReadTransaction where Self: 'db;

    fn db_type_name() -> &'static str {
        "redb"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let txn = self.db.begin_write().unwrap();
        RedbBenchWriteTransaction { txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let txn = self.db.begin_read().unwrap();
        RedbBenchReadTransaction { txn }
    }

    fn compact(&mut self) -> bool {
        self.db.compact().unwrap();
        true
    }
}

pub struct RedbBenchReadTransaction {
    txn: redb::ReadTransaction,
}

impl BenchReadTransaction for RedbBenchReadTransaction {
    type T<'txn> = RedbBenchReader where Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        let table = self.txn.open_table(X).unwrap();
        RedbBenchReader { table }
    }
}

pub struct RedbBenchReader {
    table: redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
}

impl BenchReader for RedbBenchReader {
    type Output<'out> = RedbAccessGuard<'out> where Self: 'out;
    type Iterator<'out> = RedbBenchIterator<'out> where Self: 'out;

    fn get<'a>(&'a self, key: &[u8]) -> Option<Self::Output<'a>> {
        self.table.get(key).unwrap().map(RedbAccessGuard::new)
    }

    fn range_from<'a>(&'a self, key: &'a [u8]) -> Self::Iterator<'a> {
        let iter = self.table.range(key..).unwrap();
        RedbBenchIterator { iter }
    }

    fn len(&self) -> u64 {
        self.table.len().unwrap()
    }
}

pub struct RedbBenchIterator<'a> {
    iter: redb::Range<'a, &'static [u8], &'static [u8]>,
}

impl BenchIterator for RedbBenchIterator<'_> {
    type Output<'a> = RedbAccessGuard<'a> where Self: 'a;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        self.iter.next().map(|item| {
            let (k, v) = item.unwrap();
            (RedbAccessGuard::new(k), RedbAccessGuard::new(v))
        })
    }
}

pub struct RedbAccessGuard<'a> {
    inner: AccessGuard<'a, &'static [u8]>,
}

impl<'a> RedbAccessGuard<'a> {
    fn new(inner: AccessGuard<'a, &'static [u8]>) -> Self {
        Self { inner }
    }
}

impl<'a> AsRef<[u8]> for RedbAccessGuard<'a> {
    fn as_ref(&self) -> &[u8] {
        self.inner.value()
    }
}

pub struct RedbBenchWriteTransaction {
    txn: redb::WriteTransaction,
}

impl BenchWriteTransaction for RedbBenchWriteTransaction {
    type W<'txn> = RedbBenchInserter<'txn> where Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        let table = self.txn.open_table(X).unwrap();
        RedbBenchInserter { table }
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct RedbBenchInserter<'txn> {
    table: redb::Table<'txn, &'static [u8], &'static [u8]>,
}

impl BenchInserter for RedbBenchInserter<'_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.table.insert(key, value).map(|_| ()).map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.table.remove(key).map(|_| ()).map_err(|_| ())
    }
}

pub struct SledBenchDatabase<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
}

impl<'a> SledBenchDatabase<'a> {
    pub fn new(db: &'a sled::Db, path: &'a Path) -> Self {
        SledBenchDatabase { db, db_dir: path }
    }
}

impl<'a> BenchDatabase for SledBenchDatabase<'a> {
    type W<'db> = SledBenchWriteTransaction<'db> where Self: 'db;
    type R<'db> = SledBenchReadTransaction<'db> where Self: 'db;

    fn db_type_name() -> &'static str {
        "sled"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        SledBenchWriteTransaction {
            db: self.db,
            db_dir: self.db_dir,
        }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        SledBenchReadTransaction { db: self.db }
    }
}

pub struct SledBenchReadTransaction<'db> {
    db: &'db sled::Db,
}

impl<'db> BenchReadTransaction for SledBenchReadTransaction<'db> {
    type T<'txn> = SledBenchReader<'db> where Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        SledBenchReader { db: self.db }
    }
}

pub struct SledBenchReader<'db> {
    db: &'db sled::Db,
}

impl<'db> BenchReader for SledBenchReader<'db> {
    type Output<'out> = sled::IVec where Self: 'out;
    type Iterator<'out> = SledBenchIterator where Self: 'out;

    fn get(&self, key: &[u8]) -> Option<sled::IVec> {
        self.db.get(key).unwrap()
    }

    fn range_from<'a>(&'a self, key: &'a [u8]) -> Self::Iterator<'a> {
        let iter = self.db.range(key..);
        SledBenchIterator { iter }
    }

    fn len(&self) -> u64 {
        self.db.len() as u64
    }
}

pub struct SledBenchIterator {
    iter: sled::Iter,
}

impl BenchIterator for SledBenchIterator {
    type Output<'out> = sled::IVec where Self: 'out;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        self.iter.next().map(|x| x.unwrap())
    }
}

pub struct SledBenchWriteTransaction<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
}

impl<'a> BenchWriteTransaction for SledBenchWriteTransaction<'a> {
    type W<'txn> = SledBenchInserter<'txn> where Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        SledBenchInserter { db: self.db }
    }

    fn commit(self) -> Result<(), ()> {
        self.db.flush().unwrap();
        // Workaround for sled durability
        // Fsync all the files, because sled doesn't guarantee durability (it uses sync_file_range())
        // See: https://github.com/spacejam/sled/issues/1351
        for entry in fs::read_dir(self.db_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.path().is_file() {
                let file = File::open(entry.path()).unwrap();
                file.sync_all().unwrap();
            }
        }
        Ok(())
    }
}

pub struct SledBenchInserter<'a> {
    db: &'a sled::Db,
}

impl<'a> BenchInserter for SledBenchInserter<'a> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.db.insert(key, value).map(|_| ()).map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.db.remove(key).map(|_| ()).map_err(|_| ())
    }
}

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
    type W<'db> = HeedBenchWriteTransaction<'db> where Self: 'db;
    type R<'db> = HeedBenchReadTransaction<'db> where Self: 'db;

    fn db_type_name() -> &'static str {
        "lmdb"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let env = self.env.as_ref().unwrap();
        let txn = env.write_txn().unwrap();
        Self::W { db: self.db, txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let env = self.env.as_ref().unwrap();
        let txn = env.read_txn().unwrap();
        Self::R { db: self.db, txn }
    }

    fn compact(&mut self) -> bool {
        // We take the env to be able to compact and reopen it after compaction.
        let env = self.env.take().unwrap();
        let EnvInfo { map_size, .. } = env.info();
        let path = env.path().to_owned();
        let file = env
            .copy_to_file(path.join("data2.mdb"), CompactionOption::Enabled)
            .unwrap();
        drop(file);

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

pub struct HeedBenchWriteTransaction<'db> {
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: heed::RwTxn<'db>,
}

impl<'db> BenchWriteTransaction for HeedBenchWriteTransaction<'db> {
    type W<'txn> = HeedBenchInserter<'txn, 'db> where Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        Self::W {
            db: self.db,
            txn: &mut self.txn,
        }
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct HeedBenchInserter<'txn, 'db> {
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: &'txn mut heed::RwTxn<'db>,
}

impl BenchInserter for HeedBenchInserter<'_, '_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.db.put(self.txn, key, value).map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.db.delete(self.txn, key).map(|_| ()).map_err(|_| ())
    }
}

pub struct HeedBenchReadTransaction<'db> {
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    txn: heed::RoTxn<'db>,
}

impl<'db> BenchReadTransaction for HeedBenchReadTransaction<'db> {
    type T<'txn> = HeedBenchReader<'txn, 'db> where Self: 'txn;

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

impl<'txn, 'db> BenchReader for HeedBenchReader<'txn, 'db> {
    type Output<'out> = &'out [u8] where Self: 'out;
    type Iterator<'out> = HeedBenchIterator<'out> where Self: 'out;

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.db.get(self.txn, key).unwrap()
    }

    fn range_from<'a>(&'a self, key: &'a [u8]) -> Self::Iterator<'a> {
        let range = (Bound::Included(key), Bound::Unbounded);
        let iter = self.db.range(self.txn, &range).unwrap();

        Self::Iterator { iter }
    }

    fn len(&self) -> u64 {
        self.db.stat(self.txn).unwrap().entries as u64
    }
}

pub struct HeedBenchIterator<'a> {
    iter: heed::RoRange<'a, heed::types::Bytes, heed::types::Bytes>,
}

impl BenchIterator for HeedBenchIterator<'_> {
    type Output<'out> = &'out [u8] where Self: 'out;

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

impl<'a> BenchDatabase for RocksdbBenchDatabase<'a> {
    type W<'db> = RocksdbBenchWriteTransaction<'db> where Self: 'db;
    type R<'db> = RocksdbBenchReadTransaction<'db> where Self: 'db;

    fn db_type_name() -> &'static str {
        "rocksdb"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let mut write_opt = WriteOptions::new();
        write_opt.set_sync(true);
        let mut txn_opt = OptimisticTransactionOptions::new();
        txn_opt.set_snapshot(true);
        let txn = self.db.transaction_opt(&write_opt, &txn_opt);
        RocksdbBenchWriteTransaction { txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let snapshot = self.db.snapshot();
        RocksdbBenchReadTransaction { snapshot }
    }

    fn compact(&mut self) -> bool {
        self.db.compact_range::<&[u8], &[u8]>(None, None);
        true
    }
}

pub struct RocksdbBenchWriteTransaction<'a> {
    txn: rocksdb::Transaction<'a, OptimisticTransactionDB>,
}

impl<'a> BenchWriteTransaction for RocksdbBenchWriteTransaction<'a> {
    type W<'txn> = RocksdbBenchInserter<'txn> where Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        RocksdbBenchInserter { txn: &self.txn }
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct RocksdbBenchInserter<'a> {
    txn: &'a rocksdb::Transaction<'a, OptimisticTransactionDB>,
}

impl BenchInserter for RocksdbBenchInserter<'_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.txn.put(key, value).map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.txn.delete(key).map_err(|_| ())
    }
}

pub struct RocksdbBenchReadTransaction<'db> {
    snapshot: rocksdb::SnapshotWithThreadMode<'db, OptimisticTransactionDB>,
}

impl<'db> BenchReadTransaction for RocksdbBenchReadTransaction<'db> {
    type T<'txn> = RocksdbBenchReader<'db, 'txn> where Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        RocksdbBenchReader {
            snapshot: &self.snapshot,
        }
    }
}

pub struct RocksdbBenchReader<'db, 'txn> {
    snapshot: &'txn rocksdb::SnapshotWithThreadMode<'db, OptimisticTransactionDB>,
}

impl<'db, 'txn> BenchReader for RocksdbBenchReader<'db, 'txn> {
    type Output<'out> = Vec<u8> where Self: 'out;
    type Iterator<'out> = RocksdbBenchIterator<'out> where Self: 'out;

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.snapshot.get(key).unwrap()
    }

    fn range_from<'a>(&'a self, key: &'a [u8]) -> Self::Iterator<'a> {
        let iter = self
            .snapshot
            .iterator(IteratorMode::From(key, Direction::Forward));

        RocksdbBenchIterator { iter }
    }

    fn len(&self) -> u64 {
        self.snapshot.iterator(IteratorMode::Start).count() as u64
    }
}

pub struct RocksdbBenchIterator<'a> {
    iter: rocksdb::DBIteratorWithThreadMode<'a, OptimisticTransactionDB>,
}

impl BenchIterator for RocksdbBenchIterator<'_> {
    type Output<'out> = Box<[u8]> where Self: 'out;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        self.iter.next().map(|x| {
            let x = x.unwrap();
            (x.0, x.1)
        })
    }
}

pub struct SanakirjaBenchDatabase<'a> {
    db: &'a sanakirja::Env,
}

impl<'a> SanakirjaBenchDatabase<'a> {
    #[allow(dead_code)]
    pub fn new(db: &'a sanakirja::Env) -> Self {
        let mut txn = sanakirja::Env::mut_txn_begin(db).unwrap();
        // XXX: There's no documentation on why this method is unsafe, so let's just hope we upheld the requirements for it to be safe!
        let table = unsafe {
            sanakirja::btree::create_db_::<_, [u8], [u8], page_unsized::Page<[u8], [u8]>>(&mut txn)
                .unwrap()
        };
        txn.set_root(0, table.db.into());
        txn.commit().unwrap();
        Self { db }
    }
}

impl<'a> BenchDatabase for SanakirjaBenchDatabase<'a> {
    type W<'db> = SanakirjaBenchWriteTransaction<'db> where Self: 'db;
    type R<'db> = SanakirjaBenchReadTransaction<'db> where Self: 'db;

    fn db_type_name() -> &'static str {
        "sanakirja"
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let txn = sanakirja::Env::mut_txn_begin(self.db).unwrap();
        SanakirjaBenchWriteTransaction { txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let txn = sanakirja::Env::txn_begin(self.db).unwrap();
        SanakirjaBenchReadTransaction { txn }
    }
}

pub struct SanakirjaBenchWriteTransaction<'db> {
    txn: sanakirja::MutTxn<&'db sanakirja::Env, ()>,
}

impl<'db> BenchWriteTransaction for SanakirjaBenchWriteTransaction<'db> {
    type W<'txn> = SanakirjaBenchInserter<'db, 'txn> where Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        let table = self.txn.root_db(0).unwrap();
        SanakirjaBenchInserter {
            txn: &mut self.txn,
            table,
        }
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct SanakirjaBenchInserter<'db, 'txn> {
    txn: &'txn mut sanakirja::MutTxn<&'db sanakirja::Env, ()>,
    #[allow(clippy::type_complexity)]
    table: sanakirja::btree::Db_<[u8], [u8], page_unsized::Page<[u8], [u8]>>,
}

impl BenchInserter for SanakirjaBenchInserter<'_, '_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        let result = sanakirja::btree::put(self.txn, &mut self.table, key, value)
            .map_err(|_| ())
            .map(|_| ());
        self.txn.set_root(0, self.table.db.into());
        result
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        let result = sanakirja::btree::del(self.txn, &mut self.table, key, None)
            .map_err(|_| ())
            .map(|_| ());
        self.txn.set_root(0, self.table.db.into());
        result
    }
}

pub struct SanakirjaBenchReadTransaction<'db> {
    txn: sanakirja::Txn<&'db sanakirja::Env>,
}

impl<'db> BenchReadTransaction for SanakirjaBenchReadTransaction<'db> {
    type T<'txn> = SanakirjaBenchReader<'db, 'txn> where Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        let table = self.txn.root_db(0).unwrap();
        SanakirjaBenchReader {
            txn: &self.txn,
            table,
        }
    }
}

pub struct SanakirjaBenchReader<'db, 'txn> {
    txn: &'txn sanakirja::Txn<&'db sanakirja::Env>,
    #[allow(clippy::type_complexity)]
    table: sanakirja::btree::Db_<[u8], [u8], page_unsized::Page<[u8], [u8]>>,
}

impl<'db, 'txn> BenchReader for SanakirjaBenchReader<'db, 'txn> {
    type Output<'out> = &'out [u8] where Self: 'out;
    type Iterator<'out> = SanakirjaBenchIterator<'db, 'txn> where Self: 'out;

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        sanakirja::btree::get(self.txn, &self.table, key, None)
            .unwrap()
            .map(|(_, v)| v)
    }

    fn range_from<'a>(&'a self, key: &'a [u8]) -> Self::Iterator<'a> {
        let iter = sanakirja::btree::iter(self.txn, &self.table, Some((key, None))).unwrap();

        SanakirjaBenchIterator { iter }
    }

    fn len(&self) -> u64 {
        sanakirja::btree::iter(self.txn, &self.table, None)
            .unwrap()
            .count() as u64
    }
}

pub struct SanakirjaBenchIterator<'db, 'txn> {
    #[allow(clippy::type_complexity)]
    iter: sanakirja::btree::Iter<
        'txn,
        sanakirja::Txn<&'db sanakirja::Env>,
        [u8],
        [u8],
        page_unsized::Page<[u8], [u8]>,
    >,
}

impl<'db, 'txn> BenchIterator for SanakirjaBenchIterator<'db, 'txn> {
    type Output<'out> = &'txn [u8] where Self: 'out;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        self.iter.next().map(|x| {
            let x = x.unwrap();
            (x.0, x.1)
        })
    }
}
