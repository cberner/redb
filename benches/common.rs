use redb::{ReadableTable, TableDefinition};
use std::fs;
use std::fs::File;
use std::path::Path;

const X: TableDefinition<[u8], [u8]> = TableDefinition::new("x");

pub trait BenchDatabase {
    type W: for<'a> BenchWriteTransaction<'a>;
    type R: for<'a> BenchReadTransaction<'a>;

    fn db_type_name() -> &'static str;

    fn write_transaction(&mut self) -> Self::W;

    fn read_transaction(&self) -> Self::R;
}

pub trait BenchWriteTransaction<'a> {
    type T: BenchInserter + 'a;

    fn get_inserter(&'a self) -> Self::T;

    fn commit(self) -> Result<(), ()>;
}

pub trait BenchInserter {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()>;

    fn remove(&mut self, key: &[u8]) -> Result<(), ()>;
}

pub trait BenchReadTransaction<'a> {
    type Output: AsRef<[u8]> + 'a;

    fn get(&'a self, key: &[u8]) -> Option<Self::Output>;

    // TODO: change this to a method that iterates over a range, for a more complete benchmark
    fn exists_after(&'a self, key: &[u8]) -> bool;
}

pub struct RedbBenchDatabase<'a> {
    db: &'a redb::Database,
}

impl<'a> RedbBenchDatabase<'a> {
    pub fn new(db: &'a redb::Database) -> Self {
        RedbBenchDatabase { db }
    }
}

impl<'a> BenchDatabase for RedbBenchDatabase<'a> {
    type W = RedbBenchWriteTransaction<'a>;
    type R = RedbBenchReadTransaction<'a>;

    fn db_type_name() -> &'static str {
        "redb"
    }

    fn write_transaction(&mut self) -> Self::W {
        let txn = self.db.begin_write().unwrap();
        RedbBenchWriteTransaction { txn }
    }

    fn read_transaction(&self) -> Self::R {
        let txn = self.db.begin_read().unwrap();
        let table = txn.open_table(X).unwrap();
        RedbBenchReadTransaction { _txn: txn, table }
    }
}

pub struct RedbBenchReadTransaction<'a> {
    _txn: redb::ReadOnlyDatabaseTransaction<'a>,
    table: redb::ReadOnlyTable<'a, [u8], [u8]>,
}

impl<'a, 'b> BenchReadTransaction<'b> for RedbBenchReadTransaction<'a> {
    type Output = &'b [u8];

    fn get(&'b self, key: &[u8]) -> Option<&'b [u8]> {
        self.table.get(key).unwrap()
    }

    fn exists_after(&'b self, key: &[u8]) -> bool {
        self.table.range(key..).unwrap().next().is_some()
    }
}

pub struct RedbBenchWriteTransaction<'a> {
    txn: redb::DatabaseTransaction<'a>,
}

impl<'a, 'b> BenchWriteTransaction<'b> for RedbBenchWriteTransaction<'a> {
    type T = RedbBenchInserter<'b>;

    fn get_inserter(&'b self) -> Self::T {
        RedbBenchInserter {
            table: self.txn.open_table(&X).unwrap(),
        }
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct RedbBenchInserter<'a> {
    table: redb::Table<'a, [u8], [u8]>,
}

impl BenchInserter for RedbBenchInserter<'_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.table.insert(key, value).map_err(|_| ())
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
    type W = SledBenchWriteTransaction<'a>;
    type R = SledBenchReadTransaction<'a>;

    fn db_type_name() -> &'static str {
        "sled"
    }

    fn write_transaction(&mut self) -> Self::W {
        SledBenchWriteTransaction {
            db: self.db,
            db_dir: self.db_dir,
        }
    }

    fn read_transaction(&self) -> Self::R {
        SledBenchReadTransaction { db: self.db }
    }
}

pub struct SledBenchReadTransaction<'a> {
    db: &'a sled::Db,
}

impl<'a, 'b> BenchReadTransaction<'b> for SledBenchReadTransaction<'a> {
    type Output = sled::IVec;

    fn get(&'b self, key: &[u8]) -> Option<sled::IVec> {
        self.db.get(key).unwrap()
    }

    fn exists_after(&'b self, key: &[u8]) -> bool {
        self.db.range(key..).next().is_some()
    }
}

pub struct SledBenchWriteTransaction<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
}

impl<'a, 'b> BenchWriteTransaction<'b> for SledBenchWriteTransaction<'a> {
    type T = SledBenchInserter<'b>;

    fn get_inserter(&'b self) -> Self::T {
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

pub struct LmdbRkvBenchDatabase<'a> {
    env: &'a lmdb::Environment,
    db: lmdb::Database,
}

impl<'a> LmdbRkvBenchDatabase<'a> {
    pub fn new(env: &'a lmdb::Environment) -> Self {
        let db = env.open_db(None).unwrap();
        LmdbRkvBenchDatabase { env, db }
    }
}

impl<'a> BenchDatabase for LmdbRkvBenchDatabase<'a> {
    type W = LmdbRkvBenchWriteTransaction<'a>;
    type R = LmdbRkvBenchReadTransaction<'a>;

    fn db_type_name() -> &'static str {
        "lmdb-rkv"
    }

    fn write_transaction(&mut self) -> Self::W {
        let txn = self.env.begin_rw_txn().unwrap();
        LmdbRkvBenchWriteTransaction { db: self.db, txn }
    }

    fn read_transaction(&self) -> Self::R {
        let txn = self.env.begin_ro_txn().unwrap();
        LmdbRkvBenchReadTransaction { db: self.db, txn }
    }
}

pub struct LmdbRkvBenchWriteTransaction<'a> {
    db: lmdb::Database,
    txn: lmdb::RwTransaction<'a>,
}

impl<'a, 'b> BenchWriteTransaction<'b> for LmdbRkvBenchWriteTransaction<'a> {
    type T = LmdbRkvBenchInserter<'b>;

    fn get_inserter(&'b self) -> Self::T {
        LmdbRkvBenchInserter {
            db: self.db,
            txn: &self.txn,
        }
    }

    fn commit(self) -> Result<(), ()> {
        use lmdb::Transaction;
        self.txn.commit().map_err(|_| ())
    }
}

pub struct LmdbRkvBenchInserter<'a> {
    db: lmdb::Database,
    txn: &'a lmdb::RwTransaction<'a>,
}

impl BenchInserter for LmdbRkvBenchInserter<'_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        // TODO: this might be UB, but I couldn't figure out how to fix the lifetimes without GATs
        let mut_txn =
            unsafe { &mut *(self.txn as *const lmdb::RwTransaction as *mut lmdb::RwTransaction) };
        mut_txn
            .put(self.db, &key, &value, lmdb::WriteFlags::empty())
            .map_err(|_| ())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        // TODO: this might be UB, but I couldn't figure out how to fix the lifetimes without GATs
        let mut_txn =
            unsafe { &mut *(self.txn as *const lmdb::RwTransaction as *mut lmdb::RwTransaction) };
        mut_txn.del(self.db, &key, None).map_err(|_| ())
    }
}

pub struct LmdbRkvBenchReadTransaction<'a> {
    db: lmdb::Database,
    txn: lmdb::RoTransaction<'a>,
}

impl<'a, 'b> BenchReadTransaction<'b> for LmdbRkvBenchReadTransaction<'a> {
    type Output = &'b [u8];

    fn get(&'b self, key: &[u8]) -> Option<&'b [u8]> {
        use lmdb::Transaction;
        self.txn.get(self.db, &key).ok()
    }

    fn exists_after(&'b self, key: &[u8]) -> bool {
        use lmdb::{Cursor, Transaction};
        self.txn
            .open_ro_cursor(self.db)
            .unwrap()
            .iter_from(key)
            .next()
            .is_some()
    }
}
