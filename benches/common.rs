use redb::AccessGuard;

pub trait BenchTable<T: AsRef<[u8]>> {
    type W: BenchWriteTransaction;
    type R: BenchReadTransaction<T>;

    fn db_type_name() -> &'static str;

    fn write_transaction(&mut self) -> Self::W;

    fn read_transaction(&self) -> Self::R;
}

pub trait BenchWriteTransaction {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()>;

    fn commit(self) -> Result<(), ()>;
}

pub trait BenchReadTransaction<T: AsRef<[u8]>> {
    fn get(&self, key: &[u8]) -> Option<T>;
}

pub struct RedbBenchTable<'a> {
    table: redb::Table<'a>,
}

impl<'a> RedbBenchTable<'a> {
    pub fn new(db: &'a redb::Database) -> Self {
        RedbBenchTable {
            table: db.open_table("").unwrap(),
        }
    }
}

impl<'a> BenchTable<AccessGuard<'a>> for RedbBenchTable<'a> {
    type W = RedbBenchWriteTransaction<'a>;
    type R = RedbBenchReadTransaction<'a>;

    fn db_type_name() -> &'static str {
        "redb"
    }

    fn write_transaction(&mut self) -> Self::W {
        RedbBenchWriteTransaction {
            txn: self.table.begin_write().unwrap(),
        }
    }

    fn read_transaction(&self) -> Self::R {
        RedbBenchReadTransaction {
            txn: self.table.read_transaction().unwrap(),
        }
    }
}

pub struct RedbBenchReadTransaction<'a> {
    txn: redb::ReadOnlyTransaction<'a>,
}

impl<'a> BenchReadTransaction<redb::AccessGuard<'a>> for RedbBenchReadTransaction<'a> {
    fn get(&self, key: &[u8]) -> Option<AccessGuard<'a>> {
        self.txn.get(key).unwrap()
    }
}

pub struct RedbBenchWriteTransaction<'a> {
    txn: redb::WriteTransaction<'a>,
}

impl BenchWriteTransaction for RedbBenchWriteTransaction<'_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.txn.insert(key, value).map_err(|_| ())
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct SledBenchTable<'a> {
    db: &'a sled::Db,
}

impl<'a> SledBenchTable<'a> {
    pub fn new(db: &'a sled::Db) -> Self {
        SledBenchTable { db }
    }
}

impl<'a> BenchTable<sled::IVec> for SledBenchTable<'a> {
    type W = SledBenchWriteTransaction<'a>;
    type R = SledBenchReadTransaction<'a>;

    fn db_type_name() -> &'static str {
        "sled"
    }

    fn write_transaction(&mut self) -> Self::W {
        SledBenchWriteTransaction { db: self.db }
    }

    fn read_transaction(&self) -> Self::R {
        SledBenchReadTransaction { db: self.db }
    }
}

pub struct SledBenchReadTransaction<'a> {
    db: &'a sled::Db,
}

impl<'a> BenchReadTransaction<sled::IVec> for SledBenchReadTransaction<'a> {
    fn get(&self, key: &[u8]) -> Option<sled::IVec> {
        self.db.get(key).unwrap()
    }
}

pub struct SledBenchWriteTransaction<'a> {
    db: &'a sled::Db,
}

impl BenchWriteTransaction for SledBenchWriteTransaction<'_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.db.insert(key, value).map(|_| ()).map_err(|_| ())
    }

    fn commit(self) -> Result<(), ()> {
        self.db.flush().map(|_| ()).map_err(|_| ())
    }
}
