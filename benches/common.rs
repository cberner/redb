pub trait BenchTable {
    type W: BenchWriteTransaction;
    type R: BenchReadTransaction;

    fn db_type_name() -> &'static str;

    fn write_transaction(&mut self) -> Self::W;

    fn read_transaction(&self) -> Self::R;
}

pub trait BenchWriteTransaction {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()>;

    fn commit(self) -> Result<(), ()>;
}

pub trait BenchReadTransaction {
    type T: AsRef<[u8]>;

    fn get(&self, key: &[u8]) -> Option<Self::T>;
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

impl<'a> BenchTable for RedbBenchTable<'a> {
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

impl<'a> BenchReadTransaction for RedbBenchReadTransaction<'a> {
    type T = redb::AccessGuard<'a>;

    fn get(&self, key: &[u8]) -> Option<Self::T> {
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
