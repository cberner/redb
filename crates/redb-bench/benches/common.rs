use heed::{CompactionOption, EnvFlags, EnvInfo, FlagSetMode};
use redb::{AccessGuard, Durability, ReadableDatabase, ReadableTableMetadata, TableDefinition};
use rocksdb::{
    Direction, IteratorMode, OptimisticTransactionDB, OptimisticTransactionOptions, WriteOptions,
};
use rusqlite::{Connection, Transaction};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fs, mem, thread};

#[allow(dead_code)]
const X: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

const READ_ITERATIONS: usize = 2;
const BULK_ELEMENTS: usize = 5_000_000;
const INDIVIDUAL_WRITES: usize = 1_000;
const NOSYNC_WRITES: usize = 50_000;
const BATCH_WRITES: usize = 100;
const BATCH_SIZE: usize = 1000;
const SCAN_ITERATIONS: usize = 2;
const NUM_READS: usize = 1_000_000;
const NUM_SCANS: usize = 500_000;
const SCAN_LEN: usize = 10;
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const RNG_SEED: u64 = 3;

pub const CACHE_SIZE: usize = 4 * 1_024 * 1_024 * 1_024; // 4GB

// XXX: Awful hack because Rocksdb seems to have unbounded memory usage for bulk writes
const ROCKSDB_MAX_WRITES_PER_TXN: u64 = 100_000;

/// Returns pairs of key, value
fn random_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
    let mut key = [0u8; KEY_SIZE];
    rng.fill(&mut key);
    let mut value = vec![0u8; VALUE_SIZE];
    rng.fill(&mut value);

    (key, value)
}

fn make_rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(RNG_SEED)
}

fn make_rng_shards(shards: usize, elements: usize) -> Vec<fastrand::Rng> {
    let mut rngs = vec![];
    let elements_per_shard = elements / shards;
    for i in 0..shards {
        let mut rng = make_rng();
        for _ in 0..(i * elements_per_shard) {
            random_pair(&mut rng);
        }
        rngs.push(rng);
    }

    rngs
}

#[inline(never)]
fn nosync_writes<T: BenchDatabase + Send + Sync>(
    connection: &T::C<'_>,
    rng: &mut fastrand::Rng,
) -> ResultType {
    let start = Instant::now();
    {
        for _ in 0..NOSYNC_WRITES {
            let mut txn = connection.write_transaction();
            let mut inserter = txn.get_inserter();
            let (key, value) = random_pair(rng);
            inserter.insert(&key, &value).unwrap();
            drop(inserter);
            txn.commit().unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} individual items in {}ms, with nosync",
        T::db_type_name(),
        NOSYNC_WRITES,
        duration.as_millis()
    );

    ResultType::Duration(duration)
}

pub fn benchmark<T: BenchDatabase + Send + Sync>(
    mut db: T,
    path: &Path,
) -> Vec<(String, ResultType)> {
    let mut rng = make_rng();
    let mut results = Vec::new();
    let mut connection = db.connect();

    let start = Instant::now();
    let mut txn = connection.write_transaction();
    let mut inserter = txn.get_inserter();
    {
        for _ in 0..BULK_ELEMENTS {
            let (key, value) = random_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
        }
    }
    drop(inserter);
    txn.commit().unwrap();

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Bulk loaded {} items in {}ms",
        T::db_type_name(),
        BULK_ELEMENTS,
        duration.as_millis()
    );
    results.push(("bulk load".to_string(), ResultType::Duration(duration)));

    let start = Instant::now();
    {
        for _ in 0..INDIVIDUAL_WRITES {
            let mut txn = connection.write_transaction();
            let mut inserter = txn.get_inserter();
            let (key, value) = random_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
            drop(inserter);
            txn.commit().unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} individual items in {}ms",
        T::db_type_name(),
        INDIVIDUAL_WRITES,
        duration.as_millis()
    );
    results.push((
        "individual writes".to_string(),
        ResultType::Duration(duration),
    ));

    let start = Instant::now();
    {
        for _ in 0..BATCH_WRITES {
            let mut txn = connection.write_transaction();
            let mut inserter = txn.get_inserter();
            for _ in 0..BATCH_SIZE {
                let (key, value) = random_pair(&mut rng);
                inserter.insert(&key, &value).unwrap();
            }
            drop(inserter);
            txn.commit().unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} batches of {} items in {}ms",
        T::db_type_name(),
        BATCH_WRITES,
        BATCH_SIZE,
        duration.as_millis()
    );
    results.push(("batch writes".to_string(), ResultType::Duration(duration)));

    if connection.set_sync(false) {
        let result = nosync_writes::<T>(&connection, &mut rng);
        results.push(("nosync writes".to_string(), result));
    } else {
        // Still perform the writes to make sure that future benchmarks aren't skewed
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        for _ in 0..NOSYNC_WRITES {
            let (key, value) = random_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
        }
        drop(inserter);
        txn.commit().unwrap();
        results.push(("nosync writes".to_string(), ResultType::NA));
    }
    connection.set_sync(true);

    let elements = BULK_ELEMENTS + INDIVIDUAL_WRITES + BATCH_SIZE * BATCH_WRITES + NOSYNC_WRITES;
    let txn = connection.read_transaction();
    {
        {
            let start = Instant::now();
            let len = txn.get_reader().len();
            assert_eq!(len, elements as u64);
            let end = Instant::now();
            let duration = end - start;
            println!("{}: len() in {}ms", T::db_type_name(), duration.as_millis());
            results.push(("len()".to_string(), ResultType::Duration(duration)));
        }

        for _ in 0..READ_ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            let reader = txn.get_reader();
            for _ in 0..NUM_READS {
                let (key, value) = random_pair(&mut rng);
                let result = reader.get(&key).unwrap();
                checksum += result.as_ref()[0] as u64;
                expected_checksum += value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random read {} items in {}ms",
                T::db_type_name(),
                NUM_READS,
                duration.as_millis()
            );
            results.push(("random reads".to_string(), ResultType::Duration(duration)));
        }

        for _ in 0..SCAN_ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            let reader = txn.get_reader();
            let mut value_sum = 0;
            for _ in 0..NUM_SCANS {
                let (key, _value) = random_pair(&mut rng);
                let mut iter = reader.range_from(&key);
                for _ in 0..SCAN_LEN {
                    if let Some((_, value)) = iter.next() {
                        value_sum += value.as_ref()[0];
                    } else {
                        break;
                    }
                }
            }
            assert!(value_sum > 0);
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random range read {} x {} elements in {}ms",
                T::db_type_name(),
                NUM_SCANS,
                SCAN_LEN,
                duration.as_millis()
            );
            results.push((
                "random range reads".to_string(),
                ResultType::Duration(duration),
            ));
        }
    }
    drop(txn);

    for num_threads in [4, 8, 16, 32] {
        let barrier = Arc::new(std::sync::Barrier::new(num_threads));
        let mut rngs = make_rng_shards(num_threads, elements);
        let start = Instant::now();

        thread::scope(|s| {
            for _ in 0..num_threads {
                let barrier = barrier.clone();
                let connection = db.connect();
                let rng = rngs.pop().unwrap();
                s.spawn(move || {
                    barrier.wait();
                    let txn = connection.read_transaction();
                    let mut checksum = 0u64;
                    let mut expected_checksum = 0u64;
                    let reader = txn.get_reader();
                    let mut rng = rng.clone();
                    for _ in 0..(elements / num_threads) {
                        let (key, value) = random_pair(&mut rng);
                        let result = reader.get(&key).unwrap();
                        checksum += result.as_ref()[0] as u64;
                        expected_checksum += value[0] as u64;
                    }
                    assert_eq!(checksum, expected_checksum);
                });
            }
        });

        let end = Instant::now();
        let duration = end - start;
        println!(
            "{}: Random read ({} threads) {} items in {}ms",
            T::db_type_name(),
            num_threads,
            elements,
            duration.as_millis()
        );
        results.push((
            format!("random reads ({num_threads} threads)"),
            ResultType::Duration(duration),
        ));
    }

    let start = Instant::now();
    let deletes = elements / 2;
    {
        let mut rng = make_rng();
        let mut txn = connection.write_transaction();
        let mut inserter = txn.get_inserter();
        for _ in 0..deletes {
            let (key, _value) = random_pair(&mut rng);
            inserter.remove(&key).unwrap();
        }
        drop(inserter);
        txn.commit().unwrap();
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Removed {} items in {}ms",
        T::db_type_name(),
        deletes,
        duration.as_millis()
    );
    results.push(("removals".to_string(), ResultType::Duration(duration)));

    let uncompacted_size = database_size(path);
    results.push((
        "uncompacted size".to_string(),
        ResultType::SizeInBytes(uncompacted_size),
    ));
    let start = Instant::now();
    drop(connection);
    if db.compact() {
        let end = Instant::now();
        let duration = end - start;
        println!(
            "{}: Compacted in {}ms",
            T::db_type_name(),
            duration.as_millis()
        );
        {
            let connection = db.connect();
            let mut txn = connection.write_transaction();
            let mut inserter = txn.get_inserter();
            let (key, value) = random_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
            drop(inserter);
            txn.commit().unwrap();
        }
        let compacted_size = database_size(path);
        results.push((
            "compacted size".to_string(),
            ResultType::SizeInBytes(compacted_size),
        ));
    } else {
        results.push(("compacted size".to_string(), ResultType::NA));
    }

    results
}

fn database_size(path: &Path) -> u64 {
    let mut size = 0u64;
    for result in walkdir::WalkDir::new(path) {
        let entry = result.unwrap();
        size += entry.metadata().unwrap().len();
    }
    size
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResultType {
    Duration(Duration),
    SizeInBytes(u64),
    NA,
}

impl std::fmt::Display for ResultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use byte_unit::{Byte, UnitType};

        match self {
            ResultType::NA => write!(f, "N/A"),
            ResultType::Duration(d) => write!(f, "{:?}ms", d.as_millis()),
            ResultType::SizeInBytes(s) => {
                let b = Byte::from_u64(*s).get_appropriate_unit(UnitType::Binary);
                write!(f, "{b:.2}")
            }
        }
    }
}

pub trait BenchDatabase {
    type C<'db>: BenchDatabaseConnection
    where
        Self: 'db;

    fn db_type_name() -> &'static str;

    fn connect(&self) -> Self::C<'_>;

    // Returns a boolean indicating whether compaction is supported
    fn compact(&mut self) -> bool {
        false
    }
}

pub trait BenchDatabaseConnection: Send {
    type W<'db>: BenchWriteTransaction
    where
        Self: 'db;
    type R<'db>: BenchReadTransaction
    where
        Self: 'db;

    // Returns a boolean indicating whether the database supports changing the synchronization mode
    fn set_sync(&mut self, _sync: bool) -> bool {
        false
    }

    fn write_transaction(&self) -> Self::W<'_>;

    fn read_transaction(&self) -> Self::R<'_>;
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

impl BenchDatabase for RedbBenchDatabase<'_> {
    type C<'db>
        = RedbBenchDatabaseConnection<'db>
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "redb"
    }

    fn connect(&self) -> Self::C<'_> {
        RedbBenchDatabaseConnection {
            db: self.db,
            sync: false,
        }
    }

    fn compact(&mut self) -> bool {
        self.db.compact().unwrap();
        true
    }
}

pub struct RedbBenchDatabaseConnection<'a> {
    db: &'a redb::Database,
    sync: bool,
}

impl BenchDatabaseConnection for RedbBenchDatabaseConnection<'_> {
    type W<'db>
        = RedbBenchWriteTransaction
    where
        Self: 'db;
    type R<'db>
        = RedbBenchReadTransaction
    where
        Self: 'db;

    fn set_sync(&mut self, sync: bool) -> bool {
        self.sync = sync;
        true
    }

    fn write_transaction(&self) -> Self::W<'_> {
        let mut txn = self.db.begin_write().unwrap();
        if !self.sync {
            txn.set_durability(Durability::None).unwrap();
        }
        RedbBenchWriteTransaction { txn }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let txn = self.db.begin_read().unwrap();
        RedbBenchReadTransaction { txn }
    }
}

pub struct RedbBenchReadTransaction {
    txn: redb::ReadTransaction,
}

impl BenchReadTransaction for RedbBenchReadTransaction {
    type T<'txn>
        = RedbBenchReader
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        let table = self.txn.open_table(X).unwrap();
        RedbBenchReader { table }
    }
}

pub struct RedbBenchReader {
    table: redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
}

impl BenchReader for RedbBenchReader {
    type Output<'out>
        = RedbAccessGuard<'out>
    where
        Self: 'out;
    type Iterator<'out>
        = RedbBenchIterator<'out>
    where
        Self: 'out;

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
    type Output<'a>
        = RedbAccessGuard<'a>
    where
        Self: 'a;

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

impl AsRef<[u8]> for RedbAccessGuard<'_> {
    fn as_ref(&self) -> &[u8] {
        self.inner.value()
    }
}

pub struct RedbBenchWriteTransaction {
    txn: redb::WriteTransaction,
}

impl BenchWriteTransaction for RedbBenchWriteTransaction {
    type W<'txn>
        = RedbBenchInserter<'txn>
    where
        Self: 'txn;

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

impl BenchDatabase for SledBenchDatabase<'_> {
    type C<'db>
        = SledBenchDatabaseConnection<'db>
    where
        Self: 'db;

    fn db_type_name() -> &'static str {
        "sled"
    }

    fn connect(&self) -> Self::C<'_> {
        SledBenchDatabaseConnection {
            db: self.db,
            db_dir: self.db_dir,
            sync: true,
        }
    }
}

pub struct SledBenchDatabaseConnection<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
    sync: bool,
}

impl BenchDatabaseConnection for SledBenchDatabaseConnection<'_> {
    type W<'db>
        = SledBenchWriteTransaction<'db>
    where
        Self: 'db;
    type R<'db>
        = SledBenchReadTransaction<'db>
    where
        Self: 'db;

    fn set_sync(&mut self, sync: bool) -> bool {
        self.sync = sync;
        true
    }

    fn write_transaction(&self) -> Self::W<'_> {
        SledBenchWriteTransaction {
            db: self.db,
            db_dir: self.db_dir,
            sync: self.sync,
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
    type T<'txn>
        = SledBenchReader<'db>
    where
        Self: 'txn;

    fn get_reader(&self) -> Self::T<'_> {
        SledBenchReader { db: self.db }
    }
}

pub struct SledBenchReader<'db> {
    db: &'db sled::Db,
}

impl BenchReader for SledBenchReader<'_> {
    type Output<'out>
        = sled::IVec
    where
        Self: 'out;
    type Iterator<'out>
        = SledBenchIterator
    where
        Self: 'out;

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
    type Output<'out>
        = sled::IVec
    where
        Self: 'out;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        self.iter.next().map(|x| x.unwrap())
    }
}

pub struct SledBenchWriteTransaction<'a> {
    db: &'a sled::Db,
    db_dir: &'a Path,
    sync: bool,
}

impl BenchWriteTransaction for SledBenchWriteTransaction<'_> {
    type W<'txn>
        = SledBenchInserter<'txn>
    where
        Self: 'txn;

    fn get_inserter(&mut self) -> Self::W<'_> {
        SledBenchInserter { db: self.db }
    }

    fn commit(self) -> Result<(), ()> {
        if !self.sync {
            return Ok(());
        }
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

impl BenchInserter for SledBenchInserter<'_> {
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

impl BenchInserter for RocksdbBenchInserter<'_, '_> {
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
    db: &'a mut fjall::TxKeyspace,
}

impl<'a> FjallBenchDatabase<'a> {
    #[allow(dead_code)]
    pub fn new(db: &'a mut fjall::TxKeyspace) -> Self {
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
    db: &'a fjall::TxKeyspace,
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
        let part = self.db.open_partition("test", Default::default()).unwrap();
        let txn = self.db.write_tx();
        FjallBenchWriteTransaction {
            txn,
            part,
            keyspace: self.db,
            sync: self.sync,
        }
    }

    fn read_transaction(&self) -> Self::R<'_> {
        let part = self.db.open_partition("test", Default::default()).unwrap();
        let txn = self.db.read_tx();
        FjallBenchReadTransaction { txn, part }
    }
}

pub struct FjallBenchReadTransaction {
    part: fjall::TxPartitionHandle,
    txn: fjall::ReadTransaction,
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
    part: &'a fjall::TxPartitionHandle,
    txn: &'a fjall::ReadTransaction,
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

    fn get<'a>(&'a self, key: &[u8]) -> Option<Self::Output<'a>> {
        self.txn.get(self.part, key).unwrap()
    }

    fn range_from<'a>(&'a self, key: &'a [u8]) -> Self::Iterator<'a> {
        let iter = self.txn.range(self.part, key..);
        FjallBenchIterator {
            iter: Box::new(iter),
        }
    }

    fn len(&self) -> u64 {
        self.txn.len(self.part).unwrap().try_into().unwrap()
    }
}

pub struct FjallBenchIterator {
    iter: Box<(dyn DoubleEndedIterator<Item = fjall::Result<fjall::KvPair>> + 'static)>,
}

impl BenchIterator for FjallBenchIterator {
    type Output<'a>
        = fjall::Slice
    where
        Self: 'a;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        self.iter.next().map(|item| item.unwrap())
    }
}

pub struct FjallBenchWriteTransaction<'db> {
    keyspace: &'db fjall::TxKeyspace,
    part: fjall::TxPartitionHandle,
    txn: fjall::WriteTransaction<'db>,
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
    part: &'txn fjall::TxPartitionHandle,
    txn: &'txn mut fjall::WriteTransaction<'db>,
}

impl BenchInserter for FjallBenchInserter<'_, '_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.txn.insert(self.part, key, value);
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        self.txn.remove(self.part, key);
        Ok(())
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

impl BenchInserter for SqliteBenchInserter<'_, '_> {
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
        SqliteBenchReader { conn: self.conn }
    }
}

pub struct SqliteBenchReader<'db> {
    conn: &'db Connection,
}

impl BenchReader for SqliteBenchReader<'_> {
    type Output<'out>
        = Vec<u8>
    where
        Self: 'out;
    type Iterator<'out>
        = SqliteBenchIterator
    where
        Self: 'out;

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut stmt = self
            .conn
            .prepare("SELECT value FROM kv WHERE key = ?")
            .unwrap();
        stmt.query_row([key], |row| row.get(0)).ok()
    }

    fn range_from<'a>(&'a self, key: &'a [u8]) -> Self::Iterator<'a> {
        let mut stmt = self
            .conn
            .prepare("SELECT key, value FROM kv WHERE key >= ? ORDER BY key")
            .unwrap();
        let rows = stmt
            .query_map([key], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .unwrap();

        let mut results = Vec::new();
        for row in rows {
            if let Ok(kv) = row {
                results.push(kv);
            }
            // TODO: this is kind of cheating, but I don't feel like refactoring the benchmark
            // to handle rusqlite's Statement & MappedRows lifetimes
            if results.len() > SCAN_LEN {
                break;
            }
        }

        SqliteBenchIterator {
            results: results.into_iter(),
        }
    }

    fn len(&self) -> u64 {
        let mut stmt = self.conn.prepare("SELECT COUNT(*) FROM kv").unwrap();
        stmt.query_row([], |row| row.get::<_, i64>(0)).unwrap() as u64
    }
}

pub struct SqliteBenchIterator {
    results: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
}

impl BenchIterator for SqliteBenchIterator {
    type Output<'out>
        = Vec<u8>
    where
        Self: 'out;

    fn next(&mut self) -> Option<(Self::Output<'_>, Self::Output<'_>)> {
        self.results.next()
    }
}
