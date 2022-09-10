use std::env::current_dir;
use std::mem::size_of;
use tempfile::{NamedTempFile, TempDir};

mod common;
use common::*;

use redb::{ReadableTable, TableDefinition, WriteStrategy};
use std::time::{Duration, Instant};

const ITERATIONS: usize = 3;
const ELEMENTS: usize = 1_000_000;
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const RNG_SEED: u64 = 3;

fn fill_slice(slice: &mut [u8], rng: &mut fastrand::Rng) {
    let mut i = 0;
    while i + size_of::<u128>() < slice.len() {
        let tmp = rng.u128(..);
        slice[i..(i + size_of::<u128>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u128>()
    }
    if i + size_of::<u64>() < slice.len() {
        let tmp = rng.u64(..);
        slice[i..(i + size_of::<u64>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u64>()
    }
    if i + size_of::<u32>() < slice.len() {
        let tmp = rng.u32(..);
        slice[i..(i + size_of::<u32>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u32>()
    }
    if i + size_of::<u16>() < slice.len() {
        let tmp = rng.u16(..);
        slice[i..(i + size_of::<u16>())].copy_from_slice(&tmp.to_le_bytes());
        i += size_of::<u16>()
    }
    if i + size_of::<u8>() < slice.len() {
        slice[i] = rng.u8(..);
    }
}

/// Returns pairs of key, value
fn gen_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
    let mut key = [0u8; KEY_SIZE];
    fill_slice(&mut key, rng);
    let mut value = vec![0u8; VALUE_SIZE];
    fill_slice(&mut value, rng);

    (key, value)
}

fn make_rng() -> fastrand::Rng {
    fastrand::Rng::with_seed(RNG_SEED)
}

// TODO: merge back into benchmark()
fn benchmark_redb(db: redb::Database) -> Vec<(&'static str, Duration)> {
    let mut rng = make_rng();
    let mut results = Vec::new();

    let table_def: TableDefinition<[u8], [u8]> = TableDefinition::new("x");

    let start = Instant::now();
    let txn = db.begin_write().unwrap();
    let mut inserter = txn.open_table(table_def).unwrap();
    {
        for _ in 0..ELEMENTS {
            let (key, value) = gen_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
        }
    }
    drop(inserter);
    txn.commit().unwrap();

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Bulk loaded {} items in {}ms",
        RedbBenchDatabase::db_type_name(),
        ELEMENTS,
        duration.as_millis()
    );
    results.push(("bulk load", duration));

    let start = Instant::now();
    let writes = 100;
    {
        for _ in 0..writes {
            let txn = db.begin_write().unwrap();
            let mut inserter = txn.open_table(table_def).unwrap();
            let (key, value) = gen_pair(&mut rng);
            inserter.insert(&key, &value).unwrap();
            drop(inserter);
            txn.commit().unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} individual items in {}ms",
        RedbBenchDatabase::db_type_name(),
        writes,
        duration.as_millis()
    );
    results.push(("individual writes", duration));

    let start = Instant::now();
    let batch_size = 1000;
    {
        for _ in 0..writes {
            let txn = db.begin_write().unwrap();
            let mut inserter = txn.open_table(table_def).unwrap();
            for _ in 0..batch_size {
                let (key, value) = gen_pair(&mut rng);
                inserter.insert(&key, &value).unwrap();
            }
            drop(inserter);
            txn.commit().unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} x {} items in {}ms",
        RedbBenchDatabase::db_type_name(),
        writes,
        batch_size,
        duration.as_millis()
    );
    results.push(("batch writes", duration));

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(table_def).unwrap();
    {
        for _ in 0..ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            for _ in 0..ELEMENTS {
                let (key, value) = gen_pair(&mut rng);
                let result = table.get(&key).unwrap().unwrap();
                checksum += result[0] as u64;
                expected_checksum += value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random read {} items in {}ms",
                RedbBenchDatabase::db_type_name(),
                ELEMENTS,
                duration.as_millis()
            );
            results.push(("random reads", duration));
        }

        for _ in 0..ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            for _ in 0..ELEMENTS {
                let (key, _value) = gen_pair(&mut rng);
                table.range(key.as_slice()..).unwrap();
            }
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random range read {} starts in {}ms",
                RedbBenchDatabase::db_type_name(),
                ELEMENTS,
                duration.as_millis()
            );
            results.push(("random range reads", duration));
        }
    }

    let mut rng = make_rng();
    let start = Instant::now();
    let deletes = ELEMENTS / 2;
    {
        let txn = db.begin_write().unwrap();
        let mut inserter = txn.open_table(table_def).unwrap();
        for _ in 0..deletes {
            let (key, _value) = gen_pair(&mut rng);
            inserter.remove(&key).unwrap();
        }
        drop(inserter);
        txn.commit().unwrap();
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Removed {} items in {}ms",
        RedbBenchDatabase::db_type_name(),
        deletes,
        duration.as_millis()
    );
    results.push(("removals", duration));

    results
}

fn benchmark<T: BenchDatabase>(mut db: T) -> Vec<(&'static str, Duration)> {
    let mut rng = make_rng();
    let mut results = Vec::new();

    let start = Instant::now();
    let txn = db.write_transaction();
    let mut inserter = txn.get_inserter();
    {
        for _ in 0..ELEMENTS {
            let (key, value) = gen_pair(&mut rng);
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
        ELEMENTS,
        duration.as_millis()
    );
    results.push(("bulk load", duration));

    let start = Instant::now();
    let writes = 100;
    {
        for _ in 0..writes {
            let txn = db.write_transaction();
            let mut inserter = txn.get_inserter();
            let (key, value) = gen_pair(&mut rng);
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
        writes,
        duration.as_millis()
    );
    results.push(("individual writes", duration));

    let start = Instant::now();
    let batch_size = 1000;
    {
        for _ in 0..writes {
            let txn = db.write_transaction();
            let mut inserter = txn.get_inserter();
            for _ in 0..batch_size {
                let (key, value) = gen_pair(&mut rng);
                inserter.insert(&key, &value).unwrap();
            }
            drop(inserter);
            txn.commit().unwrap();
        }
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Wrote {} x {} items in {}ms",
        T::db_type_name(),
        writes,
        batch_size,
        duration.as_millis()
    );
    results.push(("batch writes", duration));

    let txn = db.read_transaction();
    {
        for _ in 0..ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            for _ in 0..ELEMENTS {
                let (key, value) = gen_pair(&mut rng);
                let result = txn.get(&key).unwrap();
                checksum += result.as_ref()[0] as u64;
                expected_checksum += value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random read {} items in {}ms",
                T::db_type_name(),
                ELEMENTS,
                duration.as_millis()
            );
            results.push(("random reads", duration));
        }

        for _ in 0..ITERATIONS {
            let mut rng = make_rng();
            let start = Instant::now();
            for _ in 0..ELEMENTS {
                let (key, _value) = gen_pair(&mut rng);
                txn.exists_after(&key);
            }
            let end = Instant::now();
            let duration = end - start;
            println!(
                "{}: Random range read {} starts in {}ms",
                T::db_type_name(),
                ELEMENTS,
                duration.as_millis()
            );
            results.push(("random range reads", duration));
        }
    }

    let start = Instant::now();
    let deletes = ELEMENTS / 2;
    {
        let mut rng = make_rng();
        let txn = db.write_transaction();
        let mut inserter = txn.get_inserter();
        for _ in 0..deletes {
            let (key, _value) = gen_pair(&mut rng);
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
    results.push(("removals", duration));

    results
}

fn main() {
    let redb_latency_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        let db = unsafe {
            redb::Database::builder()
                .set_write_strategy(WriteStrategy::CommitLatency)
                .create(tmpfile.path(), 4096 * 1024 * 1024)
                .unwrap()
        };
        // let table = RedbBenchDatabase::new(&db);
        benchmark_redb(db)
    };

    let redb_throughput_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        let db = unsafe {
            redb::Database::builder()
                .set_write_strategy(WriteStrategy::Throughput)
                .create(tmpfile.path(), 4096 * 1024 * 1024)
                .unwrap()
        };
        // let table = RedbBenchDatabase::new(&db);
        benchmark_redb(db)
    };

    let lmdb_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(current_dir().unwrap()).unwrap();
        let env = lmdb::Environment::new().open(tmpfile.path()).unwrap();
        env.set_map_size(4096 * 1024 * 1024).unwrap();
        let table = LmdbRkvBenchDatabase::new(&env);
        benchmark(table)
    };

    let rocksdb_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(current_dir().unwrap()).unwrap();
        let db = rocksdb::TransactionDB::open_default(tmpfile.path()).unwrap();
        let table = RocksdbBenchDatabase::new(&db);
        benchmark(table)
    };

    let sled_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(current_dir().unwrap()).unwrap();
        let db = sled::Config::new().path(tmpfile.path()).open().unwrap();
        let table = SledBenchDatabase::new(&db, tmpfile.path());
        benchmark(table)
    };

    let mut rows = Vec::new();

    for (benchmark, _duration) in &redb_latency_results {
        rows.push(vec![benchmark.to_string()]);
    }

    for results in [
        redb_latency_results,
        redb_throughput_results,
        lmdb_results,
        rocksdb_results,
        sled_results,
    ] {
        for (i, (_benchmark, duration)) in results.iter().enumerate() {
            rows[i].push(format!("{}ms", duration.as_millis()));
        }
    }

    let mut table = comfy_table::Table::new();
    table.set_table_width(100);
    table.set_header(&["", "redb (1PC+C)", "redb (2PC)", "lmdb", "rocksdb", "sled"]);
    for row in rows {
        table.add_row(row);
    }

    println!();
    println!("{table}");
}
