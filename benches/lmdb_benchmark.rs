use tempfile::{NamedTempFile, TempDir};

mod common;
use common::*;

use rand::prelude::SliceRandom;
use rand::Rng;
use std::time::SystemTime;

const ITERATIONS: usize = 3;
const ELEMENTS: usize = 1_000_000;

/// Returns pairs of key, value
fn gen_data(count: usize, key_size: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut pairs = vec![];

    for _ in 0..count {
        let key: Vec<u8> = (0..key_size).map(|_| rand::thread_rng().gen()).collect();
        let value: Vec<u8> = (0..value_size).map(|_| rand::thread_rng().gen()).collect();
        pairs.push((key, value));
    }

    pairs
}

fn benchmark<T: BenchDatabase>(mut db: T) {
    let mut pairs = gen_data(1_000_000, 24, 150);
    let mut written = 0;

    let start = SystemTime::now();
    let txn = db.write_transaction();
    let mut inserter = txn.get_inserter();
    {
        for _ in 0..ELEMENTS {
            let len = pairs.len();
            let (key, value) = &mut pairs[written % len];
            key[16..].copy_from_slice(&(written as u64).to_be_bytes());
            inserter.insert(&key, value).unwrap();
            written += 1;
        }
    }
    drop(inserter);
    txn.commit().unwrap();

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "{}: Bulk loaded {} items in {}ms",
        T::db_type_name(),
        ELEMENTS,
        duration.as_millis()
    );

    let start = SystemTime::now();
    let writes = 100;
    {
        for _ in 0..writes {
            let txn = db.write_transaction();
            let mut inserter = txn.get_inserter();
            let len = pairs.len();
            let (key, value) = &mut pairs[written % len];
            key[16..].copy_from_slice(&(written as u64).to_be_bytes());
            inserter.insert(&key, value).unwrap();
            drop(inserter);
            txn.commit().unwrap();
            written += 1;
        }
    }

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "{}: Wrote {} individual items in {}ms",
        T::db_type_name(),
        writes,
        duration.as_millis()
    );

    let start = SystemTime::now();
    let batch_size = 1000;
    {
        for _ in 0..writes {
            let txn = db.write_transaction();
            let mut inserter = txn.get_inserter();
            for _ in 0..batch_size {
                let len = pairs.len();
                let (key, value) = &mut pairs[written % len];
                key[16..].copy_from_slice(&(written as u64).to_be_bytes());
                inserter.insert(&key, value).unwrap();
                written += 1;
            }
            drop(inserter);
            txn.commit().unwrap();
        }
    }

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "{}: Wrote {} x {} items in {}ms",
        T::db_type_name(),
        writes,
        batch_size,
        duration.as_millis()
    );

    let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
    key_order.shuffle(&mut rand::thread_rng());

    let txn = db.read_transaction();
    {
        for _ in 0..ITERATIONS {
            let start = SystemTime::now();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            for i in &key_order {
                let len = pairs.len();
                let (key, value) = &mut pairs[i % len];
                key[16..].copy_from_slice(&(*i as u64).to_be_bytes());
                let result = txn.get(&key).unwrap();
                checksum += result.as_ref()[0] as u64;
                expected_checksum += value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
            let end = SystemTime::now();
            let duration = end.duration_since(start).unwrap();
            println!(
                "{}: Random read {} items in {}ms",
                T::db_type_name(),
                ELEMENTS,
                duration.as_millis()
            );
        }

        for _ in 0..ITERATIONS {
            let start = SystemTime::now();
            for i in &key_order {
                let len = pairs.len();
                let (key, _) = &mut pairs[i % len];
                key[16..].copy_from_slice(&(*i as u64).to_be_bytes());
                txn.exists_after(&key);
            }
            let end = SystemTime::now();
            let duration = end.duration_since(start).unwrap();
            println!(
                "{}: Random range read {} starts in {}ms",
                T::db_type_name(),
                ELEMENTS,
                duration.as_millis()
            );
        }
    }

    let start = SystemTime::now();
    let deletes = key_order.len() / 2;
    {
        let txn = db.write_transaction();
        let mut inserter = txn.get_inserter();
        for i in 0..deletes {
            let len = pairs.len();
            let (key, _) = &mut pairs[i % len];
            key[16..].copy_from_slice(&(i as u64).to_be_bytes());
            inserter.remove(&key).unwrap();
        }
        drop(inserter);
        txn.commit().unwrap();
    }

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "{}: Removed {} items in {}ms",
        T::db_type_name(),
        deletes,
        duration.as_millis()
    );
}

fn main() {
    {
        let tmpfile: TempDir = tempfile::tempdir().unwrap();
        let env = lmdb::Environment::new().open(tmpfile.path()).unwrap();
        env.set_map_size(4096 * 1024 * 1024).unwrap();
        let table = LmdbRkvBenchDatabase::new(&env);
        benchmark(table);
    }
    {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { redb::Database::create(tmpfile.path(), 4096 * 1024 * 1024).unwrap() };
        let table = RedbBenchDatabase::new(&db);
        benchmark(table);
    }
    {
        let tmpfile: TempDir = tempfile::tempdir().unwrap();
        let db = sled::Config::new().path(tmpfile.path()).open().unwrap();
        let table = SledBenchDatabase::new(&db, tmpfile.path());
        benchmark(table);
    }
}
