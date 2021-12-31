use tempfile::{NamedTempFile, TempDir};

mod common;
use common::*;

use rand::prelude::SliceRandom;
use rand::Rng;
use std::time::SystemTime;

const ITERATIONS: usize = 3;
const ELEMENTS: usize = 100_000;

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
    let pairs = gen_data(1000, 16, 1500);
    let mut written = 0;

    let start = SystemTime::now();
    let txn = db.write_transaction();
    let mut inserter = txn.get_inserter();
    {
        for _ in 0..ELEMENTS {
            let (key, value) = &pairs[written % pairs.len()];
            let mut mut_key = key.clone();
            mut_key.extend_from_slice(&written.to_be_bytes());
            inserter.insert(&mut_key, value).unwrap();
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
            let (key, value) = &pairs[written % pairs.len()];
            let mut mut_key = key.clone();
            mut_key.extend_from_slice(&written.to_be_bytes());
            inserter.insert(&mut_key, value).unwrap();
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
                let (key, value) = &pairs[written % pairs.len()];
                let mut mut_key = key.clone();
                mut_key.extend_from_slice(&written.to_be_bytes());
                inserter.insert(&mut_key, value).unwrap();
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
                let (key, value) = &pairs[*i % pairs.len()];
                let mut mut_key = key.clone();
                mut_key.extend_from_slice(&i.to_be_bytes());
                let result = txn.get(&mut_key).unwrap();
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
                let (key, _) = &pairs[*i % pairs.len()];
                let mut mut_key = key.clone();
                mut_key.extend_from_slice(&i.to_be_bytes());
                txn.exists_after(&mut_key);
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
            let (key, value) = &pairs[i % pairs.len()];
            let mut mut_key = key.clone();
            mut_key.extend_from_slice(&i.to_be_bytes());
            inserter.insert(&mut_key, value).unwrap();
        }
        drop(inserter);
        txn.commit().unwrap();
    }

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "{}: Removed {} individual items in {}ms",
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
        let db = unsafe { redb::Database::open(tmpfile.path(), 4096 * 1024 * 1024).unwrap() };
        let table = RedbBenchDatabase::new(&db);
        benchmark(table);
    }
    {
        let tmpfile: TempDir = tempfile::tempdir().unwrap();
        let db = sled::Config::new().path(tmpfile.path()).open().unwrap();
        let table = SledBenchDatabase::new(&db);
        benchmark(table);
    }
}
