use tempfile::{NamedTempFile, TempDir};

mod common;
use common::*;

use rand::prelude::SliceRandom;
use rand::Rng;
use std::path::Path;
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

fn lmdb_zero_bench(path: &str) {
    let env = unsafe {
        lmdb_zero::EnvBuilder::new()
            .unwrap()
            .open(path, lmdb_zero::open::Flags::empty(), 0o600)
            .unwrap()
    };
    unsafe {
        env.set_mapsize(4096 * 1024 * 1024).unwrap();
    }

    let pairs = gen_data(1000, 16, 2000);

    let db =
        lmdb_zero::Database::open(&env, None, &lmdb_zero::DatabaseOptions::defaults()).unwrap();
    {
        let start = SystemTime::now();
        let txn = lmdb_zero::WriteTransaction::new(&env).unwrap();
        {
            let mut access = txn.access();
            for i in 0..ELEMENTS {
                let (key, value) = &pairs[i % pairs.len()];
                let mut mut_key = key.clone();
                mut_key.extend_from_slice(&i.to_be_bytes());
                access
                    .put(&db, &mut_key, value, lmdb_zero::put::Flags::empty())
                    .unwrap();
            }
        }
        txn.commit().unwrap();

        let end = SystemTime::now();
        let duration = end.duration_since(start).unwrap();
        println!(
            "lmdb-zero: Loaded {} items in {}ms",
            ELEMENTS,
            duration.as_millis()
        );

        let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
        key_order.shuffle(&mut rand::thread_rng());

        let txn = lmdb_zero::ReadTransaction::new(&env).unwrap();
        {
            for _ in 0..ITERATIONS {
                let start = SystemTime::now();
                let access = txn.access();
                let mut checksum = 0u64;
                let mut expected_checksum = 0u64;
                for i in &key_order {
                    let (key, value) = &pairs[*i % pairs.len()];
                    let mut mut_key = key.clone();
                    mut_key.extend_from_slice(&i.to_be_bytes());
                    let result: &[u8] = access.get(&db, &mut_key).unwrap();
                    checksum += result[0] as u64;
                    expected_checksum += value[0] as u64;
                }
                assert_eq!(checksum, expected_checksum);
                let end = SystemTime::now();
                let duration = end.duration_since(start).unwrap();
                println!(
                    "lmdb-zero: Random read {} items in {}ms",
                    ELEMENTS,
                    duration.as_millis()
                );
            }
        }
    }
}

fn lmdb_rkv_bench(path: &Path) {
    use lmdb::Transaction;
    let env = lmdb::Environment::new().open(path).unwrap();
    env.set_map_size(4096 * 1024 * 1024).unwrap();

    let pairs = gen_data(1000, 16, 2000);

    let db = env.open_db(None).unwrap();
    let start = SystemTime::now();
    let mut txn = env.begin_rw_txn().unwrap();
    {
        for i in 0..ELEMENTS {
            let (key, value) = &pairs[i % pairs.len()];
            let mut mut_key = key.clone();
            mut_key.extend_from_slice(&i.to_be_bytes());
            txn.put(db, &mut_key, value, lmdb::WriteFlags::empty())
                .unwrap();
        }
    }
    txn.commit().unwrap();

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "lmdb-rkv: Loaded {} items in {}ms",
        ELEMENTS,
        duration.as_millis()
    );

    let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
    key_order.shuffle(&mut rand::thread_rng());

    let txn = env.begin_ro_txn().unwrap();
    {
        for _ in 0..ITERATIONS {
            let start = SystemTime::now();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            for i in &key_order {
                let (key, value) = &pairs[*i % pairs.len()];
                let mut mut_key = key.clone();
                mut_key.extend_from_slice(&i.to_be_bytes());
                let result: &[u8] = txn.get(db, &mut_key).unwrap();
                checksum += result[0] as u64;
                expected_checksum += value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
            let end = SystemTime::now();
            let duration = end.duration_since(start).unwrap();
            println!(
                "lmdb-rkv: Random read {} items in {}ms",
                ELEMENTS,
                duration.as_millis()
            );
        }
    }
}

fn benchmark<'a, R: AsRef<[u8]>, T: BenchTable<R> + 'a>(mut db: T) {
    let pairs = gen_data(1000, 16, 2000);

    let start = SystemTime::now();
    let mut txn = db.write_transaction();
    {
        for i in 0..ELEMENTS {
            let (key, value) = &pairs[i % pairs.len()];
            let mut mut_key = key.clone();
            mut_key.extend_from_slice(&i.to_be_bytes());
            txn.insert(&mut_key, value).unwrap();
        }
    }
    txn.commit().unwrap();

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "{}: Loaded {} items in {}ms",
        T::db_type_name(),
        ELEMENTS,
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
    }
}

fn main() {
    {
        let tmpfile: TempDir = tempfile::tempdir().unwrap();
        let path = tmpfile.path().to_str().unwrap();
        lmdb_zero_bench(path);
    }
    {
        let tmpfile: TempDir = tempfile::tempdir().unwrap();
        lmdb_rkv_bench(tmpfile.path());
    }
    {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { redb::Database::open(tmpfile.path()).unwrap() };
        let table = RedbBenchTable::new(&db);
        benchmark(table);
    }
    {
        let tmpfile: TempDir = tempfile::tempdir().unwrap();
        let db = sled::Config::new().path(tmpfile.path()).open().unwrap();
        let table = SledBenchTable::new(&db);
        benchmark(table);
    }
}
