use std::env::current_dir;
use tempfile::{NamedTempFile, TempDir};

mod common;
use common::*;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redb::{TableDefinition, WriteStrategy};
use std::time::{Duration, Instant};

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

// TODO: merge back into benchmark()
fn benchmark_redb(db: redb::Database) -> Vec<(&'static str, Duration)> {
    let mut results = Vec::new();
    let mut pairs = gen_data(1_000_000, 24, 150);
    let mut written = 0;
    let mut rng = StdRng::seed_from_u64(0);

    let table_def: TableDefinition<[u8], [u8]> = TableDefinition::new("x");

    let mut bigpairs = gen_data(100, 24, 2_000_000);
    let bigelements = 4000;

    let start = Instant::now();
    let txn = db.begin_write().unwrap();
    let mut inserter = txn.open_table(table_def).unwrap();
    {
        for _ in 0..bigelements {
            let len = bigpairs.len();
            let (key, value) = &mut bigpairs[written % len];
            let rand_key: u64 = rng.gen();
            key[16..].copy_from_slice(&(rand_key as u64).to_le_bytes());
            inserter.insert(key, value).unwrap();
            written += 1;
        }
        for _ in 0..ELEMENTS {
            let len = pairs.len();
            let (key, value) = &mut pairs[written % len];
            let rand_key: u64 = rng.gen();
            key[16..].copy_from_slice(&(rand_key as u64).to_le_bytes());
            inserter.insert(key, value).unwrap();
            written += 1;
        }
    }
    drop(inserter);
    txn.commit().unwrap();

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Bulk loaded {} 2MB items and {} small items in {}ms",
        RedbBenchDatabase::db_type_name(),
        bigelements,
        ELEMENTS,
        duration.as_millis()
    );
    results.push(("bulk load (2MB values)", duration));

    results
}

fn benchmark<T: BenchDatabase>(mut db: T) -> Vec<(&'static str, Duration)> {
    let mut results = Vec::new();
    let mut pairs = gen_data(1_000_000, 24, 150);
    let mut written = 0;

    let mut bigpairs = gen_data(100, 24, 2_000_000);
    let bigelements = 4000;

    let start = Instant::now();
    let txn = db.write_transaction();
    let mut inserter = txn.get_inserter();
    {
        for _ in 0..bigelements {
            let len = bigpairs.len();
            let (key, value) = &mut bigpairs[written % len];
            key[16..].copy_from_slice(&(written as u64).to_le_bytes());
            inserter.insert(key, value).unwrap();
            written += 1;
        }
        for _ in 0..ELEMENTS {
            let len = pairs.len();
            let (key, value) = &mut pairs[written % len];
            key[16..].copy_from_slice(&(written as u64).to_le_bytes());
            inserter.insert(key, value).unwrap();
            written += 1;
        }
    }
    drop(inserter);
    txn.commit().unwrap();

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Bulk loaded {} 2MB items and {} small items in {}ms",
        T::db_type_name(),
        bigelements,
        ELEMENTS,
        duration.as_millis()
    );
    results.push(("bulk load (2MB values)", duration));

    results
}

fn main() {
    let redb_latency_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        let db = unsafe {
            redb::Database::builder()
                .set_write_strategy(WriteStrategy::CommitLatency)
                .create(tmpfile.path(), 10 * 4096 * 1024 * 1024)
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
                .create(tmpfile.path(), 10 * 4096 * 1024 * 1024)
                .unwrap()
        };
        // let table = RedbBenchDatabase::new(&db);
        benchmark_redb(db)
    };

    let lmdb_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(current_dir().unwrap()).unwrap();
        let env = lmdb::Environment::new().open(tmpfile.path()).unwrap();
        env.set_map_size(10 * 4096 * 1024 * 1024).unwrap();
        let table = LmdbRkvBenchDatabase::new(&env);
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
        sled_results,
    ] {
        for (i, (_benchmark, duration)) in results.iter().enumerate() {
            rows[i].push(format!("{}ms", duration.as_millis()));
        }
    }

    let mut table = comfy_table::Table::new();
    table.set_table_width(100);
    table.set_header(&[
        "",
        "redb (latency opt.)",
        "redb (throughput opt.)",
        "lmdb",
        "sled",
    ]);
    for row in rows {
        table.add_row(row);
    }

    println!();
    println!("{table}");
}
