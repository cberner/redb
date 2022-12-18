use std::env::current_dir;
use std::fs;
use tempfile::{NamedTempFile, TempDir};

mod common;
use common::*;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::{Duration, Instant};

const ELEMENTS: usize = 1_000_000;

/// Returns pairs of key, value
fn gen_data(count: usize) -> Vec<(u32, u64)> {
    let mut rng = StdRng::seed_from_u64(0);
    let mut pairs = vec![];
    for _ in 0..count {
        pairs.push(rng.gen());
    }
    pairs
}

fn benchmark<T: BenchDatabase>(db: T) -> Vec<(&'static str, Duration)> {
    let mut results = Vec::new();
    let pairs = gen_data(1_000_000);
    let mut written = 0;

    let start = Instant::now();
    let mut txn = db.write_transaction();
    let mut inserter = txn.get_inserter();
    {
        for _ in 0..ELEMENTS {
            let len = pairs.len();
            let (key, value) = pairs[written % len];
            inserter
                .insert(&key.to_le_bytes(), &value.to_le_bytes())
                .unwrap();
            written += 1;
        }
    }
    drop(inserter);
    txn.commit().unwrap();

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{}: Bulk loaded {} (u32, u64) pairs in {}ms",
        T::db_type_name(),
        ELEMENTS,
        duration.as_millis()
    );
    results.push(("bulk load", duration));

    results
}

fn main() {
    let redb_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        let db = redb::Database::create(tmpfile.path()).unwrap();
        let table = RedbBenchDatabase::new(&db);
        benchmark(table)
    };

    let lmdb_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(current_dir().unwrap()).unwrap();
        let env = lmdb::Environment::new().open(tmpfile.path()).unwrap();
        env.set_map_size(10 * 4096 * 1024 * 1024).unwrap();
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

    let sanakirja_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        fs::remove_file(tmpfile.path()).unwrap();
        let db = sanakirja::Env::new(tmpfile.path(), 4096 * 1024 * 1024, 2).unwrap();
        let table = SanakirjaBenchDatabase::new(&db);
        benchmark(table)
    };

    let mut rows = Vec::new();

    for (benchmark, _duration) in &redb_results {
        rows.push(vec![benchmark.to_string()]);
    }

    for results in [
        redb_results,
        lmdb_results,
        rocksdb_results,
        sled_results,
        sanakirja_results,
    ] {
        for (i, (_benchmark, duration)) in results.iter().enumerate() {
            rows[i].push(format!("{}ms", duration.as_millis()));
        }
    }

    let mut table = comfy_table::Table::new();
    table.set_width(100);
    table.set_header(["", "redb", "lmdb", "rocksdb", "sled", "sanakirja"]);
    for row in rows {
        table.add_row(row);
    }

    println!();
    println!("{table}");
}
