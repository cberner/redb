use std::env::current_dir;
use std::fs;
use std::mem::size_of;
use tempfile::{NamedTempFile, TempDir};

mod common;
use common::*;

use redb::WriteStrategy;
use std::time::{Duration, Instant};

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

fn populate<T: BenchDatabase>(db: T) -> Vec<(&'static str, Duration)> {
    let mut rng = make_rng();
    let mut results = Vec::new();

    let start = Instant::now();
    let mut txn = db.write_transaction();
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

    results
}

fn main() {
    let redb_latency_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        let mut results = {
            let db = unsafe {
                redb::Database::builder()
                    .set_write_strategy(WriteStrategy::Checksum)
                    .create(tmpfile.path(), 4096 * 1024 * 1024)
                    .unwrap()
            };
            let table = RedbBenchDatabase::new(&db);
            populate(table)
        };

        let start = Instant::now();

        unsafe { redb::Database::open(tmpfile.path()).unwrap() };

        let duration = start.elapsed();

        results.push(("open", duration));

        results
    };

    let redb_throughput_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        let mut results = {
            let db = unsafe {
                redb::Database::builder()
                    .set_write_strategy(WriteStrategy::TwoPhase)
                    .create(tmpfile.path(), 4096 * 1024 * 1024)
                    .unwrap()
            };
            let table = RedbBenchDatabase::new(&db);
            populate(table)
        };

        let start = Instant::now();

        unsafe { redb::Database::open(tmpfile.path()).unwrap() };

        let duration = start.elapsed();

        results.push(("open", duration));

        results
    };

    let lmdb_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(current_dir().unwrap()).unwrap();
        let mut results = {
            let env = lmdb::Environment::new().open(tmpfile.path()).unwrap();
            env.set_map_size(4096 * 1024 * 1024).unwrap();
            let table = LmdbRkvBenchDatabase::new(&env);
            populate(table)
        };

        let start = Instant::now();

        lmdb::Environment::new().open(tmpfile.path()).unwrap();

        let duration = start.elapsed();

        results.push(("open", duration));

        results
    };

    let mut rows = Vec::new();

    for (benchmark, _duration) in &redb_latency_results {
        rows.push(vec![benchmark.to_string()]);
    }

    for results in [redb_latency_results, redb_throughput_results, lmdb_results] {
        for (i, (_benchmark, duration)) in results.iter().enumerate() {
            rows[i].push(format!("{}ms", duration.as_millis()));
        }
    }

    let mut table = comfy_table::Table::new();
    table.set_width(100);
    table.set_header(["", "redb (1PC+C)", "redb (2PC)", "lmdb"]);
    for row in rows {
        table.add_row(row);
    }

    println!();
    println!("{table}");
}
