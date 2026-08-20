use tempfile::{NamedTempFile, TempDir};

use redb_bench::benchmark_dir;

mod common;
use common::*;
use redb_bench::*;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::time::Instant;

const ELEMENTS: usize = 1_000_000;

/// Returns pairs of key, value
fn random_data(count: usize) -> Vec<(u32, u64)> {
    let mut rng = StdRng::seed_from_u64(0);
    let mut pairs = vec![];
    for _ in 0..count {
        pairs.push(rng.random());
    }
    pairs
}

fn benchmark<T: BenchDatabase>(db: T) -> Vec<(String, ResultType)> {
    let mut results = Vec::new();
    let pairs = random_data(1_000_000);
    let mut written = 0;

    let start = Instant::now();
    let connection = db.connect();
    let mut txn = connection.write_transaction();
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
    let result = ResultType::keys(ELEMENTS, duration);
    println!(
        "{}: Bulk loaded {} (u32, u64) pairs in {}ms ({})",
        T::db_type_name(),
        ELEMENTS,
        duration.as_millis(),
        result.with_unit()
    );
    results.push(("bulk load".to_string(), result));

    results
}

fn main() {
    let _ = env_logger::try_init();

    let redb_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(benchmark_dir()).unwrap();
        let mut db = redb::Database::create(tmpfile.path()).unwrap();
        let table = RedbBenchDatabase::new(&mut db);
        benchmark(table)
    };

    let lmdb_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(benchmark_dir()).unwrap();
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(10 * 4096 * 1024 * 1024)
                .open(tmpfile.path())
                .unwrap()
        };
        let table = HeedBenchDatabase::new(env);
        benchmark(table)
    };

    let rocksdb_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(benchmark_dir()).unwrap();

        let mut bb = rocksdb::BlockBasedOptions::default();
        bb.set_block_cache(&rocksdb::Cache::new_lru_cache(4 * 1_024 * 1_024 * 1_024));
        bb.set_bloom_filter(10.0, false);

        let mut opts = rocksdb::Options::default();
        opts.set_block_based_table_factory(&bb);
        opts.create_if_missing(true);
        opts.increase_parallelism(
            std::thread::available_parallelism().map_or(1, |n| n.get()) as i32
        );

        let db = rocksdb::OptimisticTransactionDB::open(&opts, tmpfile.path()).unwrap();
        let table = RocksdbBenchDatabase::new(&db);
        benchmark(table)
    };

    print_results_table(&[
        ("redb", redb_results),
        ("lmdb", lmdb_results),
        ("rocksdb", rocksdb_results),
    ]);
}
