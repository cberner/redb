use std::env::current_dir;
use std::path::Path;
use std::sync::Arc;
use std::{fs, process, thread};
use tempfile::{NamedTempFile, TempDir};

mod common;
use common::*;

use std::time::{Duration, Instant};

const READ_ITERATIONS: usize = 2;
const BULK_ELEMENTS: usize = 1_000_000;
const INDIVIDUAL_WRITES: usize = 1_000;
const BATCH_WRITES: usize = 100;
const BATCH_SIZE: usize = 1000;
const SCAN_ITERATIONS: usize = 2;
const NUM_READS: usize = 1_000_000;
const NUM_SCANS: usize = 500_000;
const SCAN_LEN: usize = 10;
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const RNG_SEED: u64 = 3;

const CACHE_SIZE: usize = 4 * 1_024 * 1_024 * 1_024; // 4GB

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

fn benchmark<T: BenchDatabase + Send + Sync>(db: T, path: &Path) -> Vec<(String, ResultType)> {
    let mut rng = make_rng();
    let mut results = Vec::new();
    let mut db = Arc::new(db);

    let start = Instant::now();
    let mut txn = db.write_transaction();
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
            let mut txn = db.write_transaction();
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
            let mut txn = db.write_transaction();
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

    let elements = BULK_ELEMENTS + INDIVIDUAL_WRITES + BATCH_SIZE * BATCH_WRITES;
    let txn = db.read_transaction();
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
                let db2 = db.clone();
                let rng = rngs.pop().unwrap();
                s.spawn(move || {
                    barrier.wait();
                    let txn = db2.read_transaction();
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
        let mut txn = db.write_transaction();
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
    if Arc::get_mut(&mut db).unwrap().compact() {
        let end = Instant::now();
        let duration = end - start;
        println!(
            "{}: Compacted in {}ms",
            T::db_type_name(),
            duration.as_millis()
        );
        {
            let mut txn = db.write_transaction();
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
enum ResultType {
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

fn main() {
    let _ = env_logger::try_init();
    let tmpdir = current_dir().unwrap().join(".benchmark");
    fs::create_dir(&tmpdir).unwrap();

    let tmpdir2 = tmpdir.clone();
    ctrlc::set_handler(move || {
        fs::remove_dir_all(&tmpdir2).unwrap();
        process::exit(1);
    })
    .unwrap();

    let redb_latency_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(&tmpdir).unwrap();
        let mut db = redb::Database::builder()
            .set_cache_size(CACHE_SIZE)
            .create(tmpfile.path())
            .unwrap();
        let table = RedbBenchDatabase::new(&mut db);
        benchmark(table, tmpfile.path())
    };

    let lmdb_results = {
        let tempdir: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(4096 * 1024 * 1024)
                .open(tempdir.path())
                .unwrap()
        };
        let table = HeedBenchDatabase::new(env);
        benchmark(table, tempdir.path())
    };

    let rocksdb_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();

        let mut bb = rocksdb::BlockBasedOptions::default();
        bb.set_block_cache(&rocksdb::Cache::new_lru_cache(CACHE_SIZE));
        bb.set_bloom_filter(10.0, false);

        let mut opts = rocksdb::Options::default();
        opts.set_block_based_table_factory(&bb);
        opts.create_if_missing(true);
        opts.increase_parallelism(
            std::thread::available_parallelism().map_or(1, |n| n.get()) as i32
        );

        let db = rocksdb::OptimisticTransactionDB::open(&opts, tmpfile.path()).unwrap();
        let table = RocksdbBenchDatabase::new(&db);
        benchmark(table, tmpfile.path())
    };

    let sled_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();

        let db = sled::Config::new()
            .path(tmpfile.path())
            .cache_capacity(CACHE_SIZE as u64)
            .open()
            .unwrap();

        let table = SledBenchDatabase::new(&db, tmpfile.path());
        benchmark(table, tmpfile.path())
    };

    let sanakirja_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(&tmpdir).unwrap();
        fs::remove_file(tmpfile.path()).unwrap();
        let db = sanakirja::Env::new(tmpfile.path(), 4096 * 1024 * 1024, 2).unwrap();
        let table = SanakirjaBenchDatabase::new(&db, &tmpdir);
        benchmark(table, tmpfile.path())
    };

    let fjall_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();

        let mut db = fjall::Config::new(tmpfile.path())
            .block_cache(Arc::new(fjall::BlockCache::with_capacity_bytes(
                CACHE_SIZE.try_into().unwrap(),
            )))
            .open_transactional()
            .unwrap();

        let table = FjallBenchDatabase::new(&mut db);
        benchmark(table, tmpfile.path())
    };

    let canopydb_results = {
        let tmpdir = tempfile::tempdir_in(&tmpdir).unwrap();
        let mut env_opts = canopydb::EnvOptions::new(tmpdir.path());
        env_opts.page_cache_size = CACHE_SIZE;
        let db = canopydb::Database::with_options(env_opts, Default::default()).unwrap();
        let db_bench = CanopydbBenchDatabase::new(&db);
        benchmark(db_bench, tmpdir.path())
    };

    fs::remove_dir_all(&tmpdir).unwrap();

    let mut rows = Vec::new();

    for (benchmark, _duration) in &redb_latency_results {
        rows.push(vec![benchmark.to_string()]);
    }

    let results = [
        redb_latency_results,
        lmdb_results,
        rocksdb_results,
        sled_results,
        sanakirja_results,
        fjall_results,
        canopydb_results,
    ];

    let mut identified_smallests = vec![vec![false; results.len()]; rows.len()];
    for (i, identified_smallests_row) in identified_smallests.iter_mut().enumerate() {
        let mut smallest = None;
        for (j, _) in identified_smallests_row.iter().enumerate() {
            let (_, rt) = &results[j][i];
            smallest = match smallest {
                Some((_, prev)) if rt < prev => Some((j, rt)),
                Some((pi, prev)) => Some((pi, prev)),
                None => Some((j, rt)),
            };
        }
        let (j, _rt) = smallest.unwrap();
        identified_smallests_row[j] = true;
    }

    for (j, results) in results.iter().enumerate() {
        for (i, (_benchmark, result_type)) in results.iter().enumerate() {
            rows[i].push(if identified_smallests[i][j] {
                format!("**{result_type}**")
            } else {
                result_type.to_string()
            });
        }
    }

    let mut table = comfy_table::Table::new();
    table.load_preset(comfy_table::presets::ASCII_MARKDOWN);
    table.set_width(100);
    table.set_header([
        "",
        "redb",
        "lmdb",
        "rocksdb",
        "sled",
        "sanakirja",
        "fjall",
        "canopydb",
    ]);
    for row in rows {
        table.add_row(row);
    }

    println!();
    println!("{table}");
}
