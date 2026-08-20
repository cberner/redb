use std::{fs, process};
use tempfile::{NamedTempFile, TempDir};

use redb_bench::benchmark_dir;

mod common;
use common::*;
use redb_bench::*;

fn main() {
    let _ = env_logger::try_init();
    let tmpdir = benchmark_dir().join(".benchmark");
    fs::create_dir(&tmpdir).unwrap();

    let tmpdir2 = tmpdir.clone();
    ctrlc::set_handler(move || {
        fs::remove_dir_all(&tmpdir2).unwrap();
        process::exit(1);
    })
    .unwrap();

    let redb_results = {
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

        let cache = rocksdb::Cache::new_lru_cache(CACHE_SIZE);
        let write_buffer = rocksdb::WriteBufferManager::new_write_buffer_manager_with_cache(
            CACHE_SIZE / 2,
            false,
            cache.clone(),
        );

        let mut bb = rocksdb::BlockBasedOptions::default();
        bb.set_block_cache(&cache);
        bb.set_bloom_filter(10.0, false);
        bb.set_cache_index_and_filter_blocks(true);
        bb.set_pin_l0_filter_and_index_blocks_in_cache(false);
        bb.set_pin_top_level_index_and_filter(false);

        let mut opts = rocksdb::Options::default();
        opts.set_block_based_table_factory(&bb);
        opts.set_write_buffer_manager(&write_buffer);
        opts.set_max_write_buffer_size_to_maintain((CACHE_SIZE / 2) as i64);
        opts.create_if_missing(true);
        opts.increase_parallelism(
            std::thread::available_parallelism().map_or(1, |n| n.get()) as i32
        );

        let db = rocksdb::OptimisticTransactionDB::open(&opts, tmpfile.path()).unwrap();
        let table = RocksdbBenchDatabase::new(&db);
        benchmark(table, tmpfile.path())
    };

    let fjall_results = {
        let tmpfile: TempDir = tempfile::tempdir_in(&tmpdir).unwrap();

        let mut db = fjall::SingleWriterTxDatabase::builder(tmpfile.path())
            .cache_size(CACHE_SIZE.try_into().unwrap())
            .open()
            .unwrap();

        let table = FjallBenchDatabase::new(&mut db);
        benchmark(table, tmpfile.path())
    };

    let sqlite_results = {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(&tmpdir).unwrap();
        let table = SqliteBenchDatabase::new(tmpfile.path());
        benchmark(table, tmpfile.path())
    };

    fs::remove_dir_all(&tmpdir).unwrap();

    print_results_table(&[
        ("redb", redb_results),
        ("lmdb", lmdb_results),
        ("rocksdb", rocksdb_results),
        ("fjall", fjall_results),
        ("sqlite", sqlite_results),
    ]);
}
