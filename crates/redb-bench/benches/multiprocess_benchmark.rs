//! What the multi-process protocol costs, compared to an ordinary single-process `Database`.
//!
//! The "second handle" cases open the same directory twice from this process. File locks belong to
//! the open file description rather than the process, so a second handle takes exactly the same
//! locks, does exactly the same I/O, and keeps its own page cache -- it pays what a second process
//! pays, without the benchmark having to manage one.

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use redb::{Database, MultiProcessDatabase, ReadableDatabase, TableDefinition, WriterMode};
use redb_bench::benchmark_dir;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::{fs, thread};
use tempfile::TempDir;

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

const ELEMENTS: u64 = 500_000;
const VALUE_SIZE: usize = 100;
const BULK_BATCH: u64 = 5_000;
const SMALL_COMMITS: u64 = 300;
const READS: u64 = 200_000;
const READ_SECONDS: u64 = 3;
const RNG_SEED: u64 = 3;

fn value() -> Vec<u8> {
    vec![0xab; VALUE_SIZE]
}

/// The three ways of opening the same database that are being compared
enum Db {
    Single(Database),
    Multi(MultiProcessDatabase),
}

impl Db {
    fn begin_write(&self) -> redb::WriteTransaction {
        match self {
            Db::Single(db) => db.begin_write().unwrap(),
            Db::Multi(db) => db.begin_write().unwrap(),
        }
    }

    fn begin_read(&self) -> redb::ReadTransaction {
        match self {
            Db::Single(db) => db.begin_read().unwrap(),
            Db::Multi(db) => db.begin_read().unwrap(),
        }
    }
}

fn open(kind: Kind, dir: &TempDir) -> Db {
    let path = dir.path().join("db");
    match kind {
        Kind::SingleProcess => Db::Single(Database::create(path).unwrap()),
        Kind::MultiProcessOneWriter => Db::Multi(
            MultiProcessDatabase::builder()
                .set_writer_mode(WriterMode::SingleWriterProcess)
                .create(path)
                .unwrap(),
        ),
        Kind::MultiProcessManyWriters => Db::Multi(
            MultiProcessDatabase::builder()
                .set_writer_mode(WriterMode::MultiWriterProcess)
                .create(path)
                .unwrap(),
        ),
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum Kind {
    SingleProcess,
    MultiProcessOneWriter,
    MultiProcessManyWriters,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::SingleProcess => "Database",
            Kind::MultiProcessOneWriter => "MultiProcess (single writer process)",
            Kind::MultiProcessManyWriters => "MultiProcess (multi writer process)",
        }
    }
}

fn tempdir() -> TempDir {
    TempDir::new_in(benchmark_dir()).unwrap()
}

/// Fills the database, in batches, and returns how long it took
fn bulk_load(db: &Db) -> Duration {
    let value = value();
    let start = Instant::now();
    let mut key = 0;
    while key < ELEMENTS {
        let txn = db.begin_write();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for _ in 0..BULK_BATCH {
                table.insert(&key, value.as_slice()).unwrap();
                key += 1;
            }
        }
        txn.commit().unwrap();
    }
    start.elapsed()
}

fn small_commits(db: &Db) -> Duration {
    let value = value();
    let start = Instant::now();
    for i in 0..SMALL_COMMITS {
        let txn = db.begin_write();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            table.insert(&(i % ELEMENTS), value.as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }
    start.elapsed()
}

/// Random point reads, all within one read transaction
fn reads_in_one_transaction(txn: &redb::ReadTransaction) -> Duration {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    let table = txn.open_table(TABLE).unwrap();
    let start = Instant::now();
    for _ in 0..READS {
        let key = rng.random_range(0..ELEMENTS);
        assert_eq!(
            VALUE_SIZE,
            table.get_owned(key).unwrap().unwrap().value().len()
        );
    }
    start.elapsed()
}

/// Random point reads, each in its own read transaction, which is what pays the cross-process
/// registration cost
fn reads_in_many_transactions<F: Fn() -> redb::ReadTransaction>(begin: F) -> Duration {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    let start = Instant::now();
    for _ in 0..READS {
        let key = rng.random_range(0..ELEMENTS);
        let txn = begin();
        let table = txn.open_table(TABLE).unwrap();
        assert_eq!(
            VALUE_SIZE,
            table.get_owned(key).unwrap().unwrap().value().len()
        );
    }
    start.elapsed()
}

fn rate(count: u64, duration: Duration) -> String {
    let per_second = count as f64 / duration.as_secs_f64();
    format!("{per_second:>12.0}/s")
}

fn main() {
    println!(
        "Filling {ELEMENTS} entries of {VALUE_SIZE} bytes, in batches of {BULK_BATCH}, then \
         {SMALL_COMMITS} single-entry commits\n"
    );
    println!(
        "{:<38} {:>14} {:>16} {:>16}",
        "", "bulk load", "commits", "reads (1 txn)"
    );

    for kind in [
        Kind::SingleProcess,
        Kind::MultiProcessOneWriter,
        Kind::MultiProcessManyWriters,
    ] {
        let dir = tempdir();
        let db = open(kind, &dir);
        let load = bulk_load(&db);
        let commits = small_commits(&db);
        let reads = reads_in_one_transaction(&db.begin_read());
        println!(
            "{:<38} {:>14} {:>16} {:>16}",
            kind.name(),
            rate(ELEMENTS, load),
            rate(SMALL_COMMITS, commits),
            rate(READS, reads),
        );
        drop(db);
        fs::remove_dir_all(dir.path()).ok();
    }

    println!(
        "\nReads, one read transaction per read (worst case for the protocol's per-transaction cost):"
    );
    println!("{:<38} {:>16}", "", "reads");
    {
        let dir = tempdir();
        let db = open(Kind::SingleProcess, &dir);
        bulk_load(&db);
        let single = reads_in_many_transactions(|| db.begin_read());
        println!("{:<38} {:>16}", "Database", rate(READS, single));
    }
    for (mode, label) in [
        (
            WriterMode::SingleWriterProcess,
            "MultiProcess, same handle (single writer)",
        ),
        (
            WriterMode::MultiWriterProcess,
            "MultiProcess, same handle (multi writer)",
        ),
    ] {
        let dir = tempdir();
        let path = dir.path().join("db");
        let writer = Db::Multi(
            MultiProcessDatabase::builder()
                .set_writer_mode(mode)
                .create(&path)
                .unwrap(),
        );
        bulk_load(&writer);
        let reader = MultiProcessDatabase::open_read_only(&path).unwrap();

        println!(
            "{label:<38} {:>16}",
            rate(READS, reads_in_many_transactions(|| writer.begin_read()))
        );
        println!(
            "{:<38} {:>16}",
            "  ... from a second, read-only handle",
            rate(
                READS,
                reads_in_many_transactions(|| reader.begin_read().unwrap())
            )
        );
    }

    println!("\nReads from a second handle, 100 per read transaction, while a writer commits:");
    for (mode, label) in [
        (WriterMode::SingleWriterProcess, "single writer process"),
        (WriterMode::MultiWriterProcess, "multi writer process"),
    ] {
        let dir = tempdir();
        let path = dir.path().join("db");
        let writer = MultiProcessDatabase::builder()
            .set_writer_mode(mode)
            .create(&path)
            .unwrap();
        bulk_load(&Db::Multi(writer));
        // Reopened, so that the background thread below can own it
        let writer = MultiProcessDatabase::open(&path).unwrap();

        let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let commits = Arc::new(AtomicU64::new(0));
        let writer_thread = {
            let stop = stop.clone();
            let commits = commits.clone();
            let value = value();
            thread::spawn(move || {
                let mut key = 0;
                while !stop.load(Ordering::Acquire) {
                    let txn = writer.begin_write().unwrap();
                    {
                        let mut table = txn.open_table(TABLE).unwrap();
                        table.insert(&(key % ELEMENTS), value.as_slice()).unwrap();
                    }
                    txn.commit().unwrap();
                    commits.fetch_add(1, Ordering::Relaxed);
                    key += 1;
                }
            })
        };

        let mut rng = StdRng::seed_from_u64(RNG_SEED);
        let start = Instant::now();
        let mut reads = 0u64;
        while start.elapsed() < Duration::from_secs(READ_SECONDS) {
            let txn = reader.begin_read().unwrap();
            let table = txn.open_table(TABLE).unwrap();
            for _ in 0..100 {
                let key = rng.random_range(0..ELEMENTS);
                assert_eq!(
                    VALUE_SIZE,
                    table.get_owned(key).unwrap().unwrap().value().len()
                );
                reads += 1;
            }
        }
        let elapsed = start.elapsed();
        stop.store(true, Ordering::Release);
        writer_thread.join().unwrap();

        println!(
            "{label:<38} {:>16} while the writer made {:>16}",
            rate(reads, elapsed),
            rate(commits.load(Ordering::Relaxed), elapsed)
        );
    }

    println!("\nWrite transactions alternating between two handles (multi writer process):");
    {
        let dir = tempdir();
        let path = dir.path().join("db");
        bulk_load(&Db::Multi(
            MultiProcessDatabase::builder()
                .set_writer_mode(WriterMode::MultiWriterProcess)
                .create(&path)
                .unwrap(),
        ));
        let first = MultiProcessDatabase::open(&path).unwrap();
        let second = MultiProcessDatabase::open(&path).unwrap();

        let value = value();
        let start = Instant::now();
        for i in 0..SMALL_COMMITS {
            let db = if i % 2 == 0 { &first } else { &second };
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(TABLE).unwrap();
                table.insert(&(i % ELEMENTS), value.as_slice()).unwrap();
            }
            txn.commit().unwrap();
        }
        println!(
            "{:<38} {:>16}",
            "alternating handles",
            rate(SMALL_COMMITS, start.elapsed())
        );
    }
}
