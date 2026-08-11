//! Tests for the multi-process database interface.
//!
//! Most of these use two `MultiProcessDatabase` handles on one directory from a single test
//! process. File locks belong to the open file description rather than the process, so two handles
//! coordinate through the lock files exactly as two processes do -- while being far easier to
//! debug. The tests at the bottom of the file use real child processes, to check the parts that
//! only a separate process can exercise: a writer that dies, and the coordination actually working
//! across a process boundary.

#![cfg(not(target_os = "wasi"))]

use redb::{
    Durability, MultiProcessDatabase, ReadableDatabase, ReadableTableMetadata, TableDefinition,
    WriterMode,
};
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use tempfile::TempDir;

const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");
const BIG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("big");
// Every key holds the same value in any committed state, so a reader that sees two different
// values in one snapshot has been shown a torn or recycled page. Enough of them that the table
// spans many pages at more than one level, so that a snapshot which is not properly held back has
// somewhere to go wrong
const KEYS: u64 = 5000;

fn tempdir() -> TempDir {
    TempDir::new().unwrap()
}

fn db_path(dir: &TempDir) -> PathBuf {
    dir.path().join("db")
}

fn create(path: &Path, mode: WriterMode) -> MultiProcessDatabase {
    MultiProcessDatabase::builder()
        .set_writer_mode(mode)
        .create(path)
        .unwrap()
}

fn open(path: &Path) -> MultiProcessDatabase {
    MultiProcessDatabase::open(path).unwrap()
}

/// Writes `value` to every key, replacing whatever was there. Rewriting the whole table makes the
/// writer free and reallocate pages on every commit, which is what puts a reader's cached pages at
/// risk if the protocol is wrong.
fn write_generation(db: &MultiProcessDatabase, value: u64) {
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for key in 0..KEYS {
            table.insert(&key, &value).unwrap();
        }
    }
    txn.commit().unwrap();
}

/// Reads one key, which is enough to make a snapshot real without pulling the whole table into
/// this process's page cache. A later `read_generation` on the same snapshot then has to go to the
/// file for most of what it reads, which is where a page that was reused underneath it shows up.
fn spot_check(txn: &redb::ReadTransaction) -> u64 {
    let table = txn.open_table(TABLE).unwrap();
    table.get_owned(0).unwrap().unwrap().value()
}

/// Returns the generation a snapshot holds, asserting that it is the same for every key
fn read_generation(txn: &redb::ReadTransaction) -> u64 {
    let table = txn.open_table(TABLE).unwrap();
    let mut generation = None;
    // Descending, so that the pages a spot check may have cached are read last
    for key in (0..KEYS).rev() {
        let value = table.get_owned(key).unwrap().unwrap().value();
        match generation {
            None => generation = Some(value),
            Some(expected) => assert_eq!(
                expected, value,
                "key {key} holds {value}, but the snapshot is at generation {expected}"
            ),
        }
    }
    generation.unwrap()
}

#[test]
fn create_and_reopen() {
    let dir = tempdir();
    let path = db_path(&dir);
    {
        let db = create(&path, WriterMode::MultiWriterProcess);
        assert_eq!(WriterMode::MultiWriterProcess, db.writer_mode());
        write_generation(&db, 1);
    }
    let db = open(&path);
    assert_eq!(1, read_generation(&db.begin_read().unwrap()));
    write_generation(&db, 2);
    drop(db);

    let db = open(&path);
    assert_eq!(2, read_generation(&db.begin_read().unwrap()));
}

#[test]
fn create_makes_a_directory_of_lock_files() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = create(&path, WriterMode::MultiWriterProcess);
    write_generation(&db, 1);

    assert!(path.join("data.redb").is_file());
    assert!(path.join("write.lock").is_file());
    assert!(path.join("registry.lock").is_file());
    assert!(path.join("readers").is_dir());
    // One slot for the open handle
    assert_eq!(1, std::fs::read_dir(path.join("readers")).unwrap().count());
}

#[test]
fn the_mode_is_fixed_at_creation() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = create(&path, WriterMode::SingleWriterProcess);
    drop(db);

    // Opening without asking for a mode adopts the one the database was created with
    assert_eq!(
        WriterMode::SingleWriterProcess,
        MultiProcessDatabase::open(&path).unwrap().writer_mode()
    );
    // Asking for the wrong one is an error rather than a silently different protocol
    assert!(
        MultiProcessDatabase::builder()
            .set_writer_mode(WriterMode::MultiWriterProcess)
            .open(&path)
            .is_err()
    );
}

#[test]
fn a_second_writer_is_rejected_in_single_writer_mode() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = create(&path, WriterMode::SingleWriterProcess);
    write_generation(&db, 1);

    assert!(matches!(
        MultiProcessDatabase::open(&path),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    // ... but read-only handles are fine
    let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
    assert_eq!(1, read_generation(&reader.begin_read().unwrap()));

    drop(db);
    // Once the writer is gone the slot is free again
    let db = MultiProcessDatabase::open(&path).unwrap();
    write_generation(&db, 2);
}

#[test]
fn writers_take_turns_in_multi_writer_mode() {
    let dir = tempdir();
    let path = db_path(&dir);
    let first = create(&path, WriterMode::MultiWriterProcess);
    let second = open(&path);

    for generation in 1..10 {
        let db = if generation % 2 == 0 { &first } else { &second };
        write_generation(db, generation);
        // Both handles see the commit, whichever of them made it
        assert_eq!(generation, read_generation(&first.begin_read().unwrap()));
        assert_eq!(generation, read_generation(&second.begin_read().unwrap()));
    }
}

#[test]
fn a_read_only_handle_sees_new_commits() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = create(&path, WriterMode::SingleWriterProcess);
    write_generation(&db, 1);

    let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
    assert_eq!(1, read_generation(&reader.begin_read().unwrap()));

    for generation in 2..10 {
        write_generation(&db, generation);
        assert_eq!(generation, read_generation(&reader.begin_read().unwrap()));
    }
}

#[test]
fn a_read_only_handle_cannot_be_opened_for_writing() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = create(&path, WriterMode::MultiWriterProcess);
    write_generation(&db, 1);
    drop(db);

    let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
    assert_eq!(1, read_generation(&reader.begin_read().unwrap()));
}

/// The core safety property: a snapshot another process is reading must stay readable, however
/// much the writer churns the pages underneath it.
#[test]
fn a_snapshot_survives_a_writer_recycling_pages() {
    let dir = tempdir();
    let path = db_path(&dir);
    let writer = create(&path, WriterMode::MultiWriterProcess);
    write_generation(&writer, 1);

    let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
    let snapshot = reader.begin_read().unwrap();
    assert_eq!(1, spot_check(&snapshot));

    // Enough transactions that the writer must reuse the pages the snapshot above still points at,
    // if it is allowed to
    for generation in 2..30 {
        write_generation(&writer, generation);
    }

    // The snapshot still reads the generation it was opened at, page for page
    assert_eq!(1, read_generation(&snapshot));
    drop(snapshot);

    // And a new snapshot sees the latest generation, rather than anything left in the page cache
    assert_eq!(29, read_generation(&reader.begin_read().unwrap()));
}

/// The same, with the reader in a handle that can also write: it goes through a different refresh
/// path, since its memory is not read-only.
#[test]
fn a_snapshot_in_a_writer_handle_survives_another_writer() {
    let dir = tempdir();
    let path = db_path(&dir);
    let first = create(&path, WriterMode::MultiWriterProcess);
    write_generation(&first, 1);
    let second = open(&path);

    let snapshot = second.begin_read().unwrap();
    assert_eq!(1, spot_check(&snapshot));

    for generation in 2..30 {
        write_generation(&first, generation);
    }

    assert_eq!(1, read_generation(&snapshot));
    drop(snapshot);
    assert_eq!(29, read_generation(&second.begin_read().unwrap()));

    // The second handle can still take over the writer role afterwards
    write_generation(&second, 30);
    assert_eq!(30, read_generation(&first.begin_read().unwrap()));
}

/// A reader that has cached pages, and then falls behind, must not serve them from its cache after
/// the writer has handed those pages back out.
#[test]
fn a_stale_page_cache_is_dropped() {
    let dir = tempdir();
    let path = db_path(&dir);
    let writer = create(&path, WriterMode::MultiWriterProcess);
    write_generation(&writer, 1);

    let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
    // Fills the reader's page cache with generation 1's pages, then releases the snapshot so that
    // nothing pins them
    assert_eq!(1, read_generation(&reader.begin_read().unwrap()));

    for generation in 2..20 {
        write_generation(&writer, generation);
        assert_eq!(
            generation,
            read_generation(&reader.begin_read().unwrap()),
            "the reader served generation {generation} from a stale cache"
        );
    }
}

#[test]
fn a_reader_holds_pages_back_while_a_writer_churns() {
    let dir = tempdir();
    let path = db_path(&dir);
    let writer = create(&path, WriterMode::MultiWriterProcess);
    write_generation(&writer, 1);

    let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
    let stop = Arc::new(AtomicBool::new(false));

    let reader_thread = {
        let stop = stop.clone();
        thread::spawn(move || {
            let mut snapshots = 0;
            while !stop.load(Ordering::Acquire) {
                let txn = reader.begin_read().unwrap();
                let generation = spot_check(&txn);
                // Read the rest of the snapshot only after the writer has had a chance to commit
                // over it, so that most of it comes from the file rather than this process's cache
                thread::yield_now();
                assert_eq!(generation, read_generation(&txn));
                snapshots += 1;
            }
            snapshots
        })
    };

    for generation in 2..40 {
        write_generation(&writer, generation);
    }
    stop.store(true, Ordering::Release);
    let snapshots = reader_thread.join().unwrap();
    assert!(snapshots > 0);
}

#[test]
fn many_readers_and_one_writer() {
    let dir = tempdir();
    let path = db_path(&dir);
    let writer = create(&path, WriterMode::SingleWriterProcess);
    write_generation(&writer, 1);

    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let stop = stop.clone();
            let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
            thread::spawn(move || {
                let mut last = 0;
                while !stop.load(Ordering::Acquire) {
                    let txn = reader.begin_read().unwrap();
                    let generation = spot_check(&txn);
                    thread::yield_now();
                    assert_eq!(generation, read_generation(&txn));
                    // Snapshots never go backwards for a given reader
                    assert!(generation >= last);
                    last = generation;
                }
                last
            })
        })
        .collect();

    for generation in 2..30 {
        write_generation(&writer, generation);
    }
    stop.store(true, Ordering::Release);
    for reader in readers {
        reader.join().unwrap();
    }
}

#[test]
fn non_durable_commits_are_rejected_with_multiple_writer_processes() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = create(&path, WriterMode::MultiWriterProcess);

    let mut txn = db.begin_write().unwrap();
    assert!(matches!(
        txn.set_durability(Durability::None),
        Err(redb::SetDurabilityError::NonDurableCommitUnsupported)
    ));
    txn.abort().unwrap();
}

#[test]
fn non_durable_commits_work_with_a_single_writer_process() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = create(&path, WriterMode::SingleWriterProcess);
    write_generation(&db, 1);

    let mut txn = db.begin_write().unwrap();
    txn.set_durability(Durability::None).unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for key in 0..KEYS {
            table.insert(&key, &2).unwrap();
        }
    }
    txn.commit().unwrap();

    // Visible in the writing process...
    assert_eq!(2, read_generation(&db.begin_read().unwrap()));
    // ... but not to anyone else, until a durable commit publishes it
    let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
    assert_eq!(1, read_generation(&reader.begin_read().unwrap()));

    write_generation(&db, 3);
    assert_eq!(3, read_generation(&reader.begin_read().unwrap()));
}

#[test]
fn an_ephemeral_savepoint_holds_pages_back_across_handles() {
    let dir = tempdir();
    let path = db_path(&dir);
    let first = create(&path, WriterMode::MultiWriterProcess);
    write_generation(&first, 1);

    let savepoint = {
        let txn = first.begin_write().unwrap();
        let savepoint = txn.ephemeral_savepoint().unwrap();
        txn.commit().unwrap();
        savepoint
    };

    // Another handle churns the pages the savepoint needs
    let second = open(&path);
    for generation in 2..20 {
        write_generation(&second, generation);
    }

    // Restoring it must still find them
    let mut txn = first.begin_write().unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    txn.commit().unwrap();
    assert_eq!(1, read_generation(&first.begin_read().unwrap()));
    assert_eq!(1, read_generation(&second.begin_read().unwrap()));
}

#[test]
fn the_database_grows_and_shrinks_across_handles() {
    let dir = tempdir();
    let path = db_path(&dir);
    let first = create(&path, WriterMode::MultiWriterProcess);
    let second = open(&path);

    write_generation(&first, 1);

    // The second handle grows the file well past what the first has seen
    let txn = second.begin_write().unwrap();
    {
        let mut table = txn.open_table(BIG_TABLE).unwrap();
        let value = vec![7u8; 4096];
        for key in 0..2000u64 {
            table.insert(&key, value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    // ... and the first picks it up, including pages in regions it has never seen
    let read = first.begin_read().unwrap();
    let table = read.open_table(BIG_TABLE).unwrap();
    assert_eq!(2000, table.len().unwrap());
    assert_eq!(4096, table.get_owned(1999).unwrap().unwrap().value().len());
    drop(read);

    // The first handle can still write, having reloaded the grown layout
    let txn = first.begin_write().unwrap();
    {
        let mut table = txn.open_table(BIG_TABLE).unwrap();
        for key in 0..2000u64 {
            table.remove(&key).unwrap();
        }
    }
    txn.commit().unwrap();
    assert_eq!(
        0,
        second
            .begin_read()
            .unwrap()
            .open_table(BIG_TABLE)
            .unwrap()
            .len()
            .unwrap()
    );
}

#[test]
fn concurrent_writers_from_many_threads_and_handles() {
    let dir = tempdir();
    let path = db_path(&dir);
    let first = Arc::new(create(&path, WriterMode::MultiWriterProcess));
    let second = Arc::new(open(&path));

    let threads: Vec<_> = [first.clone(), second.clone(), first.clone(), second.clone()]
        .into_iter()
        .enumerate()
        .map(|(index, db)| {
            thread::spawn(move || {
                for i in 0..20u64 {
                    let txn = db.begin_write().unwrap();
                    {
                        let mut table = txn.open_table(TABLE).unwrap();
                        table.insert(&(index as u64 * 100 + i), &i).unwrap();
                    }
                    txn.commit().unwrap();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }

    let read = first.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(80, table.len().unwrap());
    for index in 0..4u64 {
        for i in 0..20u64 {
            assert_eq!(
                i,
                table.get_owned(index * 100 + i).unwrap().unwrap().value()
            );
        }
    }
}

/// A persistent savepoint lives in the database, not in the process that made it, so a writer that
/// has never heard of one must still hold its pages back -- and must be able to restore it.
#[test]
fn a_persistent_savepoint_is_shared_between_handles() {
    let dir = tempdir();
    let path = db_path(&dir);
    // Opened first, so it has no way of knowing about the savepoint the other handle is about to
    // create except by reading it out of the database
    let second = create(&path, WriterMode::MultiWriterProcess);
    write_generation(&second, 1);

    let savepoint_id = {
        let first = open(&path);
        let txn = first.begin_write().unwrap();
        let id = txn.persistent_savepoint().unwrap();
        txn.commit().unwrap();
        // The handle that created it is gone entirely
        id
    };

    // Enough churn that the savepoint's pages would be long gone if they were not held back
    for generation in 2..30 {
        write_generation(&second, generation);
    }
    assert_eq!(29, read_generation(&second.begin_read().unwrap()));

    let mut txn = second.begin_write().unwrap();
    let savepoint = txn.get_persistent_savepoint(savepoint_id).unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    txn.commit().unwrap();
    assert_eq!(1, read_generation(&second.begin_read().unwrap()));

    // And it can be deleted from a third handle, which never saw it created either
    let third = open(&path);
    let txn = third.begin_write().unwrap();
    assert!(txn.delete_persistent_savepoint(savepoint_id).unwrap());
    txn.commit().unwrap();
    let txn = second.begin_write().unwrap();
    assert_eq!(0, txn.list_persistent_savepoints().unwrap().count());
    txn.abort().unwrap();
}

/// Savepoint ids are handed out from a counter that lives in the database, so two handles must not
/// be able to allocate the same one.
#[test]
fn persistent_savepoint_ids_do_not_collide_between_handles() {
    let dir = tempdir();
    let path = db_path(&dir);
    let first = create(&path, WriterMode::MultiWriterProcess);
    write_generation(&first, 1);
    let second = open(&path);

    let mut ids = vec![];
    for handle in [&first, &second, &first, &second] {
        let txn = handle.begin_write().unwrap();
        ids.push(txn.persistent_savepoint().unwrap());
        txn.commit().unwrap();
    }
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(4, ids.len(), "two handles allocated the same savepoint id");

    let txn = first.begin_write().unwrap();
    assert_eq!(4, txn.list_persistent_savepoints().unwrap().count());
    txn.abort().unwrap();
}

/// Dropping a handle must not wait on another process's write transaction. Closing writes an
/// allocator state table so that the next open does not have to rebuild one, but in multi-writer
/// mode every commit has already written one.
#[test]
fn dropping_a_handle_does_not_wait_for_the_write_lock() {
    let dir = tempdir();
    let path = db_path(&dir);
    let first = create(&path, WriterMode::MultiWriterProcess);
    write_generation(&first, 1);

    let second = open(&path);
    // Held for the whole of the drop below, which must not need it
    let blocking = second.begin_write().unwrap();

    let (done, closed) = std::sync::mpsc::channel();
    thread::spawn(move || {
        drop(first);
        let _ = done.send(());
    });
    // Fails rather than hanging if the drop is waiting for the write lock
    closed
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("dropping the handle blocked on the write lock");

    blocking.abort().unwrap();
    assert_eq!(1, read_generation(&second.begin_read().unwrap()));
}

/// A writer committing as fast as it can, against a reader in another handle doing the same. Small
/// transactions and a table large enough to span many pages, so that the writer is constantly
/// freeing and reusing pages that the reader may be part way through reading.
fn churn(mode: WriterMode, rounds: u64) {
    let dir = tempdir();
    let path = db_path(&dir);
    {
        let db = create(&path, mode);
        let value = vec![0xab; 100];
        let mut key = 0;
        while key < CHURN_ELEMENTS {
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(CHURN_TABLE).unwrap();
                for _ in 0..2000 {
                    table.insert(&key, value.as_slice()).unwrap();
                    key += 1;
                }
            }
            txn.commit().unwrap();
        }
    }

    let writer = open(&path);
    let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let writer_thread = {
        let stop = stop.clone();
        thread::spawn(move || {
            let value = vec![0xcd; 100];
            let mut commits = 0u64;
            while !stop.load(Ordering::Acquire) {
                let txn = writer.begin_write().unwrap();
                {
                    let mut table = txn.open_table(CHURN_TABLE).unwrap();
                    table
                        .insert(&(commits % CHURN_ELEMENTS), value.as_slice())
                        .unwrap();
                }
                txn.commit().unwrap();
                commits += 1;
            }
            commits
        })
    };

    for round in 0..rounds {
        let txn = reader.begin_read().unwrap();
        let table = txn.open_table(CHURN_TABLE).unwrap();
        for i in 0..50 {
            let key = (round * 37 + i * 401) % CHURN_ELEMENTS;
            assert_eq!(100, table.get_owned(key).unwrap().unwrap().value().len());
        }
    }
    stop.store(true, Ordering::Release);
    assert!(writer_thread.join().unwrap() > 0);
}

const CHURN_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("churn");
const CHURN_ELEMENTS: u64 = 20_000;

#[test]
fn churn_with_a_single_writer_process() {
    churn(WriterMode::SingleWriterProcess, 2000);
}

#[test]
fn churn_with_multiple_writer_processes() {
    churn(WriterMode::MultiWriterProcess, 2000);
}

// --------------------------------------------------------------------------------------------
// Tests that need real processes
// --------------------------------------------------------------------------------------------

/// Entry point for the child processes the tests below spawn. Ignored so that it only runs when
/// named explicitly, and does nothing unless the parent asked for a role.
#[test]
#[ignore]
fn child_process_worker() {
    let Ok(role) = env::var("REDB_MP_ROLE") else {
        return;
    };
    let path = PathBuf::from(env::var("REDB_MP_PATH").unwrap());
    match role.as_str() {
        // Writes generations until told to stop, so that the parent can read across them
        "writer" => {
            let db = MultiProcessDatabase::open(&path).unwrap();
            let generations: u64 = env::var("REDB_MP_GENERATIONS").unwrap().parse().unwrap();
            for generation in 2..=generations {
                write_generation(&db, generation);
            }
        }
        // Reads snapshots until the writer has reached the last generation, checking every one
        "reader" => {
            let db = MultiProcessDatabase::open_read_only(&path).unwrap();
            let generations: u64 = env::var("REDB_MP_GENERATIONS").unwrap().parse().unwrap();
            let mut last = 0;
            let mut seen = 0;
            while last < generations {
                let txn = db.begin_read().unwrap();
                let generation = spot_check(&txn);
                assert!(generation >= last, "a snapshot went backwards");
                // Hold the snapshot across more of the writer's commits, then read all of it
                thread::yield_now();
                assert_eq!(generation, read_generation(&txn));
                last = generation;
                seen += 1;
                assert!(seen < 10_000_000, "the writer never finished");
            }
        }
        // Opens for writing and expects to be turned away
        "rejected_writer" => {
            assert!(matches!(
                MultiProcessDatabase::open(&path),
                Err(redb::DatabaseError::DatabaseAlreadyOpen)
            ));
        }
        // Starts a write transaction and dies holding the write lock, without committing
        "crasher" => {
            let db = MultiProcessDatabase::open(&path).unwrap();
            let txn = db.begin_write().unwrap();
            {
                let mut table = txn.open_table(TABLE).unwrap();
                for key in 0..KEYS {
                    table.insert(&key, &999).unwrap();
                }
            }
            // Leaves the file with a half-written transaction and the lock files as they are
            std::process::abort();
        }
        other => panic!("unknown role {other}"),
    }
}

fn spawn_child(role: &str, path: &Path, generations: u64) -> std::process::Child {
    Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "child_process_worker",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("REDB_MP_ROLE", role)
        .env("REDB_MP_PATH", path)
        .env("REDB_MP_GENERATIONS", generations.to_string())
        .spawn()
        .unwrap()
}

#[test]
fn a_child_process_reads_while_this_one_writes() {
    let dir = tempdir();
    let path = db_path(&dir);
    let writer = create(&path, WriterMode::SingleWriterProcess);
    write_generation(&writer, 1);

    let generations = 40;
    let mut reader = spawn_child("reader", &path, generations);
    for generation in 2..=generations {
        write_generation(&writer, generation);
    }
    assert!(reader.wait().unwrap().success(), "the reader child failed");
}

#[test]
fn a_child_process_writes_while_this_one_reads() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = create(&path, WriterMode::MultiWriterProcess);
    write_generation(&db, 1);

    let generations = 40;
    let mut writer = spawn_child("writer", &path, generations);
    let mut last = 0;
    while last < generations {
        let txn = db.begin_read().unwrap();
        let generation = spot_check(&txn);
        assert!(generation >= last);
        thread::yield_now();
        assert_eq!(generation, read_generation(&txn));
        last = generation;
    }
    assert!(writer.wait().unwrap().success(), "the writer child failed");
    assert_eq!(generations, read_generation(&db.begin_read().unwrap()));
}

#[test]
fn a_child_process_cannot_write_in_single_writer_mode() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = create(&path, WriterMode::SingleWriterProcess);
    write_generation(&db, 1);

    let mut child = spawn_child("rejected_writer", &path, 0);
    assert!(child.wait().unwrap().success());

    // The child's failed open must not have disturbed anything
    write_generation(&db, 2);
    assert_eq!(2, read_generation(&db.begin_read().unwrap()));
}

#[test]
fn a_writer_that_dies_mid_transaction_is_recovered() {
    let dir = tempdir();
    let path = db_path(&dir);
    {
        let db = create(&path, WriterMode::MultiWriterProcess);
        write_generation(&db, 1);
    }

    let mut child = spawn_child("crasher", &path, 0);
    let status = child.wait().unwrap();
    assert!(!status.success(), "the child was supposed to abort");

    // The uncommitted transaction is rolled back, and the database is usable again
    let db = open(&path);
    assert_eq!(1, read_generation(&db.begin_read().unwrap()));
    write_generation(&db, 2);
    assert_eq!(2, read_generation(&db.begin_read().unwrap()));
}

#[test]
fn a_reader_that_dies_stops_holding_pages_back() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = create(&path, WriterMode::MultiWriterProcess);
    write_generation(&db, 1);

    // The crasher takes a slot and pins a transaction, then dies with the slot still holding it
    let mut child = spawn_child("crasher", &path, 0);
    assert!(!child.wait().unwrap().success());

    // The dead process's slot must not stop this one from reclaiming pages, which it would if the
    // slot's contents were trusted without checking whether anyone still holds it
    let before = std::fs::metadata(path.join("data.redb")).unwrap().len();
    for generation in 2..40 {
        write_generation(&db, generation);
    }
    let after = std::fs::metadata(path.join("data.redb")).unwrap().len();
    assert!(
        after <= before * 2,
        "the file grew from {before} to {after}, so pages were not being reused"
    );
}
