//! Tests for the multi-process database interface.
//!
<<<<<<< HEAD
//! At this step the interface allows only one process to have the database open, so what there is
//! to test is the directory layout and the lock file that enforces that. The lock is exercised
//! both from a second handle in this process -- file locks belong to the open file description
//! rather than the process, so a second handle is excluded exactly as a second process is -- and
//! from a real child process, which is what the lock is ultimately for.

#![cfg(all(feature = "experimental-multiprocess", not(target_os = "wasi")))]

use redb::{
    Database, DatabaseError, MultiProcessDatabase, ReadOnlyDatabase, ReadableDatabase,
    TableDefinition, WriteTransaction,
=======
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
>>>>>>> b508cef (Add a multi-process safe database interface)
};
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;
<<<<<<< HEAD
use tempfile::TempDir;

const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");
=======
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
>>>>>>> b508cef (Add a multi-process safe database interface)

fn tempdir() -> TempDir {
    TempDir::new().unwrap()
}

fn db_path(dir: &TempDir) -> PathBuf {
    dir.path().join("db")
}

<<<<<<< HEAD
fn write(db: &MultiProcessDatabase, key: u64, value: u64) {
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        table.insert(&key, &value).unwrap();
=======
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
>>>>>>> b508cef (Add a multi-process safe database interface)
    }
    txn.commit().unwrap();
}

<<<<<<< HEAD
fn read(db: &MultiProcessDatabase, key: u64) -> Option<u64> {
    let txn = db.begin_read().unwrap();
    let table = txn.open_table(TABLE).unwrap();
    table.get_owned(key).unwrap().map(|value| value.value())
=======
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
>>>>>>> b508cef (Add a multi-process safe database interface)
}

#[test]
fn create_and_reopen() {
    let dir = tempdir();
    let path = db_path(&dir);
    {
<<<<<<< HEAD
        let db = MultiProcessDatabase::create(&path).unwrap();
        write(&db, 0, 1);
        assert_eq!(Some(1), read(&db, 0));
    }

    let db = MultiProcessDatabase::open(&path).unwrap();
    assert_eq!(Some(1), read(&db, 0));
    write(&db, 0, 2);
    drop(db);

    // create() on an existing database opens it rather than starting over
    let db = MultiProcessDatabase::create(&path).unwrap();
    assert_eq!(Some(2), read(&db, 0));
}

#[test]
fn create_makes_a_directory() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = MultiProcessDatabase::create(&path).unwrap();
    write(&db, 0, 1);

    assert!(path.join("data.redb").is_file());
    assert!(path.join("write.lock").is_file());
    assert!(path.join("metadata").is_file());
}

#[test]
fn a_second_handle_is_rejected() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = MultiProcessDatabase::create(&path).unwrap();
    write(&db, 0, 1);

    assert!(matches!(
        MultiProcessDatabase::open(&path),
        Err(DatabaseError::DatabaseAlreadyOpen)
    ));
    assert!(matches!(
        MultiProcessDatabase::create(&path),
        Err(DatabaseError::DatabaseAlreadyOpen)
    ));

    // Once the first handle is gone the directory can be opened again
    drop(db);
    let db = MultiProcessDatabase::open(&path).unwrap();
    assert_eq!(Some(1), read(&db, 0));
}

/// A live write transaction keeps the database open past the point where the handle is dropped, so
/// the lock has to outlive the handle too -- otherwise another process could start writing while
/// this transaction is still running.
#[test]
fn the_lock_outlives_a_handle_dropped_during_a_write() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = MultiProcessDatabase::create(&path).unwrap();
    write(&db, 0, 1);

    let txn: WriteTransaction = db.begin_write().unwrap();
    drop(db);

    assert!(matches!(
        MultiProcessDatabase::open(&path),
        Err(DatabaseError::DatabaseAlreadyOpen)
    ));

    {
        let mut table = txn.open_table(TABLE).unwrap();
        table.insert(&0, &2).unwrap();
    }
    txn.commit().unwrap();

    // ... and is released once the transaction that was keeping it open finishes
    let db = MultiProcessDatabase::open(&path).unwrap();
    assert_eq!(Some(2), read(&db, 0));
}

#[test]
fn opening_something_that_is_not_a_database_fails() {
    let dir = tempdir();
    assert!(MultiProcessDatabase::open(dir.path().join("missing")).is_err());

    let empty = dir.path().join("empty");
    std::fs::create_dir(&empty).unwrap();
    assert!(MultiProcessDatabase::open(&empty).is_err());

    // A directory holding a plain redb database is not one of these either -- it has no marker
    let plain = dir.path().join("plain");
    std::fs::create_dir(&plain).unwrap();
    drop(Database::create(plain.join("data.redb")).unwrap());
    assert!(MultiProcessDatabase::open(&plain).is_err());
}

#[test]
fn open_does_not_create() {
    let dir = tempdir();
    let path = db_path(&dir);
    assert!(MultiProcessDatabase::open(&path).is_err());
    assert!(!path.exists());
}

#[test]
fn the_builder_configures_the_database() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = MultiProcessDatabase::builder()
        .set_cache_size(1024 * 1024)
        .create(&path)
        .unwrap();
    write(&db, 0, 1);
    assert_eq!(Some(1), read(&db, 0));
}

/// A create() that made the directory and the marker but died before the database file was
/// initialized must not leave the directory permanently unopenable: a later create() finishes the
/// job, exactly as it would for a `Database` whose file was created and then not written to.
#[test]
fn an_interrupted_create_can_be_redone() {
    let dir = tempdir();
    let path = db_path(&dir);
    drop(MultiProcessDatabase::create(&path).unwrap());
    std::fs::write(path.join("data.redb"), []).unwrap();

    let db = MultiProcessDatabase::create(&path).unwrap();
    write(&db, 0, 1);
    assert_eq!(Some(1), read(&db, 0));
}

/// `Path::parent` is lexical, so a path ending in `..` names a child of the real directory rather
/// than its parent. Deciding which directory to fsync on that would flush the wrong one, and the
/// database directory's own entry would not be durable after `create()` returned.
#[test]
fn a_path_that_walks_back_up_still_works() {
    let dir = tempdir();
    let nested = dir.path().join("a").join("b");
    std::fs::create_dir_all(&nested).unwrap();

    let path = nested.join("..").join("db");
    let db = MultiProcessDatabase::create(&path).unwrap();
    write(&db, 0, 1);
    assert_eq!(Some(1), read(&db, 0));
    drop(db);

    // the database is where the path actually pointed, not where a lexical reading would put it
    assert!(dir.path().join("a").join("db").join("data.redb").is_file());
    assert_eq!(
        Some(1),
        read(&MultiProcessDatabase::open(&path).unwrap(), 0)
    );
}

/// The write lock is invisible to a process that reaches past the directory and opens the database
/// file itself, so that file carries the ordinary exclusive lock as well. Readers are turned away
/// along with writers: nothing yet stops the process holding the directory from freeing pages that
/// a `ReadOnlyDatabase` in another process is still reading.
#[test]
fn an_ordinary_database_cannot_open_the_data_file() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = MultiProcessDatabase::create(&path).unwrap();
    write(&db, 0, 1);

    let data = path.join("data.redb");
    assert!(matches!(
        Database::open(&data),
        Err(DatabaseError::DatabaseAlreadyOpen)
    ));
    assert!(matches!(
        Database::create(&data),
        Err(DatabaseError::DatabaseAlreadyOpen)
    ));
    assert!(matches!(
        ReadOnlyDatabase::open(&data),
        Err(DatabaseError::DatabaseAlreadyOpen)
    ));

    // ... and once the directory is closed the database file is an ordinary redb database again
    drop(db);
    assert_eq!(
        Some(1),
        ReadOnlyDatabase::open(&data)
            .unwrap()
            .begin_read()
            .unwrap()
            .open_table(TABLE)
            .unwrap()
            .get_owned(0)
            .unwrap()
            .map(|value| value.value())
    );
}

// --------------------------------------------------------------------------------------------
// Tests that need a real process
=======
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
>>>>>>> b508cef (Add a multi-process safe database interface)
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
<<<<<<< HEAD
        // Expects to be turned away by the lock the parent holds
        "rejected" => {
            assert!(matches!(
                MultiProcessDatabase::open(&path),
                Err(DatabaseError::DatabaseAlreadyOpen)
            ));
        }
        // Expects to get in, and leaves a value behind for the parent to find
        "writer" => {
            let db = MultiProcessDatabase::open(&path).unwrap();
            write(&db, 0, 7);
        }
        // Opens the database and dies without closing it, leaving the lock to the operating system
        "crasher" => {
            let db = MultiProcessDatabase::open(&path).unwrap();
            write(&db, 0, 9);
=======
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
>>>>>>> b508cef (Add a multi-process safe database interface)
            std::process::abort();
        }
        other => panic!("unknown role {other}"),
    }
}

<<<<<<< HEAD
fn run_child(role: &str, path: &Path) -> std::process::ExitStatus {
=======
fn spawn_child(role: &str, path: &Path, generations: u64) -> std::process::Child {
>>>>>>> b508cef (Add a multi-process safe database interface)
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
<<<<<<< HEAD
        .status()
=======
        .env("REDB_MP_GENERATIONS", generations.to_string())
        .spawn()
>>>>>>> b508cef (Add a multi-process safe database interface)
        .unwrap()
}

#[test]
<<<<<<< HEAD
fn a_child_process_cannot_open_a_database_this_one_holds() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = MultiProcessDatabase::create(&path).unwrap();
    write(&db, 0, 1);

    assert!(run_child("rejected", &path).success());

    // The child's failed open must not have disturbed anything
    write(&db, 0, 2);
    assert_eq!(Some(2), read(&db, 0));
}

#[test]
fn a_child_process_can_open_a_database_this_one_has_closed() {
    let dir = tempdir();
    let path = db_path(&dir);
    {
        let db = MultiProcessDatabase::create(&path).unwrap();
        write(&db, 0, 1);
    }

    assert!(run_child("writer", &path).success());

    let db = MultiProcessDatabase::open(&path).unwrap();
    assert_eq!(Some(7), read(&db, 0));
}

#[test]
fn a_process_that_dies_releases_the_lock() {
    let dir = tempdir();
    let path = db_path(&dir);
    {
        let db = MultiProcessDatabase::create(&path).unwrap();
        write(&db, 0, 1);
    }

    assert!(!run_child("crasher", &path).success());

    // Nothing has to clean up after it: the operating system dropped its lock when it died
    let db = MultiProcessDatabase::open(&path).unwrap();
    assert_eq!(Some(9), read(&db, 0));
    write(&db, 0, 10);
    assert_eq!(Some(10), read(&db, 0));
}

/// A directory can be searchable and writable without being readable, and everything this type does
/// in one apart from flushing the directory itself works there. `create()` is open-or-create, so it
/// should behave the way `open()` does rather than failing on a database that is already complete.
#[test]
#[cfg(unix)]
fn a_directory_that_cannot_be_read_still_opens() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir();
    let path = db_path(&dir);
    {
        let db = MultiProcessDatabase::create(&path).unwrap();
        write(&db, 0, 7);
    }

    let readable = std::fs::metadata(&path).unwrap().permissions();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o311)).unwrap();

    let opened = MultiProcessDatabase::open(&path).map(|db| read(&db, 0));
    let created = MultiProcessDatabase::create(&path).map(|db| read(&db, 0));

    // Restored before the assertions, so that a failure does not also leave the directory
    // unreadable for whatever has to clean it up
    std::fs::set_permissions(&path, readable).unwrap();
    assert_eq!(Some(7), opened.unwrap());
    assert_eq!(Some(7), created.unwrap());
}

/// `create()` accepts an unmarked directory because that is what an interrupted create looks like --
/// the marker goes in last. A plain redb database someone put in a directory is not that, and must
/// not be quietly converted into one of these.
#[test]
fn create_does_not_adopt_a_plain_database() {
    let dir = tempdir();
    let path = db_path(&dir);
    std::fs::create_dir(&path).unwrap();
    drop(Database::create(path.join("data.redb")).unwrap());

    assert!(MultiProcessDatabase::create(&path).is_err());
    assert!(!path.join("metadata").exists());
}

/// The other side of that: a create() interrupted before the marker went in leaves the lock file
/// behind, which is what says the half-made database is one of these, and the next create() finishes
/// the job rather than refusing it.
#[test]
fn an_interrupted_create_is_still_finished() {
    let dir = tempdir();
    let path = db_path(&dir);
    {
        let db = MultiProcessDatabase::create(&path).unwrap();
        write(&db, 0, 7);
    }
    // The state a create() that died just before writing the marker leaves behind
    std::fs::remove_file(path.join("metadata")).unwrap();

    let db = MultiProcessDatabase::create(&path).unwrap();
    assert_eq!(Some(7), read(&db, 0));
    assert!(path.join("metadata").is_file());
}

/// `write.lock` is the first thing `create()` makes, so any other file redb writes implies one
/// beside it. A directory holding one of those names *without* the lock file was filled in by
/// something else, and a `create()` pointed there by mistake must not overwrite or delete it.
#[test]
fn create_does_not_clobber_files_in_a_directory_it_did_not_make() {
    let dir = tempdir();
    let path = db_path(&dir);
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("metadata.tmp"), b"someone else's file").unwrap();

    assert!(MultiProcessDatabase::create(&path).is_err());
    assert_eq!(
        b"someone else's file".to_vec(),
        std::fs::read(path.join("metadata.tmp")).unwrap()
    );
}

/// A directory that already holds something else is a mistyped path, not a database waiting to be
/// made. The only reason to accept an existing directory without a marker is an interrupted
/// `create()`, and such a directory holds nothing but the files `create()` itself writes.
#[test]
fn create_refuses_a_directory_holding_other_files() {
    let dir = tempdir();
    let path = dir.path().join("not-a-db");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("notes.txt"), b"hello").unwrap();

    assert!(MultiProcessDatabase::create(&path).is_err());

    // Nothing at all this time -- the check runs before the lock file would be made
    assert_eq!(1, std::fs::read_dir(&path).unwrap().count());
}

/// The names `create()` will adopt have to be its own files and not symlinks wearing those names.
/// Opening `data.redb` with `create` set follows a symlink, so without this a directory planted
/// with one would have a database initialized over whatever it pointed at.
#[cfg(unix)]
#[test]
fn create_refuses_a_directory_of_symlinks() {
    let dir = tempdir();
    let outside = dir.path().join("precious");
    std::fs::write(&outside, b"not redb's to touch").unwrap();

    for name in ["data.redb", "metadata.tmp"] {
        let path = dir.path().join(format!("planted-{name}"));
        std::fs::create_dir(&path).unwrap();
        std::os::unix::fs::symlink(&outside, path.join(name)).unwrap();

        assert!(MultiProcessDatabase::create(&path).is_err());
        assert_eq!(
            b"not redb's to touch",
            &std::fs::read(&outside).unwrap()[..],
            "followed the {name} symlink"
        );
    }
}

/// A symlink that resolves to nothing is still a symlink, and `Path::exists` reports it as an
/// absent file. Deciding on that would have `create()` treat the name as free and let the
/// promoting rename replace it, so the check has to ask whether the name is taken rather than
/// whether it resolves.
#[cfg(unix)]
#[test]
fn create_refuses_a_dangling_data_file_symlink() {
    let dir = tempdir();
    let path = db_path(&dir);
    drop(MultiProcessDatabase::create(&path).unwrap());

    std::fs::remove_file(path.join("data.redb")).unwrap();
    std::os::unix::fs::symlink(dir.path().join("nowhere"), path.join("data.redb")).unwrap();

    assert!(MultiProcessDatabase::create(&path).is_err());
    assert!(MultiProcessDatabase::open(&path).is_err());
    // and the symlink is still a symlink, not a database written over the top of it
    assert!(
        std::fs::symlink_metadata(path.join("data.redb"))
            .unwrap()
            .is_symlink()
    );
}

/// ... but only when there is no marker. A directory that is already a database stays openable
/// whatever else turns up in it, or a stray `.DS_Store` would be enough to lock its owner out.
#[test]
fn a_stray_file_does_not_stop_an_existing_database_opening() {
    let dir = tempdir();
    let path = db_path(&dir);
    drop(MultiProcessDatabase::create(&path).unwrap());
    std::fs::write(path.join(".DS_Store"), b"junk").unwrap();

    let db = MultiProcessDatabase::open(&path).unwrap();
    write(&db, 0, 1);
    drop(db);

    let db = MultiProcessDatabase::create(&path).unwrap();
    assert_eq!(Some(1), read(&db, 0));
}

/// The file-type rule covers a directory that is already a database too, not just the recovery
/// path. `create()` on an existing database still rewrites the marker, and a `metadata` that is a
/// symlink would vouch for a directory holding nothing of redb's.
#[cfg(unix)]
#[test]
fn symlinks_are_refused_in_a_directory_that_has_a_marker() {
    let dir = tempdir();
    let outside = dir.path().join("precious");
    std::fs::write(&outside, b"not redb's to touch").unwrap();

    // Rewriting the marker must not be written through a planted temporary
    let rewritten = db_path(&dir);
    drop(MultiProcessDatabase::create(&rewritten).unwrap());
    std::os::unix::fs::symlink(&outside, rewritten.join("metadata.tmp")).unwrap();
    drop(MultiProcessDatabase::create(&rewritten).unwrap());
    assert_eq!(
        b"not redb's to touch",
        &std::fs::read(&outside).unwrap()[..]
    );

    // And a marker that is a symlink is not a marker, however valid the bytes it points at
    let borrowed = dir.path().join("borrowed");
    std::fs::create_dir(&borrowed).unwrap();
    std::os::unix::fs::symlink(rewritten.join("metadata"), borrowed.join("metadata")).unwrap();
    std::fs::write(borrowed.join("data.redb"), []).unwrap();
    assert!(MultiProcessDatabase::open(&borrowed).is_err());
    assert!(MultiProcessDatabase::create(&borrowed).is_err());

    // The lock file is held to the same rule: the directory lock is worth nothing if it is taken
    // on a file that turned out to be a pointer somewhere else
    let relinked = dir.path().join("relinked");
    drop(MultiProcessDatabase::create(&relinked).unwrap());
    std::fs::remove_file(relinked.join("write.lock")).unwrap();
    std::os::unix::fs::symlink(&outside, relinked.join("write.lock")).unwrap();
    assert!(MultiProcessDatabase::open(&relinked).is_err());
    assert!(MultiProcessDatabase::create(&relinked).is_err());
    assert_eq!(
        b"not redb's to touch",
        &std::fs::read(&outside).unwrap()[..]
    );
}

/// `open()` does not create anything, and that has to hold for a directory that exists but is not
/// one of these: it must be left exactly as it was found rather than gaining a lock file on the
/// way to the error.
#[test]
fn open_does_not_touch_a_directory_it_rejects() {
    let dir = tempdir();
    let path = dir.path().join("not-a-db");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("notes.txt"), b"hello").unwrap();

    assert!(MultiProcessDatabase::open(&path).is_err());
    assert_eq!(1, std::fs::read_dir(&path).unwrap().count());
}

/// The marker is what makes a directory one of these, so `create()` must not install it in a
/// directory it then fails on -- that would convert someone else's directory as a side effect of
/// refusing it, and every later open would read it as a database.
///
/// Unlike `open()`, `create()` cannot leave the directory completely untouched: the lock has to be
/// taken before the directory can be read, so an empty `write.lock` is already there by the time
/// validation fails. That file means nothing without a marker beside it, and unlinking it would
/// break the exclusion it provides, so the guarantee is about the marker rather than about the
/// directory being pristine.
#[test]
fn create_does_not_mark_a_directory_it_rejects() {
    let dir = tempdir();
    let path = dir.path().join("not-a-db");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("data.redb"), b"this is not a redb database").unwrap();

    assert!(MultiProcessDatabase::create(&path).is_err());
    assert!(!path.join("metadata").exists());

    // ... and the file it refused to read is still there, byte for byte
    assert_eq!(
        b"this is not a redb database",
        &std::fs::read(path.join("data.redb")).unwrap()[..]
    );

    // Nothing at all: a database file with no lock file beside it was put there by something that
    // is not this type, which is decided before the directory is touched
    let mut left: Vec<_> = std::fs::read_dir(&path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    left.sort();
    assert_eq!(vec!["data.redb"], left);

    // ... and it stays refused, rather than a second attempt reading the leavings of the first as
    // evidence that the directory is one of these
    assert!(MultiProcessDatabase::create(&path).is_err());
    assert!(MultiProcessDatabase::open(&path).is_err());
}

/// redb writes a new database's header with the magic number zeroed, flushes, and only then writes
/// the magic, so a crash during initialization leaves a file that is neither empty nor a database.
/// `Database::new` refuses such a file whether or not `create` is set, so under the name
/// `data.redb` it would wedge the directory for good -- indistinguishable from a file this call was
/// pointed at by mistake. Initializing under a temporary name is what keeps the two apart.
#[test]
fn an_initialization_that_crashed_partway_can_be_redone() {
    let dir = tempdir();
    let path = db_path(&dir);
    drop(MultiProcessDatabase::create(&path).unwrap());

    // Exactly what such a crash leaves: no marker, no data.redb, and a temporary file that is not
    // empty and has no magic number
    std::fs::remove_file(path.join("metadata")).unwrap();
    std::fs::rename(path.join("data.redb"), path.join("data.redb.tmp")).unwrap();
    let mut partial = std::fs::read(path.join("data.redb.tmp")).unwrap();
    partial[0..9].fill(0);
    std::fs::write(path.join("data.redb.tmp"), &partial).unwrap();

    let db = MultiProcessDatabase::create(&path).unwrap();
    write(&db, 0, 1);
    assert_eq!(Some(1), read(&db, 0));
    drop(db);

    assert!(path.join("data.redb").is_file());
    assert!(!path.join("data.redb.tmp").exists());
}

/// A temporary file only ever means "an attempt that did not finish", so one sitting next to a
/// database that *did* finish must not be moved over it. Promoting on the mere presence of a
/// temporary would swap a good database for wreckage, and leave the handle this call returns
/// writing to an inode nothing points at any more.
#[test]
fn a_stale_temporary_does_not_replace_the_database() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = MultiProcessDatabase::create(&path).unwrap();
    write(&db, 0, 42);
    drop(db);

    std::fs::write(path.join("data.redb.tmp"), b"stale wreckage").unwrap();

    let db = MultiProcessDatabase::create(&path).unwrap();
    assert_eq!(Some(42), read(&db, 0));
    write(&db, 1, 43);
    drop(db);

    // ... and what this call wrote is in the database the directory actually points at
    let db = MultiProcessDatabase::open(&path).unwrap();
    assert_eq!(Some(42), read(&db, 0));
    assert_eq!(Some(43), read(&db, 1));
    assert!(!path.join("data.redb.tmp").exists());
}

/// A `create()` that is going to fail must fail without having deleted anything. This is the case
/// the deferral is for: the directory really is one of these, so nothing turns it away on the way
/// in, and the temporary is cleared only once `Database` has accepted the file -- which here it
/// never does.
#[test]
fn a_failed_create_leaves_a_temporary_file_alone() {
    let dir = tempdir();
    let path = db_path(&dir);
    drop(MultiProcessDatabase::create(&path).unwrap());
    // The database file is wreckage now, so this create() will fail once redb reads it
    std::fs::write(path.join("data.redb"), b"not a database").unwrap();
    std::fs::write(path.join("data.redb.tmp"), b"someone else's file").unwrap();

    assert!(MultiProcessDatabase::create(&path).is_err());
    assert_eq!(
        b"someone else's file".to_vec(),
        std::fs::read(path.join("data.redb.tmp")).unwrap()
    );
}

/// The other half of that: once the database *has* been accepted, a temporary file next to it is
/// the wreckage of an earlier attempt and is cleared, so that a later `create()` can never find one
/// to move over a database that was finished.
#[test]
fn a_successful_create_clears_a_stale_temporary_file() {
    let dir = tempdir();
    let path = db_path(&dir);
    {
        let db = MultiProcessDatabase::create(&path).unwrap();
        write(&db, 0, 7);
    }
    std::fs::write(path.join("data.redb.tmp"), b"wreckage").unwrap();

    {
        let db = MultiProcessDatabase::create(&path).unwrap();
        assert_eq!(Some(7), read(&db, 0));
    }
    assert!(!path.join("data.redb.tmp").exists());

    // And the database is still the one that was there, rather than whatever the temporary held
    let db = MultiProcessDatabase::open(&path).unwrap();
    assert_eq!(Some(7), read(&db, 0));
}

/// The permission shape `a_directory_that_cannot_be_read_still_opens` covers, but for a directory
/// whose marker never landed: listing it is how a foreign directory is recognized, and a directory
/// that cannot be listed must not therefore become one a `create()` refuses to finish.
#[test]
#[cfg(unix)]
fn an_interrupted_create_finishes_in_a_directory_that_cannot_be_read() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir();
    let path = db_path(&dir);
    {
        let db = MultiProcessDatabase::create(&path).unwrap();
        write(&db, 0, 7);
    }
    // The state a create() that died just before writing the marker leaves behind
    std::fs::remove_file(path.join("metadata")).unwrap();

    let readable = std::fs::metadata(&path).unwrap().permissions();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o311)).unwrap();
    let created = MultiProcessDatabase::create(&path).map(|db| read(&db, 0));
    std::fs::set_permissions(&path, readable).unwrap();

    assert_eq!(Some(7), created.unwrap());
    assert!(path.join("metadata").is_file());
}

/// `data.redb.tmp` is redb's name too, so the foreign-directory rule lets it through, and the
/// recovery path unlinks it on the way to redoing an interrupted create. Without a `write.lock`
/// beside it there was no such create, so the file is somebody else's and must survive.
#[test]
fn create_does_not_delete_an_orphaned_temporary() {
    let dir = tempdir();
    let path = db_path(&dir);
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("data.redb.tmp"), b"someone else's file").unwrap();

    assert!(MultiProcessDatabase::create(&path).is_err());
    assert_eq!(
        b"someone else's file".to_vec(),
        std::fs::read(path.join("data.redb.tmp")).unwrap()
    );
}

/// A marked directory has held a finished database, so a missing `data.redb` with a temporary beside
/// it means the promoting rename did not survive a crash -- that temporary *is* the database.
/// Discarding it would lose it, which is the one case where the temporary is not wreckage.
#[test]
fn a_temporary_left_by_a_lost_promotion_is_not_discarded() {
    let dir = tempdir();
    let path = db_path(&dir);
    {
        let db = MultiProcessDatabase::create(&path).unwrap();
        write(&db, 0, 7);
    }
    // The state a crash between the promoting rename and the marker's own sync can leave
    std::fs::rename(path.join("data.redb"), path.join("data.redb.tmp")).unwrap();

    assert!(MultiProcessDatabase::create(&path).is_err());
    assert!(path.join("data.redb.tmp").is_file());
    assert!(!path.join("data.redb").exists());
}

/// Clearing a stale temporary is tidying, not part of opening the database, so a directory whose
/// entries cannot be changed must not turn `create()` into an error where `open()` succeeds.
#[test]
#[cfg(unix)]
fn a_stale_temporary_that_cannot_be_removed_does_not_fail_the_open() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir();
    let path = db_path(&dir);
    {
        let db = MultiProcessDatabase::create(&path).unwrap();
        write(&db, 0, 7);
    }
    std::fs::write(path.join("data.redb.tmp"), b"wreckage").unwrap();

    let writable = std::fs::metadata(&path).unwrap().permissions();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o555)).unwrap();
    let created = MultiProcessDatabase::create(&path).map(|db| read(&db, 0));
    std::fs::set_permissions(&path, writable).unwrap();

    assert_eq!(Some(7), created.unwrap());
}
=======
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
>>>>>>> b508cef (Add a multi-process safe database interface)
