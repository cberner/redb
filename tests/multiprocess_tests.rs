//! Tests for the multi-process database interface.
//!
//! Most of these use two `MultiProcessDatabase` handles on one directory from a single test
//! process: file locks belong to the open file description rather than the process, so two handles
//! coordinate through the lock files exactly as two processes do, while being far easier to debug.
//! Real child processes cover what only a separate process can: a writer that dies, and the
//! coordination working across a process boundary.

#![cfg(all(feature = "experimental-multiprocess", not(target_os = "wasi")))]

use redb::{
    Database, DatabaseError, Durability, MultiProcessDatabase, ReadableDatabase, ReadableTable,
    ReadableTableMetadata, TableDefinition, WriteTransaction, WriterMode,
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

// A table big enough that rewriting it churns through many pages, for the child-process tests
const CHURN_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("churn");
const CHURN_ELEMENTS: u64 = 20_000;

fn tempdir() -> TempDir {
    TempDir::new().unwrap()
}

fn db_path(dir: &TempDir) -> PathBuf {
    dir.path().join("db")
}

fn write(db: &MultiProcessDatabase, key: u64, value: u64) {
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        table.insert(&key, &value).unwrap();
    }
    txn.commit().unwrap();
}

fn read(db: &MultiProcessDatabase, key: u64) -> Option<u64> {
    let txn = db.begin_read().unwrap();
    let table = txn.open_table(TABLE).unwrap();
    table.get_owned(key).unwrap().map(|value| value.value())
}

#[test]
fn create_and_reopen() {
    let dir = tempdir();
    let path = db_path(&dir);
    {
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
    // The exclusion this checks is what SingleWriterProcess mode promises. In the other mode a
    // second handle is admitted and serialized per transaction instead
    let db = create(&path, WriterMode::SingleWriterProcess);
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
    // SingleWriterProcess, so that a second open reports the lock is held rather than waiting for
    // it: in MultiWriterProcess mode this same test would deadlock against its own transaction
    let db = create(&path, WriterMode::SingleWriterProcess);
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

/// A read transaction does not keep the database open the way a write transaction does: dropping
/// the handle closes the storage underneath it, so the guard the transaction still holds must not
/// go on holding the directory's locks -- the next open would be refused for as long as the
/// unusable guard existed.
#[test]
fn a_lingering_read_guard_does_not_hold_the_database_open() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = create(&path, WriterMode::SingleWriterProcess);
    write(&db, 0, 1);

    let txn = db.begin_read().unwrap();
    drop(db);

    let db = MultiProcessDatabase::open(&path).unwrap();
    assert_eq!(Some(1), read(&db, 0));
    drop(txn);
}

/// The same rule for the transaction pin: in MultiWriterProcess mode a read transaction holds a
/// shared lock on its `txn/<id>` file, and a guard that outlives the close must not go on holding
/// it -- the transaction can never be read again, and the lock would stop every other process
/// reclaiming pages freed after its snapshot.
#[test]
fn a_lingering_read_guard_does_not_hold_its_pin() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = create(&path, WriterMode::MultiWriterProcess);
    write(&db, 0, 1);

    let txn = db.begin_read().unwrap();
    drop(db);

    // Every file left in txn/ must be lockable: a released pin leaves its file behind for the
    // next writer's scan, but never the lock
    for entry in std::fs::read_dir(path.join("txn")).unwrap() {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(entry.unwrap().path())
            .unwrap();
        file.try_lock().unwrap();
    }
    drop(txn);
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

// --------------------------------------------------------------------------------------------
// Tests that need a real process
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
    let generations = || -> u64 { env::var("REDB_MP_GENERATIONS").unwrap().parse().unwrap() };
    match role.as_str() {
        // -- the directory and its lock ------------------------------------------------------
        // Expects to be turned away by the lock the parent holds
        "rejected" => {
            assert!(matches!(
                MultiProcessDatabase::open(&path),
                Err(DatabaseError::DatabaseAlreadyOpen)
            ));
        }
        // Expects to get in, and leaves a value behind for the parent to find
        "single_write" => {
            let db = MultiProcessDatabase::open(&path).unwrap();
            write(&db, 0, 7);
        }
        // Opens the database and dies without closing it, leaving the lock to the operating system
        "abort_after_write" => {
            let db = MultiProcessDatabase::open(&path).unwrap();
            write(&db, 0, 9);
            std::process::abort();
        }
        // -- the cross-process protocol ------------------------------------------------------
        // Writes generations until told to stop, so that the parent can read across them
        "writer" => {
            let db = MultiProcessDatabase::open(&path).unwrap();
            for generation in 2..=generations() {
                write_generation(&db, generation);
            }
        }
        // Reads snapshots until the writer has reached the last generation, checking every one
        "reader" => {
            let db = MultiProcessDatabase::open_read_only(&path).unwrap();
            let mut last = 0;
            let mut seen = 0;
            while last < generations() {
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
                Err(DatabaseError::DatabaseAlreadyOpen)
            ));
        }
        // Pins a transaction with a read and dies holding it, leaving its file in txn/
        "abort_with_read" => {
            let db = MultiProcessDatabase::open(&path).unwrap();
            let txn = db.begin_read().unwrap();
            let _ = spot_check(&txn);
            std::process::abort();
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

fn run_child(role: &str, path: &Path) -> std::process::ExitStatus {
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
        .status()
        .unwrap()
}

#[test]
fn a_child_process_cannot_open_a_database_this_one_holds() {
    let dir = tempdir();
    let path = db_path(&dir);
    // SingleWriterProcess, which is the mode that promises this: the other one admits the child and
    // serializes it against this process one transaction at a time
    let db = create(&path, WriterMode::SingleWriterProcess);
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

    assert!(run_child("single_write", &path).success());

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

    assert!(!run_child("abort_after_write", &path).success());

    // Nothing has to clean up after it: the operating system dropped its lock when it died
    let db = MultiProcessDatabase::open(&path).unwrap();
    assert_eq!(Some(9), read(&db, 0));
    write(&db, 0, 10);
    assert_eq!(Some(10), read(&db, 0));
}

/// A directory holding a plain [`Database`] under `data.redb` is someone else's data, not an
/// interrupted create: without the marker, the missing `write.lock` beside the file is what tells
/// the two apart, and create() refuses rather than adopting the file.
#[test]
fn create_does_not_adopt_a_plain_database() {
    let dir = tempdir();
    let path = db_path(&dir);
    std::fs::create_dir(&path).unwrap();
    drop(Database::create(path.join("data.redb")).unwrap());

    assert!(MultiProcessDatabase::create(&path).is_err());

    // The refusal left no trace, and the file is still an ordinary database
    assert!(!path.join("write.lock").exists());
    assert!(!path.join("metadata").exists());
    drop(Database::open(path.join("data.redb")).unwrap());
}

/// The same refusal covers every name create() writes: a directory already using one of them
/// belongs to something else, and is left exactly as it was found.
#[test]
fn create_does_not_clobber_files_in_a_directory_it_did_not_make() {
    let contents: &[u8] = b"not redb's, and bigger than its marker";
    let dir = tempdir();
    let path = db_path(&dir);
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("metadata.tmp"), contents).unwrap();

    assert!(MultiProcessDatabase::create(&path).is_err());

    assert!(!path.join("write.lock").exists());
    assert_eq!(contents, &std::fs::read(path.join("metadata.tmp")).unwrap());

    // Even beside a lock file, a temporary bigger than the marker cannot be an interrupted create
    // of redb's, and finishing one would delete it
    std::fs::write(path.join("write.lock"), []).unwrap();
    assert!(MultiProcessDatabase::create(&path).is_err());
    assert_eq!(contents, &std::fs::read(path.join("metadata.tmp")).unwrap());
}

/// An empty lock file is the only kind redb ever writes, so one with contents was put there by
/// something else, and does not make the directory an interrupted create.
#[test]
fn create_does_not_trust_a_nonempty_lock_file() {
    let dir = tempdir();
    let path = db_path(&dir);
    std::fs::create_dir(&path).unwrap();
    drop(Database::create(path.join("data.redb")).unwrap());
    std::fs::write(path.join("write.lock"), b"pid 1234").unwrap();

    assert!(MultiProcessDatabase::create(&path).is_err());

    assert!(!path.join("metadata").exists());
    assert_eq!(
        b"pid 1234",
        &std::fs::read(path.join("write.lock")).unwrap()[..]
    );
    drop(Database::open(path.join("data.redb")).unwrap());
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
#[test]
fn create_does_not_mark_a_directory_it_rejects() {
    let dir = tempdir();
    let path = dir.path().join("not-a-db");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("data.redb"), b"this is not a redb database").unwrap();

    assert!(MultiProcessDatabase::create(&path).is_err());
    assert!(!path.join("metadata").exists());

    // ... and the file it refused to read is still there, byte for byte, with nothing beside it
    let mut left: Vec<_> = std::fs::read_dir(&path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    left.sort();
    assert_eq!(vec!["data.redb"], left);
    assert_eq!(
        b"this is not a redb database",
        &std::fs::read(path.join("data.redb")).unwrap()[..]
    );

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

    // The same crash with the marker already durable -- initialization was itself a redo -- is
    // recovered the same way rather than refused: the zeroed magic is what says the temporary is
    // wreckage, not the marker's absence
    std::fs::rename(path.join("data.redb"), path.join("data.redb.tmp")).unwrap();
    let mut partial = std::fs::read(path.join("data.redb.tmp")).unwrap();
    partial[0..9].fill(0);
    std::fs::write(path.join("data.redb.tmp"), &partial).unwrap();

    let db = MultiProcessDatabase::create(&path).unwrap();
    write(&db, 0, 2);
    drop(db);
    assert!(path.join("data.redb").is_file());
    assert!(!path.join("data.redb.tmp").exists());
}

/// A temporary bearing redb's magic number is a finished database -- the magic is written last --
/// left under the wrong name by a promoting rename that did not survive a crash. It is refused
/// for a person to recover, marker or no marker, rather than deleted as wreckage.
#[test]
fn a_temporary_holding_a_finished_database_is_not_discarded() {
    let dir = tempdir();
    let path = db_path(&dir);
    {
        let db = MultiProcessDatabase::create(&path).unwrap();
        write(&db, 0, 7);
    }
    std::fs::rename(path.join("data.redb"), path.join("data.redb.tmp")).unwrap();
    let saved = std::fs::read(path.join("data.redb.tmp")).unwrap();

    assert!(MultiProcessDatabase::create(&path).is_err());

    // ... even when the marker rename was lost along with the database file's
    std::fs::remove_file(path.join("metadata")).unwrap();
    assert!(MultiProcessDatabase::create(&path).is_err());
    assert_eq!(saved, std::fs::read(path.join("data.redb.tmp")).unwrap());

    // Renaming it back, as the error says to, recovers the database and the data in it
    std::fs::rename(path.join("data.redb.tmp"), path.join("data.redb")).unwrap();
    let db = MultiProcessDatabase::create(&path).unwrap();
    assert_eq!(Some(7), read(&db, 0));
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
fn create_makes_a_directory_of_lock_files() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = create(&path, WriterMode::MultiWriterProcess);
    write_generation(&db, 1);

    assert!(path.join("data.redb").is_file());
    assert!(path.join("metadata").is_file());
    assert!(path.join("extended-header").is_file());
    assert!(path.join("write.lock").is_file());
    assert!(path.join("registry.lock").is_file());
    assert!(path.join("txn").is_dir());
    // Nothing is pinned: the handle has no read transaction open
    assert_eq!(0, std::fs::read_dir(path.join("txn")).unwrap().count());

    // ... and one that does pins it under its own transaction id
    let read = db.begin_read().unwrap();
    let pinned: Vec<_> = std::fs::read_dir(path.join("txn"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(1, pinned.len(), "{pinned:?}");
    drop(read);
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

/// A write transaction that dies in a panic the caller catches leaves its pages allocated until
/// another process takes over, but the bytes it wrote must die with it. A page that overflowed
/// the write buffer mid-transaction was written to the file and, once read back, cached like any
/// committed page -- yet the pages themselves are free in every committed state, so the next
/// writer fills them without the collection that normally drops a stale cache. The reader's pin
/// here holds the collection horizon still while the pages change hands.
#[test]
fn a_caught_panic_does_not_poison_the_page_cache() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = MultiProcessDatabase::builder()
        .set_writer_mode(WriterMode::MultiWriterProcess)
        // Small enough that the doomed transaction below overflows the write buffer, putting its
        // pages in the file -- and, where they are read back, the page cache -- before the panic
        .set_cache_size(1024 * 1024)
        .create(&path)
        .unwrap();
    write(&db, 0, 1);

    let pin = db.begin_read().unwrap();

    let doomed = vec![0xAAu8; 4096];
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(BIG_TABLE).unwrap();
            for key in 0..1024u64 {
                table.insert(&key, doomed.as_slice()).unwrap();
            }
            // Pull the spilled pages back in through the cache
            for key in 0..1024u64 {
                assert!(table.get(&key).unwrap().is_some());
            }
        }
        panic!("the transaction dies with its pages in the cache");
    }));
    assert!(panicked.is_err());

    let second = open(&path);
    let committed = vec![0x55u8; 4096];
    let txn = second.begin_write().unwrap();
    {
        let mut table = txn.open_table(BIG_TABLE).unwrap();
        for key in 0..1024u64 {
            table.insert(&key, committed.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    // The first handle must see what was committed, not what its dead transaction wrote. Probed
    // key by key: scanning the table end to end would stream enough fresh pages through the small
    // cache to evict the dead transaction's entries before the scan reached their offsets.
    let txn = db.begin_read().unwrap();
    let table = txn.open_table(BIG_TABLE).unwrap();
    for key in (0..1024u64).step_by(16) {
        match table.get_owned(key).unwrap() {
            Some(value) => assert!(
                value.value().iter().all(|&byte| byte == 0x55),
                "key {key} was served the dead transaction's bytes"
            ),
            None => panic!("key {key} was lost to a stale page from the dead transaction"),
        }
    }
    drop(txn);
    drop(pin);
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
    let durable = db.last_durable_commit().unwrap();

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
    // ... but not to anyone else, until a durable commit publishes it -- and not reported as
    // durable, even by the process that made it
    let reader = MultiProcessDatabase::open_read_only(&path).unwrap();
    assert_eq!(1, read_generation(&reader.begin_read().unwrap()));
    assert_eq!(durable, db.last_durable_commit().unwrap());

    write_generation(&db, 3);
    assert_eq!(3, read_generation(&reader.begin_read().unwrap()));
    assert!(db.last_durable_commit().unwrap() > durable);
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

#[test]
fn churn_with_a_single_writer_process() {
    churn(WriterMode::SingleWriterProcess, 2000);
}

#[test]
fn churn_with_multiple_writer_processes() {
    churn(WriterMode::MultiWriterProcess, 2000);
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

    // The child pins a transaction in txn/ with a read, then dies with the file still there
    let mut child = spawn_child("abort_with_read", &path, 0);
    assert!(!child.wait().unwrap().success());
    assert!(std::fs::read_dir(path.join("txn")).unwrap().count() > 0);

    // The dead process's pin must not stop this one from reclaiming pages, which it would if the
    // file's presence were trusted without checking whether anyone still holds it locked
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
