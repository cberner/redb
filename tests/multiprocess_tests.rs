//! Tests for the multi-process database interface.
//!
//! The lock is exercised both from a second handle in this process -- file locks belong to the
//! open file description rather than the process, so a second handle is excluded exactly as a
//! second process is -- and from real child processes.

#![cfg(all(feature = "experimental-multiprocess", not(target_os = "wasi")))]

use redb::{Database, DatabaseError, MultiProcessDatabase, ReadOnlyDatabase};
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

fn tempdir() -> TempDir {
    TempDir::new().unwrap()
}

fn db_path(dir: &TempDir) -> PathBuf {
    dir.path().join("db")
}

#[test]
fn create_and_reopen() {
    let dir = tempdir();
    let path = db_path(&dir);
    drop(MultiProcessDatabase::create(&path).unwrap());

    drop(MultiProcessDatabase::open(&path).unwrap());

    // create() on an existing database opens it rather than starting over
    drop(MultiProcessDatabase::create(&path).unwrap());
}

#[test]
fn create_makes_a_directory() {
    let dir = tempdir();
    let path = db_path(&dir);
    let _db = MultiProcessDatabase::create(&path).unwrap();

    assert!(path.join("data.redb").is_file());
    assert!(path.join("write.lock").is_file());
    assert!(path.join("metadata").is_file());
}

#[test]
fn a_second_handle_is_rejected() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = MultiProcessDatabase::create(&path).unwrap();

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
    drop(MultiProcessDatabase::open(&path).unwrap());
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

/// A create() that made the directory but died before the database file was initialized must not
/// leave the directory permanently unopenable: a later create() finishes the job, exactly as it
/// would for a `Database` whose file was created and then not written to.
#[test]
fn an_interrupted_create_can_be_redone() {
    let dir = tempdir();
    let path = db_path(&dir);
    drop(MultiProcessDatabase::create(&path).unwrap());
    std::fs::write(path.join("data.redb"), []).unwrap();

    drop(MultiProcessDatabase::create(&path).unwrap());
}

/// The write lock is invisible to a process that reaches past the directory and opens the database
/// file itself, so that file carries the ordinary exclusive lock as well. Readers are turned away
/// along with writers: nothing coordinates a reader that attaches this way with the pages the
/// process holding the directory frees.
#[test]
fn an_ordinary_database_cannot_open_the_data_file() {
    let dir = tempdir();
    let path = db_path(&dir);
    let db = MultiProcessDatabase::create(&path).unwrap();

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
    drop(ReadOnlyDatabase::open(&data).unwrap());
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
    match role.as_str() {
        // Expects to be turned away by the lock the parent holds
        "rejected" => {
            assert!(matches!(
                MultiProcessDatabase::open(&path),
                Err(DatabaseError::DatabaseAlreadyOpen)
            ));
        }
        // Expects to get in
        "opener" => {
            drop(MultiProcessDatabase::open(&path).unwrap());
        }
        // Opens the database and dies without closing it, leaving the lock to the operating system
        "crasher" => {
            let db = MultiProcessDatabase::open(&path).unwrap();
            let _ = &db;
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
    let _db = MultiProcessDatabase::create(&path).unwrap();

    assert!(run_child("rejected", &path).success());
}

#[test]
fn a_child_process_can_open_a_database_this_one_has_closed() {
    let dir = tempdir();
    let path = db_path(&dir);
    drop(MultiProcessDatabase::create(&path).unwrap());

    assert!(run_child("opener", &path).success());

    // ... and the child released the lock on its way out
    drop(MultiProcessDatabase::open(&path).unwrap());
}

/// The whole point of using OS locks: a process that dies holding the database releases it with no
/// cleanup and no timeouts.
#[test]
fn a_process_that_dies_releases_the_lock() {
    let dir = tempdir();
    let path = db_path(&dir);
    drop(MultiProcessDatabase::create(&path).unwrap());

    assert!(!run_child("crasher", &path).success());

    drop(MultiProcessDatabase::open(&path).unwrap());
}
