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

/// A create() that made the directory and the marker but died before the database file was
/// initialized must not leave the directory permanently unopenable: a later create() finishes the
/// job, exactly as it would for a `Database` whose file was created and then not written to.
#[test]
fn an_interrupted_create_can_be_redone() {
    let dir = tempdir();
    let path = db_path(&dir);
    drop(MultiProcessDatabase::create(&path).unwrap());
    std::fs::write(path.join("data.redb"), []).unwrap();

    drop(MultiProcessDatabase::create(&path).unwrap());
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

    drop(MultiProcessDatabase::open(&path).unwrap());
    drop(MultiProcessDatabase::create(&path).unwrap());
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
/// `data.redb` it would wedge the directory for good -- indistinguishable from a file this call
/// was pointed at by mistake. Initializing under a temporary name is what keeps the two apart.
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

    drop(MultiProcessDatabase::create(&path).unwrap());

    assert!(path.join("data.redb").is_file());
    assert!(!path.join("data.redb.tmp").exists());

    // The same crash with the marker already durable -- initialization was itself a redo -- is
    // recovered the same way rather than refused: the zeroed magic is what says the temporary is
    // wreckage, not the marker's absence
    std::fs::rename(path.join("data.redb"), path.join("data.redb.tmp")).unwrap();
    let mut partial = std::fs::read(path.join("data.redb.tmp")).unwrap();
    partial[0..9].fill(0);
    std::fs::write(path.join("data.redb.tmp"), &partial).unwrap();

    drop(MultiProcessDatabase::create(&path).unwrap());
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
    drop(MultiProcessDatabase::create(&path).unwrap());
    std::fs::rename(path.join("data.redb"), path.join("data.redb.tmp")).unwrap();
    let saved = std::fs::read(path.join("data.redb.tmp")).unwrap();

    assert!(MultiProcessDatabase::create(&path).is_err());

    // ... even when the marker rename was lost along with the database file's
    std::fs::remove_file(path.join("metadata")).unwrap();
    assert!(MultiProcessDatabase::create(&path).is_err());
    assert_eq!(saved, std::fs::read(path.join("data.redb.tmp")).unwrap());

    // Renaming it back, as the error says to, recovers the database
    std::fs::rename(path.join("data.redb.tmp"), path.join("data.redb")).unwrap();
    drop(MultiProcessDatabase::create(&path).unwrap());
}

/// A temporary file only ever means "an attempt that did not finish", so one sitting next to a
/// database that *did* finish must not be moved over it: promoting on the mere presence of a
/// temporary would swap a good database for wreckage.
#[test]
fn a_stale_temporary_does_not_replace_the_database() {
    let dir = tempdir();
    let path = db_path(&dir);
    drop(MultiProcessDatabase::create(&path).unwrap());
    std::fs::write(path.join("data.redb.tmp"), b"stale wreckage").unwrap();

    // create() opens the finished database -- the wreckage would have been refused -- and throws
    // the temporary away
    drop(MultiProcessDatabase::create(&path).unwrap());
    assert!(!path.join("data.redb.tmp").exists());
    drop(MultiProcessDatabase::open(&path).unwrap());
}
