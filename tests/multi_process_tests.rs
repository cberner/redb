//! Tests for the multi-process database interface.
//!
//! At this step the interface allows only one process to have the database open, so what there is
//! to test is the directory layout and the lock file that enforces that. The lock is exercised
//! both from a second handle in this process -- file locks belong to the open file description
//! rather than the process, so a second handle is excluded exactly as a second process is -- and
//! from a real child process, which is what the lock is ultimately for.

#![cfg(all(feature = "experimental-multiprocess", not(target_os = "wasi")))]

use redb::{
    Database, DatabaseError, MultiProcessDatabase, ReadOnlyDatabase, ReadableDatabase,
    TableDefinition, WriteTransaction,
};
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

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
        // Expects to get in, and leaves a value behind for the parent to find
        "writer" => {
            let db = MultiProcessDatabase::open(&path).unwrap();
            write(&db, 0, 7);
        }
        // Opens the database and dies without closing it, leaving the lock to the operating system
        "crasher" => {
            let db = MultiProcessDatabase::open(&path).unwrap();
            write(&db, 0, 9);
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
