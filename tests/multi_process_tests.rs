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

    // Nothing beyond the lock file it had to take to check at all
    let mut left: Vec<_> = std::fs::read_dir(&path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    left.sort();
    assert_eq!(vec!["data.redb", "write.lock"], left);

    // ... and having refused it once, it refuses it again rather than reading its own lock file as
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

/// A `create()` that is going to fail must fail without having deleted anything. The temporary name
/// is redb's, so a directory holding one gets past the foreign-directory check, but a `create()`
/// pointed there by mistake still has no business removing a file it did not put there.
#[test]
fn a_failed_create_leaves_a_temporary_file_alone() {
    let dir = tempdir();
    let path = db_path(&dir);
    std::fs::create_dir(&path).unwrap();
    // Under redb's own names, but not redb's: a mistyped path, not a database
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
