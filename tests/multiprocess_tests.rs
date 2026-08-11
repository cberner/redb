#![cfg(not(target_os = "wasi"))]

use redb::{
    Builder, Database, Durability, ReadableDatabase, ReadableTable, SetDurabilityError,
    TableDefinition,
};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const TABLE: TableDefinition<u64, u64> = TableDefinition::new("data");
const CHILD_ACTION: &str = "REDB_MULTIPROCESS_TEST_ACTION";
const CHILD_MULTIPLE_WRITERS: &str = "REDB_MULTIPROCESS_TEST_MULTIPLE_WRITERS";

fn multiprocess_builder(multiple_writers: bool) -> Builder {
    let mut builder = Database::builder();
    builder.set_multiprocess_multiple_writers(multiple_writers);
    builder
}

fn create_multiprocess(path: &Path, multiple_writers: bool) -> Database {
    multiprocess_builder(multiple_writers)
        .create_multiprocess(path)
        .unwrap()
}

fn open_multiprocess(path: &Path, multiple_writers: bool) -> Database {
    multiprocess_builder(multiple_writers)
        .open_multiprocess(path)
        .unwrap()
}

fn write_value(db: &Database, value: u64) {
    let transaction = db.begin_write().unwrap();
    transaction
        .open_table(TABLE)
        .unwrap()
        .insert(&0, &value)
        .unwrap();
    transaction.commit().unwrap();
}

fn read_value(db: &Database) -> u64 {
    let transaction = db.begin_read().unwrap();
    transaction
        .open_table(TABLE)
        .unwrap()
        .get(&0)
        .unwrap()
        .unwrap()
        .value()
}

fn wait_for_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !path.exists() {
        assert!(Instant::now() < deadline, "timed out waiting for {path:?}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn opened_file(ready: &Path) -> PathBuf {
    let mut path = ready.as_os_str().to_os_string();
    path.push(".opened");
    path.into()
}

fn spawn_child(
    action: &str,
    database: &Path,
    ready: &Path,
    release: Option<&Path>,
    multiple_writers: bool,
) -> Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg("--exact")
        .arg("multiprocess_child")
        .arg("--nocapture")
        .env(CHILD_ACTION, action)
        .env(
            CHILD_MULTIPLE_WRITERS,
            u8::from(multiple_writers).to_string(),
        )
        .env("REDB_MULTIPROCESS_DATABASE", database)
        .env("REDB_MULTIPROCESS_READY", ready);
    if let Some(release) = release {
        command.env("REDB_MULTIPROCESS_RELEASE", release);
    }
    command.spawn().unwrap()
}

fn wait_for_success(mut child: Child) {
    let status = child.wait().unwrap();
    assert!(status.success(), "child failed with {status}");
}

fn wait_for_success_with_timeout(mut child: Child) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "child failed with {status}");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!("child timed out");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn multiprocess_child() {
    let Ok(action) = std::env::var(CHILD_ACTION) else {
        return;
    };
    let database = PathBuf::from(std::env::var_os("REDB_MULTIPROCESS_DATABASE").unwrap());
    let ready = PathBuf::from(std::env::var_os("REDB_MULTIPROCESS_READY").unwrap());
    let multiple_writers = std::env::var(CHILD_MULTIPLE_WRITERS).unwrap() == "1";
    let db = open_multiprocess(&database, multiple_writers);

    match action.as_str() {
        "reader" => {
            let transaction = db.begin_read().unwrap();
            let table = transaction.open_table(TABLE).unwrap();
            assert_eq!(table.get(&0).unwrap().unwrap().value(), 0);
            std::fs::write(&ready, []).unwrap();
            let release = PathBuf::from(std::env::var_os("REDB_MULTIPROCESS_RELEASE").unwrap());
            wait_for_file(&release);
            assert_eq!(table.get(&0).unwrap().unwrap().value(), 0);
            drop(table);
            drop(transaction);
            assert_eq!(read_value(&db), 20);
        }
        "growth-reader" => {
            let transaction = db.begin_read().unwrap();
            let table = transaction.open_table(TABLE).unwrap();
            assert_eq!(table.get(&0).unwrap().unwrap().value(), 0);
            std::fs::write(&ready, []).unwrap();
            let release = PathBuf::from(std::env::var_os("REDB_MULTIPROCESS_RELEASE").unwrap());
            wait_for_file(&release);
            assert_eq!(table.get(&0).unwrap().unwrap().value(), 0);
            drop(table);
            drop(transaction);
            assert_eq!(read_value(&db), 42);
        }
        "writer" => {
            std::fs::write(opened_file(&ready), []).unwrap();
            let transaction = db.begin_write().unwrap();
            std::fs::write(&ready, []).unwrap();
            transaction
                .open_table(TABLE)
                .unwrap()
                .insert(&0, &99)
                .unwrap();
            transaction.commit().unwrap();
        }
        "crash-reader" => {
            let _transaction = db.begin_read().unwrap();
            std::fs::write(&ready, []).unwrap();
            std::process::exit(0);
        }
        "crash-writer" => {
            let transaction = db.begin_write().unwrap();
            transaction
                .open_table(TABLE)
                .unwrap()
                .insert(&0, &77)
                .unwrap();
            std::fs::write(&ready, []).unwrap();
            std::process::exit(0);
        }
        "lock-order" => {
            let db2 = Arc::new(open_multiprocess(
                &PathBuf::from(std::env::var_os("REDB_MULTIPROCESS_DATABASE").unwrap()),
                multiple_writers,
            ));
            let first_writer = db.begin_write().unwrap();

            let writer_db = db2.clone();
            let writer = thread::spawn(move || {
                writer_db.begin_write().unwrap().commit().unwrap();
            });
            thread::sleep(Duration::from_millis(100));

            let reader_db = db2.clone();
            let reader = thread::spawn(move || {
                drop(reader_db.begin_read().unwrap());
            });
            thread::sleep(Duration::from_millis(100));

            first_writer.commit().unwrap();
            writer.join().unwrap();
            reader.join().unwrap();
            std::fs::write(&ready, []).unwrap();
        }
        other => panic!("unknown child action: {other}"),
    }
}

#[test]
fn directory_layout_reopen_and_durability() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let db1 = Database::create_multiprocess(&path).unwrap();

    assert!(path.join("database.redb").is_file());
    assert!(path.join("locks/initialization.lock").is_file());
    assert!(path.join("locks/writer.lock").is_file());
    assert!(path.join("locks/reader-gate.lock").is_file());
    assert!(path.join("locks/protocol-v1").is_file());

    write_value(&db1, 1);
    let db2 = Database::open_multiprocess(&path).unwrap();
    assert_eq!(read_value(&db2), 1);

    let mut transaction = db1.begin_write().unwrap();
    assert!(matches!(
        transaction.set_durability(Durability::None),
        Err(SetDurabilityError::MultiProcessDurabilityRequired)
    ));
    transaction.abort().unwrap();

    assert!(multiprocess_builder(true).open_multiprocess(&path).is_err());
}

#[test]
fn write_transaction_keeps_multiprocess_database_open() {
    let root = tempfile::tempdir().unwrap();
    for multiple_writers in [false, true] {
        let name = if multiple_writers {
            "multiple"
        } else {
            "single"
        };
        let database = root.path().join(name);
        let db = create_multiprocess(&database, multiple_writers);
        let transaction = db.begin_write().unwrap();
        drop(db);

        transaction
            .open_table(TABLE)
            .unwrap()
            .insert(&0, &17)
            .unwrap();
        transaction.commit().unwrap();

        let reopened = open_multiprocess(&database, multiple_writers);
        assert_eq!(read_value(&reopened), 17);
    }
}

#[test]
fn persistent_savepoints_are_synchronized_between_processes() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("database");
    let db1 = create_multiprocess(&database, true);
    write_value(&db1, 0);
    let db2 = open_multiprocess(&database, true);

    let transaction = db1.begin_write().unwrap();
    let savepoint_id = transaction.persistent_savepoint().unwrap();
    transaction.commit().unwrap();

    for value in 1..=5 {
        write_value(&db2, value);
    }
    let mut transaction = db2.begin_write().unwrap();
    let savepoint = transaction.get_persistent_savepoint(savepoint_id).unwrap();
    transaction.restore_savepoint(&savepoint).unwrap();
    transaction.commit().unwrap();
    assert_eq!(read_value(&db1), 0);

    let transaction = db1.begin_write().unwrap();
    assert!(
        transaction
            .delete_persistent_savepoint(savepoint_id)
            .unwrap()
    );
    transaction.commit().unwrap();
}

#[test]
fn single_writer_ownership_is_held_until_database_drop() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("database");
    let ready = root.path().join("child-writer-ready");
    let db = Database::create_multiprocess(&database).unwrap();
    write_value(&db, 0);

    let child = spawn_child("writer", &database, &ready, None, false);
    wait_for_file(&opened_file(&ready));
    thread::sleep(Duration::from_millis(250));
    assert!(!ready.exists(), "second process acquired writer ownership");

    drop(db);
    wait_for_file(&ready);
    wait_for_success(child);
    assert_eq!(
        read_value(&Database::open_multiprocess(&database).unwrap()),
        99
    );
}

#[test]
fn multiple_writers_are_serialized_by_transaction() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("database");
    let ready = root.path().join("child-writer-ready");
    let db = create_multiprocess(&database, true);
    write_value(&db, 0);
    assert!(Database::open_multiprocess(&database).is_err());

    let transaction = db.begin_write().unwrap();
    let child = spawn_child("writer", &database, &ready, None, true);
    wait_for_file(&opened_file(&ready));
    thread::sleep(Duration::from_millis(250));
    assert!(!ready.exists(), "second writer acquired the lock too early");

    transaction.abort().unwrap();
    wait_for_file(&ready);
    wait_for_success(child);
    assert_eq!(read_value(&db), 99);
}

#[test]
fn readers_keep_their_snapshot_during_cross_process_writes() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("database");
    let db = Database::create_multiprocess(&database).unwrap();
    write_value(&db, 0);

    let mut children = vec![];
    let mut ready_files = vec![];
    let release = root.path().join("release-readers");
    for index in 0..6 {
        let ready = root.path().join(format!("reader-{index}-ready"));
        children.push(spawn_child(
            "reader",
            &database,
            &ready,
            Some(&release),
            false,
        ));
        ready_files.push(ready);
    }
    for ready in &ready_files {
        wait_for_file(ready);
    }

    for value in 1..=20 {
        write_value(&db, value);
    }
    std::fs::write(&release, []).unwrap();
    for child in children {
        wait_for_success(child);
    }
    assert_eq!(read_value(&db), 20);
}

#[test]
fn reader_can_open_while_writer_has_grown_the_file() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("database");
    let database_file = database.join("database.redb");
    let ready = root.path().join("growth-reader-ready");
    let release = root.path().join("release-growth-reader");
    let db = Database::create_multiprocess(&database).unwrap();
    write_value(&db, 0);

    let initial_len = std::fs::metadata(&database_file).unwrap().len();
    let transaction = db.begin_write().unwrap();
    {
        let mut table = transaction.open_table(TABLE).unwrap();
        table.insert(&0, &42).unwrap();
        for value in 1..=200_000 {
            table.insert(&value, &value).unwrap();
            if value % 1_000 == 0 && std::fs::metadata(&database_file).unwrap().len() > initial_len
            {
                break;
            }
        }
    }
    assert!(std::fs::metadata(&database_file).unwrap().len() > initial_len);

    let child = spawn_child("growth-reader", &database, &ready, Some(&release), false);
    wait_for_file(&ready);
    transaction.commit().unwrap();
    std::fs::write(&release, []).unwrap();
    wait_for_success(child);
}

#[test]
fn crashed_reader_slot_does_not_pin_the_database() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("database");
    let ready = root.path().join("crash-reader-ready");
    let db = Database::create_multiprocess(&database).unwrap();
    write_value(&db, 0);

    let child = spawn_child("crash-reader", &database, &ready, None, false);
    wait_for_file(&ready);
    wait_for_success(child);

    for value in 1..=5 {
        write_value(&db, value);
    }
    assert_eq!(read_value(&db), 5);
}

#[test]
fn crashed_writer_releases_ownership_in_both_modes() {
    let root = tempfile::tempdir().unwrap();
    for multiple_writers in [false, true] {
        let name = if multiple_writers {
            "multiple"
        } else {
            "single"
        };
        let database = root.path().join(name);
        let ready = root.path().join(format!("{name}-crash-writer-ready"));
        let db = create_multiprocess(&database, multiple_writers);

        let child = spawn_child("crash-writer", &database, &ready, None, multiple_writers);
        wait_for_file(&ready);
        wait_for_success(child);

        write_value(&db, 88);
        assert_eq!(read_value(&db), 88);
    }
}

#[test]
fn queued_reader_does_not_block_current_writer_commit() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("database");
    let ready = root.path().join("lock-order-ready");
    let db = create_multiprocess(&database, true);
    write_value(&db, 0);
    drop(db);

    let child = spawn_child("lock-order", &database, &ready, None, true);
    wait_for_success_with_timeout(child);
    assert!(ready.exists());
}
