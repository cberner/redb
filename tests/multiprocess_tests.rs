#![cfg(not(target_os = "wasi"))]

use redb::multiprocess::{Builder, Database, ReadOnlyDatabase, WriterMode};
use redb::{
    DatabaseError, Durability, ReadableDatabase, ReadableTable, SavepointError, SetDurabilityError,
    StorageError, TableDefinition,
};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const TABLE: TableDefinition<u64, u64> = TableDefinition::new("data");
const GENERATIONS: TableDefinition<u64, u64> = TableDefinition::new("generations");
const COUNTER: TableDefinition<(), u64> = TableDefinition::new("counter");
const CHILD_ACTION: &str = "REDB_MULTIPROCESS_ACTION";
const HEADER_FLAGS_OFFSET: usize = 9;
const RECOVERY_REQUIRED: u8 = 1 << 1;
const TWO_PHASE_COMMIT: u8 = 1 << 2;

fn builder(mode: WriterMode) -> Builder {
    let mut builder = Builder::new();
    builder.set_writer_mode(mode).set_cache_size(1024 * 1024);
    builder
}

fn write(db: &Database, value: u64) {
    let transaction = db.begin_write().unwrap();
    transaction
        .open_table(TABLE)
        .unwrap()
        .insert(0, value)
        .unwrap();
    transaction.commit().unwrap();
}

fn read(db: &impl ReadableDatabase) -> u64 {
    db.begin_read()
        .unwrap()
        .open_table(TABLE)
        .unwrap()
        .get_owned(0)
        .unwrap()
        .unwrap()
        .value()
}

fn write_generation(db: &Database, generation: u64) {
    let transaction = db.begin_write().unwrap();
    let mut table = transaction.open_table(GENERATIONS).unwrap();
    for key in 0..1_000 {
        table.insert(key, generation).unwrap();
    }
    drop(table);
    transaction.commit().unwrap();
}

fn read_generation(db: &impl ReadableDatabase) -> u64 {
    let transaction = db.begin_read().unwrap();
    let table = transaction.open_table(GENERATIONS).unwrap();
    let generation = table.get_owned(0).unwrap().unwrap().value();
    for key in 1..1_000 {
        assert_eq!(table.get_owned(key).unwrap().unwrap().value(), generation);
    }
    generation
}

fn header_flags(path: &Path) -> u8 {
    std::fs::read(path.join("data.redb")).unwrap()[HEADER_FLAGS_OFFSET]
}

#[test]
fn directory_layout_and_table_api() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let db = Database::create(&path).unwrap();
    assert_eq!(db.writer_mode(), WriterMode::MultipleWriters);

    write(&db, 7);
    let transaction = db.begin_write().unwrap();
    let mut table: redb::Table<'_, u64, u64> = transaction.open_table(TABLE).unwrap();
    table.insert(1, 2).unwrap();
    drop(table);
    transaction.commit().unwrap();

    for name in [
        "data.redb",
        "metadata",
        "extended-header",
        "write.lock",
        "registry.lock",
    ] {
        assert!(path.join(name).is_file(), "missing {name}");
    }
    assert!(path.join("txn").is_dir());
    let metadata = std::fs::read(path.join("metadata")).unwrap();
    assert_eq!(metadata.len(), 13);
    assert_eq!(&metadata[..11], b"redbMP\x1a\x0a\xa9\x0d\x0a");
    assert_eq!(metadata[11], 1);
    assert_eq!(metadata[12], 2);
    assert_eq!(
        std::fs::metadata(path.join("extended-header"))
            .unwrap()
            .len(),
        48
    );
    assert!(matches!(
        builder(WriterMode::SingleWriter).open_read_only(&path),
        Err(DatabaseError::Storage(StorageError::Corrupted(_)))
    ));
}

#[test]
fn interrupted_initialization_can_be_retried() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("data.redb.tmp"), b"incomplete database header").unwrap();

    let db = Database::create(&path).unwrap();
    write(&db, 5);
    assert_eq!(read(&db), 5);
    assert!(path.join("data.redb").is_file());
    assert!(!path.join("data.redb.tmp").exists());
}

#[test]
fn single_writer_and_concurrent_read_only_process_role() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let writer = builder(WriterMode::SingleWriter).create(&path).unwrap();
    write(&writer, 1);

    assert!(matches!(
        Database::open(&path),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));

    let ready = root.path().join("reader-ready");
    let release = root.path().join("reader-release");
    let reader = spawn_child("read_only_updates", &path, &ready, Some(&release));
    wait_for_file(&ready);
    write(&writer, 2);
    std::fs::write(release, []).unwrap();
    wait_for_success(reader);

    wait_for_success(spawn_child(
        "expect_writer_rejected",
        &path,
        root.path().join("unused"),
        None,
    ));
}

#[test]
fn write_transaction_keeps_database_open() {
    let root = tempfile::tempdir().unwrap();
    for mode in [WriterMode::SingleWriter, WriterMode::MultipleWriters] {
        let path = root.path().join(format!("{mode:?}"));
        let db = builder(mode).create(&path).unwrap();
        let transaction = db.begin_write().unwrap();
        drop(db);

        transaction
            .open_table(TABLE)
            .unwrap()
            .insert(0, 17)
            .unwrap();
        transaction.commit().unwrap();

        assert_eq!(read(&Database::open(&path).unwrap()), 17);
    }
}

#[test]
fn multiple_writers_handoff_and_keep_old_snapshots() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let first = builder(WriterMode::MultipleWriters).create(&path).unwrap();
    let second = Database::open(&path).unwrap();
    assert_eq!(second.writer_mode(), WriterMode::MultipleWriters);

    write(&first, 0);
    let old_transaction = first.begin_read().unwrap();
    let old_table = old_transaction.open_table(TABLE).unwrap();
    assert_eq!(old_table.get_owned(0).unwrap().unwrap().value(), 0);

    for value in 1..20 {
        if value % 2 == 0 {
            write(&first, value);
        } else {
            write(&second, value);
        }
    }
    assert_eq!(old_table.get_owned(0).unwrap().unwrap().value(), 0);
    drop(old_table);
    drop(old_transaction);
    assert_eq!(read(&first), 19);
    assert_eq!(read(&second), 19);
}

#[test]
fn closing_a_stale_multiple_writer_does_not_rewrite_shared_state() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let first = builder(WriterMode::MultipleWriters).create(&path).unwrap();
    write(&first, 1);
    let second = Database::open(&path).unwrap();

    let transaction = first.begin_write().unwrap();
    let mut table = transaction.open_table(TABLE).unwrap();
    for key in 0..1_000 {
        table.insert(key, key).unwrap();
    }
    drop(table);
    transaction.abort().unwrap();

    write(&second, 2);
    drop(first);

    let observer = ReadOnlyDatabase::open(&path).unwrap();
    assert_eq!(read(&observer), 2);
    assert_ne!(header_flags(&path) & RECOVERY_REQUIRED, 0);
    drop(observer);
    drop(second);

    assert_ne!(header_flags(&path) & RECOVERY_REQUIRED, 0);
    assert_eq!(read(&Database::open(&path).unwrap()), 2);
}

#[test]
fn closing_a_multiple_writer_does_not_wait_for_another_writer() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let active = Database::create(&path).unwrap();
    let closing = Database::open(&path).unwrap();
    let transaction = active.begin_write().unwrap();

    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let closer = thread::spawn(move || {
        drop(closing);
        finished_tx.send(()).unwrap();
    });
    finished_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("closing an idle handle waited for write.lock");

    transaction.abort().unwrap();
    closer.join().unwrap();
}

#[test]
fn multiprocess_transaction_restrictions_and_persistent_savepoints() {
    let root = tempfile::tempdir().unwrap();

    for mode in [WriterMode::SingleWriter, WriterMode::MultipleWriters] {
        let mode_path = root.path().join(format!("{mode:?}"));
        let database = builder(mode).create(&mode_path).unwrap();
        let mut transaction = database.begin_write().unwrap();
        assert!(matches!(
            transaction.set_durability(Durability::None),
            Err(SetDurabilityError::MultiprocessDurabilityRequired)
        ));
        transaction.set_two_phase_commit(false);
        match mode {
            WriterMode::SingleWriter => drop(transaction.ephemeral_savepoint().unwrap()),
            WriterMode::MultipleWriters => assert!(matches!(
                transaction.ephemeral_savepoint(),
                Err(SavepointError::EphemeralSavepointUnsupported)
            )),
        }
        transaction.commit().unwrap();
        assert_ne!(header_flags(&mode_path) & TWO_PHASE_COMMIT, 0);
    }

    let path = root.path().join("database");
    let first = builder(WriterMode::MultipleWriters).create(&path).unwrap();
    let second = Database::open(&path).unwrap();
    write(&first, 1);

    let transaction = first.begin_write().unwrap();
    let savepoint = transaction.persistent_savepoint().unwrap();
    transaction.commit().unwrap();

    write(&second, 2);

    let mut transaction = second.begin_write().unwrap();
    assert_eq!(
        transaction
            .list_persistent_savepoints()
            .unwrap()
            .collect::<Vec<_>>(),
        vec![savepoint]
    );
    let saved = transaction.get_persistent_savepoint(savepoint).unwrap();
    transaction.restore_savepoint(&saved).unwrap();
    transaction.commit().unwrap();
    assert_eq!(read(&first), 1);

    let transaction = first.begin_write().unwrap();
    assert!(transaction.delete_persistent_savepoint(savepoint).unwrap());
    transaction.commit().unwrap();

    let transaction = second.begin_write().unwrap();
    assert!(
        transaction
            .list_persistent_savepoints()
            .unwrap()
            .next()
            .is_none()
    );
    transaction.abort().unwrap();
}

#[test]
fn corrupted_extended_header_uses_safe_fallback() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let writer = builder(WriterMode::MultipleWriters).create(&path).unwrap();
    let reader = ReadOnlyDatabase::open(&path).unwrap();
    write(&writer, 11);
    assert_eq!(read(&reader), 11);

    std::fs::write(path.join("extended-header"), [0; 48]).unwrap();
    assert_eq!(read(&reader), 11);
    write(&writer, 12);
    assert_eq!(read(&reader), 12);
}

#[test]
fn read_only_open_defers_data_corruption_detection() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let writer = Database::create(&path).unwrap();
    write(&writer, 11);
    drop(writer);

    std::fs::OpenOptions::new()
        .write(true)
        .open(path.join("data.redb"))
        .unwrap()
        .set_len(4096)
        .unwrap();

    let reader = ReadOnlyDatabase::open(&path).unwrap();
    assert!(reader.begin_read().is_err());
}

#[test]
fn read_only_cache_follows_reused_pages() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let writer = Database::create(&path).unwrap();
    write_generation(&writer, 1);
    let reader = ReadOnlyDatabase::open(&path).unwrap();
    assert_eq!(read_generation(&reader), 1);

    for generation in 2..20 {
        write_generation(&writer, generation);
        assert_eq!(read_generation(&reader), generation);
    }
}

#[test]
fn concurrent_writer_handles_serialize_updates() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let mut handles = vec![Database::create(&path).unwrap()];
    for _ in 1..4 {
        handles.push(Database::open(&path).unwrap());
    }

    let writers: Vec<_> = handles
        .into_iter()
        .map(|db| {
            thread::spawn(move || {
                for _ in 0..25 {
                    let transaction = db.begin_write().unwrap();
                    let mut table = transaction.open_table(COUNTER).unwrap();
                    let value = table.get(()).unwrap().map_or(0, |value| value.value());
                    table.insert((), value + 1).unwrap();
                    drop(table);
                    transaction.commit().unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }

    let observer = ReadOnlyDatabase::open(&path).unwrap();
    let transaction = observer.begin_read().unwrap();
    assert_eq!(
        transaction
            .open_table(COUNTER)
            .unwrap()
            .get_owned(())
            .unwrap()
            .unwrap()
            .value(),
        100
    );
}

#[test]
fn concurrent_read_only_snapshots_survive_writer_churn() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let writer = Database::create(&path).unwrap();
    write_generation(&writer, 1);

    let reader = ReadOnlyDatabase::open(&path).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let reader_thread = {
        let stop = stop.clone();
        thread::spawn(move || {
            ready_tx.send(()).unwrap();
            let mut snapshots = 0;
            while !stop.load(Ordering::Acquire) {
                let generation = read_generation(&reader);
                assert!((1..20).contains(&generation));
                snapshots += 1;
            }
            snapshots
        })
    };
    ready_rx.recv().unwrap();

    for generation in 2..20 {
        write_generation(&writer, generation);
    }
    stop.store(true, Ordering::Release);
    assert!(reader_thread.join().unwrap() > 0);
}

#[test]
fn persistent_savepoint_ids_are_shared_between_handles() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let first = Database::create(&path).unwrap();
    let second = Database::open(&path).unwrap();

    let mut ids = Vec::new();
    for database in [&first, &second, &first, &second] {
        let transaction = database.begin_write().unwrap();
        ids.push(transaction.persistent_savepoint().unwrap());
        transaction.commit().unwrap();
    }
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 4);

    let transaction = first.begin_write().unwrap();
    assert_eq!(transaction.list_persistent_savepoints().unwrap().count(), 4);
    transaction.abort().unwrap();
}

#[test]
fn writer_handoff_recovers_state_after_a_caught_panic() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let first = Database::create(&path).unwrap();
    let second = Database::open(&path).unwrap();

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let transaction = first.begin_write().unwrap();
        transaction.open_table(TABLE).unwrap().insert(0, 1).unwrap();
        panic!("simulated application panic");
    }));
    assert!(panic.is_err());

    write(&first, 2);
    write(&second, 3);
    assert_eq!(read(&first), 3);
}

#[test]
fn multiprocess_child() {
    let Ok(action) = std::env::var(CHILD_ACTION) else {
        return;
    };
    let database = PathBuf::from(std::env::var_os("REDB_MULTIPROCESS_DATABASE").unwrap());
    let ready = PathBuf::from(std::env::var_os("REDB_MULTIPROCESS_READY").unwrap());
    if action == "expect_writer_rejected" {
        assert!(matches!(
            Database::open(&database),
            Err(redb::DatabaseError::DatabaseAlreadyOpen)
        ));
        return;
    }
    if action == "read_only_updates" {
        let db = ReadOnlyDatabase::open(&database).unwrap();
        assert_eq!(read(&db), 1);
        std::fs::write(&ready, []).unwrap();
        let release = PathBuf::from(std::env::var_os("REDB_MULTIPROCESS_RELEASE").unwrap());
        wait_for_file(&release);
        assert_eq!(read(&db), 2);
        return;
    }
    let db = Database::open(&database).unwrap();

    match action.as_str() {
        "write" => write(&db, 99),
        "crash" => {
            let transaction = db.begin_write().unwrap();
            transaction
                .open_table(TABLE)
                .unwrap()
                .insert(0, 777)
                .unwrap();
            std::process::exit(86);
        }
        "snapshot" => {
            let transaction = db.begin_read().unwrap();
            let table = transaction.open_table(TABLE).unwrap();
            assert_eq!(table.get_owned(0).unwrap().unwrap().value(), 100);
            std::fs::write(&ready, []).unwrap();
            let release = PathBuf::from(std::env::var_os("REDB_MULTIPROCESS_RELEASE").unwrap());
            wait_for_file(&release);
            assert_eq!(table.get_owned(0).unwrap().unwrap().value(), 100);
            drop(table);
            drop(transaction);
            assert_eq!(read(&db), 123);
        }
        other => panic!("unknown child action: {other}"),
    }
}

#[test]
fn real_process_writer_handoff_and_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let parent = builder(WriterMode::MultipleWriters).create(&path).unwrap();
    write(&parent, 1);

    wait_for_success(spawn_child(
        "write",
        &path,
        root.path().join("unused"),
        None,
    ));
    assert_eq!(read(&parent), 99);

    wait_for_failure(spawn_child(
        "crash",
        &path,
        root.path().join("unused"),
        None,
    ));
    write(&parent, 100);
    assert_eq!(read(&parent), 100);

    let ready = root.path().join("ready");
    let release = root.path().join("release");
    let child = spawn_child("snapshot", &path, &ready, Some(&release));
    wait_for_file(&ready);
    for value in 100..=123 {
        write(&parent, value);
    }
    std::fs::write(release, []).unwrap();
    wait_for_success(child);
}

fn spawn_child(
    action: &str,
    database: &Path,
    ready: impl AsRef<Path>,
    release: Option<&Path>,
) -> Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg("--exact")
        .arg("multiprocess_child")
        .arg("--nocapture")
        .env(CHILD_ACTION, action)
        .env("REDB_MULTIPROCESS_DATABASE", database)
        .env("REDB_MULTIPROCESS_READY", ready.as_ref());
    if let Some(release) = release {
        command.env("REDB_MULTIPROCESS_RELEASE", release);
    }
    command.spawn().unwrap()
}

fn wait_for_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !path.exists() {
        assert!(Instant::now() < deadline, "timed out waiting for {path:?}");
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_success(mut child: Child) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "child failed: {status}");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!("child process timed out");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_failure(mut child: Child) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(!status.success(), "child unexpectedly succeeded");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!("child process timed out");
        }
        thread::sleep(Duration::from_millis(10));
    }
}
