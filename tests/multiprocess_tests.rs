#![cfg(feature = "experimental-multiprocess")]

use redb::backends::InMemoryBackend;
use redb::{ConcurrencyMode, Database, DatabaseError, StorageError};

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
mod shared_reader {
    use super::*;
    use redb::{ReadableDatabase, ReadableTable, TableDefinition};
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    fn shared() -> redb::Builder {
        let mut builder = Database::builder();
        builder.set_concurrency_mode(ConcurrencyMode::MultiWriterProcess);
        builder
    }

    fn create(path: &Path) -> Database {
        let db = shared().create(path).unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(TABLE).unwrap();
            table.insert(0, 0).unwrap();
        }
        write.commit().unwrap();
        db
    }

    fn value(db: &impl ReadableDatabase) -> u64 {
        let read = db.begin_read().unwrap();
        let table = read.open_table(TABLE).unwrap();
        table.get(0).unwrap().unwrap().value()
    }

    /// Following a peer's commits means seeing its data, not only its newer id
    #[test]
    fn a_shared_reader_sees_a_peers_writes() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let writer = create(tmpfile.path());
        let reader = shared().open_read_only(tmpfile.path()).unwrap();

        // Caches the page the value lives on
        assert_eq!(value(&reader), 0);

        // Staleness needs the allocator to recycle the cached page, so churn until it does
        for expected in 1..60u64 {
            let write = writer.begin_write().unwrap();
            {
                let mut table = write.open_table(TABLE).unwrap();
                table.insert(0, expected).unwrap();
            }
            write.commit().unwrap();

            assert_eq!(
                value(&reader),
                expected,
                "the reader served a cached page after the writer committed {expected}"
            );
        }
    }

    /// A shared reader reads pages by the immutable geometry alone, so the region counts, which an
    /// unclean header leaves unvalidated, never reach it
    #[test]
    fn a_shared_reader_ignores_the_layout() {
        use std::io::{Seek, SeekFrom, Write};

        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let writer = create(tmpfile.path());
        let write = writer.begin_write().unwrap();
        {
            let mut table = write.open_table(TABLE).unwrap();
            table.insert(0, 7).unwrap();
        }
        write.commit().unwrap();

        // The region counts, torn to zero under the live writer
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(tmpfile.path())
            .unwrap();
        file.seek(SeekFrom::Start(24)).unwrap();
        file.write_all(&[0u8; 8]).unwrap();
        drop(file);

        let reader = shared().open_read_only(tmpfile.path()).unwrap();
        assert_eq!(value(&reader), 7);
        // Its close would rewrite the header, which is not what this tests
        std::mem::forget(writer);
    }

    /// Simulates power loss: once `dead`, every write is dropped
    #[derive(Debug)]
    struct CrashBackend {
        inner: redb::backends::FileBackend,
        dead: Arc<AtomicBool>,
    }

    impl redb::StorageBackend for CrashBackend {
        fn len(&self) -> Result<u64, std::io::Error> {
            self.inner.len()
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
            self.inner.read(offset, out)
        }
        fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
            if self.dead.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.inner.set_len(len)
        }
        fn sync_data(&self) -> Result<(), std::io::Error> {
            if self.dead.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.inner.sync_data()
        }
        fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
            if self.dead.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.inner.write(offset, data)
        }
    }

    /// A writer takes `SHARED_WRITER_BYTE` before `do_repair()` runs, so a reader admitted on that
    /// byte alone would walk a tree the repair is discarding. The consistent byte keeps it out
    #[test]
    fn a_repairing_writer_does_not_admit_a_shared_reader() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();

        // Crashed after a commit, so the next open has to repair it
        let dead = Arc::new(AtomicBool::new(false));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmpfile.path())
            .unwrap();
        let db = Database::builder()
            .create_with_backend(CrashBackend {
                inner: redb::backends::FileBackend::new(file).unwrap(),
                dead: Arc::clone(&dead),
            })
            .unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(TABLE).unwrap();
            table.insert(0, 9).unwrap();
        }
        write.commit().unwrap();
        dead.store(true, Ordering::SeqCst);
        drop(db);

        // The repair callback fires inside do_repair()
        let path = tmpfile.path().to_path_buf();
        let during: Arc<std::sync::Mutex<Option<String>>> = Arc::new(std::sync::Mutex::new(None));
        let recorder = Arc::clone(&during);

        let mut builder = shared();
        builder.set_repair_callback(move |_| {
            let mut recorded = recorder.lock().unwrap();
            if recorded.is_some() {
                return;
            }
            *recorded = Some(match shared().open_read_only(&path) {
                Ok(_) => "opened".to_string(),
                Err(err) => format!("{err:?}"),
            });
        });
        let repaired = builder.open(tmpfile.path()).unwrap();

        assert_eq!(
            during.lock().unwrap().take().as_deref(),
            Some("RepairAborted"),
            "a shared reader was admitted while the database was still being repaired"
        );

        let reader = shared().open_read_only(tmpfile.path()).unwrap();
        assert_eq!(value(&reader), 9);
        drop(repaired);
    }
}

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
mod writer_byte {
    use super::*;
    use redb::TableDefinition;
    use std::path::Path;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    fn create(path: &Path, mode: ConcurrencyMode) -> Result<Database, DatabaseError> {
        let mut builder = Database::builder();
        builder.set_concurrency_mode(mode);
        builder.create(path)
    }

    /// Which is what makes one write transaction at a time across the cohort
    #[test]
    fn a_write_transaction_excludes_another_process() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();
        let peer = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();

        let held = db.begin_write().unwrap();

        let (tx, rx) = mpsc::channel();
        let waiting = thread::spawn(move || {
            let write = peer.begin_write().unwrap();
            tx.send(()).unwrap();
            write.abort().unwrap();
        });

        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "a second process began a write transaction while the first was open"
        );
        held.commit().unwrap();
        rx.recv_timeout(Duration::from_secs(10))
            .expect("the waiting process never began its transaction");
        waiting.join().unwrap();
    }

    /// The byte is released by the whole transaction ending, not by the commit alone
    #[test]
    fn an_aborted_transaction_releases_the_byte() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();
        let peer = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();

        db.begin_write().unwrap().abort().unwrap();
        // Dropped rather than aborted, which aborts through Drop
        drop(db.begin_write().unwrap());

        let write = peer.begin_write().unwrap();
        write.commit().unwrap();
    }

    /// It locks the whole file, which covers the writer byte
    #[test]
    fn a_single_process_transaction_does_not_puncture_the_whole_file_lock() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let db = create(tmpfile.path(), ConcurrencyMode::SingleProcess).unwrap();

        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(TABLE).unwrap();
            table.insert(0, 0).unwrap();
        }
        write.commit().unwrap();

        assert!(matches!(
            Database::open(tmpfile.path()),
            Err(DatabaseError::DatabaseAlreadyOpen)
        ));
    }
}

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn the_concurrency_mode_excludes_incompatible_opens() {
    fn create(path: &std::path::Path, mode: ConcurrencyMode) -> Result<Database, DatabaseError> {
        let mut builder = Database::builder();
        builder.set_concurrency_mode(mode);
        builder.create(path)
    }

    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let db = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();

    let peer = create(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();
    assert!(matches!(
        create(tmpfile.path(), ConcurrencyMode::SingleWriterProcess),
        Err(DatabaseError::DatabaseAlreadyOpen)
    ));
    assert!(matches!(
        Database::open(tmpfile.path()),
        Err(DatabaseError::DatabaseAlreadyOpen)
    ));

    drop(peer);
    drop(db);
    Database::open(tmpfile.path()).unwrap();
}

/// A multi-writer check runs on the file as another process last committed it, and the handle
/// goes on from there: its savepoint ids continue past the one it adopted, and its next commit,
/// the close's here, follows the commits it adopted
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn an_integrity_check_adopts_a_peers_commits() {
    use redb::{ReadableDatabase, ReadableTable, TableDefinition};
    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let mut db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let peer = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    let txn = peer.begin_write().unwrap();
    txn.open_table(TABLE).unwrap().insert(1, 1).unwrap();
    txn.commit().unwrap();
    let txn = peer.begin_write().unwrap();
    let peers_savepoint = txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();
    drop(peer);

    assert!(db.check_integrity().unwrap());
    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.get(1).unwrap().unwrap().value(), 1);
    drop(table);
    drop(read);
    let txn = db.begin_write().unwrap();
    let own_savepoint = txn.persistent_savepoint().unwrap();
    assert_ne!(own_savepoint, peers_savepoint);
    txn.commit().unwrap();
    drop(db);

    let db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.get(1).unwrap().unwrap().value(), 1);
    let txn = db.begin_write().unwrap();
    let mut savepoints: Vec<u64> = txn.list_persistent_savepoints().unwrap().collect();
    savepoints.sort_unstable();
    assert_eq!(savepoints, vec![peers_savepoint, own_savepoint]);
}

/// Two handles that commit from the same header use the same transaction ids, and the later
/// commit replaces the earlier. The check tells the peer's from the handle's own by its roots:
/// the savepoint it carries is held, and the handle's allocator is not compared as the file's
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn an_integrity_check_adopts_a_peers_commit_under_the_same_id() {
    use redb::{ReadableDatabase, TableDefinition, TableError};
    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let mut db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    let peer = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    // Neither reloads the other's commits, so the peer's two commits, its close's the second,
    // carry the ids of the handle's two and replace them
    for key in [1, 2] {
        let txn = db.begin_write().unwrap();
        txn.open_table(TABLE).unwrap().insert(key, key).unwrap();
        txn.commit().unwrap();
    }
    let txn = peer.begin_write().unwrap();
    let peers_savepoint = txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();
    drop(peer);

    assert!(db.check_integrity().unwrap());
    let read = db.begin_read().unwrap();
    assert!(matches!(
        read.open_table(TABLE),
        Err(TableError::TableDoesNotExist(_))
    ));
    drop(read);
    let txn = db.begin_write().unwrap();
    let own_savepoint = txn.persistent_savepoint().unwrap();
    assert_ne!(own_savepoint, peers_savepoint);
    txn.commit().unwrap();
    drop(db);

    let db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    let txn = db.begin_write().unwrap();
    let mut savepoints: Vec<u64> = txn.list_persistent_savepoints().unwrap().collect();
    savepoints.sort_unstable();
    assert_eq!(savepoints, vec![peers_savepoint, own_savepoint]);
}

/// A shared commit is 2-phase, under which a corrupt primary slot is corruption rather than a
/// torn write to fall back from, so a check finds nothing to repair from under a process still
/// reading that slot
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn a_corrupt_primary_is_not_repaired_from_under_a_peers_read() {
    use redb::{ReadableDatabase, ReadableTable, TableDefinition};
    use std::io::{Read, Seek, SeekFrom, Write};
    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let mut db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    for key in [1, 2] {
        let txn = db.begin_write().unwrap();
        txn.open_table(TABLE).unwrap().insert(key, key).unwrap();
        txn.commit().unwrap();
    }
    let peer = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open_read_only(tmpfile.path())
        .unwrap();
    let read = peer.begin_read().unwrap();

    // The header: the god byte at 9, whose low bit names the primary of the two 128-byte commit
    // slots at 64 and 192, each ending in a 16-byte checksum
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(tmpfile.path())
        .unwrap();
    let mut god_byte = [0u8];
    file.seek(SeekFrom::Start(9)).unwrap();
    file.read_exact(&mut god_byte).unwrap();
    let checksum = 64 + 128 * u64::from(god_byte[0] & 1) + 112;
    let mut bytes = [0u8; 16];
    file.seek(SeekFrom::Start(checksum)).unwrap();
    file.read_exact(&mut bytes).unwrap();
    for byte in &mut bytes {
        *byte ^= 0xFF;
    }
    file.seek(SeekFrom::Start(checksum)).unwrap();
    file.write_all(&bytes).unwrap();
    file.sync_all().unwrap();
    drop(file);

    assert!(matches!(
        db.check_integrity(),
        Err(DatabaseError::Storage(StorageError::Corrupted(_)))
    ));
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.get(1).unwrap().unwrap().value(), 1);
    assert_eq!(table.get(2).unwrap().unwrap().value(), 2);
}

/// A persistent savepoint another process deleted is gone from the tables the check adopts, and
/// the handle forgets it: neither its pin nor its refusal of compaction outlives the check
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn an_integrity_check_forgets_a_savepoint_a_peer_deleted() {
    use redb::CompactionError;

    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let mut db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let txn = db.begin_write().unwrap();
    let savepoint = txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();
    assert!(matches!(
        db.compact(),
        Err(CompactionError::PersistentSavepointExists)
    ));

    let peer = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    let txn = peer.begin_write().unwrap();
    assert!(txn.delete_persistent_savepoint(savepoint).unwrap());
    txn.commit().unwrap();
    drop(peer);

    assert!(db.check_integrity().unwrap());
    db.compact().unwrap();
}

/// A persistent savepoint another process created under the id of one this handle holds, over a
/// commit that replaced the one the handle's was created in, is another savepoint: the check pins
/// its transaction in place of the one it held, and deleting it releases that pin
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn an_integrity_check_moves_a_savepoint_a_peer_replaced() {
    use redb::TableDefinition;
    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let mut db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    let peer = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    let txn = db.begin_write().unwrap();
    let savepoint = txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();
    // The peer never reloads the handle's commit, so its first replaces it, and its savepoint's
    // id counts from the same header
    let txn = peer.begin_write().unwrap();
    txn.open_table(TABLE).unwrap().insert(1, 1).unwrap();
    txn.commit().unwrap();
    let txn = peer.begin_write().unwrap();
    assert_eq!(txn.persistent_savepoint().unwrap(), savepoint);
    txn.commit().unwrap();
    drop(peer);

    assert!(db.check_integrity().unwrap());
    let txn = db.begin_write().unwrap();
    assert!(txn.delete_persistent_savepoint(savepoint).unwrap());
    txn.commit().unwrap();
    db.compact().unwrap();
}

/// The check waits for another process's write transaction to end, as a write transaction would
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn an_integrity_check_waits_for_a_peers_write_transaction() {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let mut db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let peer = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    let txn = peer.begin_write().unwrap();

    let (tx, rx) = mpsc::channel();
    let checking = thread::spawn(move || {
        tx.send(db.check_integrity().unwrap()).unwrap();
        db
    });
    assert!(
        rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "the check ran under another process's write transaction"
    );
    txn.abort().unwrap();
    assert!(
        rx.recv_timeout(Duration::from_secs(10))
            .expect("the check never ran")
    );
    checking.join().unwrap();
}

/// A caller-supplied backend has no locks to negotiate with, which is reported like a platform
/// without them
#[test]
fn sharing_a_caller_supplied_backend_is_unsupported() {
    let mut builder = Database::builder();
    builder.set_concurrency_mode(ConcurrencyMode::MultiWriterProcess);
    let err = builder
        .create_with_backend(InMemoryBackend::new())
        .unwrap_err();
    assert!(matches!(
        err,
        DatabaseError::Storage(StorageError::Io(err)) if err.kind() == std::io::ErrorKind::Unsupported
    ));

    // ... while the default mode opens on one as it always has
    Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
}

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
mod reclamation {
    use super::*;
    use redb::{ReadableDatabase, ReadableTable, TableDefinition};

    const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

    /// A read transaction in another process pins its snapshot: the writer's reclamation stops
    /// at it, however many commits go by
    #[test]
    fn a_readers_pin_survives_heavy_reclamation() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let writer = Database::builder()
            .set_concurrency_mode(ConcurrencyMode::SingleWriterProcess)
            .create(tmpfile.path())
            .unwrap();

        let value = vec![7u8; 512];
        let txn = writer.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for key in 0..128u64 {
                t.insert(&key, value.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let reader = Database::builder()
            .set_concurrency_mode(ConcurrencyMode::SingleWriterProcess)
            .open_read_only(tmpfile.path())
            .unwrap();
        let pinned = reader.begin_read().unwrap();

        // Every round frees the previous round's pages; without the reader's pin bounding the
        // writer's reclamation, the pinned snapshot's pages would be reused under it
        for round in 0..20u8 {
            let overwrite = vec![round; 512];
            let txn = writer.begin_write().unwrap();
            {
                let mut t = txn.open_table(TABLE).unwrap();
                for key in 0..128u64 {
                    t.insert(&key, overwrite.as_slice()).unwrap();
                }
            }
            txn.commit().unwrap();
        }

        let t = pinned.open_table(TABLE).unwrap();
        for key in 0..128u64 {
            assert_eq!(t.get(&key).unwrap().unwrap().value(), value.as_slice());
        }
        drop(t);
        drop(pinned);

        // With the pin gone the writer keeps going, and a fresh read sees the last round
        let txn = writer.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            t.insert(&0, [0u8; 4].as_slice()).unwrap();
        }
        txn.commit().unwrap();
        let read = reader.begin_read().unwrap();
        let t = read.open_table(TABLE).unwrap();
        assert_eq!(t.get(&0).unwrap().unwrap().value(), [0u8; 4].as_slice());
    }
}

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
mod compaction {
    use super::*;
    use redb::{
        CompactionError, ReadOnlyDatabase, ReadableDatabase, ReadableTable, TableDefinition,
    };
    use std::path::Path;

    const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

    fn create(path: &Path, mode: ConcurrencyMode) -> Database {
        Database::builder()
            .set_concurrency_mode(mode)
            .create(path)
            .unwrap()
    }

    fn open_read_only(path: &Path, mode: ConcurrencyMode) -> ReadOnlyDatabase {
        Database::builder()
            .set_concurrency_mode(mode)
            .open_read_only(path)
            .unwrap()
    }

    fn insert(db: &Database, keys: std::ops::Range<u64>, value: &[u8]) {
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for key in keys {
                t.insert(&key, value).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    /// A read transaction in another process is a transaction in progress, as a local one is
    #[test]
    fn compaction_refuses_a_peers_read_transaction() {
        for mode in [
            ConcurrencyMode::SingleWriterProcess,
            ConcurrencyMode::MultiWriterProcess,
        ] {
            let tmpfile = tempfile::NamedTempFile::new().unwrap();
            let mut writer = create(tmpfile.path(), mode);
            insert(&writer, 0..1, &[1u8; 512]);

            let reader = open_read_only(tmpfile.path(), mode);
            let pinned = reader.begin_read().unwrap();
            assert!(
                matches!(
                    writer.compact(),
                    Err(CompactionError::TransactionInProgress)
                ),
                "{mode:?}"
            );

            drop(pinned);
            writer.compact().unwrap();
            let read = reader.begin_read().unwrap();
            let t = read.open_table(TABLE).unwrap();
            assert_eq!(t.get(&0).unwrap().unwrap().value(), [1u8; 512].as_slice());
        }
    }

    /// The file shrinks under a peer that has it open, and the peer's next read follows the pages
    /// that moved
    #[test]
    fn compaction_shrinks_a_file_a_peer_has_open() {
        for mode in [
            ConcurrencyMode::SingleWriterProcess,
            ConcurrencyMode::MultiWriterProcess,
        ] {
            let tmpfile = tempfile::NamedTempFile::new().unwrap();
            let mut writer = create(tmpfile.path(), mode);
            let value = vec![7u8; 1024];
            insert(&writer, 0..512, &value);

            // Caches the pages the compaction is about to move
            let reader = open_read_only(tmpfile.path(), mode);
            {
                let read = reader.begin_read().unwrap();
                let t = read.open_table(TABLE).unwrap();
                assert_eq!(t.get(&511).unwrap().unwrap().value(), value.as_slice());
            }

            let txn = writer.begin_write().unwrap();
            {
                let mut t = txn.open_table(TABLE).unwrap();
                for key in 8..512u64 {
                    t.remove(&key).unwrap();
                }
            }
            txn.commit().unwrap();

            let before = std::fs::metadata(tmpfile.path()).unwrap().len();
            assert!(writer.compact().unwrap(), "{mode:?}");
            let after = std::fs::metadata(tmpfile.path()).unwrap().len();
            assert!(after < before, "{mode:?}: {before} -> {after}");

            let read = reader.begin_read().unwrap();
            let t = read.open_table(TABLE).unwrap();
            for key in 0..8u64 {
                assert_eq!(t.get(&key).unwrap().unwrap().value(), value.as_slice());
            }
            assert!(t.get(&8).unwrap().is_none());
        }
    }
}
