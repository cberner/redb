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

/// Writes `with`'s contents over `path`'s, in place, so that a handle open on `path` finds
/// them: a file replaced from a copy, out from under the database
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
fn overwrite(path: &std::path::Path, with: &std::path::Path) {
    use std::io::Write;

    let contents = std::fs::read(with).unwrap();
    let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.write_all(&contents).unwrap();
    file.set_len(contents.len() as u64).unwrap();
    file.sync_all().unwrap();
}

/// A handle to a persistent savepoint outlives the transaction that read it, and a file copied
/// in can replace the savepoint under its id with one of the same transaction and another root,
/// whose pages the allocator state the handle adopts allocates without: the handle is invalid,
/// and one read from the file restores
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn a_savepoint_handle_a_peer_replaced_under_its_id_is_invalid() {
    use redb::{ReadableDatabase, ReadableTable, SavepointError, TableDefinition};
    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let copy = tempfile::NamedTempFile::new().unwrap();
    std::fs::copy(tmpfile.path(), copy.path()).unwrap();
    let db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    // The handle and the copy each commit a row of their own, then a savepoint of that commit:
    // the same savepoint and transaction ids, counted from the same header, naming different
    // roots
    let txn = db.begin_write().unwrap();
    txn.open_table(TABLE).unwrap().insert(1, 1).unwrap();
    txn.commit().unwrap();
    let txn = db.begin_write().unwrap();
    let savepoint = txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();
    let txn = db.begin_write().unwrap();
    let handle = txn.get_persistent_savepoint(savepoint).unwrap();
    txn.abort().unwrap();
    let other = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(copy.path())
        .unwrap();
    let txn = other.begin_write().unwrap();
    txn.open_table(TABLE).unwrap().insert(2, 2).unwrap();
    txn.commit().unwrap();
    let txn = other.begin_write().unwrap();
    assert_eq!(txn.persistent_savepoint().unwrap(), savepoint);
    txn.commit().unwrap();
    drop(other);
    overwrite(tmpfile.path(), copy.path());

    // The transaction adopts the copy's commit, and its savepoint under the id
    let mut txn = db.begin_write().unwrap();
    assert!(matches!(
        txn.restore_savepoint(&handle),
        Err(SavepointError::InvalidSavepoint)
    ));
    let current = txn.get_persistent_savepoint(savepoint).unwrap();
    // Deleting the record leaves the stale handle invalid: it is compared against the record
    // deleted, which the handle read from the file matches, so that one restores still
    assert!(txn.delete_persistent_savepoint(savepoint).unwrap());
    assert!(matches!(
        txn.restore_savepoint(&handle),
        Err(SavepointError::InvalidSavepoint)
    ));
    txn.restore_savepoint(&current).unwrap();
    txn.commit().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert!(table.get(1).unwrap().is_none());
    assert_eq!(table.get(2).unwrap().unwrap().value(), 2);
}

/// A handle to a persistent savepoint outlives the transaction that read it, and a check can
/// reload a file copied in that lacks the savepoint while the tracker still holds its id: the
/// handle is invalid, rather than restoring a root the rebuilt allocator state allocates without
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn a_savepoint_handle_a_reloaded_file_lacks_is_invalid() {
    use redb::SavepointError;

    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let copy = tempfile::NamedTempFile::new().unwrap();
    std::fs::copy(tmpfile.path(), copy.path()).unwrap();
    let mut db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    let txn = db.begin_write().unwrap();
    let savepoint = txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();
    let txn = db.begin_write().unwrap();
    let handle = txn.get_persistent_savepoint(savepoint).unwrap();
    txn.abort().unwrap();
    // The copy, from before the savepoint, written over the file and reloaded by a check
    overwrite(tmpfile.path(), copy.path());
    db.check_integrity().unwrap();

    let mut txn = db.begin_write().unwrap();
    assert!(matches!(
        txn.restore_savepoint(&handle),
        Err(SavepointError::InvalidSavepoint)
    ));
    txn.abort().unwrap();
}

/// A multi-writer handle's write transaction begins from the file as another process last
/// committed it, and so does its close
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
mod peer_commits {
    use super::*;
    use redb::{CompactionError, ReadableDatabase, ReadableTable, TableDefinition};
    use std::path::Path;

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    fn open(path: &Path) -> Database {
        Database::builder()
            .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
            .open(path)
            .unwrap()
    }

    /// Two handles on a file a clean close left, so that neither open commits
    fn two_handles(path: &Path) -> (Database, Database) {
        Database::builder()
            .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
            .create(path)
            .unwrap();
        (open(path), open(path))
    }

    fn insert(db: &Database, key: u64) {
        let txn = db.begin_write().unwrap();
        txn.open_table(TABLE).unwrap().insert(key, key).unwrap();
        txn.commit().unwrap();
    }

    fn value(db: &Database, key: u64) -> u64 {
        let read = db.begin_read().unwrap();
        let table = read.open_table(TABLE).unwrap();
        table.get(key).unwrap().unwrap().value()
    }

    /// The transaction sees the peer's commit, and its own commit follows it, so neither is lost
    #[test]
    fn a_write_transaction_adopts_a_peers_commit() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let (db, peer) = two_handles(tmpfile.path());
        insert(&peer, 1);

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            assert_eq!(table.get(1).unwrap().unwrap().value(), 1);
            table.insert(2, 2).unwrap();
        }
        txn.commit().unwrap();
        let txn = peer.begin_write().unwrap();
        {
            let table = txn.open_table(TABLE).unwrap();
            assert_eq!(table.get(1).unwrap().unwrap().value(), 1);
            assert_eq!(table.get(2).unwrap().unwrap().value(), 2);
        }
        txn.abort().unwrap();
        drop(peer);
        drop(db);

        let db = open(tmpfile.path());
        assert_eq!(value(&db, 1), 1);
        assert_eq!(value(&db, 2), 2);
    }

    /// The persistent savepoint the peer created is held from the transaction on, and the
    /// transaction's own takes a fresh id; the savepoint the peer deleted is forgotten from the
    /// transaction on, with the pin it held
    #[test]
    fn a_write_transaction_adopts_a_peers_savepoint() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let (mut db, peer) = two_handles(tmpfile.path());
        let txn = peer.begin_write().unwrap();
        let peers_savepoint = txn.persistent_savepoint().unwrap();
        txn.commit().unwrap();

        db.begin_write().unwrap().abort().unwrap();
        assert!(matches!(
            db.compact(),
            Err(CompactionError::PersistentSavepointExists)
        ));
        let txn = db.begin_write().unwrap();
        let own_savepoint = txn.persistent_savepoint().unwrap();
        assert_ne!(own_savepoint, peers_savepoint);
        txn.commit().unwrap();
        {
            let txn = db.begin_write().unwrap();
            let mut savepoints: Vec<u64> = txn.list_persistent_savepoints().unwrap().collect();
            savepoints.sort_unstable();
            assert_eq!(savepoints, vec![peers_savepoint, own_savepoint]);
            txn.abort().unwrap();
        }

        let txn = peer.begin_write().unwrap();
        assert!(txn.delete_persistent_savepoint(peers_savepoint).unwrap());
        assert!(txn.delete_persistent_savepoint(own_savepoint).unwrap());
        txn.commit().unwrap();
        db.begin_write().unwrap().abort().unwrap();
        db.compact().unwrap();
    }

    /// The close's commit adopts the file's latest commit, and where it cannot, the shutdown
    /// header is not written: it would put this handle's stale header, marked clean, over the
    /// commit another process made
    #[test]
    fn a_close_writes_no_header_over_a_commit_it_could_not_adopt() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let (db, peer) = two_handles(tmpfile.path());
        insert(&peer, 1);
        drop(peer);
        // The peer's commit with its primary slot's checksum flipped on disk: a commit the close
        // cannot adopt, since a shared commit is 2-phase, under which a corrupt primary is
        // corruption rather than a torn write to fall back from. The god byte at 9 names the
        // primary of the two 128-byte commit slots at 64 and 192, each ending in a 16-byte
        // checksum
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmpfile.path())
            .unwrap();
        let mut god_byte = [0u8];
        file.seek(SeekFrom::Start(9)).unwrap();
        file.read_exact(&mut god_byte).unwrap();
        let checksum = 64 + 128 * u64::from(god_byte[0] & 1) + 112;
        let mut flipped = [0u8; 16];
        file.seek(SeekFrom::Start(checksum)).unwrap();
        file.read_exact(&mut flipped).unwrap();
        for byte in &mut flipped {
            *byte ^= 0xFF;
        }
        file.seek(SeekFrom::Start(checksum)).unwrap();
        file.write_all(&flipped).unwrap();
        file.sync_all().unwrap();

        drop(db);
        let mut after = [0u8; 16];
        file.seek(SeekFrom::Start(checksum)).unwrap();
        file.read_exact(&mut after).unwrap();
        assert_eq!(
            after, flipped,
            "the close wrote its header over the peer's commit"
        );
    }

    /// The close commits from the file as the peer last committed it
    #[test]
    fn a_close_records_a_peers_commit() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let (db, peer) = two_handles(tmpfile.path());
        insert(&peer, 1);
        drop(peer);
        drop(db);

        let db = open(tmpfile.path());
        assert_eq!(value(&db, 1), 1);
    }

    /// A repair's commit records no allocator state, so the transaction that adopts one rebuilds
    /// it from the trees
    #[test]
    fn a_write_transaction_rebuilds_the_allocator_a_peers_repair_left_unsaved() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        // The create's commit is a repair's, so the peer's open repairs and commits too
        let db = Database::builder()
            .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
            .create(tmpfile.path())
            .unwrap();
        let peer = open(tmpfile.path());
        insert(&db, 1);
        insert(&peer, 2);
        drop(peer);
        drop(db);

        let db = open(tmpfile.path());
        assert_eq!(value(&db, 1), 1);
        assert_eq!(value(&db, 2), 2);
    }

    /// A transaction dropped while a panic unwinds leaves its pages in the write buffer, and the
    /// peer can allocate and commit the same pages: the adoption discards the buffer with the
    /// rest of the state it replaces, so the peer's page reads as committed
    #[test]
    fn a_write_transaction_reads_a_peers_page_over_a_page_a_panic_left_buffered() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let (db, peer) = two_handles(tmpfile.path());
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let txn = db.begin_write().unwrap();
            txn.open_table(TABLE).unwrap().insert(1, 1).unwrap();
            panic!("unwinding through the transaction");
        }));
        assert!(unwound.is_err());
        let txn = peer.begin_write().unwrap();
        txn.open_table(TABLE).unwrap().insert(1, 2).unwrap();
        txn.commit().unwrap();

        let txn = db.begin_write().unwrap();
        assert_eq!(
            txn.open_table(TABLE)
                .unwrap()
                .get(1)
                .unwrap()
                .unwrap()
                .value(),
            2
        );
    }
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
