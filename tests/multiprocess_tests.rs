#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
use redb::backends::FileBackend;
use redb::backends::InMemoryBackend;
use redb::{ConcurrencyMode, Database, DatabaseError, StorageError};

/// The modes in which processes share a file
fn shared_modes() -> &'static [ConcurrencyMode] {
    &[ConcurrencyMode::SingleWriter, ConcurrencyMode::MultiWriter]
}

#[cfg(all(
    any(target_os = "linux", target_vendor = "apple", windows),
    feature = "experimental-api-5"
))]
fn file_backend(file: std::fs::File) -> FileBackend {
    FileBackend::new(file)
}

#[cfg(all(
    any(target_os = "linux", target_vendor = "apple", windows),
    not(feature = "experimental-api-5")
))]
fn file_backend(file: std::fs::File) -> FileBackend {
    FileBackend::new(file).unwrap()
}

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
mod shared_reader {
    use super::*;
    #[cfg(feature = "experimental-api-5")]
    use redb::ReadableTable;
    use redb::{ReadableDatabase, TableDefinition};
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    // The error type of StorageBackend's I/O methods
    #[cfg(feature = "experimental-api-5")]
    type BackendIoError = redb::BackendError;
    #[cfg(not(feature = "experimental-api-5"))]
    type BackendIoError = std::io::Error;

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    fn shared(mode: ConcurrencyMode) -> redb::Builder {
        let mut builder = Database::builder();
        builder.set_concurrency_mode(mode);
        builder
    }

    fn create(path: &Path, mode: ConcurrencyMode) -> Database {
        let db = shared(mode).create(path).unwrap();
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
        for &mode in shared_modes() {
            let tmpfile = tempfile::NamedTempFile::new().unwrap();
            let writer = create(tmpfile.path(), mode);
            let reader = shared(mode).open_read_only(tmpfile.path()).unwrap();

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
                    "{mode:?}: the reader served a cached page after the writer committed {expected}"
                );
            }
        }
    }

    /// A shared reader reads pages by the immutable geometry alone, so the region counts, which an
    /// unclean header leaves unvalidated, never reach it
    #[test]
    fn a_shared_reader_ignores_the_layout() {
        use std::io::{Seek, SeekFrom, Write};

        for &mode in shared_modes() {
            let tmpfile = tempfile::NamedTempFile::new().unwrap();
            let writer = create(tmpfile.path(), mode);
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

            let reader = shared(mode).open_read_only(tmpfile.path()).unwrap();
            assert_eq!(value(&reader), 7, "{mode:?}");
            // Its close would rewrite the header, which is not what this tests
            std::mem::forget(writer);
        }
    }

    /// Simulates power loss: once `dead`, every write is dropped
    #[derive(Debug)]
    struct CrashBackend {
        inner: redb::backends::FileBackend,
        dead: Arc<AtomicBool>,
    }

    impl redb::StorageBackend for CrashBackend {
        fn len(&self) -> Result<u64, BackendIoError> {
            self.inner.len()
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), BackendIoError> {
            self.inner.read(offset, out)
        }
        fn set_len(&self, len: u64) -> Result<(), BackendIoError> {
            if self.dead.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.inner.set_len(len)
        }
        fn sync_data(&self) -> Result<(), BackendIoError> {
            if self.dead.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.inner.sync_data()
        }
        fn write(&self, offset: u64, data: &[u8]) -> Result<(), BackendIoError> {
            if self.dead.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.inner.write(offset, data)
        }
    }

    /// Leaves a database that crashed after a commit, so the next open has to repair it
    fn crash_after_commit(path: &Path) {
        let dead = Arc::new(AtomicBool::new(false));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let db = Database::builder()
            .create_with_backend(CrashBackend {
                inner: file_backend(file),
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
    }

    /// A writer takes `SHARED_WRITER_BYTE` before `do_repair()` runs, so a reader admitted on that
    /// byte alone would walk a tree the repair is discarding. The consistent byte keeps it out
    #[test]
    fn a_repairing_writer_does_not_admit_a_shared_reader() {
        for &mode in shared_modes() {
            let tmpfile = tempfile::NamedTempFile::new().unwrap();
            crash_after_commit(tmpfile.path());

            // The repair callback fires inside do_repair()
            let path = tmpfile.path().to_path_buf();
            let during: Arc<std::sync::Mutex<Option<String>>> =
                Arc::new(std::sync::Mutex::new(None));
            let recorder = Arc::clone(&during);

            let mut builder = shared(mode);
            builder.set_repair_callback(move |_| {
                let mut recorded = recorder.lock().unwrap();
                if recorded.is_some() {
                    return;
                }
                *recorded = Some(match shared(mode).open_read_only(&path) {
                    Ok(_) => "opened".to_string(),
                    Err(err) => format!("{err:?}"),
                });
            });
            let repaired = builder.open(tmpfile.path()).unwrap();

            assert_eq!(
                during.lock().unwrap().take().as_deref(),
                Some("RepairAborted"),
                "{mode:?}: a shared reader was admitted while the database was still being repaired"
            );

            let reader = shared(mode).open_read_only(tmpfile.path()).unwrap();
            assert_eq!(value(&reader), 9);
            drop(repaired);
        }
    }

    /// A read-only exclusive-writer open is not a writer, so it leaves the consistent byte free:
    /// a shared reader that opens while it is under way still finds the database unrepaired
    #[test]
    fn a_read_only_exclusive_open_does_not_admit_a_shared_reader() {
        use std::sync::atomic::AtomicU64;

        for &mode in shared_modes() {
            let tmpfile = tempfile::NamedTempFile::new().unwrap();
            crash_after_commit(tmpfile.path());

            let stop = Arc::new(AtomicBool::new(false));
            let attempts = Arc::new(AtomicU64::new(0));
            let opener = {
                let (stop, attempts) = (stop.clone(), attempts.clone());
                let path = tmpfile.path().to_path_buf();
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        assert!(matches!(
                            redb::ReadOnlyDatabase::open(&path),
                            Err(DatabaseError::RepairAborted)
                        ));
                        attempts.fetch_add(1, Ordering::Relaxed);
                    }
                })
            };
            while attempts.load(Ordering::Relaxed) == 0 {
                std::thread::yield_now();
            }
            let admitted = (0..2000)
                .filter(|_| shared(mode).open_read_only(tmpfile.path()).is_ok())
                .count();
            stop.store(true, Ordering::Relaxed);
            opener.join().unwrap();
            assert_eq!(
                admitted, 0,
                "{mode:?}: a shared reader was admitted to a database that needs repair"
            );
        }
    }

    /// Without `experimental-api-5`, as in redb 4, an exclusive-writer open is refused by a
    /// shared-mode handle only after it has taken the range that holds the active transaction
    /// bytes, so a read that begins meanwhile waits for it
    #[test]
    fn a_refused_exclusive_open_does_not_fail_a_read() {
        use std::sync::atomic::AtomicU64;

        for &mode in shared_modes() {
            let tmpfile = tempfile::NamedTempFile::new().unwrap();
            let db = Arc::new(create(tmpfile.path(), mode));
            let stop = Arc::new(AtomicBool::new(false));
            let reads = Arc::new(AtomicU64::new(0));
            let reader = {
                let (db, stop, reads) = (db.clone(), stop.clone(), reads.clone());
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        assert_eq!(value(&*db), 0);
                        reads.fetch_add(1, Ordering::Relaxed);
                    }
                })
            };
            while reads.load(Ordering::Relaxed) == 0 {
                std::thread::yield_now();
            }
            for _ in 0..2000 {
                assert!(matches!(
                    Database::create(tmpfile.path()),
                    Err(DatabaseError::DatabaseAlreadyOpen)
                ));
            }
            stop.store(true, Ordering::Relaxed);
            reader
                .join()
                .unwrap_or_else(|_| panic!("{mode:?}: a read failed"));
        }
    }
}

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
mod writer_byte {
    use super::*;
    use redb::TableDefinition;
    use std::path::Path;

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    fn create(path: &Path, mode: ConcurrencyMode) -> Result<Database, DatabaseError> {
        let mut builder = Database::builder();
        builder.set_concurrency_mode(mode);
        builder.create(path)
    }

    /// Which is what makes one write transaction at a time across the cohort
    #[test]
    fn a_write_transaction_excludes_another_process() {
        use std::sync::mpsc;
        use std::thread;
        use std::time::Duration;

        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();
        let peer = create(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();

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
        let db = create(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();
        let peer = create(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();

        db.begin_write().unwrap().abort().unwrap();
        // Dropped rather than aborted, which aborts through Drop
        drop(db.begin_write().unwrap());

        let write = peer.begin_write().unwrap();
        write.commit().unwrap();
    }

    /// It locks the whole file, which covers the writer byte
    #[test]
    fn an_exclusive_writer_transaction_does_not_puncture_the_whole_file_lock() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let db = create(tmpfile.path(), ConcurrencyMode::ExclusiveWriter).unwrap();

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
    let db = create(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();

    let peer = create(tmpfile.path(), ConcurrencyMode::MultiWriter).unwrap();
    assert!(matches!(
        create(tmpfile.path(), ConcurrencyMode::SingleWriter),
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

/// An exclusive-writer open that a shared-mode handle refuses takes no active transaction byte,
/// where its lock would read as a transaction in progress. 4.x takes those bytes first
#[cfg(all(
    any(target_os = "linux", target_vendor = "apple", windows),
    feature = "experimental-api-5"
))]
#[test]
fn a_refused_exclusive_open_does_not_refuse_a_compaction() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    for &mode in shared_modes() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let mut builder = Database::builder();
        builder.set_concurrency_mode(mode);
        let mut db = builder.create(tmpfile.path()).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let refused = Arc::new(AtomicU64::new(0));
        let opener = {
            let (stop, refused) = (stop.clone(), refused.clone());
            let path = tmpfile.path().to_path_buf();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    assert!(matches!(
                        Database::create(&path),
                        Err(DatabaseError::DatabaseAlreadyOpen)
                    ));
                    refused.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        while refused.load(Ordering::Relaxed) == 0 {
            std::thread::yield_now();
        }
        let compacted = (0..300).try_for_each(|_| db.compact().map(|_| ()));
        stop.store(true, Ordering::Relaxed);
        opener.join().unwrap();
        compacted.unwrap_or_else(|err| panic!("{mode:?}: {err:?}"));
    }
}

/// A multi-writer check runs on the file as another process last committed it, and the handle
/// goes on from there: fresh savepoint ids, and a close that commits after what it synced to
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn an_integrity_check_syncs_to_a_peers_commits() {
    #[cfg(feature = "experimental-api-5")]
    use redb::ReadableTable;
    use redb::{ReadableDatabase, TableDefinition};
    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let mut db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriter)
        .create(tmpfile.path())
        .unwrap();
    let peer = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriter)
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
        .set_concurrency_mode(ConcurrencyMode::MultiWriter)
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

/// A shared commit is 2-phase, so a corrupt primary slot is corruption rather than a torn write
/// to fall back from: the check repairs nothing from under a process reading that slot
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn a_corrupt_primary_is_not_repaired_from_under_a_peers_read() {
    #[cfg(feature = "experimental-api-5")]
    use redb::ReadableTable;
    use redb::{ReadableDatabase, TableDefinition};
    use std::io::{Read, Seek, SeekFrom, Write};
    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    for &mode in shared_modes() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let mut db = Database::builder()
            .set_concurrency_mode(mode)
            .create(tmpfile.path())
            .unwrap();
        for key in [1, 2] {
            let txn = db.begin_write().unwrap();
            txn.open_table(TABLE).unwrap().insert(key, key).unwrap();
            txn.commit().unwrap();
        }
        let peer = Database::builder()
            .set_concurrency_mode(mode)
            .open_read_only(tmpfile.path())
            .unwrap();
        let read = peer.begin_read().unwrap();

        // The header: the god byte at 9, whose low bit names the primary of the two 128-byte
        // commit slots at 64 and 192, each ending in a 16-byte checksum
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

        assert!(
            matches!(
                db.check_integrity(),
                Err(DatabaseError::Storage(StorageError::Corrupted(_)))
            ),
            "{mode:?}"
        );
        let table = read.open_table(TABLE).unwrap();
        assert_eq!(table.get(1).unwrap().unwrap().value(), 1);
        assert_eq!(table.get(2).unwrap().unwrap().value(), 2);
    }
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
        .set_concurrency_mode(ConcurrencyMode::MultiWriter)
        .create(tmpfile.path())
        .unwrap();
    let peer = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriter)
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

/// A multi-writer handle's write transaction begins from the file as another process last
/// committed it, and so does the commit its close makes.
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
mod peer_commits {
    use super::*;
    use redb::{
        CompactionError, ReadableDatabase, ReadableTable, TableDefinition, TransactionError,
    };
    use std::path::Path;

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    fn open(path: &Path) -> Database {
        Database::builder()
            .set_concurrency_mode(ConcurrencyMode::MultiWriter)
            .open(path)
            .unwrap()
    }

    /// Two handles on a file that a clean close left behind, so that neither open has to
    /// repair it and commit.
    fn two_handles(path: &Path) -> (Database, Database) {
        Database::builder()
            .set_concurrency_mode(ConcurrencyMode::MultiWriter)
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

    /// The check reads the allocator snapshot a peer committed; on a corrupted file that must fail
    /// as `Corrupted`, not panic.
    #[test]
    fn check_integrity_reports_a_corrupted_file_after_a_peers_commit() {
        const PAGE_SIZE: usize = 4096;
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let path = tmpfile.path();
        let (_, peer) = two_handles(path);
        let pages = std::fs::read(path).unwrap().len() / PAGE_SIZE;

        let mut corrupted = 0;
        for page in 1..pages {
            // A fresh handle, so that the peer's commit below is one it has not seen
            let mut db = open(path);
            insert(&peer, page as u64);
            let pristine = std::fs::read(path).unwrap();
            let mut data = pristine.clone();
            data[page * PAGE_SIZE..(page + 1) * PAGE_SIZE].fill(0);
            std::fs::write(path, &data).unwrap();
            match db.check_integrity() {
                Err(DatabaseError::Storage(StorageError::Corrupted(_))) => corrupted += 1,
                Ok(_) => {}
                Err(err) => panic!("page {page}: unexpected error {err:?}"),
            }
            drop(db);
            std::fs::write(path, &pristine).unwrap();
        }
        // Zeroing a free page is tolerated, zeroing a page of the committed trees is not
        assert!(
            corrupted > 0,
            "{corrupted} of {pages} zeroed pages were rejected"
        );
    }

    /// A read on a writable handle reads the file, so it sees a peer's commit without this
    /// handle writing anything itself.
    #[test]
    fn a_read_transaction_on_a_writable_handle_sees_a_peers_commit() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let (db, peer) = two_handles(tmpfile.path());
        insert(&peer, 1);

        assert_eq!(value(&db, 1), 1);
    }

    /// The transaction sees the peer's commit, and its own commit lands after it, so that
    /// neither one is lost.
    #[test]
    fn a_write_transaction_syncs_to_a_peers_commit() {
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

    /// The peer's persistent savepoint is held from that transaction onwards, and a savepoint
    /// the transaction creates itself takes a fresh id. A savepoint the peer has deleted is
    /// forgotten, releasing the pin it held.
    #[test]
    fn a_write_transaction_syncs_to_a_peers_savepoint() {
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
        // Straight to the compaction: it refreshes the savepoints the peer deleted itself
        db.compact().unwrap();
    }

    #[test]
    fn an_aborted_savepoint_is_not_revived_by_a_peer() {
        check_abandoned_savepoint(|txn| txn.abort().unwrap());
    }

    #[test]
    fn a_dropped_transactions_savepoint_is_not_revived_by_a_peer() {
        check_abandoned_savepoint(drop);
    }

    #[test]
    fn a_panicked_transactions_savepoint_is_not_revived_by_a_peer() {
        check_abandoned_savepoint(|txn| {
            let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _txn = txn;
                panic!("unwinding through the transaction");
            }));
            assert!(unwound.is_err());
        });
    }

    fn check_abandoned_savepoint(abandon: impl Fn(redb::WriteTransaction)) {
        for advance_snapshot in [false, true] {
            let tmpfile = tempfile::NamedTempFile::new().unwrap();
            let (mut db, peer) = two_handles(tmpfile.path());
            insert(&db, 0);

            let txn = db.begin_write().unwrap();
            let id = txn.persistent_savepoint().unwrap();
            let stale = txn.get_persistent_savepoint(id).unwrap();
            abandon(txn);

            if advance_snapshot {
                insert(&peer, 1);
            }
            let txn = peer.begin_write().unwrap();
            assert_eq!(txn.persistent_savepoint().unwrap(), id);
            txn.commit().unwrap();

            let mut txn = db.begin_write().unwrap();
            assert!(matches!(
                txn.restore_savepoint(&stale),
                Err(redb::SavepointError::InvalidSavepoint)
            ));
            let replacement = txn.get_persistent_savepoint(id).unwrap();
            let another_handle = txn.get_persistent_savepoint(id).unwrap();
            txn.abort().unwrap();
            drop(stale);

            // Unchanged registrations retain their identity across peer commits and lookups.
            insert(&peer, 2);
            let mut txn = db.begin_write().unwrap();
            txn.restore_savepoint(&replacement).unwrap();
            txn.restore_savepoint(&another_handle).unwrap();
            txn.commit().unwrap();
            {
                let read = peer.begin_read().unwrap();
                let table = read.open_table(TABLE).unwrap();
                assert_eq!(table.get(0).unwrap().unwrap().value(), 0);
                assert_eq!(table.get(1).unwrap().is_some(), advance_snapshot);
                assert!(table.get(2).unwrap().is_none());
            }

            let txn = peer.begin_write().unwrap();
            assert!(txn.delete_persistent_savepoint(id).unwrap());
            txn.commit().unwrap();
            let mut txn = db.begin_write().unwrap();
            assert!(matches!(
                txn.restore_savepoint(&replacement),
                Err(redb::SavepointError::InvalidSavepoint)
            ));
            txn.abort().unwrap();
            drop(replacement);
            drop(another_handle);
            db.compact().unwrap();
            assert!(db.check_integrity().unwrap());
        }
    }

    /// When the close's commit cannot sync to the file's latest commit, the close writes no
    /// shutdown header. Writing one would put this handle's stale header, marked clean, over
    /// that commit.
    #[test]
    fn a_close_writes_no_header_over_a_commit_it_could_not_sync_to() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let (db, peer) = two_handles(tmpfile.path());
        insert(&peer, 1);
        drop(peer);
        // The peer's commit with its primary slot's checksum flipped: corruption, not a torn
        // write to fall back from, since a shared commit is 2-phase. The god byte at 9 names the
        // primary of the two 128-byte slots at 64 and 192, each ending in a 16-byte checksum
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

    /// The close commits from the file as the peer last committed it, rather than from the
    /// state this handle had.
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

    #[test]
    fn try_begin_write_does_not_wait_for_a_peers_write_transaction() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let (db, peer) = two_handles(tmpfile.path());
        let txn = peer.begin_write().unwrap();
        // The peer holds the writer byte; begin_write() would wait for it
        assert!(matches!(
            db.try_begin_write(),
            Err(TransactionError::WouldBlock)
        ));
        txn.commit().unwrap();

        let txn = db.try_begin_write().unwrap();
        txn.open_table(TABLE).unwrap().insert(1, 1).unwrap();
        txn.commit().unwrap();
        assert_eq!(value(&peer, 1), 1);
    }

    #[test]
    fn a_close_does_not_wait_for_a_peers_write_transaction() {
        use std::sync::mpsc;
        use std::thread;
        use std::time::Duration;

        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let (db, peer) = two_handles(tmpfile.path());
        insert(&db, 1);
        insert(&peer, 2);
        let txn = peer.begin_write().unwrap();
        txn.open_table(TABLE).unwrap().insert(3, 3).unwrap();

        let (tx, rx) = mpsc::channel();
        let closing = thread::spawn(move || {
            drop(db);
            tx.send(()).unwrap();
        });
        let closed_while_writing = rx.recv_timeout(Duration::from_secs(5)).is_ok();
        // Release the writer before asserting, so a regression does not leave the test hung.
        txn.commit().unwrap();
        closing.join().unwrap();
        assert!(
            closed_while_writing,
            "close waited for the peer's writer lock"
        );
        drop(peer);

        let mut db = open(tmpfile.path());
        for key in 1..=3 {
            assert_eq!(value(&db, key), key);
        }
        assert!(db.check_integrity().unwrap());
    }

    /// A transaction dropped while a panic unwinds leaves its pages in the write buffer, and
    /// the peer can then commit those same pages. The sync discards the buffer, so the page
    /// reads back as the peer committed it.
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

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
#[test]
fn file_backend_custom_open_supports_shared_modes() {
    #[cfg(feature = "experimental-api-5")]
    use redb::ReadableTable;
    use redb::{ReadableDatabase, TableDefinition};

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");
    for &mode in shared_modes() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let mut builder = Database::builder();
        builder.set_concurrency_mode(mode);
        let open = || builder.create_with_backend(file_backend(tmpfile.reopen().unwrap()));
        let db = open().unwrap();
        let write = db.begin_write().unwrap();
        write.open_table(TABLE).unwrap().insert(0, 0).unwrap();
        write.commit().unwrap();

        let reader = builder.open_read_only(tmpfile.path()).unwrap();
        let snapshot = reader.begin_read().unwrap().open_table(TABLE).unwrap();
        assert!(matches!(
            Database::open(tmpfile.path()),
            Err(DatabaseError::DatabaseAlreadyOpen)
        ));

        // A single writer excludes a second one; a multi-writer admits it
        let peer = if mode == ConcurrencyMode::SingleWriter {
            assert!(matches!(open(), Err(DatabaseError::DatabaseAlreadyOpen)));
            None
        } else {
            Some(open().unwrap())
        };
        for value in 1..4 {
            let writer = peer.as_ref().unwrap_or(&db);
            let write = writer.begin_write().unwrap();
            write.open_table(TABLE).unwrap().insert(0, value).unwrap();
            write.commit().unwrap();

            assert_eq!(snapshot.get(0).unwrap().unwrap().value(), 0);
            for current in [reader.begin_read().unwrap(), db.begin_read().unwrap()] {
                assert_eq!(
                    current
                        .open_table(TABLE)
                        .unwrap()
                        .get(0)
                        .unwrap()
                        .unwrap()
                        .value(),
                    value
                );
            }
        }
        drop(snapshot);
        drop(reader);
        drop(peer);
        drop(db);
        Database::open(tmpfile.path()).unwrap();
    }
}

/// A backend without locks can only be used in exclusive-writer mode.
#[test]
fn sharing_a_backend_without_locks_is_unsupported() {
    for &mode in shared_modes() {
        let mut builder = Database::builder();
        builder.set_concurrency_mode(mode);
        let err = builder
            .create_with_backend(InMemoryBackend::new())
            .unwrap_err();
        assert!(
            matches!(err, DatabaseError::Storage(StorageError::Unsupported)),
            "{mode:?}"
        );
    }

    // ... while the default mode opens on one as it always has
    Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
}

/// An unsupported lock reported as an I/O error, rather than as `BackendError::Unsupported`
mod unsupported_io_error {
    use super::*;
    use redb::{BackendError, StorageBackend};
    use std::io;
    use std::ops::Bound;

    // The error type of StorageBackend's I/O methods
    #[cfg(feature = "experimental-api-5")]
    type BackendIoError = redb::BackendError;
    #[cfg(not(feature = "experimental-api-5"))]
    type BackendIoError = std::io::Error;

    #[derive(Debug)]
    struct NoLocks {
        inner: InMemoryBackend,
        wrap_in_other: bool,
    }

    impl NoLocks {
        fn unsupported<T>(&self) -> Result<T, BackendError> {
            let err = io::Error::new(io::ErrorKind::Unsupported, "no locks here");
            if self.wrap_in_other {
                Err(BackendError::Other(Box::new(err)))
            } else {
                Err(BackendError::Io(err))
            }
        }
    }

    impl StorageBackend for NoLocks {
        fn len(&self) -> Result<u64, BackendIoError> {
            self.inner.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), BackendIoError> {
            self.inner.read(offset, out)
        }

        fn set_len(&self, len: u64) -> Result<(), BackendIoError> {
            self.inner.set_len(len)
        }

        fn sync_data(&self) -> Result<(), BackendIoError> {
            self.inner.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> Result<(), BackendIoError> {
            self.inner.write(offset, data)
        }

        fn try_lock_range(&self, _: Bound<u64>, _: Bound<u64>) -> Result<bool, BackendError> {
            self.unsupported()
        }

        fn try_lock_shared_range(
            &self,
            _: Bound<u64>,
            _: Bound<u64>,
        ) -> Result<bool, BackendError> {
            self.unsupported()
        }

        fn lock_range(&self, _: Bound<u64>, _: Bound<u64>) -> Result<(), BackendError> {
            self.unsupported()
        }

        fn lock_shared_range(&self, _: Bound<u64>, _: Bound<u64>) -> Result<(), BackendError> {
            self.unsupported()
        }

        fn unlock_range(&self, _: Bound<u64>, _: Bound<u64>) -> Result<(), BackendError> {
            self.unsupported()
        }

        fn query_lock_range(&self, _: Bound<u64>, _: Bound<u64>) -> Result<bool, BackendError> {
            self.unsupported()
        }
    }

    fn no_locks(wrap_in_other: bool) -> NoLocks {
        NoLocks {
            inner: InMemoryBackend::new(),
            wrap_in_other,
        }
    }

    /// It is treated as `BackendError::Unsupported`, as `BackendError::Io` and wrapped in
    /// `BackendError::Other`
    #[test]
    fn is_unsupported() {
        for wrap_in_other in [false, true] {
            for &mode in shared_modes() {
                let mut builder = Database::builder();
                builder.set_concurrency_mode(mode);
                let err = builder
                    .create_with_backend(no_locks(wrap_in_other))
                    .unwrap_err();
                assert!(
                    matches!(err, DatabaseError::Storage(StorageError::Unsupported)),
                    "{mode:?}, wrapped in Other: {wrap_in_other}: {err:?}"
                );
            }

            Database::builder()
                .create_with_backend(no_locks(wrap_in_other))
                .unwrap();
        }
    }

    #[derive(Debug)]
    struct UnsupportedSync(InMemoryBackend);

    impl StorageBackend for UnsupportedSync {
        fn len(&self) -> Result<u64, BackendIoError> {
            self.0.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), BackendIoError> {
            self.0.read(offset, out)
        }

        fn set_len(&self, len: u64) -> Result<(), BackendIoError> {
            self.0.set_len(len)
        }

        fn sync_data(&self) -> Result<(), BackendIoError> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "fdatasync is not supported",
            ))?
        }

        fn write(&self, offset: u64, data: &[u8]) -> Result<(), BackendIoError> {
            self.0.write(offset, data)
        }
    }

    /// An I/O method's error keeps its message
    #[test]
    fn io_method_error_keeps_message() {
        let err = Database::builder()
            .create_with_backend(UnsupportedSync(InMemoryBackend::new()))
            .unwrap_err();
        match err {
            DatabaseError::Storage(StorageError::Io(err)) => {
                assert_eq!(err.kind(), io::ErrorKind::Unsupported);
                assert_eq!(err.to_string(), "fdatasync is not supported");
            }
            err => panic!("expected the backend's I/O error, got {err:?}"),
        }
    }
}

/// A lock whose release fails is still held, where it blocks the processes that share the file
#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
mod failed_unlock {
    use super::*;
    use redb::{BackendError, ReadableDatabase, StorageBackend, TableDefinition, TransactionError};
    use std::io;
    use std::ops::Bound;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    // The error type of StorageBackend's I/O methods
    #[cfg(feature = "experimental-api-5")]
    type BackendIoError = redb::BackendError;
    #[cfg(not(feature = "experimental-api-5"))]
    type BackendIoError = std::io::Error;

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("x");

    /// Fails to release the header lock while `stuck`, and leaves it held
    #[derive(Debug)]
    struct StuckHeaderLock {
        inner: FileBackend,
        stuck: Arc<AtomicBool>,
    }

    impl StorageBackend for StuckHeaderLock {
        fn len(&self) -> Result<u64, BackendIoError> {
            self.inner.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), BackendIoError> {
            self.inner.read(offset, out)
        }

        fn set_len(&self, len: u64) -> Result<(), BackendIoError> {
            self.inner.set_len(len)
        }

        fn sync_data(&self) -> Result<(), BackendIoError> {
            self.inner.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> Result<(), BackendIoError> {
            self.inner.write(offset, data)
        }

        fn close(&self) -> Result<(), BackendIoError> {
            self.inner.close()
        }

        fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
            self.inner.try_lock_range(start, end)
        }

        fn try_lock_shared_range(
            &self,
            start: Bound<u64>,
            end: Bound<u64>,
        ) -> Result<bool, BackendError> {
            self.inner.try_lock_shared_range(start, end)
        }

        fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
            self.inner.lock_range(start, end)
        }

        fn lock_shared_range(
            &self,
            start: Bound<u64>,
            end: Bound<u64>,
        ) -> Result<(), BackendError> {
            self.inner.lock_shared_range(start, end)
        }

        fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
            // The header lock is the only one that starts at the first byte of the file
            if start == Bound::Included(0) && self.stuck.load(Ordering::SeqCst) {
                return Err(BackendError::Io(io::Error::other(
                    "the lock could not be released",
                )));
            }
            self.inner.unlock_range(start, end)
        }

        fn query_lock_range(
            &self,
            start: Bound<u64>,
            end: Bound<u64>,
        ) -> Result<bool, BackendError> {
            self.inner.query_lock_range(start, end)
        }
    }

    fn write(db: &Database) -> Result<(), redb::Error> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(TABLE)?;
            table.insert(0, 0)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// The database stops working, so that the application closes it, which releases the lock
    #[test]
    fn fails_the_database_until_it_is_closed() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let stuck = Arc::new(AtomicBool::new(false));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmpfile.path())
            .unwrap();
        let mut builder = Database::builder();
        builder.set_concurrency_mode(ConcurrencyMode::MultiWriter);
        let db = builder
            .create_with_backend(StuckHeaderLock {
                inner: file_backend(file),
                stuck: stuck.clone(),
            })
            .unwrap();
        let peer = builder.create(tmpfile.path()).unwrap();

        // Every release of the header lock in this transaction fails. Whether the transaction
        // itself reports that depends on which release came first
        stuck.store(true, Ordering::SeqCst);
        let _ = write(&db);
        stuck.store(false, Ordering::SeqCst);

        assert!(matches!(
            db.begin_read(),
            Err(TransactionError::Storage(StorageError::PreviousIo))
        ));
        assert!(matches!(
            db.begin_write(),
            Err(TransactionError::Storage(StorageError::PreviousIo))
        ));

        // The lock is still held, so the peer waits for it ...
        let (tx, rx) = mpsc::channel();
        let waiting = std::thread::spawn(move || {
            let result = write(&peer);
            tx.send(()).unwrap();
            result.unwrap();
        });
        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "the peer was not blocked by the lock that failed to release"
        );

        // ... until the database is closed
        drop(db);
        rx.recv_timeout(Duration::from_secs(10))
            .expect("the peer was still blocked after the database was closed");
        waiting.join().unwrap();
    }
}

#[cfg(any(target_os = "linux", target_vendor = "apple", windows))]
mod reclamation {
    use super::*;
    #[cfg(feature = "experimental-api-5")]
    use redb::ReadableTable;
    use redb::{ReadableDatabase, TableDefinition};

    const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

    /// A read transaction in another process pins its snapshot: the writer's reclamation stops
    /// at it, however many commits go by
    #[test]
    fn a_readers_pin_survives_heavy_reclamation() {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let writer = Database::builder()
            .set_concurrency_mode(ConcurrencyMode::SingleWriter)
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
            .set_concurrency_mode(ConcurrencyMode::SingleWriter)
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
    #[cfg(feature = "experimental-api-5")]
    use redb::ReadableTable;
    use redb::{
        CompactionError, MultimapTableDefinition, ReadOnlyDatabase, ReadableDatabase,
        ReadableTableMetadata, TableDefinition,
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
        for &mode in shared_modes() {
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

    #[test]
    fn untyped_tables_pin_a_peers_snapshot_until_dropped() {
        const MULTIMAP: MultimapTableDefinition<u64, &[u8]> = MultimapTableDefinition::new("multi");
        for &mode in shared_modes() {
            for multimap in [false, true] {
                let tmpfile = tempfile::NamedTempFile::new().unwrap();
                let mut writer = create(tmpfile.path(), mode);
                let value = [7u8; 512];
                let write = writer.begin_write().unwrap();
                {
                    let mut table = write.open_table(TABLE).unwrap();
                    let mut multi = write.open_multimap_table(MULTIMAP).unwrap();
                    for key in 0..128u64 {
                        table.insert(key, value.as_slice()).unwrap();
                        multi.insert(key, value.as_slice()).unwrap();
                    }
                }
                write.commit().unwrap();

                let reader = Database::builder()
                    .set_concurrency_mode(mode)
                    .set_cache_size(0)
                    .open_read_only(tmpfile.path())
                    .unwrap();
                let read = reader.begin_read().unwrap();
                let table: Box<dyn ReadableTableMetadata> = if multimap {
                    Box::new(read.open_untyped_multimap_table(MULTIMAP).unwrap())
                } else {
                    Box::new(read.open_untyped_table(TABLE).unwrap())
                };
                let before = table.stats().unwrap();
                assert!(before.branch_pages() > 0);
                drop(read);

                let write = writer.begin_write().unwrap();
                write.delete_table(TABLE).unwrap();
                write.delete_multimap_table(MULTIMAP).unwrap();
                write.commit().unwrap();
                for _ in 0..3 {
                    writer.begin_write().unwrap().commit().unwrap();
                }
                assert!(
                    matches!(
                        writer.compact(),
                        Err(CompactionError::TransactionInProgress)
                    ),
                    "{mode:?}, multimap={multimap}"
                );

                // The table alone pins the snapshot, and an empty cache forces stats to read it.
                let after = table.stats().unwrap();
                assert_eq!(table.len().unwrap(), 128);
                assert_eq!(after.stored_bytes(), before.stored_bytes());
                assert_eq!(after.tree_height(), before.tree_height());
                assert_eq!(after.leaf_pages(), before.leaf_pages());
                assert_eq!(after.branch_pages(), before.branch_pages());

                drop(table);
                writer.compact().unwrap();
            }
        }
    }

    /// The file shrinks under a peer that has it open, and the peer's next read follows the pages
    /// that moved
    #[test]
    fn compaction_shrinks_a_file_a_peer_has_open() {
        for &mode in shared_modes() {
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
