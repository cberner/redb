#![cfg(redb_multiprocess)]

use redb::backends::InMemoryBackend;
use redb::{
    ConcurrencyMode, Database, DatabaseError, ReadableDatabase, ReadableTable,
    ReadableTableMetadata, StorageError, TableDefinition,
};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

fn create_tempfile() -> tempfile::NamedTempFile {
    tempfile::NamedTempFile::new().unwrap()
}

fn allocated_pages(db: &Database) -> u64 {
    let txn = db.begin_write().unwrap();
    let pages = txn.stats().unwrap().allocated_pages();
    txn.abort().unwrap();
    pages
}

// Two databases opened on one path are two open file descriptions, which the byte-range
// locks treat exactly as two processes: every test here drives the cross-process protocol
// for real, in one address space.

#[test]
fn a_reader_process_follows_the_writers_commits() {
    let tmpfile = create_tempfile();
    let writer = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::SingleWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let reader = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::SingleWriterProcess)
        .open_read_only(tmpfile.path())
        .unwrap();

    let txn = writer.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        t.insert(&1, [1u8; 64].as_slice()).unwrap();
    }
    txn.commit().unwrap();

    // The commit is in the file, and the reader's next transaction reads the file
    let read = reader.begin_read().unwrap();
    let t = read.open_table(TABLE).unwrap();
    assert_eq!(t.get(&1).unwrap().unwrap().value(), [1u8; 64].as_slice());
    drop(t);

    let txn = writer.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        t.insert(&1, [2u8; 64].as_slice()).unwrap();
        t.insert(&2, [3u8; 64].as_slice()).unwrap();
    }
    txn.commit().unwrap();

    // The old read transaction goes on reading its own snapshot ...
    let t = read.open_table(TABLE).unwrap();
    assert_eq!(t.get(&1).unwrap().unwrap().value(), [1u8; 64].as_slice());
    assert!(t.get(&2).unwrap().is_none());
    drop(t);
    drop(read);

    // ... and a new one sees the new state
    let read = reader.begin_read().unwrap();
    let t = read.open_table(TABLE).unwrap();
    assert_eq!(t.get(&1).unwrap().unwrap().value(), [2u8; 64].as_slice());
    assert_eq!(t.get(&2).unwrap().unwrap().value(), [3u8; 64].as_slice());
}

#[test]
fn writer_processes_alternate_and_see_each_other() {
    let tmpfile = create_tempfile();
    let a = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let b = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();

    for round in 0..10u64 {
        let txn = a.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            t.insert(&(round * 2), [1u8; 32].as_slice()).unwrap();
        }
        txn.commit().unwrap();

        // B's transaction begins by adopting A's commit from the file
        let txn = b.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            assert!(t.get(&(round * 2)).unwrap().is_some());
            t.insert(&(round * 2 + 1), [2u8; 32].as_slice()).unwrap();
        }
        txn.commit().unwrap();

        let read = a.begin_read().unwrap();
        let t = read.open_table(TABLE).unwrap();
        assert!(t.get(&(round * 2 + 1)).unwrap().is_some());
    }

    drop(a);
    drop(b);
    // The cohort is gone; the plain single-process open reads everything they wrote
    let plain = Database::open(tmpfile.path()).unwrap();
    let read = plain.begin_read().unwrap();
    let t = read.open_table(TABLE).unwrap();
    assert_eq!(t.len().unwrap(), 20);
}

#[test]
fn a_writers_cache_is_invalidated_by_the_others_reuse() {
    let tmpfile = create_tempfile();
    let a = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let b = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();

    // A writes and reads its own data, filling its page cache
    let txn = a.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        for key in 0..256u64 {
            t.insert(&key, vec![0xAAu8; 256].as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
    let read = a.begin_read().unwrap();
    let t = read.open_table(TABLE).unwrap();
    for key in 0..256u64 {
        assert_eq!(t.get(&key).unwrap().unwrap().value()[0], 0xAA);
    }
    drop(t);
    drop(read);

    // B overwrites everything repeatedly, freeing and reusing the pages A has cached
    for round in 0..8u8 {
        let txn = b.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for key in 0..256u64 {
                t.insert(&key, vec![round; 256].as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    // A's next read picks up B's commits and drops its cache: every value read is B's
    let read = a.begin_read().unwrap();
    let t = read.open_table(TABLE).unwrap();
    for key in 0..256u64 {
        assert_eq!(t.get(&key).unwrap().unwrap().value()[0], 7);
    }
}

#[test]
fn the_concurrency_mode_excludes_incompatible_opens() {
    let tmpfile = create_tempfile();

    // A single-process holder refuses every multi-process open
    let plain = Database::create(tmpfile.path()).unwrap();
    assert!(matches!(
        Database::builder()
            .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
            .open(tmpfile.path()),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    assert!(matches!(
        Database::builder()
            .set_concurrency_mode(ConcurrencyMode::SingleWriterProcess)
            .open_read_only(tmpfile.path()),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    drop(plain);

    // A single-writer holder refuses a second writer of either kind, but admits readers
    let sole = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::SingleWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    assert!(matches!(
        Database::builder()
            .set_concurrency_mode(ConcurrencyMode::SingleWriterProcess)
            .open(tmpfile.path()),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    assert!(matches!(
        Database::builder()
            .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
            .open(tmpfile.path()),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    assert!(matches!(
        Database::open(tmpfile.path()),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    let reader = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::SingleWriterProcess)
        .open_read_only(tmpfile.path())
        .unwrap();
    drop(sole);

    // A live reader keeps refusing the single-process open on its own
    assert!(matches!(
        Database::open(tmpfile.path()),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    drop(reader);

    // A single-process read-only holder refuses both writing modes
    let plain_reader = Database::builder().open_read_only(tmpfile.path()).unwrap();
    assert!(matches!(
        Database::builder()
            .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
            .open(tmpfile.path()),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    assert!(matches!(
        Database::builder()
            .set_concurrency_mode(ConcurrencyMode::SingleWriterProcess)
            .open(tmpfile.path()),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    // Two single-process readers still share
    let second_reader = Database::builder().open_read_only(tmpfile.path()).unwrap();
    drop(second_reader);
    drop(plain_reader);

    // Multi-writer handles admit each other ...
    let a = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    let b = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();

    // ... and refuse a single-writer open and both single-process opens
    assert!(matches!(
        Database::builder()
            .set_concurrency_mode(ConcurrencyMode::SingleWriterProcess)
            .open(tmpfile.path()),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    assert!(matches!(
        Database::open(tmpfile.path()),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    assert!(matches!(
        Database::builder().open_read_only(tmpfile.path()),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    drop(a);
    drop(b);

    // Everyone gone: the plain open works again
    Database::open(tmpfile.path()).unwrap();
}

#[test]
fn a_plain_database_opens_in_a_multi_process_mode_and_plainly_again() {
    let tmpfile = create_tempfile();
    {
        let db = Database::create(tmpfile.path()).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            t.insert(&42, [42u8; 64].as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }

    // A database written plainly opens in a multi-process mode with its data intact
    let mp = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    let read = mp.begin_read().unwrap();
    let t = read.open_table(TABLE).unwrap();
    assert_eq!(t.get(&42).unwrap().unwrap().value(), [42u8; 64].as_slice());
    drop(t);
    drop(read);
    drop(mp);

    // ... and the plain open takes it back afterwards
    let plain = Database::open(tmpfile.path()).unwrap();
    let txn = plain.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        t.insert(&43, [43u8; 64].as_slice()).unwrap();
    }
    txn.commit().unwrap();
}

#[test]
fn ephemeral_savepoints_are_refused_only_in_multi_writer() {
    let tmpfile = create_tempfile();
    {
        let db = Database::builder()
            .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
            .create(tmpfile.path())
            .unwrap();
        let txn = db.begin_write().unwrap();
        assert!(matches!(
            txn.ephemeral_savepoint(),
            Err(redb::SavepointError::InvalidSavepoint)
        ));
        // The persistent kind takes its id from the shared counter, and works
        txn.persistent_savepoint().unwrap();
        txn.commit().unwrap();
    }
    let db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::SingleWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    let txn = db.begin_write().unwrap();
    let savepoint = txn.ephemeral_savepoint().unwrap();
    drop(savepoint);
    txn.abort().unwrap();
}

#[test]
fn a_persistent_savepoint_holds_pages_across_processes() {
    let tmpfile = create_tempfile();
    let savepoint_id = {
        let a = Database::builder()
            .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
            .create(tmpfile.path())
            .unwrap();
        let txn = a.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for key in 0..64u64 {
                t.insert(&key, vec![0x5Au8; 256].as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        // A savepoint is taken before the transaction touches anything
        let txn = a.begin_write().unwrap();
        let id = txn.persistent_savepoint().unwrap();
        txn.commit().unwrap();
        id
        // A closes: only the savepoint's record in the database protects its pages now
    };

    let b = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    // B adopts the savepoint at its first write transaction, and its reclamation spares the
    // savepoint's pages through every overwrite
    for round in 0..10u8 {
        let txn = b.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for key in 0..64u64 {
                t.insert(&key, vec![round; 256].as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    // Restoring in B rolls back to the state A saved
    let mut txn = b.begin_write().unwrap();
    let savepoint = txn.get_persistent_savepoint(savepoint_id).unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    drop(savepoint);
    txn.commit().unwrap();

    let read = b.begin_read().unwrap();
    let t = read.open_table(TABLE).unwrap();
    for key in 0..64u64 {
        assert_eq!(t.get(&key).unwrap().unwrap().value(), vec![0x5Au8; 256]);
    }
}

#[test]
fn compaction_is_refused_in_the_multiprocess_modes() {
    for mode in [
        ConcurrencyMode::SingleWriterProcess,
        ConcurrencyMode::MultiWriterProcess,
    ] {
        let tmpfile = create_tempfile();
        let mut db = Database::builder()
            .set_concurrency_mode(mode)
            .create(tmpfile.path())
            .unwrap();
        // Under a coordinator every commit leaves frees pending, so the drain compaction starts
        // with would never finish
        match db.compact().unwrap_err() {
            redb::CompactionError::Storage(redb::StorageError::Io(err)) => {
                assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

#[test]
fn a_shared_open_adopts_a_plain_databases_savepoint() {
    let tmpfile = create_tempfile();
    // A plain database: data, a savepoint guarding it, then an overwrite that queues the
    // savepoint's pages as pending frees
    let savepoint_id = {
        let db = Database::create(tmpfile.path()).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for key in 0..64u64 {
                t.insert(&key, vec![0x5Au8; 256].as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        let txn = db.begin_write().unwrap();
        let id = txn.persistent_savepoint().unwrap();
        txn.commit().unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for key in 0..64u64 {
                t.insert(&key, vec![0xA5u8; 256].as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        id
    };

    // Every writer adopts the file's savepoints before it can free a page, so the commits
    // below must spare what this one holds
    let db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();
    for round in 0..10u8 {
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for key in 0..64u64 {
                t.insert(&key, vec![round; 256].as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    let mut txn = db.begin_write().unwrap();
    let savepoint = txn.get_persistent_savepoint(savepoint_id).unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    drop(savepoint);
    txn.commit().unwrap();

    let read = db.begin_read().unwrap();
    let t = read.open_table(TABLE).unwrap();
    for key in 0..64u64 {
        assert_eq!(t.get(&key).unwrap().unwrap().value(), vec![0x5Au8; 256]);
    }
}

#[test]
fn a_lingering_reader_does_not_block_the_plain_reopen() {
    let tmpfile = create_tempfile();
    let db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        t.insert(&1, [1u8; 16].as_slice()).unwrap();
    }
    txn.commit().unwrap();

    // The guard outlives the database, but the close takes the storage with it: the pin is
    // released too, so the plain open's whole-range lock is not refused by a dead reader
    let read = db.begin_read().unwrap();
    drop(db);
    let plain = Database::open(tmpfile.path()).unwrap();
    let t = plain.begin_read().unwrap().open_table(TABLE).unwrap();
    assert_eq!(t.get(&1).unwrap().unwrap().value(), [1u8; 16]);
    drop(read);
}

#[test]
fn an_aborted_multi_writer_transaction_leaves_nothing_stale() {
    let tmpfile = create_tempfile();
    let a = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let b = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();

    let txn = a.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        for key in 0..64u64 {
            t.insert(&key, vec![0x11u8; 512].as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    // A writes and reads back pages it then abandons; nothing of them may survive the abort,
    // since B can fill the same pages with no horizon movement to invalidate copies
    let txn = a.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        for key in 0..64u64 {
            t.insert(&key, vec![0x22u8; 512].as_slice()).unwrap();
        }
        for key in 0..64u64 {
            assert_eq!(t.get(&key).unwrap().unwrap().value()[0], 0x22);
        }
    }
    txn.abort().unwrap();

    let txn = b.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        for key in 0..64u64 {
            t.insert(&key, vec![0x33u8; 512].as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let read = a.begin_read().unwrap();
    let t = read.open_table(TABLE).unwrap();
    for key in 0..64u64 {
        assert_eq!(t.get(&key).unwrap().unwrap().value()[0], 0x33);
    }
}

#[test]
fn integrity_checks_are_refused_in_multi_writer_mode() {
    let tmpfile = create_tempfile();
    let mut db = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    // Nothing serializes the check's reloads and repair commits against another process
    match db.check_integrity().unwrap_err() {
        redb::DatabaseError::Storage(redb::StorageError::Io(err)) => {
            assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

/// A savepoint one process deletes must stop holding pages back everywhere, including in a
/// handle that adopted it and then went idle
#[test]
fn a_deleted_savepoint_stops_holding_pages_in_an_idle_handle() {
    let tmpfile = create_tempfile();
    let writer = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let idle = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
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

    let txn = writer.begin_write().unwrap();
    let savepoint = txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();

    // The other handle adopts the savepoint, and then does nothing for the rest of the test
    idle.begin_write().unwrap().commit().unwrap();

    let txn = writer.begin_write().unwrap();
    txn.delete_persistent_savepoint(savepoint).unwrap();
    txn.commit().unwrap();

    let mut steady = 0;
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
        // Every round frees the last round's pages, so the count settles once the deleted
        // savepoint stops holding them, and climbs every round while anything still does
        if round == 9 {
            steady = allocated_pages(&writer);
        }
    }
    assert_eq!(steady, allocated_pages(&writer));
}

/// The pin a savepoint's creation takes must go when the savepoint becomes persistent: the
/// creator may sit idle long after another process deletes it
#[test]
fn a_savepoints_creator_stops_holding_pages_once_it_is_persistent() {
    let tmpfile = create_tempfile();
    let creator = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .create(tmpfile.path())
        .unwrap();
    let other = Database::builder()
        .set_concurrency_mode(ConcurrencyMode::MultiWriterProcess)
        .open(tmpfile.path())
        .unwrap();

    let value = vec![7u8; 512];
    let txn = creator.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        for key in 0..128u64 {
            t.insert(&key, value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    // The creator makes the savepoint and then does nothing for the rest of the test
    let txn = creator.begin_write().unwrap();
    let savepoint = txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();

    let txn = other.begin_write().unwrap();
    txn.delete_persistent_savepoint(savepoint).unwrap();
    txn.commit().unwrap();

    let mut steady = 0;
    for round in 0..20u8 {
        let overwrite = vec![round; 512];
        let txn = other.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for key in 0..128u64 {
                t.insert(&key, overwrite.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        if round == 9 {
            steady = allocated_pages(&other);
        }
    }
    assert_eq!(steady, allocated_pages(&other));
}

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

/// The multi-process modes need the file itself, which neither a caller-supplied backend nor a
/// caller-supplied `File` hands over
#[test]
fn sharing_a_caller_supplied_backend_is_unsupported() {
    for mode in [
        ConcurrencyMode::SingleWriterProcess,
        ConcurrencyMode::MultiWriterProcess,
    ] {
        let mut builder = Database::builder();
        builder.set_concurrency_mode(mode);
        let err = builder
            .create_with_backend(InMemoryBackend::new())
            .unwrap_err();
        assert!(matches!(
            err,
            DatabaseError::Storage(StorageError::Io(err)) if err.kind() == std::io::ErrorKind::Unsupported
        ));

        let tmpfile = create_tempfile();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmpfile.path())
            .unwrap();
        let err = builder.create_file(file).unwrap_err();
        assert!(matches!(
            err,
            DatabaseError::Storage(StorageError::Io(err)) if err.kind() == std::io::ErrorKind::Unsupported
        ));
    }

    // ... while the default mode opens on both as it always has
    Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let tmpfile = create_tempfile();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(tmpfile.path())
        .unwrap();
    Database::builder().create_file(file).unwrap();
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
