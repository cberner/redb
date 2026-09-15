#[cfg(feature = "experimental-api-5")]
use redb::ReadableTable;
use redb::backends::InMemoryBackend;
use redb::{Database, Durability, ReadableDatabase, TableDefinition, WriteTransaction};
use redb::{StorageBackend, StorageError, TransactionError};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("x");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

fn insert_rows(txn: &WriteTransaction, start: u64, end: u64) {
    let mut table = txn.open_table(TABLE).unwrap();
    let value = vec![0xAB; 2000];
    for key in start..end {
        table.insert(key, value.as_slice()).unwrap();
    }
}

#[test]
fn abort_restores_file_size() {
    for drop_transaction in [false, true] {
        for cache_size in [0, 1024 * 1024] {
            for durability in [Durability::Immediate, Durability::None] {
                let tmpfile = create_tempfile();
                let mut db = Database::builder()
                    .set_cache_size(cache_size)
                    .create(tmpfile.path())
                    .unwrap();
                let txn = db.begin_write().unwrap();
                txn.open_table(TABLE)
                    .unwrap()
                    .insert(0, b"committed".as_slice())
                    .unwrap();
                txn.commit().unwrap();

                let mut txn = db.begin_write().unwrap();
                txn.set_durability(durability).unwrap();
                insert_rows(&txn, 1, 600);
                txn.commit().unwrap();
                let original_len = tmpfile.as_file().metadata().unwrap().len();
                let reader = db.begin_read().unwrap();

                // Repeat to exercise allocation after the file has been shrunk.
                for _ in 0..2 {
                    let txn = db.begin_write().unwrap();
                    insert_rows(&txn, 0, 1800);
                    assert!(tmpfile.as_file().metadata().unwrap().len() > original_len);
                    if drop_transaction {
                        drop(txn);
                    } else {
                        txn.abort().unwrap();
                    }
                    assert_eq!(tmpfile.as_file().metadata().unwrap().len(), original_len);
                    let table = reader.open_table(TABLE).unwrap();
                    assert_eq!(table.get(0).unwrap().unwrap().value(), b"committed");
                    assert_eq!(table.get(599).unwrap().unwrap().value(), vec![0xAB; 2000]);
                    assert!(table.get(600).unwrap().is_none());
                }
                drop(reader);
                assert!(db.check_integrity().unwrap());

                let txn = db.begin_write().unwrap();
                insert_rows(&txn, 600, 1800);
                txn.commit().unwrap();
                drop(db);

                let mut db = Database::open(tmpfile.path()).unwrap();
                assert!(db.check_integrity().unwrap());
                let reader = db.begin_read().unwrap();
                let table = reader.open_table(TABLE).unwrap();
                assert_eq!(table.get(0).unwrap().unwrap().value(), b"committed");
                assert_eq!(table.get(1799).unwrap().unwrap().value(), vec![0xAB; 2000]);
            }
        }
    }
}

#[test]
fn abort_restores_empty_database_size() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let original_len = tmpfile.as_file().metadata().unwrap().len();
    let txn = db.begin_write().unwrap();
    insert_rows(&txn, 0, 1800);
    assert!(tmpfile.as_file().metadata().unwrap().len() > original_len);
    txn.abort().unwrap();
    assert_eq!(tmpfile.as_file().metadata().unwrap().len(), original_len);
    assert!(
        db.begin_read()
            .unwrap()
            .list_tables()
            .unwrap()
            .next()
            .is_none()
    );
    assert!(db.check_integrity().unwrap());
}

#[derive(Clone, Debug, Default)]
struct TestBackend {
    inner: Arc<InMemoryBackend>,
    fail_truncate: Arc<AtomicBool>,
}

impl StorageBackend for TestBackend {
    fn len(&self) -> std::io::Result<u64> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.inner.read(offset, out)
    }

    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.inner.write(offset, data)
    }

    fn set_len(&self, len: u64) -> std::io::Result<()> {
        if len < self.inner.len()? && self.fail_truncate.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("truncate failed"));
        }
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> std::io::Result<()> {
        self.inner.sync_data()
    }
}

#[test]
fn durable_commit_after_abort_discards_aborted_writes() {
    for cache_size in [1024 * 1024, 32 * 1024 * 1024] {
        for drop_transaction in [false, true] {
            // InMemoryBackend rejects writes past EOF, even if a later resize would hide them.
            let backend = TestBackend::default();
            let mut db = Database::builder()
                .set_cache_size(cache_size)
                .create_with_backend(backend.clone())
                .unwrap();
            let txn = db.begin_write().unwrap();
            insert_rows(&txn, 0, 600);
            txn.commit().unwrap();

            let mut txn = db.begin_write().unwrap();
            txn.set_durability(Durability::None).unwrap();
            txn.open_table(TABLE)
                .unwrap()
                .insert(0, b"buffered commit".as_slice())
                .unwrap();
            txn.commit().unwrap();
            let original_len = backend.len().unwrap();

            let txn = db.begin_write().unwrap();
            insert_rows(&txn, 0, 1800);
            assert!(backend.len().unwrap() > original_len);
            if drop_transaction {
                drop(txn);
            } else {
                txn.abort().unwrap();
            }
            assert_eq!(backend.len().unwrap(), original_len);

            // Flush immediately, before integrity checking or another transaction grows the file.
            db.begin_write().unwrap().commit().unwrap();
            assert!(backend.len().unwrap() <= original_len);
            assert!(db.check_integrity().unwrap());
            drop(db);

            let db = Database::builder().create_with_backend(backend).unwrap();
            let reader = db.begin_read().unwrap();
            let table = reader.open_table(TABLE).unwrap();
            assert_eq!(table.get(0).unwrap().unwrap().value(), b"buffered commit");
            for key in 1..600 {
                assert_eq!(table.get(key).unwrap().unwrap().value(), vec![0xAB; 2000]);
            }
            assert!(table.get(600).unwrap().is_none());
        }
    }
}

#[test]
fn abort_reports_truncation_failure() {
    let backend = TestBackend::default();
    let db = Database::builder()
        .create_with_backend(backend.clone())
        .unwrap();
    let txn = db.begin_write().unwrap();
    txn.open_table(TABLE)
        .unwrap()
        .insert(0, b"committed".as_slice())
        .unwrap();
    txn.commit().unwrap();

    let original_len = backend.len().unwrap();
    let txn = db.begin_write().unwrap();
    insert_rows(&txn, 0, 1800);
    assert!(backend.len().unwrap() > original_len);
    backend.fail_truncate.store(true, Ordering::SeqCst);
    assert!(matches!(txn.abort(), Err(StorageError::Io(_))));
    assert!(matches!(
        db.begin_write(),
        Err(TransactionError::Storage(StorageError::PreviousIo))
    ));
    drop(db);

    backend.fail_truncate.store(false, Ordering::SeqCst);
    let mut db = Database::builder().create_with_backend(backend).unwrap();
    assert!(db.check_integrity().unwrap());
    let reader = db.begin_read().unwrap();
    let table = reader.open_table(TABLE).unwrap();
    assert_eq!(table.get(0).unwrap().unwrap().value(), b"committed");
    assert!(table.get(1).unwrap().is_none());
}
