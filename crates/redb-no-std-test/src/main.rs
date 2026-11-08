use redb::backends::InMemoryBackend;
use redb::{
    BackendError, Database, DatabaseError, ReadableDatabase, ReadableTable, StorageBackend,
    StorageError, TableDefinition,
};

// Without std, a backend reports its failures with BackendError::Other
#[derive(Debug)]
struct FailingSync(InMemoryBackend);

impl StorageBackend for FailingSync {
    fn len(&self) -> Result<u64, BackendError> {
        self.0.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), BackendError> {
        self.0.read(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), BackendError> {
        self.0.set_len(len)
    }

    fn sync_data(&self) -> Result<(), BackendError> {
        Err(BackendError::Other("flash write failed".into()))
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), BackendError> {
        self.0.write(offset, data)
    }
}

fn main() {
    let backend = InMemoryBackend::new();
    assert!(matches!(
        backend.try_lock_range(core::ops::Bound::Unbounded, core::ops::Bound::Unbounded),
        Err(BackendError::Unsupported)
    ));

    match Database::builder().create_with_backend(FailingSync(InMemoryBackend::new())) {
        Err(DatabaseError::Storage(StorageError::Backend(err))) => {
            assert_eq!(err.to_string(), "flash write failed");
        }
        other => panic!("expected the backend's error, got {other:?}"),
    }

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("test");
    let db = Database::builder().create_with_backend(backend).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(TABLE).unwrap();
        table.insert(1, 2).unwrap();
    }
    write.commit().unwrap();
    let read = db.begin_read().unwrap();
    assert_eq!(
        read.open_table(TABLE)
            .unwrap()
            .get(1)
            .unwrap()
            .unwrap()
            .value(),
        2
    );
}
