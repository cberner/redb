use redb::backends::InMemoryBackend;
use redb::{
    BackendError, Database, DatabaseError, ReadableDatabase, ReadableTable, StorageBackend,
    StorageError, TableDefinition,
};
use redb_no_std_test::{Named, Point};
use std::sync::Arc;

// Storage that outlives a Database, so that it can be opened again read-only
#[derive(Debug, Clone)]
struct Shared(Arc<InMemoryBackend>);

impl StorageBackend for Shared {
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
        self.0.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), BackendError> {
        self.0.write(offset, data)
    }
}

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
    let storage = Shared(Arc::new(backend));
    let db = Database::builder()
        .create_with_backend(storage.clone())
        .unwrap();
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
    drop(read);
    db.close().unwrap();

    let db = Database::builder()
        .open_read_only_with_backend(storage)
        .unwrap();
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

    // Types whose implementations were derived in a no_std crate
    const DERIVED: TableDefinition<Point, Named> = TableDefinition::new("derived");
    let db = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(DERIVED).unwrap();
        table
            .insert(Point { x: 1, y: 2 }, Named { name: "a", id: 3 })
            .unwrap();
    }
    write.commit().unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(DERIVED).unwrap();
    let value = table.get(Point { x: 1, y: 2 }).unwrap().unwrap();
    assert_eq!((value.value().name, value.value().id), ("a", 3));
}
