use redb::backends::InMemoryBackend;
use redb::{
    BackendError, Database, ReadableDatabase, ReadableTable, StorageBackend, TableDefinition,
};

fn main() {
    let backend = InMemoryBackend::new();
    assert!(matches!(
        backend.try_lock_range(core::ops::Bound::Unbounded, core::ops::Bound::Unbounded),
        Err(BackendError::Unsupported)
    ));

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
