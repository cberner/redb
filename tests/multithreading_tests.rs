use redb::{Database, ReadableTable, TableDefinition};
use std::sync::Arc;
use std::thread;
use tempfile::NamedTempFile;

const SLICE_TABLE: TableDefinition<[u8], [u8]> = TableDefinition::new("x");

#[test]
fn len() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let db = Arc::new(db);
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
        table.insert(b"hello2", b"world2").unwrap();
        table.insert(b"hi", b"world").unwrap();
    }
    write_txn.commit().unwrap();

    let db2 = db.clone();
    let t = thread::spawn(move || {
        let read_txn = db2.begin_read().unwrap();
        let table = read_txn.open_table(SLICE_TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 3);
    });
    t.join().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 3);
}
