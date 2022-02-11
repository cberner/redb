use redb::{Database, ReadOnlyTable, ReadableTable, Table};
use std::sync::Arc;
use std::thread;
use tempfile::NamedTempFile;

#[test]
fn len() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let db = Arc::new(db);
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    table.insert(b"hello2", b"world2").unwrap();
    table.insert(b"hi", b"world").unwrap();
    write_txn.commit().unwrap();

    let db2 = db.clone();
    let t = thread::spawn(move || {
        let read_txn = db2.begin_read().unwrap();
        let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
        assert_eq!(table.len().unwrap(), 3);
    });
    t.join().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    assert_eq!(table.len().unwrap(), 3);
}
