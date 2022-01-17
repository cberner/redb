use redb::{Database, Error, ReadOnlyTable, ReadableTable, Table};
use tempfile::NamedTempFile;

#[test]
fn len() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    table.insert(b"hello2", b"world2").unwrap();
    table.insert(b"hi", b"world").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
    assert_eq!(table.len().unwrap(), 3);
}

#[test]
fn multiple_tables() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"1").unwrap();
    let mut table2: Table<[u8], [u8]> = write_txn.open_table(b"2").unwrap();

    table.insert(b"hello", b"world").unwrap();
    table2.insert(b"hello", b"world2").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"1").unwrap();
    let table2: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"2").unwrap();
    assert_eq!(table.len().unwrap(), 1);
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());
    assert_eq!(table2.len().unwrap(), 1);
    assert_eq!(b"world2", table2.get(b"hello").unwrap().unwrap().as_ref());
}

#[test]
fn is_empty() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };

    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
    assert!(!table.is_empty().unwrap());
}

#[test]
fn abort() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };

    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
    table.insert(b"hello", b"aborted").unwrap();
    assert_eq!(b"aborted", table.get(b"hello").unwrap().unwrap().as_ref());
    write_txn.abort().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: Result<ReadOnlyTable<[u8], [u8]>, Error> = read_txn.open_table(b"x");
    assert!(table.is_err());

    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn insert_overwrite() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());

    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
    table.insert(b"hello", b"replaced").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
    assert_eq!(b"replaced", table.get(b"hello").unwrap().unwrap().as_ref());
}

#[test]
fn insert_reserve() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
    let value = b"world";
    let mut reserved = table.insert_reserve(b"hello", value.len()).unwrap();
    reserved.as_mut().copy_from_slice(value);
    drop(reserved);
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
    assert_eq!(value, table.get(b"hello").unwrap().unwrap().as_ref());
}

#[test]
fn delete() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    table.insert(b"hello2", b"world").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());
    assert_eq!(table.len().unwrap(), 2);

    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
    table.remove(b"hello").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
    assert!(table.get(b"hello").unwrap().is_none());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn no_dirty_reads() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
    table.insert(b"hello", b"world").unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: Result<ReadOnlyTable<[u8], [u8]>, Error> = read_txn.open_table(b"x");
    assert!(table.is_err());
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());
}

#[test]
fn read_isolation() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table(b"x").unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());

    let write_txn = db.begin_write().unwrap();
    let mut write_table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
    write_table.remove(b"hello").unwrap();
    write_table.insert(b"hello2", b"world2").unwrap();
    write_table.insert(b"hello3", b"world3").unwrap();
    write_txn.commit().unwrap();

    let read_txn2 = db.begin_read().unwrap();
    let table2: ReadOnlyTable<[u8], [u8]> = read_txn2.open_table(b"x").unwrap();
    assert!(table2.get(b"hello").unwrap().is_none());
    assert_eq!(b"world2", table2.get(b"hello2").unwrap().unwrap().as_ref());
    assert_eq!(b"world3", table2.get(b"hello3").unwrap().unwrap().as_ref());
    assert_eq!(table2.len().unwrap(), 2);

    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().as_ref());
    assert!(table.get(b"hello2").unwrap().is_none());
    assert!(table.get(b"hello3").unwrap().is_none());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn u64_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = write_txn.open_table(b"x").unwrap();
    table.insert(&0, &1).unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<u64, u64> = read_txn.open_table(b"x").unwrap();
    assert_eq!(1, table.get(&0).unwrap().unwrap().to_value());
}

#[test]
fn i128_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<i128, i128> = write_txn.open_table(b"x").unwrap();
    for i in -10..=10 {
        table.insert(&i, &(i - 1)).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<i128, i128> = read_txn.open_table(b"x").unwrap();
    assert_eq!(-2, table.get(&-1).unwrap().unwrap().to_value());
    // TODO: enable this test
    // let mut iter: RangeIter<RangeFull, i128, i128, i128> = table.get_range(..).unwrap();
    // for i in -11..10 {
    //     assert_eq!(iter.next().unwrap().1, i);
    // }
}

#[test]
fn f32_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<u8, f32> = write_txn.open_table(b"x").unwrap();
    table.insert(&0, &0.3).unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<u8, f32> = read_txn.open_table(b"x").unwrap();
    assert_eq!(0.3, table.get(&0).unwrap().unwrap().to_value());
}
