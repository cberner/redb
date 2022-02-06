use redb::{Database, Error, MultimapTable, RangeIter, ReadOnlyTable, ReadableTable, Table};
use std::ops::{Range, RangeFull};
use tempfile::NamedTempFile;

#[test]
fn len() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    table.insert(b"hello2", b"world2").unwrap();
    table.insert(b"hi", b"world").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    assert_eq!(table.len().unwrap(), 3);
}

#[test]
fn create_open() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = write_txn.open_table("x").unwrap();
    table.insert(&0, &1).unwrap();
    write_txn.commit().unwrap();
    drop(db);

    let db2 = unsafe { Database::open(tmpfile.path()).unwrap() };

    let read_txn = db2.begin_read().unwrap();
    let table: ReadOnlyTable<u64, u64> = read_txn.open_table("x").unwrap();
    assert_eq!(1, table.get(&0).unwrap().unwrap().to_value());
}

#[test]
fn multiple_tables() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("1").unwrap();
    let mut table2: Table<[u8], [u8]> = write_txn.open_table("2").unwrap();

    table.insert(b"hello", b"world").unwrap();
    table2.insert(b"hello", b"world2").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("1").unwrap();
    let table2: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("2").unwrap();
    assert_eq!(table.len().unwrap(), 1);
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().to_value());
    assert_eq!(table2.len().unwrap(), 1);
    assert_eq!(b"world2", table2.get(b"hello").unwrap().unwrap().to_value());
}

#[test]
fn list_tables() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let write_txn = db.begin_write().unwrap();
    let _: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    let _: Table<[u8], [u8]> = write_txn.open_table("y").unwrap();
    let _: MultimapTable<[u8], [u8]> = write_txn.open_multimap_table("mx").unwrap();
    let _: MultimapTable<[u8], [u8]> = write_txn.open_multimap_table("my").unwrap();

    let tables: Vec<String> = write_txn.list_tables().unwrap().collect();
    let multimap_tables: Vec<String> = write_txn.list_multimap_tables().unwrap().collect();
    assert_eq!(tables, &["x", "y"]);
    assert_eq!(multimap_tables, &["mx", "my"]);
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let tables: Vec<String> = read_txn.list_tables().unwrap().collect();
    let multimap_tables: Vec<String> = read_txn.list_multimap_tables().unwrap().collect();
    assert_eq!(tables, &["x", "y"]);
    assert_eq!(multimap_tables, &["mx", "my"]);
}

#[test]
fn is_empty() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    assert!(!table.is_empty().unwrap());
}

#[test]
fn abort() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    table.insert(b"hello", b"aborted").unwrap();
    assert_eq!(b"aborted", table.get(b"hello").unwrap().unwrap().to_value());
    write_txn.abort().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: Result<ReadOnlyTable<[u8], [u8]>, Error> = read_txn.open_table("x");
    assert!(table.is_err());

    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().to_value());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn insert_overwrite() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().to_value());

    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    table.insert(b"hello", b"replaced").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    assert_eq!(
        b"replaced",
        table.get(b"hello").unwrap().unwrap().to_value()
    );
}

#[test]
fn insert_reserve() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    let value = b"world";
    let mut reserved = table.insert_reserve(b"hello", value.len()).unwrap();
    reserved.as_mut().copy_from_slice(value);
    drop(reserved);
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    assert_eq!(value, table.get(b"hello").unwrap().unwrap().to_value());
}

#[test]
fn delete() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    table.insert(b"hello2", b"world").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().to_value());
    assert_eq!(table.len().unwrap(), 2);

    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    table.remove(b"hello").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    assert!(table.get(b"hello").unwrap().is_none());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn no_dirty_reads() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    table.insert(b"hello", b"world").unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: Result<ReadOnlyTable<[u8], [u8]>, Error> = read_txn.open_table("x");
    assert!(table.is_err());
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().to_value());
}

#[test]
fn read_isolation() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    table.insert(b"hello", b"world").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().to_value());

    let write_txn = db.begin_write().unwrap();
    let mut write_table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    write_table.remove(b"hello").unwrap();
    write_table.insert(b"hello2", b"world2").unwrap();
    write_table.insert(b"hello3", b"world3").unwrap();
    write_txn.commit().unwrap();

    let read_txn2 = db.begin_read().unwrap();
    let table2: ReadOnlyTable<[u8], [u8]> = read_txn2.open_table("x").unwrap();
    assert!(table2.get(b"hello").unwrap().is_none());
    assert_eq!(
        b"world2",
        table2.get(b"hello2").unwrap().unwrap().to_value()
    );
    assert_eq!(
        b"world3",
        table2.get(b"hello3").unwrap().unwrap().to_value()
    );
    assert_eq!(table2.len().unwrap(), 2);

    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().to_value());
    assert!(table.get(b"hello2").unwrap().is_none());
    assert!(table.get(b"hello3").unwrap().is_none());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn u64_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<u64, u64> = write_txn.open_table("x").unwrap();
    table.insert(&0, &1).unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<u64, u64> = read_txn.open_table("x").unwrap();
    assert_eq!(1, table.get(&0).unwrap().unwrap().to_value());
}

#[test]
fn i128_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<i128, i128> = write_txn.open_table("x").unwrap();
    for i in -10..=10 {
        table.insert(&i, &(i - 1)).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<i128, i128> = read_txn.open_table("x").unwrap();
    assert_eq!(-2, table.get(&-1).unwrap().unwrap().to_value());
    let mut iter: RangeIter<RangeFull, i128, i128, i128> = table.get_range(..).unwrap();
    for i in -11..10 {
        assert_eq!(iter.next().unwrap().1, i);
    }
    assert!(iter.next().is_none());
}

#[test]
fn f32_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<u8, f32> = write_txn.open_table("x").unwrap();
    table.insert(&0, &0.3).unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<u8, f32> = read_txn.open_table("x").unwrap();
    assert_eq!(0.3, table.get(&0).unwrap().unwrap().to_value());
}

#[test]
fn str_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<str, str> = write_txn.open_table("x").unwrap();
    table.insert("hello", "world").unwrap();
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<str, str> = read_txn.open_table("x").unwrap();
    let hello = "hello".to_string();
    assert_eq!("world", table.get(&hello).unwrap().unwrap().to_value());

    let mut iter: RangeIter<RangeFull, &str, str, str> = table.get_range(..).unwrap();
    assert_eq!(iter.next().unwrap().1, "world");
    assert!(iter.next().is_none());

    let mut iter: RangeIter<Range<String>, String, str, str> =
        table.get_range("a".to_string().."z".to_string()).unwrap();
    assert_eq!(iter.next().unwrap().1, "world");
    assert!(iter.next().is_none());

    let mut iter: RangeIter<Range<&str>, &str, str, str> = table.get_range("a".."z").unwrap();
    assert_eq!(iter.next().unwrap().1, "world");
    assert!(iter.next().is_none());
}

#[test]
fn owned_get_signatures() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<u32, u32> = write_txn.open_table("x").unwrap();
    for i in 0..10 {
        table.insert(&i, &(i + 1)).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<u32, u32> = read_txn.open_table("x").unwrap();

    assert_eq!(2, table.get(&1).unwrap().unwrap().to_value());

    let mut iter: RangeIter<RangeFull, u32, u32, u32> = table.get_range(..).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1, i + 1);
    }
    assert!(iter.next().is_none());
    let mut iter: RangeIter<Range<u32>, u32, u32, u32> = table.get_range(0..10).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1, i + 1);
    }
    assert!(iter.next().is_none());
    let mut iter: RangeIter<Range<&u32>, u32, u32, u32> = table.get_range(&0..&10).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1, i + 1);
    }
    assert!(iter.next().is_none());
}

#[test]
fn ref_get_signatures() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    let mut table: Table<[u8], [u8]> = write_txn.open_table("x").unwrap();
    for i in 0..10 {
        table.insert(&[i], &[i + 1]).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table: ReadOnlyTable<[u8], [u8]> = read_txn.open_table("x").unwrap();

    let zero = vec![0u8];
    assert_eq!(&[1], table.get(&[0]).unwrap().unwrap().to_value());
    assert_eq!(&[1], table.get(b"\0").unwrap().unwrap().to_value());
    assert_eq!(&[1], table.get(&zero).unwrap().unwrap().to_value());

    let start = vec![0u8];
    let end = vec![10u8];
    let mut iter: RangeIter<RangeFull, &[u8], [u8], [u8]> = table.get_range(..).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1, &[i + 1]);
    }
    assert!(iter.next().is_none());

    let mut iter: RangeIter<Range<&[u8]>, &[u8], [u8], [u8]> =
        table.get_range(start.as_slice()..&end).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1, &[i + 1]);
    }
    assert!(iter.next().is_none());

    let mut iter = table.get_range(start..end).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1, &[i + 1]);
    }
    assert!(iter.next().is_none());

    let mut iter = table
        .get_range([0u8].as_slice()..[10u8].as_slice())
        .unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1, &[i + 1]);
    }
    assert!(iter.next().is_none());
}
