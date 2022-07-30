use redb::{Database, MultimapTableDefinition, RangeIter, ReadableTable, TableDefinition};
use std::ops::{Range, RangeFull};
use tempfile::NamedTempFile;

const SLICE_TABLE: TableDefinition<[u8], [u8]> = TableDefinition::new("x");
const U64_TABLE: TableDefinition<u64, u64> = TableDefinition::new("u64");

#[test]
fn custom_region_size() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe {
        Database::builder()
            .set_region_size(512 * 1024)
            .create(tmpfile.path(), 20 * 1024 * 1024)
            .unwrap()
    };
    let rows = 4 * 1024;
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        let big_value = vec![0u8; 1024];
        for i in 0..rows {
            table.insert(&(i as u32).to_le_bytes(), &big_value).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), rows);
}

#[test]
fn len() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
        table.insert(b"hello2", b"world2").unwrap();
        table.insert(b"hi", b"world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 3);
}

#[test]
fn stored_size() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    assert_eq!(write_txn.stats().unwrap().stored_bytes(), 10);
    assert!(write_txn.stats().unwrap().fragmented_bytes() > 0);
    assert!(write_txn.stats().unwrap().metadata_bytes() > 0);
    write_txn.abort().unwrap();
}

#[test]
fn create_open() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        table.insert(&0, &1).unwrap();
    }
    write_txn.commit().unwrap();
    drop(db);

    let db2 = unsafe { Database::open(tmpfile.path()).unwrap() };

    let read_txn = db2.begin_read().unwrap();
    let table = read_txn.open_table(U64_TABLE).unwrap();
    assert_eq!(1, table.get(&0).unwrap().unwrap());
}

#[test]
fn multiple_tables() {
    let definition1: TableDefinition<[u8], [u8]> = TableDefinition::new("1");
    let definition2: TableDefinition<[u8], [u8]> = TableDefinition::new("2");

    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition1).unwrap();
        let mut table2 = write_txn.open_table(definition2).unwrap();

        table.insert(b"hello", b"world").unwrap();
        table2.insert(b"hello", b"world2").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition1).unwrap();
    let table2 = read_txn.open_table(definition2).unwrap();
    assert_eq!(table.len().unwrap(), 1);
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap());
    assert_eq!(table2.len().unwrap(), 1);
    assert_eq!(b"world2", table2.get(b"hello").unwrap().unwrap());
}

#[test]
fn list_tables() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let definition_x: TableDefinition<[u8], [u8]> = TableDefinition::new("x");
    let definition_y: TableDefinition<[u8], [u8]> = TableDefinition::new("y");
    let definition_mx: MultimapTableDefinition<[u8], [u8]> = MultimapTableDefinition::new("mx");
    let definition_my: MultimapTableDefinition<[u8], [u8]> = MultimapTableDefinition::new("my");

    let write_txn = db.begin_write().unwrap();
    {
        write_txn.open_table(definition_x).unwrap();
        write_txn.open_table(definition_y).unwrap();
        write_txn.open_multimap_table(definition_mx).unwrap();
        write_txn.open_multimap_table(definition_my).unwrap();
    }

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
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert!(!table.is_empty().unwrap());
}

#[test]
fn abort() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"aborted").unwrap();
        assert_eq!(b"aborted", table.get(b"hello").unwrap().unwrap());
    }
    write_txn.abort().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE);
    assert!(table.is_err());

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn insert_overwrite() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        assert!(table.insert(b"hello", b"world").unwrap().is_none());
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap());

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        let old_value = table.insert(b"hello", b"replaced").unwrap();
        assert_eq!(old_value.unwrap().to_value(), b"world");
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(b"replaced", table.get(b"hello").unwrap().unwrap());
}

#[test]
fn insert_reserve() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let value = b"world";
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        let mut reserved = table.insert_reserve(b"hello", value.len()).unwrap();
        reserved.as_mut().copy_from_slice(value);
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(value, table.get(b"hello").unwrap().unwrap());
}

#[test]
fn delete() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
        table.insert(b"hello2", b"world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap());
    assert_eq!(table.len().unwrap(), 2);

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        assert_eq!(
            b"world",
            table.remove(b"hello").unwrap().unwrap().to_value()
        );
        assert!(table.remove(b"hello").unwrap().is_none());
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert!(table.get(b"hello").unwrap().is_none());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn no_dirty_reads() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE);
    assert!(table.is_err());
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap());
}

#[test]
fn read_isolation() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap());

    let write_txn = db.begin_write().unwrap();
    {
        let mut write_table = write_txn.open_table(SLICE_TABLE).unwrap();
        write_table.remove(b"hello").unwrap();
        write_table.insert(b"hello2", b"world2").unwrap();
        write_table.insert(b"hello3", b"world3").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn2 = db.begin_read().unwrap();
    let table2 = read_txn2.open_table(SLICE_TABLE).unwrap();
    assert!(table2.get(b"hello").unwrap().is_none());
    assert_eq!(b"world2", table2.get(b"hello2").unwrap().unwrap());
    assert_eq!(b"world3", table2.get(b"hello3").unwrap().unwrap());
    assert_eq!(table2.len().unwrap(), 2);

    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap());
    assert!(table.get(b"hello2").unwrap().is_none());
    assert!(table.get(b"hello3").unwrap().is_none());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn reopen_table() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        table.insert(&0, &0).unwrap();
    }
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        table.insert(&1, &1).unwrap();
    }
    write_txn.commit().unwrap();
}

#[test]
fn u64_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        table.insert(&0, &1).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(U64_TABLE).unwrap();
    assert_eq!(1, table.get(&0).unwrap().unwrap());
}

#[test]
fn i128_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();

    let definition: TableDefinition<i128, i128> = TableDefinition::new("x");

    {
        let mut table = write_txn.open_table(definition).unwrap();
        for i in -10..=10 {
            table.insert(&i, &(i - 1)).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    assert_eq!(-2, table.get(&-1).unwrap().unwrap());
    let mut iter: RangeIter<i128, i128> = table.range::<RangeFull, i128>(..).unwrap();
    for i in -11..10 {
        assert_eq!(iter.next().unwrap().1, i);
    }
    assert!(iter.next().is_none());
}

#[test]
fn f32_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let definition: TableDefinition<u8, f32> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert(&0, &0.3).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    assert_eq!(0.3, table.get(&0).unwrap().unwrap());
}

#[test]
fn str_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let definition: TableDefinition<str, str> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert("hello", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    let hello = "hello".to_string();
    assert_eq!("world", table.get(&hello).unwrap().unwrap());

    let mut iter: RangeIter<str, str> = table.range::<RangeFull, &str>(..).unwrap();
    assert_eq!(iter.next().unwrap().1, "world");
    assert!(iter.next().is_none());

    let mut iter: RangeIter<str, str> = table.range("a".to_string().."z".to_string()).unwrap();
    assert_eq!(iter.next().unwrap().1, "world");
    assert!(iter.next().is_none());

    let mut iter: RangeIter<str, str> = table.range("a".."z").unwrap();
    assert_eq!(iter.next().unwrap().1, "world");
    assert!(iter.next().is_none());
}

#[test]
fn empty_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let definition: TableDefinition<u8, ()> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert(&0, &()).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    assert!(!table.is_empty().unwrap());
}

#[test]
fn array_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let definition: TableDefinition<[u8; 5], [u8; 9]> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert(b"hello", b"world_123").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    let hello = b"hello";
    assert_eq!(b"world_123", table.get(hello).unwrap().unwrap());

    let mut iter: RangeIter<[u8; 5], [u8; 9]> = table.range::<RangeFull, &[u8; 5]>(..).unwrap();
    assert_eq!(iter.next().unwrap().1, b"world_123");
    assert!(iter.next().is_none());
}

#[test]
fn owned_get_signatures() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let definition: TableDefinition<u32, u32> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        for i in 0..10 {
            table.insert(&i, &(i + 1)).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();

    assert_eq!(2, table.get(&1).unwrap().unwrap());

    let mut iter: RangeIter<u32, u32> = table.range::<RangeFull, u32>(..).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1, i + 1);
    }
    assert!(iter.next().is_none());
    let mut iter: RangeIter<u32, u32> = table.range(0..10).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1, i + 1);
    }
    assert!(iter.next().is_none());
    let mut iter = table.range::<Range<&u32>, &u32>(&0..&10).unwrap();
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
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..10 {
            table.insert(&[i], &[i + 1]).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();

    let zero = vec![0u8];
    assert_eq!(&[1], table.get(&[0]).unwrap().unwrap());
    assert_eq!(&[1], table.get(b"\0").unwrap().unwrap());
    assert_eq!(&[1], table.get(&zero).unwrap().unwrap());

    let start = vec![0u8];
    let end = vec![10u8];
    let mut iter = table.range::<RangeFull, &[u8]>(..).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1, &[i + 1]);
    }
    assert!(iter.next().is_none());

    let mut iter = table.range(start.as_slice()..&end).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1, &[i + 1]);
    }
    assert!(iter.next().is_none());

    let mut iter = table.range(start..end).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1, &[i + 1]);
    }
    assert!(iter.next().is_none());

    let mut iter = table.range([0u8].as_slice()..[10u8].as_slice()).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1, &[i + 1]);
    }
    assert!(iter.next().is_none());
}
