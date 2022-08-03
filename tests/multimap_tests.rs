use redb::{Database, Error, MultimapTableDefinition, ReadableMultimapTable};
use tempfile::NamedTempFile;

const SLICE_TABLE: MultimapTableDefinition<[u8], [u8]> =
    MultimapTableDefinition::new("slice_to_slice");

fn get_vec(table: &impl ReadableMultimapTable<[u8], [u8]>, key: &[u8]) -> Vec<Vec<u8>> {
    let mut result = vec![];
    let mut iter = table.get(key).unwrap();
    loop {
        let item = iter.next();
        if let Some(item_value) = item {
            result.push(item_value.to_vec());
        } else {
            return result;
        }
    }
}

#[test]
fn len() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
        table.insert(b"hello", b"world2").unwrap();
        table.insert(b"hi", b"world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(SLICE_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 3);
}

#[test]
fn is_empty() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(SLICE_TABLE).unwrap();
    assert!(!table.is_empty().unwrap());
}

#[test]
fn insert() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(SLICE_TABLE).unwrap();
        assert!(!table.insert(b"hello", b"world").unwrap());
        assert!(!table.insert(b"hello", b"world2").unwrap());
        assert!(table.insert(b"hello", b"world2").unwrap());
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(SLICE_TABLE).unwrap();
    assert_eq!(
        vec![b"world".to_vec(), b"world2".to_vec()],
        get_vec(&table, b"hello")
    );
    assert_eq!(table.len().unwrap(), 2);
}

#[test]
fn range_query() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(SLICE_TABLE).unwrap();
        for i in 0..5u8 {
            let value = vec![i];
            table.insert(b"0", &value).unwrap();
        }
        for i in 5..10u8 {
            let value = vec![i];
            table.insert(b"1", &value).unwrap();
        }
        for i in 10..15u8 {
            let value = vec![i];
            table.insert(b"2", &value).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(SLICE_TABLE).unwrap();
    let start = b"0".as_ref();
    let end = b"1".as_ref();
    let mut iter = table.range(start..=end).unwrap();

    let (key, mut values) = iter.next().unwrap();
    for i in 0..5u8 {
        assert_eq!(b"0", key);
        let value = values.next().unwrap();
        assert_eq!(&[i], value);
    }

    let (key, mut values) = iter.next().unwrap();
    for i in 5..10u8 {
        assert_eq!(b"1", key);
        let value = values.next().unwrap();
        assert_eq!(&[i], value);
    }

    assert!(iter.next().is_none());
}

#[test]
fn delete() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
        table.insert(b"hello", b"world2").unwrap();
        table.insert(b"hello", b"world3").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(SLICE_TABLE).unwrap();
    assert_eq!(
        vec![b"world".to_vec(), b"world2".to_vec(), b"world3".to_vec()],
        get_vec(&table, b"hello")
    );
    assert_eq!(table.len().unwrap(), 3);

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(SLICE_TABLE).unwrap();
        table.remove(b"hello", b"world2").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(SLICE_TABLE).unwrap();
    assert_eq!(
        vec![b"world".to_vec(), b"world3".to_vec()],
        get_vec(&table, b"hello")
    );
    assert_eq!(table.len().unwrap(), 2);

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(SLICE_TABLE).unwrap();
        let mut iter = table.remove_all(b"hello").unwrap();
        assert_eq!(b"world", iter.next().unwrap());
        assert_eq!(b"world3", iter.next().unwrap());
        assert!(iter.next().is_none());
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(SLICE_TABLE).unwrap();
    assert!(table.is_empty().unwrap());
    let empty: Vec<Vec<u8>> = vec![];
    assert_eq!(empty, get_vec(&table, b"hello"));
}

#[test]
fn wrong_types() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };

    let definition: MultimapTableDefinition<u32, u32> = MultimapTableDefinition::new("x");
    let wrong_definition: MultimapTableDefinition<u64, u64> = MultimapTableDefinition::new("x");

    let txn = db.begin_write().unwrap();
    txn.open_multimap_table(definition).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    assert!(matches!(
        txn.open_multimap_table(wrong_definition),
        Err(Error::TableTypeMismatch(_))
    ));
    txn.abort().unwrap();

    let txn = db.begin_read().unwrap();
    txn.open_multimap_table(definition).unwrap();
    assert!(matches!(
        txn.open_multimap_table(wrong_definition),
        Err(Error::TableTypeMismatch(_))
    ));
}

#[test]
fn efficient_storage() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db_size = 1024 * 1024;
    // Write enough values that big_key.len() * entries > db_size to check that duplicate key data is not stored
    // and entries * sizeof(u32) > page_size to validate that large numbers of values can be stored per key
    let entries = 10000;
    let db = unsafe { Database::create(tmpfile.path(), db_size).unwrap() };
    let table_def: MultimapTableDefinition<[u8], u32> = MultimapTableDefinition::new("x");
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(table_def).unwrap();
        let big_key = [0u8; 1000];
        for i in 0..entries {
            table.insert(&big_key, &i).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(table_def).unwrap();
    assert_eq!(table.len().unwrap(), entries as usize);
}

#[test]
fn reopen_table() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path(), 1024 * 1024).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(SLICE_TABLE).unwrap();
        table.insert(&[0], &[0]).unwrap();
    }
    {
        let mut table = write_txn.open_multimap_table(SLICE_TABLE).unwrap();
        table.insert(&[1], &[1]).unwrap();
    }
    write_txn.commit().unwrap();
}
