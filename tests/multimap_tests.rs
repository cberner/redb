use redb::{Database, Error, MultimapTableDefinition, ReadableMultimapTable};
use tempfile::NamedTempFile;

const STR_TABLE: MultimapTableDefinition<&str, &str> = MultimapTableDefinition::new("str_to_str");
const SLICE_U64_TABLE: MultimapTableDefinition<&[u8], u64> =
    MultimapTableDefinition::new("slice_to_u64");
const U64_TABLE: MultimapTableDefinition<u64, u64> = MultimapTableDefinition::new("u64");

fn get_vec(
    table: &impl ReadableMultimapTable<&'static str, &'static str>,
    key: &str,
) -> Vec<String> {
    let mut result = vec![];
    let mut iter = table.get(key).unwrap();
    loop {
        let item = iter.next();
        if let Some(item_value) = item {
            result.push(item_value.value().to_string());
        } else {
            return result;
        }
    }
}

#[test]
fn len() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(STR_TABLE).unwrap();
        table.insert("hello", "world").unwrap();
        table.insert("hello", "world2").unwrap();
        table.insert("hi", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(STR_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 3);
}

#[test]
fn is_empty() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(STR_TABLE).unwrap();
        table.insert("hello", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(STR_TABLE).unwrap();
    assert!(!table.is_empty().unwrap());
}

#[test]
fn insert() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(STR_TABLE).unwrap();
        assert!(!table.insert("hello", "world").unwrap());
        assert!(!table.insert("hello", "world2").unwrap());
        assert!(table.insert("hello", "world2").unwrap());
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(STR_TABLE).unwrap();
    assert_eq!(
        vec!["world".to_string(), "world2".to_string()],
        get_vec(&table, "hello")
    );
    assert_eq!(table.len().unwrap(), 2);
}

#[test]
fn range_query() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(SLICE_U64_TABLE).unwrap();
        for i in 0..5 {
            table.insert(b"0", &i).unwrap();
        }
        for i in 5..10 {
            table.insert(b"1", &i).unwrap();
        }
        for i in 10..15 {
            table.insert(b"2", &i).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(SLICE_U64_TABLE).unwrap();
    let start = b"0".as_ref();
    let end = b"1".as_ref();
    let mut iter = table.range(start..=end).unwrap();

    {
        let (key, mut values) = iter.next().unwrap();
        for i in 0..5 {
            assert_eq!(b"0", key.value());
            let value = values.next().unwrap();
            assert_eq!(i, value.value());
        }
    }
    {
        let (key, mut values) = iter.next().unwrap();
        for i in 5..10 {
            assert_eq!(b"1", key.value());
            let value = values.next().unwrap();
            assert_eq!(i, value.value());
        }
    }
    assert!(iter.next().is_none());

    let mut total: u64 = 0;
    for (_, values) in table.range(&start..=&end).unwrap() {
        total += values.map(|x| x.value()).sum::<u64>();
    }
    assert_eq!(total, 45);
}

#[test]
fn delete() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(STR_TABLE).unwrap();
        table.insert("hello", "world").unwrap();
        table.insert("hello", "world2").unwrap();
        table.insert("hello", "world3").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(STR_TABLE).unwrap();
    assert_eq!(
        vec![
            "world".to_string(),
            "world2".to_string(),
            "world3".to_string()
        ],
        get_vec(&table, "hello")
    );
    assert_eq!(table.len().unwrap(), 3);

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(STR_TABLE).unwrap();
        table.remove("hello", "world2").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(STR_TABLE).unwrap();
    assert_eq!(
        vec!["world".to_string(), "world3".to_string()],
        get_vec(&table, "hello")
    );
    assert_eq!(table.len().unwrap(), 2);

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(STR_TABLE).unwrap();
        let mut iter = table.remove_all("hello").unwrap();
        assert_eq!("world", iter.next().unwrap().value());
        assert_eq!("world3", iter.next().unwrap().value());
        assert!(iter.next().is_none());
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(STR_TABLE).unwrap();
    assert!(table.is_empty().unwrap());
    let empty: Vec<String> = vec![];
    assert_eq!(empty, get_vec(&table, "hello"));
}

#[test]
fn wrong_types() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };

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
    let expected_max_size = 1024 * 1024;
    // Write enough values that big_key.len() * entries > db_size to check that duplicate key data is not stored
    // and entries * sizeof(u32) > page_size to validate that large numbers of values can be stored per key
    let entries = 10000;
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let table_def: MultimapTableDefinition<&[u8], u32> = MultimapTableDefinition::new("x");
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(table_def).unwrap();
        let big_key = [0u8; 1000];
        for i in 0..entries {
            table.insert(&big_key, &i).unwrap();
        }
    }
    assert!(write_txn.stats().unwrap().stored_bytes() <= expected_max_size);
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(table_def).unwrap();
    assert_eq!(table.len().unwrap(), entries as usize);
}

#[test]
fn reopen_table() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(STR_TABLE).unwrap();
        table.insert("0", "0").unwrap();
    }
    {
        let mut table = write_txn.open_multimap_table(STR_TABLE).unwrap();
        table.insert("1", "1").unwrap();
    }
    write_txn.commit().unwrap();
}

#[test]
fn iter() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = unsafe { Database::create(tmpfile.path()).unwrap() };
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(U64_TABLE).unwrap();
        for i in 0..10 {
            for j in 0..10 {
                table.insert(&i, &j).unwrap();
            }
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(U64_TABLE).unwrap();
    let mut iter = table.iter().unwrap();
    for i in 0..10 {
        let (k, mut values) = iter.next().unwrap();
        assert_eq!(k.value(), i);
        for j in 0..10 {
            assert_eq!(values.next().unwrap().value(), j);
        }
    }
}
