use redb::{Database, MultimapTableDefinition, RangeIter, ReadableTable, TableDefinition};
use std::sync;
use tempfile::NamedTempFile;

const SLICE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");
const U64_TABLE: TableDefinition<u64, u64> = TableDefinition::new("u64");

#[test]
fn len() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
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
fn pop() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"a", b"world").unwrap();
        table.insert(b"b", b"world2").unwrap();
        table.insert(b"c", b"world3").unwrap();
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        {
            let (key, value) = table.pop_first().unwrap().unwrap();
            assert_eq!(key.value(), b"a");
            assert_eq!(value.value(), b"world");
        }
        {
            let (key, value) = table.pop_last().unwrap().unwrap();
            assert_eq!(key.value(), b"c");
            assert_eq!(value.value(), b"world3");
        }
        {
            let (key, value) = table.pop_last().unwrap().unwrap();
            assert_eq!(key.value(), b"b");
            assert_eq!(value.value(), b"world2");
        }
        assert!(table.pop_last().unwrap().is_none());
    }
    write_txn.commit().unwrap();
}

#[test]
fn stored_size() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
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
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        table.insert(&0, &1).unwrap();
    }
    write_txn.commit().unwrap();
    drop(db);

    let db2 = Database::open(tmpfile.path()).unwrap();

    let read_txn = db2.begin_read().unwrap();
    let table = read_txn.open_table(U64_TABLE).unwrap();
    assert_eq!(1, table.get(&0).unwrap().unwrap().value());
}

#[test]
fn multiple_tables() {
    let definition1: TableDefinition<&[u8], &[u8]> = TableDefinition::new("1");
    let definition2: TableDefinition<&[u8], &[u8]> = TableDefinition::new("2");

    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
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
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().value());
    assert_eq!(table2.len().unwrap(), 1);
    assert_eq!(b"world2", table2.get(b"hello").unwrap().unwrap().value());
}

#[test]
fn list_tables() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition_x: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");
    let definition_y: TableDefinition<&[u8], &[u8]> = TableDefinition::new("y");
    let definition_mx: MultimapTableDefinition<&[u8], &[u8]> = MultimapTableDefinition::new("mx");
    let definition_my: MultimapTableDefinition<&[u8], &[u8]> = MultimapTableDefinition::new("my");

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
// Test that these signatures compile
fn tuple_type_function_lifetime() {
    #[allow(dead_code)]
    fn insert_inferred_lifetime(table: &mut redb::Table<(&str, u8), u64>) {
        table
            .insert(&(String::from("hello").as_str(), 8), &1)
            .unwrap();
    }

    #[allow(dead_code)]
    fn insert_explicit_lifetime<'a>(table: &mut redb::Table<(&'a str, u8), u64>) {
        table
            .insert(&(String::from("hello").as_str(), 8), &1)
            .unwrap();
    }
}

#[test]
fn tuple_type_lifetime() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<(&str, u8), (u16, u32)> = TableDefinition::new("table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table
            .insert(&(String::from("hello").as_str(), 5), &(0, 123))
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(table.get(&("hello", 5)).unwrap().unwrap().value(), (0, 123));
}

#[test]
fn tuple2_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<(&str, u8), (u16, u32)> = TableDefinition::new("table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table.insert(&("hello", 5), &(0, 123)).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(table.get(&("hello", 5)).unwrap().unwrap().value(), (0, 123));
}

#[test]
fn tuple3_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<(&str, u8, u16), (u16, u32)> = TableDefinition::new("table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table.insert(&("hello", 5, 6), &(0, 123)).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(
        table.get(&("hello", 5, 6)).unwrap().unwrap().value(),
        (0, 123)
    );
}

#[test]
fn tuple4_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<(&str, u8, u16, u32), (u16, u32)> =
        TableDefinition::new("table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table.insert(&("hello", 5, 6, 7), &(0, 123)).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(
        table.get(&("hello", 5, 6, 7)).unwrap().unwrap().value(),
        (0, 123)
    );
}

#[test]
#[allow(clippy::type_complexity)]
fn tuple5_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<(&str, u8, u16, u32, u64), (u16, u32)> =
        TableDefinition::new("table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table.insert(&("hello", 5, 6, 7, 8), &(0, 123)).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(
        table.get(&("hello", 5, 6, 7, 8)).unwrap().unwrap().value(),
        (0, 123)
    );
}

#[test]
#[allow(clippy::type_complexity)]
fn tuple6_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<(&str, u8, u16, u32, u64, u128), (u16, u32)> =
        TableDefinition::new("table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table.insert(&("hello", 5, 6, 7, 8, 9), &(0, 123)).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(
        table
            .get(&("hello", 5, 6, 7, 8, 9))
            .unwrap()
            .unwrap()
            .value(),
        (0, 123)
    );
}

#[test]
#[allow(clippy::type_complexity)]
fn tuple7_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<(&str, u8, u16, u32, u64, u128, i8), (u16, u32)> =
        TableDefinition::new("table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table
            .insert(&("hello", 5, 6, 7, 8, 9, -1), &(0, 123))
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(
        table
            .get(&("hello", 5, 6, 7, 8, 9, -1))
            .unwrap()
            .unwrap()
            .value(),
        (0, 123)
    );
}

#[test]
#[allow(clippy::type_complexity)]
fn tuple8_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<(&str, u8, u16, u32, u64, u128, i8, i16), (u16, u32)> =
        TableDefinition::new("table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table
            .insert(&("hello", 5, 6, 7, 8, 9, -1, -2), &(0, 123))
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(
        table
            .get(&("hello", 5, 6, 7, 8, 9, -1, -2))
            .unwrap()
            .unwrap()
            .value(),
        (0, 123)
    );
}

#[test]
#[allow(clippy::type_complexity)]
fn tuple9_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<(&str, u8, u16, u32, u64, u128, i8, i16, i32), (u16, u32)> =
        TableDefinition::new("table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table
            .insert(&("hello", 5, 6, 7, 8, 9, -1, -2, -3), &(0, 123))
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(
        table
            .get(&("hello", 5, 6, 7, 8, 9, -1, -2, -3))
            .unwrap()
            .unwrap()
            .value(),
        (0, 123)
    );
}

#[test]
#[allow(clippy::type_complexity)]
fn tuple10_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<(&str, u8, u16, u32, u64, u128, i8, i16, i32, i64), (u16, u32)> =
        TableDefinition::new("table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table
            .insert(&("hello", 5, 6, 7, 8, 9, -1, -2, -3, -4), &(0, 123))
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(
        table
            .get(&("hello", 5, 6, 7, 8, 9, -1, -2, -3, -4))
            .unwrap()
            .unwrap()
            .value(),
        (0, 123)
    );
}

#[test]
#[allow(clippy::type_complexity)]
fn tuple11_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<
        (&str, u8, u16, u32, u64, u128, i8, i16, i32, i64, i128),
        (u16, u32),
    > = TableDefinition::new("table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table
            .insert(&("hello", 5, 6, 7, 8, 9, -1, -2, -3, -4, -5), &(0, 123))
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(
        table
            .get(&("hello", 5, 6, 7, 8, 9, -1, -2, -3, -4, -5))
            .unwrap()
            .unwrap()
            .value(),
        (0, 123)
    );
}

#[test]
#[allow(clippy::type_complexity)]
fn tuple12_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<
        (&str, u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, &str),
        (u16, u32),
    > = TableDefinition::new("table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table
            .insert(
                &("hello", 5, 6, 7, 8, 9, -1, -2, -3, -4, -5, "end"),
                &(0, 123),
            )
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(
        table
            .get(&("hello", 5, 6, 7, 8, 9, -1, -2, -3, -4, -5, "end"))
            .unwrap()
            .unwrap()
            .value(),
        (0, 123)
    );
}

#[test]
fn is_empty() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

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
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"aborted").unwrap();
        assert_eq!(b"aborted", table.get(b"hello").unwrap().unwrap().value());
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
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().value());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn insert_overwrite() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        assert!(table.insert(b"hello", b"world").unwrap().is_none());
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().value());

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        let old_value = table.insert(b"hello", b"replaced").unwrap();
        assert_eq!(old_value.unwrap().value(), b"world");
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(b"replaced", table.get(b"hello").unwrap().unwrap().value());
}

#[test]
fn insert_reserve() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
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
    assert_eq!(value, table.get(b"hello").unwrap().unwrap().value());
}

#[test]
fn delete() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
        table.insert(b"hello2", b"world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().value());
    assert_eq!(table.len().unwrap(), 2);

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        assert_eq!(b"world", table.remove(b"hello").unwrap().unwrap().value());
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
    let db = Database::create(tmpfile.path()).unwrap();
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
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().value());
}

#[test]
fn read_isolation() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().value());

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
    assert_eq!(b"world2", table2.get(b"hello2").unwrap().unwrap().value());
    assert_eq!(b"world3", table2.get(b"hello3").unwrap().unwrap().value());
    assert_eq!(table2.len().unwrap(), 2);

    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().value());
    assert!(table.get(b"hello2").unwrap().is_none());
    assert!(table.get(b"hello3").unwrap().is_none());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn read_isolation2() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        table.insert(b"hello", b"world").unwrap();
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();
    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().value());
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
    assert_eq!(b"world2", table2.get(b"hello2").unwrap().unwrap().value());
    assert_eq!(b"world3", table2.get(b"hello3").unwrap().unwrap().value());
    assert_eq!(table2.len().unwrap(), 2);

    assert_eq!(b"world", table.get(b"hello").unwrap().unwrap().value());
    assert!(table.get(b"hello2").unwrap().is_none());
    assert!(table.get(b"hello3").unwrap().is_none());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn reopen_table() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
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
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        table.insert(&0, &1).unwrap();
        table.insert(&1, &1).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(U64_TABLE).unwrap();
    assert_eq!(
        2u64,
        table.range(0..2).unwrap().map(|(_, x)| x.value()).sum()
    );
    assert_eq!(1, table.get(&0).unwrap().unwrap().value());
}

#[test]
fn i128_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
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
    assert_eq!(-2, table.get(&-1).unwrap().unwrap().value());
    let mut iter: RangeIter<i128, i128> = table.range::<i128>(..).unwrap();
    for i in -11..10 {
        assert_eq!(iter.next().unwrap().1.value(), i);
    }
    assert!(iter.next().is_none());
}

#[test]
fn f32_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<u8, f32> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert(&0, &0.3).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    assert_eq!(0.3, table.get(&0).unwrap().unwrap().value());
}

#[test]
fn str_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<&str, &str> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert("hello", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    assert_eq!("world", table.get("hello").unwrap().unwrap().value());

    let mut iter = table.iter().unwrap();
    assert_eq!(iter.next().unwrap().1.value(), "world");
    assert!(iter.next().is_none());

    let mut iter: RangeIter<&str, &str> = table.range("a".."z").unwrap();
    assert_eq!(iter.next().unwrap().1.value(), "world");
    assert!(iter.next().is_none());
}

#[test]
fn empty_type() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

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
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<&[u8; 5], &[u8; 9]> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert(b"hello", b"world_123").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    let hello = b"hello";
    assert_eq!(b"world_123", table.get(hello).unwrap().unwrap().value());

    let mut iter: RangeIter<&[u8; 5], &[u8; 9]> = table.range::<&[u8; 5]>(..).unwrap();
    assert_eq!(iter.next().unwrap().1.value(), b"world_123");
    assert!(iter.next().is_none());
}

#[test]
fn owned_get_signatures() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

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

    assert_eq!(2, table.get(&1).unwrap().unwrap().value());

    let mut iter: RangeIter<u32, u32> = table.range::<u32>(..).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1.value(), i + 1);
    }
    assert!(iter.next().is_none());
    let mut iter: RangeIter<u32, u32> = table.range(0..10).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1.value(), i + 1);
    }
    assert!(iter.next().is_none());
    let mut iter = table.range::<&u32>(&0..&10).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1.value(), i + 1);
    }
    assert!(iter.next().is_none());
}

#[test]
fn ref_get_signatures() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..10u8 {
            table.insert(&[i], &[i + 1]).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();

    let zero = vec![0u8];
    assert_eq!(&[1], table.get(&[0]).unwrap().unwrap().value());
    assert_eq!(&[1], table.get(b"\0").unwrap().unwrap().value());
    assert_eq!(&[1], table.get(&zero).unwrap().unwrap().value());

    let start = vec![0u8];
    let end = vec![10u8];
    let mut iter = table.range::<&[u8]>(..).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1.value(), &[i + 1]);
    }
    assert!(iter.next().is_none());

    let mut iter = table.range(start.as_slice()..&end).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1.value(), &[i + 1]);
    }
    assert!(iter.next().is_none());
    drop(iter);

    let mut iter = table.range(start..end).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1.value(), &[i + 1]);
    }
    assert!(iter.next().is_none());

    let mut iter = table.range([0u8]..[10u8]).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().1.value(), &[i + 1]);
    }
    assert!(iter.next().is_none());
}

#[test]
fn concurrent_write_transactions_block() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = sync::Arc::new(Database::create(tmpfile.path()).unwrap());
    let wtx = db.begin_write().unwrap();
    let (sender, receiver) = sync::mpsc::channel();

    let t = {
        let db = db.clone();
        std::thread::spawn(move || {
            sender.send(()).unwrap();
            db.begin_write().unwrap().commit().unwrap();
        })
    };

    receiver.recv().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(1));
    wtx.commit().unwrap();
    t.join().unwrap();
}

#[test]
fn iter() {
    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..10 {
            table.insert(&i, &i).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(U64_TABLE).unwrap();
    let mut iter = table.iter().unwrap();
    for i in 0..10 {
        let (k, v) = iter.next().unwrap();
        assert_eq!(i, k.value());
        assert_eq!(i, v.value());
    }
}
