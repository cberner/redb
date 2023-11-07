use redb::backends::InMemoryBackend;
use redb::{
    Database, MultimapTableDefinition, MultimapTableHandle, Range, ReadableTable, RedbKey,
    RedbValue, TableDefinition, TableError, TableHandle, TypeName,
};
use std::cmp::Ordering;
#[cfg(not(target_os = "wasi"))]
use std::sync;

const SLICE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("slice");
const STR_TABLE: TableDefinition<&str, &str> = TableDefinition::new("x");
const U64_TABLE: TableDefinition<u64, u64> = TableDefinition::new("u64");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

#[test]
fn len() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        table.insert("hello", "world").unwrap();
        table.insert("hello2", "world2").unwrap();
        table.insert("hi", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 3);
}

#[test]
fn in_memory() {
    let db = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        table.insert("hello", "world").unwrap();
        table.insert("hello2", "world2").unwrap();
        table.insert("hi", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 3);
}

#[test]
fn first_last() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        assert!(table.first().unwrap().is_none());
        assert!(table.last().unwrap().is_none());
        table.insert("a", "world1").unwrap();
        assert_eq!(table.first().unwrap().unwrap().0.value(), "a");
        assert_eq!(table.last().unwrap().unwrap().0.value(), "a");
        table.insert("b", "world2").unwrap();
        table.insert("c", "world3").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE).unwrap();
    assert_eq!(table.first().unwrap().unwrap().0.value(), "a");
    assert_eq!(table.last().unwrap().unwrap().0.value(), "c");
}

#[test]
fn pop() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();

        assert!(table.pop_first().unwrap().is_none());
        assert!(table.pop_last().unwrap().is_none());

        table.insert("a", "world").unwrap();
        table.insert("b", "world2").unwrap();
        table.insert("c", "world3").unwrap();
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        {
            let (key, value) = table.pop_first().unwrap().unwrap();
            assert_eq!(key.value(), "a");
            assert_eq!(value.value(), "world");
        }
        {
            let (key, value) = table.pop_last().unwrap().unwrap();
            assert_eq!(key.value(), "c");
            assert_eq!(value.value(), "world3");
        }
        {
            let (key, value) = table.pop_last().unwrap().unwrap();
            assert_eq!(key.value(), "b");
            assert_eq!(value.value(), "world2");
        }

        assert!(table.pop_first().unwrap().is_none());
        assert!(table.pop_last().unwrap().is_none());
    }
    write_txn.commit().unwrap();
}

#[test]
fn drain() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..10 {
            table.insert(&i, &i).unwrap();
        }
        // Test draining uncommitted data
        drop(table.drain(0..10).unwrap());
        for i in 0..10 {
            table.insert(&i, &i).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 10);
        for (i, item) in table.drain(0..5).unwrap().enumerate() {
            let (k, v) = item.unwrap();
            assert_eq!(i as u64, k.value());
            assert_eq!(i as u64, v.value());
        }
        assert_eq!(table.len().unwrap(), 5);
        let mut i = 5u64;
        for item in table.range(0..10).unwrap() {
            let (k, v) = item.unwrap();
            assert_eq!(i, k.value());
            assert_eq!(i, v.value());
            i += 1;
        }
    }
    write_txn.abort().unwrap();

    // Check that dropping the iter early works too
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 10);
        drop(table.drain(0..5).unwrap());
        assert_eq!(table.len().unwrap(), 5);
    }
    write_txn.abort().unwrap();
}

#[test]
fn drain_filter() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..10 {
            table.insert(&i, &i).unwrap();
        }
        // Test draining uncommitted data
        drop(table.drain_filter(0..10, |k, _| k < 5).unwrap());
        for i in 0..5 {
            table.insert(&i, &i).unwrap();
        }
        assert_eq!(table.len().unwrap(), 10);

        // Test matching on the value
        drop(table.drain_filter(0..10, |_, v| v < 5).unwrap());
        for i in 0..5 {
            table.insert(&i, &i).unwrap();
        }
        assert_eq!(table.len().unwrap(), 10);
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 10);
        for (i, item) in table.drain_filter(0.., |x, _| x < 5).unwrap().enumerate() {
            let (k, v) = item.unwrap();
            assert_eq!(i as u64, k.value());
            assert_eq!(i as u64, v.value());
        }
        assert_eq!(table.len().unwrap(), 5);
        let mut i = 5u64;
        for item in table.range(0..10).unwrap() {
            let (k, v) = item.unwrap();
            assert_eq!(i, k.value());
            assert_eq!(i, v.value());
            i += 1;
        }
    }
    write_txn.abort().unwrap();

    // Check that dropping the iter early works too
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 10);
        drop(table.drain_filter(0.., |x, _| x < 5).unwrap());
        assert_eq!(table.len().unwrap(), 5);
    }
    write_txn.abort().unwrap();
}

#[test]
fn stored_size() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        table.insert("hello", "world").unwrap();
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
    let tmpfile = create_tempfile();
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
    let definition1: TableDefinition<&str, &str> = TableDefinition::new("1");
    let definition2: TableDefinition<&str, &str> = TableDefinition::new("2");

    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition1).unwrap();
        let mut table2 = write_txn.open_table(definition2).unwrap();

        table.insert("hello", "world").unwrap();
        table2.insert("hello", "world2").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition1).unwrap();
    let table2 = read_txn.open_table(definition2).unwrap();
    assert_eq!(table.len().unwrap(), 1);
    assert_eq!("world", table.get("hello").unwrap().unwrap().value());
    assert_eq!(table2.len().unwrap(), 1);
    assert_eq!("world2", table2.get("hello").unwrap().unwrap().value());
}

#[test]
fn list_tables() {
    let tmpfile = create_tempfile();
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

    let tables: Vec<String> = write_txn
        .list_tables()
        .unwrap()
        .map(|h| h.name().to_string())
        .collect();
    let multimap_tables: Vec<String> = write_txn
        .list_multimap_tables()
        .unwrap()
        .map(|h| h.name().to_string())
        .collect();
    assert_eq!(tables, &["x", "y"]);
    assert_eq!(multimap_tables, &["mx", "my"]);
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let tables: Vec<String> = read_txn
        .list_tables()
        .unwrap()
        .map(|h| h.name().to_string())
        .collect();
    let multimap_tables: Vec<String> = read_txn
        .list_multimap_tables()
        .unwrap()
        .map(|h| h.name().to_string())
        .collect();
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
    #[allow(clippy::needless_lifetimes)]
    fn insert_explicit_lifetime<'a>(table: &mut redb::Table<(&'a str, u8), u64>) {
        table
            .insert(&(String::from("hello").as_str(), 8), &1)
            .unwrap();
    }
}

#[test]
fn tuple_type_lifetime() {
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        assert!(table.is_empty().unwrap());
        table.insert("hello", "world").unwrap();
        assert!(!table.is_empty().unwrap());
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE).unwrap();
    assert!(!table.is_empty().unwrap());
}

#[test]
fn abort() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        table.insert("hello", "aborted").unwrap();
        assert_eq!("aborted", table.get("hello").unwrap().unwrap().value());
    }
    write_txn.abort().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE);
    assert!(table.is_err());

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        table.insert("hello", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE).unwrap();
    assert_eq!("world", table.get("hello").unwrap().unwrap().value());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn insert_overwrite() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        assert!(table.insert("hello", "world").unwrap().is_none());
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE).unwrap();
    assert_eq!("world", table.get("hello").unwrap().unwrap().value());

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        let old_value = table.insert("hello", "replaced").unwrap();
        assert_eq!(old_value.unwrap().value(), "world");
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE).unwrap();
    assert_eq!("replaced", table.get("hello").unwrap().unwrap().value());
}

#[test]
fn insert_reserve() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let def: TableDefinition<&str, &[u8]> = TableDefinition::new("x");
    let value = "world";
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(def).unwrap();
        let mut reserved = table
            .insert_reserve("hello", value.len().try_into().unwrap())
            .unwrap();
        reserved.as_mut().copy_from_slice(value.as_bytes());
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(def).unwrap();
    assert_eq!(
        value.as_bytes(),
        table.get("hello").unwrap().unwrap().value()
    );
}

#[test]
fn delete() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        table.insert("hello", "world").unwrap();
        table.insert("hello2", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE).unwrap();
    assert_eq!("world", table.get("hello").unwrap().unwrap().value());
    assert_eq!(table.len().unwrap(), 2);

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        assert_eq!("world", table.remove("hello").unwrap().unwrap().value());
        assert!(table.remove("hello").unwrap().is_none());
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE).unwrap();
    assert!(table.get("hello").unwrap().is_none());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn delete_open_table() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let table = write_txn.open_table(STR_TABLE).unwrap();
        assert!(matches!(
            write_txn.delete_table(STR_TABLE).unwrap_err(),
            TableError::TableAlreadyOpen(_, _)
        ));
        drop(table);
    }
    write_txn.commit().unwrap();
}

#[test]
fn delete_table() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let table = write_txn.open_table(STR_TABLE).unwrap();
        assert!(write_txn.delete_table(table).unwrap());
    }
    write_txn.commit().unwrap();
}

#[test]
fn no_dirty_reads() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        table.insert("hello", "world").unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE);
    assert!(table.is_err());
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE).unwrap();
    assert_eq!("world", table.get("hello").unwrap().unwrap().value());
}

#[test]
fn read_isolation() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        table.insert("hello", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE).unwrap();
    assert_eq!("world", table.get("hello").unwrap().unwrap().value());

    let write_txn = db.begin_write().unwrap();
    {
        let mut write_table = write_txn.open_table(STR_TABLE).unwrap();
        write_table.remove("hello").unwrap();
        write_table.insert("hello2", "world2").unwrap();
        write_table.insert("hello3", "world3").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn2 = db.begin_read().unwrap();
    let table2 = read_txn2.open_table(STR_TABLE).unwrap();
    assert!(table2.get("hello").unwrap().is_none());
    assert_eq!("world2", table2.get("hello2").unwrap().unwrap().value());
    assert_eq!("world3", table2.get("hello3").unwrap().unwrap().value());
    assert_eq!(table2.len().unwrap(), 2);

    assert_eq!("world", table.get("hello").unwrap().unwrap().value());
    assert!(table.get("hello2").unwrap().is_none());
    assert!(table.get("hello3").unwrap().is_none());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn read_isolation2() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        table.insert("hello", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE).unwrap();
    assert_eq!("world", table.get("hello").unwrap().unwrap().value());
    {
        let mut write_table = write_txn.open_table(STR_TABLE).unwrap();
        write_table.remove("hello").unwrap();
        write_table.insert("hello2", "world2").unwrap();
        write_table.insert("hello3", "world3").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn2 = db.begin_read().unwrap();
    let table2 = read_txn2.open_table(STR_TABLE).unwrap();
    assert!(table2.get("hello").unwrap().is_none());
    assert_eq!("world2", table2.get("hello2").unwrap().unwrap().value());
    assert_eq!("world3", table2.get("hello3").unwrap().unwrap().value());
    assert_eq!(table2.len().unwrap(), 2);

    assert_eq!("world", table.get("hello").unwrap().unwrap().value());
    assert!(table.get("hello2").unwrap().is_none());
    assert!(table.get("hello3").unwrap().is_none());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn reopen_table() {
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
        table
            .range(0..2)
            .unwrap()
            .map(|item| item.unwrap().1.value())
            .sum()
    );
    assert_eq!(1, table.get(&0).unwrap().unwrap().value());
}

#[test]
fn i128_type() {
    let tmpfile = create_tempfile();
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
    let mut iter: Range<i128, i128> = table.range::<i128>(..).unwrap();
    for i in -11..10 {
        assert_eq!(iter.next().unwrap().unwrap().1.value(), i);
    }
    assert!(iter.next().is_none());
}

#[test]
fn f32_type() {
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
    assert_eq!(iter.next().unwrap().unwrap().1.value(), "world");
    assert!(iter.next().is_none());

    let mut iter: Range<&str, &str> = table.range("a".."z").unwrap();
    assert_eq!(iter.next().unwrap().unwrap().1.value(), "world");
    assert!(iter.next().is_none());
}

#[test]
fn empty_type() {
    let tmpfile = create_tempfile();
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
#[allow(clippy::bool_assert_comparison)]
fn bool_type() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<bool, bool> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert(true, &false).unwrap();
        table.insert(&false, false).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    assert_eq!(false, table.get(&true).unwrap().unwrap().value());
    assert_eq!(false, table.get(&false).unwrap().unwrap().value());

    let mut iter = table.iter().unwrap();
    assert_eq!(iter.next().unwrap().unwrap().0.value(), false);
    assert_eq!(iter.next().unwrap().unwrap().0.value(), true);
    assert!(iter.next().is_none());
}

#[test]
fn option_type() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<Option<u8>, Option<u32>> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert(None, None).unwrap();
        table.insert(None, Some(0)).unwrap();
        table.insert(Some(1), Some(1)).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    assert_eq!(table.get(None).unwrap().unwrap().value(), Some(0));
    assert_eq!(table.get(Some(1)).unwrap().unwrap().value(), Some(1));
    let mut iter = table.iter().unwrap();
    assert_eq!(iter.next().unwrap().unwrap().0.value(), None);
    assert_eq!(iter.next().unwrap().unwrap().0.value(), Some(1));
}

#[test]
fn array_type() {
    let tmpfile = create_tempfile();
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

    let mut iter: Range<&[u8; 5], &[u8; 9]> = table.range::<&[u8; 5]>(..).unwrap();
    assert_eq!(iter.next().unwrap().unwrap().1.value(), b"world_123");
    assert!(iter.next().is_none());
}

#[test]
fn vec_fixed_width_value_type() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<u8, Vec<u64>> = TableDefinition::new("x");

    let value = vec![0, 1, 2, 3];
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert(0, &value).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    assert_eq!(value, table.get(0).unwrap().unwrap().value());
}

#[test]
fn vec_var_width_value_type() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<u8, Vec<&str>> = TableDefinition::new("x");

    let value = vec!["hello", "world"];
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert(0, &value).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    assert_eq!(value, table.get(0).unwrap().unwrap().value());
}

#[test]
fn vec_vec_type() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<u8, Vec<Vec<&str>>> = TableDefinition::new("x");

    let value = vec![vec!["hello", "world"], vec!["this", "is", "a", "test"]];
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert(0, &value).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    assert_eq!(value, table.get(0).unwrap().unwrap().value());
}

#[test]
fn range_lifetime() {
    let tmpfile = create_tempfile();
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

    let mut iter = {
        let start = "hello".to_string();
        table.range::<&str>(start.as_str()..).unwrap()
    };
    assert_eq!(iter.next().unwrap().unwrap().1.value(), "world");
    assert!(iter.next().is_none());
}

#[test]
fn drain_lifetime() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<&str, &str> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert("hello", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table = txn.open_table(definition).unwrap();

    let mut iter = {
        let start = "hello".to_string();
        table.drain::<&str>(start.as_str()..).unwrap()
    };
    assert_eq!(iter.next().unwrap().unwrap().1.value(), "world");
    assert!(iter.next().is_none());
}

#[test]
fn drain_filter_lifetime() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<&str, &str> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert("hello", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let mut table = txn.open_table(definition).unwrap();

    let mut iter = {
        let start = "hello".to_string();
        table.drain_filter(start.as_str().., |_, _| true).unwrap()
    };
    assert_eq!(iter.next().unwrap().unwrap().1.value(), "world");
    assert!(iter.next().is_none());
}

#[test]
fn custom_ordering() {
    #[derive(Debug)]
    struct ReverseKey(Vec<u8>);

    impl RedbValue for ReverseKey {
        type SelfType<'a> = ReverseKey
        where
        Self: 'a;
        type AsBytes<'a> = &'a [u8]
        where
        Self: 'a;

        fn fixed_width() -> Option<usize> {
            None
        }

        fn from_bytes<'a>(data: &'a [u8]) -> ReverseKey
        where
            Self: 'a,
        {
            ReverseKey(data.to_vec())
        }

        fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'a [u8]
        where
            Self: 'a,
            Self: 'b,
        {
            &value.0
        }

        fn type_name() -> TypeName {
            TypeName::new("test::ReverseKey")
        }
    }

    impl RedbKey for ReverseKey {
        fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
            data2.cmp(data1)
        }
    }

    let definition: TableDefinition<ReverseKey, &str> = TableDefinition::new("x");

    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        for i in 0..10u8 {
            let key = vec![i];
            table.insert(&ReverseKey(key), "value").unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    let start = ReverseKey(vec![7u8]); // ReverseKey is used, so 7 < 3
    let end = ReverseKey(vec![3u8]);
    let mut iter = table.range(start..=end).unwrap();
    for i in (3..=7u8).rev() {
        let (key, value) = iter.next().unwrap().unwrap();
        assert_eq!(&[i], key.value().0.as_slice());
        assert_eq!("value", value.value());
    }
    assert!(iter.next().is_none());
}

#[test]
fn owned_get_signatures() {
    let tmpfile = create_tempfile();
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

    let mut iter: Range<u32, u32> = table.range::<u32>(..).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().unwrap().1.value(), i + 1);
    }
    assert!(iter.next().is_none());
    let mut iter: Range<u32, u32> = table.range(0..10).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().unwrap().1.value(), i + 1);
    }
    assert!(iter.next().is_none());
    let mut iter = table.range::<&u32>(&0..&10).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().unwrap().1.value(), i + 1);
    }
    assert!(iter.next().is_none());
}

#[test]
fn ref_get_signatures() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(SLICE_TABLE).unwrap();
        for i in 0..10u8 {
            table.insert([i].as_slice(), [i + 1].as_slice()).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(SLICE_TABLE).unwrap();

    let zero = vec![0u8];
    assert_eq!(&[1], table.get([0].as_slice()).unwrap().unwrap().value());
    assert_eq!(&[1], table.get(b"\0".as_slice()).unwrap().unwrap().value());
    assert_eq!(&[1], table.get(zero.as_slice()).unwrap().unwrap().value());

    let start = vec![0u8];
    let end = vec![10u8];
    let mut iter = table.range::<&[u8]>(..).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().unwrap().1.value(), &[i + 1]);
    }
    assert!(iter.next().is_none());

    let mut iter = table.range(start.as_slice()..&end).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().unwrap().1.value(), &[i + 1]);
    }
    assert!(iter.next().is_none());
    drop(iter);

    let mut iter = table.range(start.as_slice()..end.as_slice()).unwrap();
    for i in 0..10 {
        assert_eq!(iter.next().unwrap().unwrap().1.value(), &[i + 1]);
    }
    assert!(iter.next().is_none());

    let mut iter = table.range([0u8].as_slice()..[10u8].as_slice()).unwrap();
    for i in 0..10u8 {
        assert_eq!(iter.next().unwrap().unwrap().1.value(), [i + 1].as_slice());
    }
    assert!(iter.next().is_none());
}

#[cfg(not(target_os = "wasi"))]
#[test]
fn concurrent_write_transactions_block() {
    let tmpfile = create_tempfile();
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
    let tmpfile = create_tempfile();
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
        let (k, v) = iter.next().unwrap().unwrap();
        assert_eq!(i, k.value());
        assert_eq!(i, v.value());
    }
}

#[test]
fn drain_next_back() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..10 {
            table.insert(&i, &i).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    let mut table = write_txn.open_table(U64_TABLE).unwrap();
    let mut iter = table.drain(0..10).unwrap();
    for i in (0..10).rev() {
        let (k, v) = iter.next_back().unwrap().unwrap();
        assert_eq!(i, k.value());
        assert_eq!(i, v.value());
    }
}

#[test]
fn drain_filter_all_elements_next_back() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..10 {
            table.insert(&i, &i).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    let mut table = write_txn.open_table(U64_TABLE).unwrap();
    let mut iter = table.drain_filter(0..10, |_, _| true).unwrap();
    for i in (0..10).rev() {
        let (k, v) = iter.next_back().unwrap().unwrap();
        assert_eq!(i, k.value());
        assert_eq!(i, v.value());
    }
}

#[test]
fn signature_lifetimes() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();

        let _ = {
            let key = "hi".to_string();
            let value = "1".to_string();
            table.insert(key.as_str(), value.as_str()).unwrap()
        };

        let _ = {
            let key = "hi".to_string();
            table.get(key.as_str()).unwrap()
        };

        let _ = {
            let key = "hi".to_string();
            table.remove(key.as_str()).unwrap()
        };

        let _ = {
            let key = "hi".to_string();
            table.range(key.as_str()..).unwrap()
        };

        let _ = {
            let key = "hi".to_string();
            table.drain(key.as_str()..).unwrap()
        };

        let _ = {
            let key = "hi".to_string();
            table.drain_filter(key.as_str().., |_, _| true).unwrap()
        };
    }
    write_txn.commit().unwrap();
}

#[test]
fn generic_signature_lifetimes() {
    fn write_key_generic<K: RedbKey>(
        table: TableDefinition<K, &[u8]>,
        key: K::SelfType<'_>,
        db: &Database,
    ) {
        let buf = [1, 2, 3];
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(table).unwrap();
            table.insert(key, buf.as_slice()).unwrap();
        }
        write_txn.commit().unwrap();
    }

    fn read_key_generic<K: RedbKey>(
        table: TableDefinition<K, &[u8]>,
        key: K::SelfType<'_>,
        db: &Database,
    ) {
        let buf = [1, 2, 3];
        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(table).unwrap();
        assert_eq!(table.get(key).unwrap().unwrap().value(), buf);
    }

    let tmpfile = create_tempfile();
    let db = &Database::create(tmpfile.path()).unwrap();
    {
        let (table, key) = (TableDefinition::<&str, _>::new("&str"), "key");
        write_key_generic(table, key, db);
        read_key_generic(table, key, db);
    }
    {
        let (table, key) = (TableDefinition::<(), _>::new("()"), ());
        write_key_generic(table, key, db);
        read_key_generic(table, key, db);
    }
}
