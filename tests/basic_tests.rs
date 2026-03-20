use rand::random;
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
use shodh_redb::DatabaseError;
use shodh_redb::backends::InMemoryBackend;
use shodh_redb::{
    Database, Durability, Key, Legacy, MultimapTableDefinition, MultimapTableHandle, Range,
    ReadOnlyDatabase, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
    TableError, TableHandle, TypeName, Value, VerifyLevel,
};
use std::cmp::Ordering;
#[cfg(not(target_os = "wasi"))]
use std::sync;

const SLICE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("slice");
const STR_TABLE: TableDefinition<&str, &str> = TableDefinition::new("x");
const U64_TABLE: TableDefinition<u64, u64> = TableDefinition::new("u64");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
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
    let untyped_table = read_txn.open_untyped_table(STR_TABLE).unwrap();
    assert_eq!(untyped_table.len().unwrap(), 3);
}

#[test]
fn read_only() {
    let tmpfile = create_tempfile();
    {
        let db = Database::create(tmpfile.path()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(STR_TABLE).unwrap();
            table.insert("hello", "world").unwrap();
            table.insert("hello2", "world2").unwrap();
            table.insert("hi", "world").unwrap();
        }
        write_txn.commit().unwrap();

        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
        assert!(matches!(
            ReadOnlyDatabase::open(tmpfile.path()),
            Err(DatabaseError::DatabaseAlreadyOpen)
        ));
        drop(db);
    }

    let db = ReadOnlyDatabase::open(tmpfile.path()).unwrap();
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(STR_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 3);

    let db2 = ReadOnlyDatabase::open(tmpfile.path()).unwrap();
    let read_txn2 = db.begin_read().unwrap();
    let table2 = read_txn2.open_table(STR_TABLE).unwrap();
    assert_eq!(table2.len().unwrap(), 3);

    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    assert!(matches!(
        Database::open(tmpfile.path()),
        Err(DatabaseError::DatabaseAlreadyOpen)
    ));
    drop(db);
    drop(db2);
}

#[test]
fn table_stats() {
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
    let untyped_table = read_txn.open_untyped_table(STR_TABLE).unwrap();
    assert_eq!(table.stats().unwrap().tree_height(), 1);
    assert_eq!(untyped_table.stats().unwrap().tree_height(), 1);
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
fn extract_if() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..10 {
            table.insert(&i, &i).unwrap();
        }
        // Test retain uncommitted data
        let mut extracted = table.extract_if(|k, _| k >= 5).unwrap();
        assert_eq!(extracted.next().unwrap().unwrap().0.value(), 5);
        drop(extracted);
        assert_eq!(table.len().unwrap(), 9);

        let mut extracted = table.extract_from_if(5.., |k, _| k < 8).unwrap();
        assert_eq!(extracted.next().unwrap().unwrap().0.value(), 6);
        assert_eq!(extracted.next().unwrap().unwrap().0.value(), 7);
        assert!(extracted.next().is_none());
        drop(extracted);
        assert_eq!(table.len().unwrap(), 7);

        for i in 5..8 {
            assert!(table.insert(&i, &i).unwrap().is_none());
        }
        assert_eq!(table.len().unwrap(), 10);
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 10);
        let mut extracted = table.extract_if(|_, _| true).unwrap();
        assert_eq!(extracted.next().unwrap().unwrap().1.value(), 0);
        drop(extracted);
        assert_eq!(table.len().unwrap(), 9);
    }
    write_txn.abort().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 10);
        for _ in table.extract_if(|x, _| x % 2 != 0).unwrap() {}
        table.extract_if(|_, _| true).unwrap().next_back();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_write().unwrap();
    {
        let table = read_txn.open_table(U64_TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 4);
        let mut iter = table.iter().unwrap();
        for x in [0, 2, 4, 6] {
            let (k, v) = iter.next().unwrap().unwrap();
            assert_eq!(k.value(), x);
            assert_eq!(k.value(), v.value());
        }
    }
}

#[test]
fn retain() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..10 {
            table.insert(&i, &i).unwrap();
        }
        // Test retain uncommitted data
        table.retain(|k, _| k >= 5).unwrap();
        for i in 0..5 {
            assert!(table.insert(&i, &i).unwrap().is_none());
        }
        assert_eq!(table.len().unwrap(), 10);

        // Test matching on the value
        table.retain(|_, v| v >= 5).unwrap();
        for i in 0..5 {
            assert!(table.insert(&i, &i).unwrap().is_none());
        }
        assert_eq!(table.len().unwrap(), 10);

        // Test retain_in
        table.retain_in(..5, |_, _| false).unwrap();
        for i in 0..5 {
            assert!(table.insert(&i, &i).unwrap().is_none());
        }
        assert_eq!(table.len().unwrap(), 10);
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 10);
        table.retain(|x, _| x >= 5).unwrap();
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

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        table.retain(|x, _| x % 2 == 0).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_write().unwrap();
    {
        let table = read_txn.open_table(U64_TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 5);
        for entry in table.iter().unwrap() {
            let (k, v) = entry.unwrap();
            assert_eq!(k.value() % 2, 0);
            assert_eq!(k.value(), v.value());
        }
    }
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
    fn insert_inferred_lifetime(table: &mut shodh_redb::Table<(&str, u8), u64>) {
        table
            .insert(&(String::from("hello").as_str(), 8), &1)
            .unwrap();
    }

    #[allow(dead_code)]
    #[allow(clippy::needless_lifetimes)]
    fn insert_explicit_lifetime<'a>(table: &mut shodh_redb::Table<(&'a str, u8), u64>) {
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
        (
            &str,
            u8,
            u16,
            u32,
            u64,
            u128,
            &str,
            i16,
            i32,
            i64,
            i128,
            &str,
        ),
        (u16, u32),
    > = TableDefinition::new("table");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table
            .insert(
                &("hello", 5, 6, 7, 8, 9, "mid", -2, -3, -4, -5, "end"),
                &(0, 123),
            )
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(table_def).unwrap();
    assert_eq!(
        table
            .get(&("hello", 5, 6, 7, 8, 9, "mid", -2, -3, -4, -5, "end"))
            .unwrap()
            .unwrap()
            .value(),
        (0, 123)
    );
}

#[test]
fn legacy_tuple2_type() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    #[expect(clippy::type_complexity)]
    let table_def: TableDefinition<Legacy<(&str, u8)>, Legacy<(u16, u32)>> =
        TableDefinition::new("table");

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
#[allow(clippy::type_complexity)]
fn legacy_tuple12_type() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def: TableDefinition<
        Legacy<(&str, u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, &str)>,
        Legacy<(u16, u32)>,
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
#[allow(clippy::type_complexity)]
fn generic_array_type() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let table_def1: TableDefinition<[u8; 3], [u64; 2]> = TableDefinition::new("table1");
    let table_def2: TableDefinition<[(u8, &str); 2], [Option<&str>; 2]> =
        TableDefinition::new("table2");
    let table_def3: TableDefinition<[&[u8]; 2], [f32; 2]> = TableDefinition::new("table3");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table1 = write_txn.open_table(table_def1).unwrap();
        let mut table2 = write_txn.open_table(table_def2).unwrap();
        let mut table3 = write_txn.open_table(table_def3).unwrap();
        table1.insert([0, 1, 2], &[4, 5]).unwrap();
        table2
            .insert([(0, "hi"), (1, "world")], [None, Some("test")])
            .unwrap();
        table3
            .insert([b"hi".as_slice(), b"world".as_slice()], [4.0, 5.0])
            .unwrap();
        table3
            .insert([b"longlong".as_slice(), b"longlong".as_slice()], [0.0, 0.0])
            .unwrap();
        table3
            .insert([b"s".as_slice(), b"s".as_slice()], [0.0, 0.0])
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table1 = read_txn.open_table(table_def1).unwrap();
    let table2 = read_txn.open_table(table_def2).unwrap();
    let table3 = read_txn.open_table(table_def3).unwrap();
    assert_eq!(table1.get(&[0, 1, 2]).unwrap().unwrap().value(), [4, 5]);
    assert_eq!(
        table2
            .get(&[(0, "hi"), (1, "world")])
            .unwrap()
            .unwrap()
            .value(),
        [None, Some("test")]
    );
    assert_eq!(
        table3
            .get(&[b"hi".as_slice(), b"world".as_slice()])
            .unwrap()
            .unwrap()
            .value(),
        [4.0, 5.0]
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
        let mut reserved = table.insert_reserve("hello", value.len()).unwrap();
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
fn get_mut() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(STR_TABLE).unwrap();
        assert!(table.insert("hello", "world").unwrap().is_none());
    }
    write_txn.commit().unwrap();

    {
        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(STR_TABLE).unwrap();
        assert_eq!("world", table.get("hello").unwrap().unwrap().value());
    }

    let mut very_long_string = String::from("hello");
    for _ in 0..10_000 {
        very_long_string.push('x');
    }

    let mut last_value = "world";

    for new_value in ["earth", "mars", very_long_string.as_str()].iter() {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(STR_TABLE).unwrap();
            let mut value = table.get_mut("hello").unwrap().unwrap();
            if value.value() == last_value {
                value.insert(new_value).unwrap();
            } else {
                panic!();
            }
            assert_eq!(value.value(), *new_value);
            last_value = new_value;
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(STR_TABLE).unwrap();
        assert_eq!(*new_value, table.get("hello").unwrap().unwrap().value());
    }
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
fn rename_table() {
    let table_def: TableDefinition<&str, &str> = TableDefinition::new("x");
    let table_def2: TableDefinition<&str, &str> = TableDefinition::new("x2");
    let multitable_def: MultimapTableDefinition<&str, &str> = MultimapTableDefinition::new("x");
    let multitable_def2: MultimapTableDefinition<&str, &str> = MultimapTableDefinition::new("x2");

    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(table_def).unwrap();
        table.insert("hi", "hi").unwrap();
        write_txn.rename_table(table, table_def2).unwrap();
        assert!(matches!(
            write_txn.rename_table(table_def, table_def2).unwrap_err(),
            TableError::TableDoesNotExist(_)
        ));

        let table = write_txn.open_table(table_def).unwrap();
        assert!(matches!(
            write_txn.rename_table(table, table_def2).unwrap_err(),
            TableError::TableExists(_)
        ));

        assert!(matches!(
            write_txn
                .rename_multimap_table(multitable_def, multitable_def2)
                .unwrap_err(),
            TableError::TableIsNotMultimap(_)
        ));
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let table = write_txn.open_table(table_def).unwrap();
        assert!(table.is_empty().unwrap());
        let table2 = write_txn.open_table(table_def2).unwrap();
        assert_eq!(table2.get("hi").unwrap().unwrap().value(), "hi");
    }
}

#[test]
fn rename_open_table() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let table = write_txn.open_table(STR_TABLE).unwrap();
        assert!(matches!(
            write_txn.rename_table(STR_TABLE, STR_TABLE).unwrap_err(),
            TableError::TableAlreadyOpen(_, _)
        ));
        drop(table);
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
            .sum::<u64>()
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
fn string_type() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<String, String> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table
            .insert("hello".to_string(), "world".to_string())
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    assert_eq!(
        "world",
        table.get("hello".to_string()).unwrap().unwrap().value()
    );

    let mut iter = table.iter().unwrap();
    assert_eq!(iter.next().unwrap().unwrap().1.value(), "world");
    assert!(iter.next().is_none());

    let mut iter: Range<String, String> = table.range("a".to_string().."z".to_string()).unwrap();
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
fn range_empty() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<u128, u128> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        for i in 0..1000 {
            table.insert(i, i).unwrap();
        }
        #[expect(clippy::reversed_empty_ranges)]
        let mut iter = table.range(500..0).unwrap();
        assert!(iter.next().is_none());
    }
    write_txn.commit().unwrap();
}

#[test]
fn extract_from_if_empty() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<u128, u128> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        for i in 0..1000 {
            table.insert(i, i).unwrap();
        }
        #[expect(clippy::reversed_empty_ranges)]
        let mut iter = table.extract_from_if(500..0, |_, _| true).unwrap();
        assert!(iter.next().is_none());
    }
    write_txn.commit().unwrap();
}

#[test]
fn retain_in_empty() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<u128, u128> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        for i in 0..1000 {
            table.insert(i, i).unwrap();
        }
        #[expect(clippy::reversed_empty_ranges)]
        table.retain_in(500..0, |_, _| false).unwrap();
        assert_eq!(table.len().unwrap(), 1000);
    }
    write_txn.commit().unwrap();
}

#[test]
fn range_arc() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<&str, &str> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert("hello", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let mut iter = {
        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(definition).unwrap();
        let start = "hello".to_string();
        table.range::<&str>(start.as_str()..).unwrap()
    };
    assert_eq!(iter.next().unwrap().unwrap().1.value(), "world");
    assert!(iter.next().is_none());
}

#[test]
fn range_clone() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<&str, &str> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert("hello", "world").unwrap();
        let mut iter1 = table.iter().unwrap();
        let mut iter2 = iter1.clone();
        let (k1, v1) = iter1.next().unwrap().unwrap();
        let (k2, v2) = iter2.next().unwrap().unwrap();
        assert_eq!(k1.value(), k2.value());
        assert_eq!(v1.value(), v2.value());
    }
    write_txn.commit().unwrap();
}

#[test]
fn custom_ordering() {
    #[derive(Debug)]
    struct ReverseKey(Vec<u8>);

    impl Value for ReverseKey {
        type SelfType<'a>
            = ReverseKey
        where
            Self: 'a;
        type AsBytes<'a>
            = &'a [u8]
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

    impl Key for ReverseKey {
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

        let _ = { table.extract_if(|_, _| true).unwrap() };

        let _ = {
            let key = "hi".to_string();
            table.extract_from_if(key.as_str().., |_, _| true).unwrap()
        };
    }
    write_txn.commit().unwrap();
}

#[test]
fn generic_signature_lifetimes() {
    fn write_key_generic<K: Key>(
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

    fn read_key_generic<K: Key>(
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

#[test]
fn char_type() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: TableDefinition<char, char> = TableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(definition).unwrap();
        table.insert('a', &'b').unwrap();
        table.insert(&'b', 'a').unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(definition).unwrap();
    assert_eq!('a', table.get(&'b').unwrap().unwrap().value());
    assert_eq!('b', table.get(&'a').unwrap().unwrap().value());

    let mut iter = table.iter().unwrap();
    assert_eq!(iter.next().unwrap().unwrap().0.value(), 'a');
    assert_eq!(iter.next().unwrap().unwrap().0.value(), 'b');
    assert!(iter.next().is_none());
}

#[test]
fn drain_range() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..100 {
            table.insert(&i, &(i * 10)).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Drain a subrange
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        let removed = table.drain(50u64..80).unwrap();
        assert_eq!(removed, 30);
        assert_eq!(table.len().unwrap(), 70);
    }
    write_txn.commit().unwrap();

    // Verify remaining keys
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(U64_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 70);
    // Keys 0..50 should exist
    for i in 0..50 {
        assert!(table.get(&i).unwrap().is_some());
    }
    // Keys 50..80 should be gone
    for i in 50..80 {
        assert!(table.get(&i).unwrap().is_none());
    }
    // Keys 80..100 should exist
    for i in 80..100 {
        assert!(table.get(&i).unwrap().is_some());
    }
}

#[test]
fn drain_all() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..50 {
            table.insert(&i, &i).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        let removed = table.drain_all().unwrap();
        assert_eq!(removed, 50);
        assert_eq!(table.len().unwrap(), 0);
    }
    write_txn.commit().unwrap();
}

#[cfg(not(target_os = "wasi"))]
#[test]
fn backup_and_restore() {
    let tmpfile = create_tempfile();
    let backup_file = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Insert test data
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..1000 {
            table.insert(&i, &(i * 3)).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Create backup
    db.backup(backup_file.path()).unwrap();

    // Open backup with Database::open (handles quick repair on first open)
    let backup_db = Database::open(backup_file.path()).unwrap();
    let read_txn = backup_db.begin_read().unwrap();
    let table = read_txn.open_table(U64_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 1000);
    for i in 0..1000 {
        assert_eq!(table.get(&i).unwrap().unwrap().value(), i * 3);
    }
}

#[cfg(not(target_os = "wasi"))]
#[test]
fn backup_while_reading() {
    let tmpfile = create_tempfile();
    let backup_file = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..100 {
            table.insert(&i, &i).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Hold a read transaction while backing up
    let _read_txn = db.begin_read().unwrap();
    db.backup(backup_file.path()).unwrap();

    let backup_db = Database::open(backup_file.path()).unwrap();
    let read_txn = backup_db.begin_read().unwrap();
    let table = read_txn.open_table(U64_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 100);
}

// Test that &[u8; N] and [u8; N] are effectively the same
#[test]
fn u8_array_serialization() {
    assert_eq!(
        <&[u8; 7] as Value>::type_name(),
        <[u8; 7] as Value>::type_name()
    );
    let fixed_value: u128 = random();
    let fixed_serialized = fixed_value.to_le_bytes();
    for _ in 0..1000 {
        let x: u128 = random();
        let x_serialized = x.to_le_bytes();
        let ref_x_serialized = &x_serialized;
        let u8_ref_serialized = <&[u8; 16] as Value>::as_bytes(&ref_x_serialized);
        let u8_generic_serialized = <[u8; 16] as Value>::as_bytes(&x_serialized);
        assert_eq!(
            u8_ref_serialized.as_slice(),
            u8_generic_serialized.as_slice()
        );
        assert_eq!(u8_ref_serialized.as_slice(), x_serialized.as_slice());
        let ref_order = <&[u8; 16] as Key>::compare(&x_serialized, &fixed_serialized);
        let generic_order = <[u8; 16] as Key>::compare(&x_serialized, &fixed_serialized);
        assert_eq!(ref_order, generic_order);
    }
}

#[test]
fn verify_backup_header_level() {
    let tmpfile = create_tempfile();
    let backup_file = create_tempfile();

    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..100 {
            table.insert(i, i * 10).unwrap();
        }
    }
    write_txn.commit().unwrap();
    db.backup(backup_file.path()).unwrap();
    drop(db);

    let report = Database::verify_backup(backup_file.path(), VerifyLevel::Header).unwrap();
    assert!(report.valid);
    assert!(report.header_valid);
    assert_eq!(report.pages_checked, 0);
    assert_eq!(report.pages_corrupt, 0);
    assert!(report.structural_valid.is_none());
    assert!(report.corrupt_details.is_empty());
}

#[test]
fn verify_backup_pages_level() {
    let tmpfile = create_tempfile();
    let backup_file = create_tempfile();

    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..100 {
            table.insert(i, i * 10).unwrap();
        }
    }
    write_txn.commit().unwrap();
    db.backup(backup_file.path()).unwrap();
    drop(db);

    let report = Database::verify_backup(backup_file.path(), VerifyLevel::Pages).unwrap();
    assert!(report.valid);
    assert!(report.header_valid);
    assert!(report.pages_checked > 0);
    assert_eq!(report.pages_corrupt, 0);
    assert!(report.structural_valid.is_none());
    assert!(report.corrupt_details.is_empty());
}

#[test]
fn verify_backup_full_level() {
    let tmpfile = create_tempfile();
    let backup_file = create_tempfile();

    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..100 {
            table.insert(i, i * 10).unwrap();
        }
    }
    write_txn.commit().unwrap();
    db.backup(backup_file.path()).unwrap();
    drop(db);

    let report = Database::verify_backup(backup_file.path(), VerifyLevel::Full).unwrap();
    assert!(report.valid);
    assert!(report.header_valid);
    assert!(report.pages_checked > 0);
    assert_eq!(report.pages_corrupt, 0);
    assert_eq!(report.structural_valid, Some(true));
    assert!(report.corrupt_details.is_empty());
}

#[test]
fn verify_backup_corrupt_header() {
    let tmpfile = create_tempfile();
    let backup_file = create_tempfile();

    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        table.insert(1u64, 2u64).unwrap();
    }
    write_txn.commit().unwrap();
    db.backup(backup_file.path()).unwrap();
    drop(db);

    // Corrupt the magic number (first 9 bytes)
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(backup_file.path())
            .unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&[0xFF; 4]).unwrap();
        f.sync_all().unwrap();
    }

    // Should return an error (invalid magic number is detected before header parsing)
    let result = Database::verify_backup(backup_file.path(), VerifyLevel::Header);
    assert!(result.is_err());
}

#[test]
fn verify_backup_corrupt_slot() {
    let tmpfile = create_tempfile();
    let backup_file = create_tempfile();

    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..100 {
            table.insert(i, i * 10).unwrap();
        }
    }
    write_txn.commit().unwrap();
    db.backup(backup_file.path()).unwrap();
    drop(db);

    // Corrupt the checksum of commit slot 0 (last 16 bytes of the 128-byte slot)
    // Slot 0 starts at file offset 64, checksum at offset 64 + 112 = 176
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(backup_file.path())
            .unwrap();
        f.seek(SeekFrom::Start(176)).unwrap();
        f.write_all(&[0xDE; 16]).unwrap();
        f.sync_all().unwrap();
    }

    let report = Database::verify_backup(backup_file.path(), VerifyLevel::Pages).unwrap();
    // Primary slot checksum was corrupted -- verification falls back to secondary
    // but header_valid should be false (primary wasn't valid)
    assert!(!report.header_valid);
    assert!(!report.valid);
}

#[test]
fn verify_integrity_open_db() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..50 {
            table.insert(i, i * 100).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let report = db.verify_integrity(VerifyLevel::Full).unwrap();
    assert!(report.valid);
    assert!(report.header_valid);
    assert!(report.pages_checked > 0);
    assert_eq!(report.pages_corrupt, 0);
    assert_eq!(report.structural_valid, Some(true));
}

#[test]
fn verify_backup_nonexistent_file() {
    let result = Database::verify_backup("/nonexistent/path/to/db.redb", VerifyLevel::Header);
    assert!(result.is_err());
}

#[test]
fn verify_backup_with_multimap_table() {
    let tmpfile = create_tempfile();
    let backup_file = create_tempfile();
    const MM_TABLE: MultimapTableDefinition<u64, u64> = MultimapTableDefinition::new("mm");

    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(U64_TABLE).unwrap();
        for i in 0..50 {
            table.insert(i, i).unwrap();
        }
    }
    {
        let mut mm = write_txn.open_multimap_table(MM_TABLE).unwrap();
        for i in 0..20u64 {
            for j in 0..5u64 {
                mm.insert(i, i * 10 + j).unwrap();
            }
        }
    }
    write_txn.commit().unwrap();
    db.backup(backup_file.path()).unwrap();
    drop(db);

    let report = Database::verify_backup(backup_file.path(), VerifyLevel::Full).unwrap();
    assert!(report.valid);
    assert!(report.pages_checked > 0);
    assert_eq!(report.pages_corrupt, 0);
    assert_eq!(report.structural_valid, Some(true));
}

#[test]
fn page_reuse_across_transactions() {
    let tmpfile = create_tempfile();
    let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("data");
    let value = vec![0u8; 1024];

    let db = Database::create(tmpfile.path()).unwrap();

    // Fill with data
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..1000u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    // Do a delete + re-insert cycle to establish baseline (first cycle causes file growth
    // due to COW during delete)
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..1000u64 {
            table.remove(&i).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..1000u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let file_size_after_cycle_1 = tmpfile.as_file().metadata().unwrap().len();

    // Second delete + re-insert cycle -- with early freed page processing, the file
    // should NOT grow because freed pages from cycle 1 are reused
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..1000u64 {
            table.remove(&i).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..1000u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let file_size_after_cycle_2 = tmpfile.as_file().metadata().unwrap().len();

    // With page reuse, the second cycle should not cause significant growth beyond
    // the first cycle. Without early freed page processing, each cycle would add ~2x.
    assert!(
        file_size_after_cycle_2 <= file_size_after_cycle_1 * 110 / 100,
        "File grew from {} to {} bytes ({:.1}x) across cycles, expected stable size with page reuse",
        file_size_after_cycle_1,
        file_size_after_cycle_2,
        file_size_after_cycle_2 as f64 / file_size_after_cycle_1 as f64,
    );
}

#[test]
fn page_reuse_with_live_reader() {
    let tmpfile = create_tempfile();
    let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("data");
    let value = vec![0u8; 1024];

    let db = Database::create(tmpfile.path()).unwrap();

    // Fill with data
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..500u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    // Hold a read transaction open -- this prevents freed pages from being reclaimed
    let read_txn = db.begin_read().unwrap();
    let _table = read_txn.open_table(table_def).unwrap();

    // Delete all data
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..500u64 {
            table.remove(&i).unwrap();
        }
    }
    txn.commit().unwrap();

    // Re-insert -- freed pages should NOT be reused because the reader holds them
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..500u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let file_size_with_reader = tmpfile.as_file().metadata().unwrap().len();

    // Drop the reader
    drop(_table);
    drop(read_txn);

    // Delete and re-insert again -- now freed pages SHOULD be reused
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..500u64 {
            table.remove(&i).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..500u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let file_size_after_reader_dropped = tmpfile.as_file().metadata().unwrap().len();

    // After reader is dropped, the file should not have grown much beyond the reader-held size
    // because freed pages are now reclaimable
    assert!(
        file_size_after_reader_dropped <= file_size_with_reader * 110 / 100,
        "File grew from {} to {} after reader dropped, expected page reuse",
        file_size_with_reader,
        file_size_after_reader_dropped,
    );
}

#[test]
fn page_reuse_non_durable() {
    let tmpfile = create_tempfile();
    let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("data");
    let value = vec![0u8; 1024];

    let db = Database::create(tmpfile.path()).unwrap();

    // Fill with data
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..500u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    // First cycle: delete + re-insert with non-durable commits
    let mut txn = db.begin_write().unwrap();
    let _ = txn.set_durability(Durability::None);
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..500u64 {
            table.remove(&i).unwrap();
        }
    }
    txn.commit().unwrap();

    let mut txn = db.begin_write().unwrap();
    let _ = txn.set_durability(Durability::None);
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..500u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    // Durable commit to flush
    let txn = db.begin_write().unwrap();
    txn.commit().unwrap();

    let file_size_after_cycle_1 = tmpfile.as_file().metadata().unwrap().len();

    // Second cycle with non-durable commits
    let mut txn = db.begin_write().unwrap();
    let _ = txn.set_durability(Durability::None);
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..500u64 {
            table.remove(&i).unwrap();
        }
    }
    txn.commit().unwrap();

    let mut txn = db.begin_write().unwrap();
    let _ = txn.set_durability(Durability::None);
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..500u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    // Durable commit to flush
    let txn = db.begin_write().unwrap();
    txn.commit().unwrap();

    let file_size_after_cycle_2 = tmpfile.as_file().metadata().unwrap().len();

    assert!(
        file_size_after_cycle_2 <= file_size_after_cycle_1 * 110 / 100,
        "Non-durable: file grew from {} to {} bytes ({:.1}x) across cycles, expected stable size",
        file_size_after_cycle_1,
        file_size_after_cycle_2,
        file_size_after_cycle_2 as f64 / file_size_after_cycle_1 as f64,
    );
}

#[test]
fn database_stats_free_pages() {
    let tmpfile = create_tempfile();
    let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new("data");
    let value = vec![0u8; 4096];

    let db = Database::create(tmpfile.path()).unwrap();

    // Insert data
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..100u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    // Delete data -- creates freed pages
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(table_def).unwrap();
        for i in 0..100u64 {
            table.remove(&i).unwrap();
        }
    }
    txn.commit().unwrap();

    // Start a new transaction -- early freed page processing should have run,
    // making free_pages > 0
    let txn = db.begin_write().unwrap();
    let stats = txn.stats().unwrap();
    assert!(
        stats.free_pages() > 0,
        "Expected free_pages > 0 after deleting data, got {}",
        stats.free_pages()
    );
    txn.commit().unwrap();
}
