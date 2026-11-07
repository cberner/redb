use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition, TableHandle,
};
use tempfile::TempDir;

const STRINGS: TableDefinition<&str, &str> = TableDefinition::new("strings");
const NUMS: TableDefinition<u64, u64> = TableDefinition::new("nums");
const WIDE: TableDefinition<u32, (u128, u128, u128, u128)> = TableDefinition::new("wide");

fn make_db() -> (TempDir, Database) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("untyped.rdb")).unwrap();
    (dir, db)
}

fn untyped_entries(db: &Database, name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let read = db.begin_read().unwrap();
    let handle = read
        .list_tables()
        .unwrap()
        .find(|handle| handle.name() == name)
        .expect("table handle");
    let table = read.open_untyped_table(handle).unwrap();
    table.iter().map(|entry| entry.unwrap()).collect()
}

#[test]
fn iterates_string_table_in_stored_order() {
    let (_dir, db) = make_db();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(STRINGS).unwrap();
        table.insert("gamma", "3").unwrap();
        table.insert("alpha", "1").unwrap();
        table.insert("beta", "2").unwrap();
    }
    write.commit().unwrap();

    let entries = untyped_entries(&db, "strings");
    let pairs: Vec<(String, String)> = entries
        .into_iter()
        .map(|(key, value)| {
            (
                String::from_utf8(key).unwrap(),
                String::from_utf8(value).unwrap(),
            )
        })
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("alpha".to_string(), "1".to_string()),
            ("beta".to_string(), "2".to_string()),
            ("gamma".to_string(), "3".to_string()),
        ]
    );
}

#[test]
fn matches_typed_iterator_across_many_pages() {
    let (_dir, db) = make_db();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(STRINGS).unwrap();
        for i in 0..2_000u32 {
            let key = format!("key-{i:05}");
            let value = format!("value-{i:05}");
            table.insert(key.as_str(), value.as_str()).unwrap();
        }
    }
    write.commit().unwrap();

    let read = db.begin_read().unwrap();
    let typed: Vec<(String, String)> = {
        let table = read.open_table(STRINGS).unwrap();
        assert!(
            table.stats().unwrap().leaf_pages() > 1,
            "test needs a multi-leaf table"
        );
        table
            .iter()
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.value().to_string(), value.value().to_string())
            })
            .collect()
    };
    drop(read);

    let untyped: Vec<(String, String)> = untyped_entries(&db, "strings")
        .into_iter()
        .map(|(key, value)| {
            (
                String::from_utf8(key).unwrap(),
                String::from_utf8(value).unwrap(),
            )
        })
        .collect();

    assert_eq!(untyped, typed);
}

#[test]
fn decodes_fixed_width_entries() {
    let (_dir, db) = make_db();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(NUMS).unwrap();
        table.insert(2u64, 20u64).unwrap();
        table.insert(1u64, 10u64).unwrap();
    }
    write.commit().unwrap();

    let entries = untyped_entries(&db, "nums");
    assert_eq!(entries.len(), 2);
    assert_eq!(
        u64::from_le_bytes(entries[0].0.clone().try_into().unwrap()),
        1
    );
    assert_eq!(
        u64::from_le_bytes(entries[0].1.clone().try_into().unwrap()),
        10
    );
    assert_eq!(
        u64::from_le_bytes(entries[1].0.clone().try_into().unwrap()),
        2
    );
    assert_eq!(
        u64::from_le_bytes(entries[1].1.clone().try_into().unwrap()),
        20
    );
}

#[test]
fn tuple_values_are_reachable_without_typed_dispatch() {
    let (_dir, db) = make_db();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(WIDE).unwrap();
        table
            .insert(7u32, (10u128, 20u128, 30u128, 40u128))
            .unwrap();
    }
    write.commit().unwrap();

    let entries = untyped_entries(&db, "wide");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, 7u32.to_le_bytes().to_vec());
    assert_eq!(entries[0].1.len(), 4 * 16);

    let mut decoded = Vec::new();
    for chunk in entries[0].1.chunks_exact(16) {
        decoded.push(u128::from_le_bytes(chunk.try_into().unwrap()));
    }
    assert_eq!(decoded, vec![10, 20, 30, 40]);
}

#[test]
fn empty_table_yields_nothing() {
    let (_dir, db) = make_db();
    let write = db.begin_write().unwrap();
    {
        write.open_table(STRINGS).unwrap();
    }
    write.commit().unwrap();

    assert!(untyped_entries(&db, "strings").is_empty());
}

#[test]
fn reflects_updates_and_removals() {
    let (_dir, db) = make_db();
    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(STRINGS).unwrap();
        table.insert("a", "1").unwrap();
        table.insert("b", "2").unwrap();
        table.insert("c", "3").unwrap();
    }
    write.commit().unwrap();

    let write = db.begin_write().unwrap();
    {
        let mut table = write.open_table(STRINGS).unwrap();
        table.remove("b").unwrap();
        table.insert("a", "updated").unwrap();
    }
    write.commit().unwrap();

    let pairs: Vec<(String, String)> = untyped_entries(&db, "strings")
        .into_iter()
        .map(|(key, value)| {
            (
                String::from_utf8(key).unwrap(),
                String::from_utf8(value).unwrap(),
            )
        })
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("a".to_string(), "updated".to_string()),
            ("c".to_string(), "3".to_string()),
        ]
    );
}
