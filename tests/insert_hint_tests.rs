use redb::{
    Database, InsertHint, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

fn create_tempfile() -> tempfile::NamedTempFile {
    tempfile::NamedTempFile::new().unwrap()
}

// Zero padded so that lexicographic order matches numeric order
fn key(i: u64) -> String {
    format!("{i:016}")
}

fn load(hint: Option<InsertHint>, keys: &[u64]) -> (u64, u64) {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let value = vec![0u8; 150];

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for i in keys {
            match hint {
                Some(hint) => table
                    .insert_with_hint(key(*i).as_bytes(), value.as_slice(), hint)
                    .unwrap(),
                None => table.insert(key(*i).as_bytes(), value.as_slice()).unwrap(),
            };
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(TABLE).unwrap();
    let stats = table.stats().unwrap();
    (table.len().unwrap(), stats.leaf_pages())
}

#[test]
fn append_hint_packs_fewer_leaves() {
    let ascending: Vec<u64> = (0..20_000).collect();
    let (plain_len, plain_pages) = load(None, &ascending);
    let (hinted_len, hinted_pages) = load(Some(InsertHint::Append), &ascending);

    assert_eq!(plain_len, hinted_len);
    assert!(
        hinted_pages < plain_pages,
        "{hinted_pages} should be fewer than {plain_pages}"
    );
}

#[test]
fn append_hint_does_not_change_stored_data() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for i in 0..5_000u64 {
            table
                .insert_with_hint(key(i).as_bytes(), b"v".as_slice(), InsertHint::Append)
                .unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 5_000);
    let mut expected = 0u64;
    for entry in table.iter().unwrap() {
        let (k, v) = entry.unwrap();
        assert_eq!(k.value(), key(expected).as_bytes());
        assert_eq!(v.value(), b"v");
        expected += 1;
    }
    assert_eq!(expected, 5_000);
}

// The hint is advisory: a caller may be wrong about the order without corrupting anything.
#[test]
fn a_wrong_append_hint_is_harmless() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();

    let mut descending: Vec<u64> = (0..5_000).collect();
    descending.reverse();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for i in &descending {
            table
                .insert_with_hint(key(*i).as_bytes(), b"v".as_slice(), InsertHint::Append)
                .unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 5_000);
    for i in 0..5_000u64 {
        assert_eq!(table.get(key(i).as_bytes()).unwrap().unwrap().value(), b"v");
    }
    drop(table);
    drop(txn);
    db.check_integrity().unwrap();
}

#[test]
fn append_hint_replaces_an_existing_key() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        table.insert(key(1).as_bytes(), b"old".as_slice()).unwrap();
        let old = table
            .insert_with_hint(key(1).as_bytes(), b"new".as_slice(), InsertHint::Append)
            .unwrap();
        assert_eq!(old.unwrap().value(), b"old");
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 1);
    assert_eq!(
        table.get(key(1).as_bytes()).unwrap().unwrap().value(),
        b"new"
    );
}

#[test]
fn hinted_and_unhinted_inserts_interleave() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for i in 0..10_000u64 {
            if i % 3 == 0 {
                table.insert(key(i).as_bytes(), b"v".as_slice()).unwrap();
            } else {
                table
                    .insert_with_hint(key(i).as_bytes(), b"v".as_slice(), InsertHint::Append)
                    .unwrap();
            }
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 10_000);
    for i in 0..10_000u64 {
        assert!(table.get(key(i).as_bytes()).unwrap().is_some());
    }
    drop(table);
    drop(txn);
    db.check_integrity().unwrap();
}
