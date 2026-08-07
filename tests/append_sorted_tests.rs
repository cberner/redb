use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, StorageError, TableDefinition,
};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

fn key(i: u64) -> String {
    format!("{i:016}")
}

// Spans a root that is still a leaf, and enough pairs to grow the tree several
// levels. These stay inside one buffer; splicing is covered separately.
const COUNTS: [u64; 5] = [1, 2, 100, 5_000, 50_000];

#[test]
fn append_sorted_reads_back() {
    for count in COUNTS {
        let tmpfile = create_tempfile();
        let mut db = Database::create(tmpfile.path()).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            let owned: Vec<String> = (0..count).map(key).collect();
            let pairs = owned.iter().map(|k| (k.as_bytes(), b"v".as_slice()));
            table.append_sorted(pairs).unwrap();
        }
        txn.commit().unwrap();

        let txn = db.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        assert_eq!(table.len().unwrap(), count, "count={count}");
        let mut expected = 0u64;
        for entry in table.iter().unwrap() {
            let (k, v) = entry.unwrap();
            assert_eq!(k.value(), key(expected).as_bytes(), "count={count}");
            assert_eq!(v.value(), b"v");
            expected += 1;
        }
        assert_eq!(expected, count, "count={count}");
        drop(table);
        drop(txn);
        db.check_integrity().unwrap();
    }
}

#[test]
fn append_sorted_extends_a_populated_table() {
    for (existing, appended) in [(1u64, 1u64), (5, 5_000), (20_000, 20_000)] {
        let tmpfile = create_tempfile();
        let mut db = Database::create(tmpfile.path()).unwrap();
        let label = format!("{existing}+{appended}");

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for i in 0..existing {
                table.insert(key(i).as_bytes(), b"v".as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            let owned: Vec<String> = (existing..existing + appended).map(key).collect();
            let pairs = owned.iter().map(|k| (k.as_bytes(), b"v".as_slice()));
            table.append_sorted(pairs).unwrap();
        }
        txn.commit().unwrap();

        let txn = db.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        let total = existing + appended;
        assert_eq!(table.len().unwrap(), total, "{label}");
        let mut expected = 0u64;
        for entry in table.iter().unwrap() {
            let (k, v) = entry.unwrap();
            assert_eq!(k.value(), key(expected).as_bytes(), "{label}");
            assert_eq!(v.value(), b"v", "{label}");
            expected += 1;
        }
        assert_eq!(expected, total, "{label}");
        drop(table);
        drop(txn);
        db.check_integrity().unwrap();
    }
}

#[test]
fn append_sorted_accepts_an_empty_stream() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        table
            .append_sorted(std::iter::empty::<(&[u8], &[u8])>())
            .unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    assert_eq!(txn.open_table(TABLE).unwrap().len().unwrap(), 0);
    drop(txn);
    db.check_integrity().unwrap();
}

// Out of order within one call, at a batch size that stays inside the buffer and
// at one that spans several splices.
#[test]
fn append_sorted_rejects_keys_that_do_not_ascend() {
    for (label, keys) in [
        ("descending", vec![key(5), key(3)]),
        ("duplicate", vec![key(5), key(5)]),
        ("dip after a run", {
            let mut keys: Vec<String> = (0..20_000).map(key).collect();
            keys.push(key(1));
            keys
        }),
    ] {
        let tmpfile = create_tempfile();
        let mut db = Database::create(tmpfile.path()).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            let pairs = keys.iter().map(|k| (k.as_bytes(), b"v".as_slice()));
            let error = table.append_sorted(pairs).unwrap_err();
            assert!(matches!(error, StorageError::KeysNotAscending), "{label}");
            assert!(error.to_string().contains("strictly greater"), "{label}");
            let converted: redb::Error = error.into();
            assert!(
                matches!(converted, redb::Error::KeysNotAscending),
                "{label}"
            );
            assert!(
                converted.to_string().contains("strictly greater"),
                "{label}"
            );
        }
        drop(txn);
        db.check_integrity().unwrap();
    }
}

// The first key of a call is checked against what the table already holds, not
// just against the keys the call has seen.
#[test]
fn append_sorted_rejects_keys_below_an_existing_table() {
    for offset in [0u64, 1, 500] {
        let tmpfile = create_tempfile();
        let mut db = Database::create(tmpfile.path()).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for i in 0..1_000u64 {
                table.insert(key(i).as_bytes(), b"v".as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            // 999 is the highest key present, so none of these are an append.
            let clashing = [key(999 - offset)];
            let pairs = clashing.iter().map(|k| (k.as_bytes(), b"v".as_slice()));
            assert!(table.append_sorted(pairs).is_err(), "offset={offset}");
        }
        drop(txn);
        db.check_integrity().unwrap();
    }
}

// Values large enough that the append buffer fills several times, which is the
// only way to reach the splice path partway through a stream rather than once
// at the end.
#[test]
fn append_sorted_splices_partway_through() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let value = vec![0xABu8; 512];
    let count = 8_000u64;

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        let owned: Vec<String> = (0..count).map(key).collect();
        table
            .append_sorted(owned.iter().map(|k| (k.as_bytes(), value.as_slice())))
            .unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), count);
    let mut expected = 0u64;
    for entry in table.iter().unwrap() {
        let (k, v) = entry.unwrap();
        assert_eq!(k.value(), key(expected).as_bytes());
        assert_eq!(v.value(), value.as_slice());
        expected += 1;
    }
    assert_eq!(expected, count);

    // Splicing hands the parent branch a batch of leaves at a time, so it has to
    // split like any other branch. Left to grow instead it stays one page deep,
    // and eventually exceeds the u16 key count a branch can store.
    let stats = table.stats().unwrap();
    assert!(stats.tree_height() >= 3, "{stats:?}");
    assert!(stats.branch_pages() > 1, "{stats:?}");

    drop(table);
    drop(txn);
    db.check_integrity().unwrap();
}

// A leaf holding one value larger than a page must not be pulled through the
// append buffer, which would copy the whole value to add one pair after it.
#[test]
fn append_sorted_past_a_large_value() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let large = vec![0xCDu8; 8 * 1024 * 1024];

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        table.insert(key(0).as_bytes(), large.as_slice()).unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        let owned: Vec<String> = (1..500u64).map(key).collect();
        table
            .append_sorted(owned.iter().map(|k| (k.as_bytes(), b"v".as_slice())))
            .unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 500);
    assert_eq!(
        table.get(key(0).as_bytes()).unwrap().unwrap().value(),
        large.as_slice()
    );
    assert_eq!(
        table.get(key(499).as_bytes()).unwrap().unwrap().value(),
        b"v"
    );
    drop(table);
    drop(txn);
    db.check_integrity().unwrap();
}
