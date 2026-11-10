use redb::{
    Database, MultimapTableDefinition, ReadableDatabase, ReadableMultimapTable,
    ReadableTableMetadata, TableError, TransactionError,
};

const STR_TABLE: MultimapTableDefinition<&str, &str> = MultimapTableDefinition::new("str_to_str");
const SLICE_U64_TABLE: MultimapTableDefinition<&[u8], u64> =
    MultimapTableDefinition::new("slice_to_u64");
const U64_TABLE: MultimapTableDefinition<u64, u64> = MultimapTableDefinition::new("u64");
const U64_SLICE_TABLE: MultimapTableDefinition<u64, &[u8]> =
    MultimapTableDefinition::new("u64_to_slice");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

fn get_vec(
    table: &impl ReadableMultimapTable<&'static str, &'static str>,
    key: &str,
) -> Vec<String> {
    let mut result = vec![];
    let mut iter = table.get(key).unwrap();
    loop {
        let item = iter.next();
        if let Some(item_value) = item {
            result.push(item_value.unwrap().value().to_string());
        } else {
            return result;
        }
    }
}

#[test]
fn len() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
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
    let untyped_table = read_txn.open_untyped_multimap_table(STR_TABLE).unwrap();
    assert_eq!(untyped_table.len().unwrap(), 3);
}

#[test]
fn is_empty() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

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
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
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
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(SLICE_U64_TABLE).unwrap();
        for i in 0..5 {
            table.insert(b"0".as_slice(), &i).unwrap();
        }
        for i in 5..10 {
            table.insert(b"1".as_slice(), &i).unwrap();
        }
        for i in 10..15 {
            table.insert(b"2".as_slice(), &i).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(SLICE_U64_TABLE).unwrap();
    let start = b"0".as_ref();
    let end = b"1".as_ref();
    let mut iter = table.range(start..=end).unwrap();

    {
        let (key, mut values) = iter.next().unwrap().unwrap();
        for i in 0..5 {
            assert_eq!(b"0", key.value());
            let value = values.next().unwrap().unwrap();
            assert_eq!(i, value.value());
        }
    }
    {
        let (key, mut values) = iter.next().unwrap().unwrap();
        for i in 5..10 {
            assert_eq!(b"1", key.value());
            let value = values.next().unwrap().unwrap();
            assert_eq!(i, value.value());
        }
    }
    assert!(iter.next().is_none());

    let mut total: u64 = 0;
    for item in table.range(start..=end).unwrap() {
        let (_, values) = item.unwrap();
        total += values.map(|x| x.unwrap().value()).sum::<u64>();
    }
    assert_eq!(total, 45);
}

#[test]
fn range_lifetime() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: MultimapTableDefinition<&str, &str> = MultimapTableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(definition).unwrap();
        table.insert("hello", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(definition).unwrap();

    let mut iter = {
        let start = "hello".to_string();
        table.range(start.as_str()..).unwrap()
    };
    assert_eq!(
        iter.next()
            .unwrap()
            .unwrap()
            .1
            .next()
            .unwrap()
            .unwrap()
            .value(),
        "world"
    );
    assert!(iter.next().is_none());
}

#[test]
fn range_arc_lifetime() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: MultimapTableDefinition<&str, &str> = MultimapTableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(definition).unwrap();
        table.insert("hello", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let mut iter = {
        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_multimap_table(definition).unwrap();
        let start = "hello".to_string();
        // The 'static range() does not keep the transaction alive, so experimental-api-5 drops it
        // in favour of range_owned()
        #[cfg(feature = "experimental-api-5")]
        let iter = table.range_owned(start.as_str()..).unwrap();
        #[cfg(not(feature = "experimental-api-5"))]
        let iter = table.range(start.as_str()..).unwrap();
        iter
    };
    assert_eq!(
        iter.next()
            .unwrap()
            .unwrap()
            .1
            .next()
            .unwrap()
            .unwrap()
            .value(),
        "world"
    );
    assert!(iter.next().is_none());
}

#[test]
fn get_arc_lifetime() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: MultimapTableDefinition<&str, &str> = MultimapTableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(definition).unwrap();
        table.insert("hello", "world").unwrap();
    }
    write_txn.commit().unwrap();

    let mut iter = {
        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_multimap_table(definition).unwrap();
        let start = "hello".to_string();
        // The 'static get() does not keep the transaction alive, so experimental-api-5 drops it
        // in favour of get_owned()
        #[cfg(feature = "experimental-api-5")]
        let iter = table.get_owned(start.as_str()).unwrap();
        #[cfg(not(feature = "experimental-api-5"))]
        let iter = table.get(start.as_str()).unwrap();
        iter
    };
    assert_eq!(iter.next().unwrap().unwrap().value(), "world");
    assert!(iter.next().is_none());
}

#[test]
fn delete() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
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
    assert_eq!(3, table.get("hello").unwrap().len());
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
        assert_eq!("world", iter.next().unwrap().unwrap().value());
        assert_eq!("world3", iter.next().unwrap().unwrap().value());
        assert!(iter.next().is_none());
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(STR_TABLE).unwrap();
    assert!(table.is_empty().unwrap());
    let empty: Vec<String> = vec![];
    assert_eq!(empty, get_vec(&table, "hello"));
}

// Regression test: remove_all on a key whose values live in an uncommitted subtree must
// return guards that remain valid after the iterator is dropped (and therefore after the
// subtree pages have been freed). Previously the subtree iteration handed out AccessGuards
// that held references to the page bytes; dropping the iterator then freed those pages
// via free_if_uncommitted while the collected guards were still outstanding, tripping
// the read_page_ref_counts debug assertion.
#[test]
fn remove_all_uncommitted_subtree_collect() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(U64_TABLE).unwrap();
        // Enough distinct values that the collection is promoted to a subtree of
        // uncommitted pages before we remove_all in the same transaction.
        for v in 0..2000u64 {
            table.insert(&0u64, &v).unwrap();
        }
        let iter = table.remove_all(&0u64).unwrap();
        let values: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(values.len(), 2000);
        for (i, guard) in values.iter().enumerate() {
            assert_eq!(guard.value(), i as u64);
        }
    }
    write_txn.commit().unwrap();
}

#[test]
fn wrong_types() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let definition: MultimapTableDefinition<u32, u32> = MultimapTableDefinition::new("x");
    let wrong_definition: MultimapTableDefinition<u64, u64> = MultimapTableDefinition::new("x");

    let txn = db.begin_write().unwrap();
    txn.open_multimap_table(definition).unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    assert!(matches!(
        txn.open_multimap_table(wrong_definition),
        Err(TableError::TableTypeMismatch { .. })
    ));
    txn.abort().unwrap();

    let txn = db.begin_read().unwrap();
    txn.open_multimap_table(definition).unwrap();
    assert!(matches!(
        txn.open_multimap_table(wrong_definition),
        Err(TableError::TableTypeMismatch { .. })
    ));

    let message = txn
        .open_multimap_table(wrong_definition)
        .err()
        .unwrap()
        .to_string();
    #[cfg(feature = "experimental-api-5")]
    let expected = "x has key type u32 and value type u32, but was opened with key type u64 and \
                    value type u64";
    #[cfg(not(feature = "experimental-api-5"))]
    let expected = "x has key type u32 and value type u32";
    assert_eq!(message, expected);
}

#[test]
fn efficient_storage() {
    let tmpfile = create_tempfile();
    let expected_max_size = 1024 * 1024;
    // Write enough values that big_key.len() * entries > db_size to check that duplicate key data is not stored
    // and entries * sizeof(u32) > page_size to validate that large numbers of values can be stored per key
    let entries = 10000;
    let db = Database::create(tmpfile.path()).unwrap();
    let table_def: MultimapTableDefinition<&[u8], u32> = MultimapTableDefinition::new("x");
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(table_def).unwrap();
        let big_key = [0u8; 1000];
        for i in 0..entries {
            table.insert(big_key.as_slice(), &i).unwrap();
        }
    }
    assert!(write_txn.stats().unwrap().stored_bytes() <= expected_max_size);
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(table_def).unwrap();
    assert_eq!(table.len().unwrap(), entries as u64);
}

#[test]
fn reopen_table() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
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
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
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
        let (k, mut values) = iter.next().unwrap().unwrap();
        assert_eq!(k.value(), i);
        for j in 0..10 {
            assert_eq!(values.next().unwrap().unwrap().value(), j);
        }
    }
}

#[test]
fn multimap_signature_lifetimes() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let def: MultimapTableDefinition<&str, u64> = MultimapTableDefinition::new("x");

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(def).unwrap();
        table.insert("bye", 0).unwrap();

        let _ = {
            let key = "hi".to_string();
            table.get(key.as_str()).unwrap()
        };

        let _ = {
            let key = "hi".to_string();
            table.range(key.as_str()..).unwrap()
        };

        let _ = {
            let key = "hi".to_string();
            table.remove_all(key.as_str()).unwrap()
        };
    }
    write_txn.commit().unwrap();
}

// Exercises DoubleEndedIterator::next_back() on MultimapValue when the values for a key
// are stored in a subtree (rather than inline).
#[test]
fn multimap_value_next_back_subtree() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(U64_TABLE).unwrap();
        // Enough distinct values to promote the collection from inline to a subtree.
        for v in 0..1000u64 {
            table.insert(&0u64, &v).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(U64_TABLE).unwrap();
    let mut iter = table.get(&0u64).unwrap();
    assert_eq!(iter.len(), 1000);
    for expected in (0..1000u64).rev() {
        let guard = iter.next_back().unwrap().unwrap();
        assert_eq!(guard.value(), expected);
    }
    assert!(iter.next_back().is_none());
    assert!(iter.is_empty());
}

#[test]
fn multimap_remove_subtree_backed_key() {
    // Exercises MultimapTable::remove() when the values for a key are stored in a B-tree
    // subtree (SubtreeV2), covers the missing-key and value-not-in-inline cases, and the
    // path where enough removals shrink the subtree back to inline storage.
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(U64_TABLE).unwrap();
        // Enough values for key 0 to be promoted from inline to a subtree.
        for v in 0..1000u64 {
            table.insert(&0u64, &v).unwrap();
        }
        // Key 1 stays inline (only 2 values).
        table.insert(&1u64, &100u64).unwrap();
        table.insert(&1u64, &101u64).unwrap();
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(U64_TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 1002);

        // Key does not exist; returns false without touching the tree.
        assert!(!table.remove(&99u64, &0u64).unwrap());

        // Key exists with an inline collection, but the value is absent.
        assert!(!table.remove(&1u64, &999u64).unwrap());

        // Key exists with a subtree-backed collection; remove a present value.
        assert!(table.remove(&0u64, &500u64).unwrap());
        assert_eq!(table.len().unwrap(), 1001);

        // Removing the same value again returns false (no longer in the subtree).
        assert!(!table.remove(&0u64, &500u64).unwrap());
        assert_eq!(table.len().unwrap(), 1001);
    }
    write_txn.commit().unwrap();

    // Confirm the removal persists and the subtree still has the remaining 999 values.
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(U64_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 1001);
    assert_eq!(table.get(&0u64).unwrap().len(), 999);

    // Remove almost all remaining values for key 0. The subtree shrinks until its single
    // remaining leaf is smaller than half a page, at which point the collection is rewritten
    // back to inline storage.
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(U64_TABLE).unwrap();
        for v in 0..999u64 {
            if v != 500 {
                table.remove(&0u64, &v).unwrap();
            }
        }
        assert_eq!(table.get(&0u64).unwrap().len(), 1);
        assert_eq!(table.len().unwrap(), 3);
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(U64_TABLE).unwrap();
    let mut iter = table.get(&0u64).unwrap();
    assert_eq!(iter.next().unwrap().unwrap().value(), 999);
    assert!(iter.next().is_none());
}

#[test]
fn multimap_remove_collapses_committed_subtree_to_inline() {
    // A subtree whose branch root collapses to a *committed* leaf drives
    // MultimapTable::remove()'s subtree->inline conversion down the path that frees a
    // committed page. Two small values plus one large value build a subtree with a branch
    // (small values in one leaf, the large value in another); after committing, removing the
    // large value collapses the branch and leaves the untouched committed small-value leaf as
    // the new root, which is below half a page and so is rewritten back to inline storage.
    let small_a = [0x00u8; 300];
    let small_b = [0x01u8; 300];
    // Sorts last (0xFF), so the split keeps both small values together in the first leaf.
    let large = [0xFFu8; 3700];

    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(U64_SLICE_TABLE).unwrap();
        table.insert(&0u64, small_a.as_slice()).unwrap();
        table.insert(&0u64, small_b.as_slice()).unwrap();
        table.insert(&0u64, large.as_slice()).unwrap();
    }
    write_txn.commit().unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(U64_SLICE_TABLE).unwrap();
        assert!(table.remove(&0u64, large.as_slice()).unwrap());
        let values: Vec<Vec<u8>> = table
            .get(&0u64)
            .unwrap()
            .map(|v| v.unwrap().value().to_vec())
            .collect();
        assert_eq!(values, vec![small_a.to_vec(), small_b.to_vec()]);
    }
    write_txn.commit().unwrap();

    // The two small values survive and the large value is gone after the collapse.
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(U64_SLICE_TABLE).unwrap();
    let values: Vec<Vec<u8>> = table
        .get(&0u64)
        .unwrap()
        .map(|v| v.unwrap().value().to_vec())
        .collect();
    assert_eq!(values, vec![small_a.to_vec(), small_b.to_vec()]);
}

#[test]
fn multimap_remove_nonexistent_value_from_subtree_does_not_churn() {
    // Removing a value that isn't present from a subtree-backed key is a logical no-op and must
    // not rewrite the parent tree. There was a bug where MultimapTable::remove() re-inserted the
    // unchanged collection into the parent tree unconditionally, copy-on-writing the root->leaf
    // path and queuing the old pages to be freed even though nothing changed.
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(U64_TABLE).unwrap();
        // Enough values for key 0 to be promoted from inline storage to a subtree.
        for v in 0..1000u64 {
            table.insert(&0u64, &v).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Finalize the cleanup of the freed pages from the setup so the baseline is at steady state.
    db.begin_write().unwrap().commit().unwrap();
    db.begin_write().unwrap().commit().unwrap();

    let txn = db.begin_write().unwrap();
    let baseline = txn.stats().unwrap().allocated_pages();
    txn.commit().unwrap();

    // Remove a value that was never inserted. This returns false and, with the fix, leaves the
    // tree entirely untouched. Without the fix the parent tree is copy-on-written, so the old
    // pages linger as not-yet-reclaimed frees and inflate the allocated page count.
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_multimap_table(U64_TABLE).unwrap();
        assert!(!table.remove(&0u64, &123_456u64).unwrap());
        assert_eq!(table.len().unwrap(), 1000);
    }
    write_txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let after = txn.stats().unwrap().allocated_pages();
    txn.commit().unwrap();
    assert_eq!(
        baseline, after,
        "removing a nonexistent value from a subtree-backed key must not allocate or free pages"
    );

    // The contents are unchanged and still fully readable.
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(U64_TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 1000);
    assert_eq!(table.get(&0u64).unwrap().len(), 1000);
}

fn overwrite_multimap_10_times(db: &Database) {
    for _ in 0..10 {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_multimap_table(U64_TABLE).unwrap();
            table.remove_all(&0u64).unwrap();
            // Enough values for key 0 to be promoted from inline storage to a subtree
            for v in 0..1000u64 {
                table.insert(&0u64, &v).unwrap();
            }
            table.remove_all(&1u64).unwrap();
            // Few enough values for key 1 to stay inline
            for v in 0..3u64 {
                table.insert(&1u64, &v).unwrap();
            }
        }
        write_txn.commit().unwrap();
    }
}

#[test]
fn read_only_get_value_guards_keep_transaction_alive() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    overwrite_multimap_10_times(&db);

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(U64_TABLE).unwrap();
    let mut subtree_values = table.get_owned(&0u64).unwrap();
    assert_eq!(subtree_values.len(), 1000);
    assert!(!subtree_values.is_empty());
    let subtree_value = subtree_values.next().unwrap().unwrap();
    let subtree_back_value = subtree_values.next_back().unwrap().unwrap();
    assert_eq!(subtree_values.len(), 998);
    let mut inline_values = table.get_owned(&1u64).unwrap();
    let inline_value = inline_values.next().unwrap().unwrap();

    // The yielded guards alone must keep the transaction registered
    drop(subtree_values);
    drop(inline_values);
    drop(table);
    assert!(matches!(
        read_txn.close(),
        Err(TransactionError::ReadTransactionStillInUse(_))
    ));

    // Concurrent writers must not reclaim the pages referenced by the guards, even though
    // every other object referencing the read transaction has been dropped
    overwrite_multimap_10_times(&db);

    assert_eq!(subtree_value.value(), 0);
    assert_eq!(subtree_back_value.value(), 999);
    assert_eq!(inline_value.value(), 0);
}

#[test]
fn read_only_range_entries_keep_transaction_alive() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    overwrite_multimap_10_times(&db);

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_multimap_table(U64_TABLE).unwrap();
    let mut range = table.range_owned(0u64..).unwrap();
    let (key, mut values) = range.next().unwrap().unwrap();
    let value = values.next().unwrap().unwrap();
    let (back_key, mut back_values) = range.next_back().unwrap().unwrap();
    let back_value = back_values.next_back().unwrap().unwrap();

    // The yielded guards alone must keep the transaction registered
    drop(values);
    drop(back_values);
    drop(range);
    drop(table);
    assert!(matches!(
        read_txn.close(),
        Err(TransactionError::ReadTransactionStillInUse(_))
    ));

    // Concurrent writers must not reclaim the pages referenced by the guards, even though
    // every other object referencing the read transaction has been dropped
    overwrite_multimap_10_times(&db);

    assert_eq!(key.value(), 0);
    assert_eq!(value.value(), 0);
    assert_eq!(back_key.value(), 1);
    assert_eq!(back_value.value(), 2);
}

// insert() and remove() update a key's values before the entry that names them, so a panic in
// between, from the key type, must not leave a transaction that can commit
#[test]
#[cfg(not(target_os = "wasi"))]
fn a_panic_in_insert_or_remove_poisons_the_transaction() {
    use redb::{CommitError, Key, MultimapTable, TypeName, Value};
    use std::cmp::Ordering;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};

    static COMPARES: AtomicU64 = AtomicU64::new(0);
    static PANIC_AT: AtomicI64 = AtomicI64::new(-1);

    // A u64 whose comparison panics on the chosen call
    #[derive(Debug)]
    struct PanickingKey;

    impl Value for PanickingKey {
        type SelfType<'a> = u64;
        type AsBytes<'a> = [u8; 8];

        fn fixed_width() -> Option<usize> {
            Some(8)
        }

        fn from_bytes<'a>(data: &'a [u8]) -> u64
        where
            Self: 'a,
        {
            u64::from_le_bytes(data.try_into().unwrap())
        }

        fn as_bytes<'a, 'b: 'a>(value: &'a u64) -> [u8; 8]
        where
            Self: 'b,
        {
            value.to_le_bytes()
        }

        fn type_name() -> TypeName {
            TypeName::new("PanickingKey")
        }
    }

    impl Key for PanickingKey {
        fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
            let call = COMPARES.fetch_add(1, AtomicOrdering::Relaxed);
            assert_ne!(
                PANIC_AT.load(AtomicOrdering::Relaxed),
                i64::try_from(call).unwrap(),
                "compare panic"
            );
            Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
        }
    }

    const TABLE: MultimapTableDefinition<PanickingKey, u64> = MultimapTableDefinition::new("x");
    // Enough values for the key to keep them in a subtree
    const VALUES: u64 = 2000;

    for remove in [false, true] {
        let operation = |table: &mut MultimapTable<PanickingKey, u64>| {
            if remove {
                table.remove(7, 1000).unwrap();
            } else {
                table.insert(7, 1001).unwrap();
            }
        };

        let tmpfile = create_tempfile();
        let mut db = Database::create(tmpfile.path()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_multimap_table(TABLE).unwrap();
            for value in 0..VALUES {
                table.insert(7, value * 2).unwrap();
            }
        }
        write_txn.commit().unwrap();

        // Count the operation's comparisons, so that its last one can panic
        let write_txn = db.begin_write().unwrap();
        let comparisons = {
            let mut table = write_txn.open_multimap_table(TABLE).unwrap();
            COMPARES.store(0, AtomicOrdering::Relaxed);
            operation(&mut table);
            COMPARES.load(AtomicOrdering::Relaxed)
        };
        write_txn.abort().unwrap();

        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_multimap_table(TABLE).unwrap();
            COMPARES.store(0, AtomicOrdering::Relaxed);
            PANIC_AT.store(
                i64::try_from(comparisons).unwrap() - 1,
                AtomicOrdering::Relaxed,
            );
            let result = catch_unwind(AssertUnwindSafe(|| operation(&mut table)));
            PANIC_AT.store(-1, AtomicOrdering::Relaxed);
            assert!(result.is_err());
        }
        assert!(matches!(
            write_txn.commit(),
            Err(CommitError::TransactionPoisoned)
        ));

        {
            let read_txn = db.begin_read().unwrap();
            let table = read_txn.open_multimap_table(TABLE).unwrap();
            let values = table.get(7).unwrap();
            assert_eq!(values.len(), VALUES);
            assert_eq!(values.count(), usize::try_from(VALUES).unwrap());
        }
        assert!(db.check_integrity().unwrap());
    }
}
