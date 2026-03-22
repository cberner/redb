use shodh_redb::{
    CdcConfig, ChangeOp, Database, MultimapTableDefinition, ReadableDatabase, TableDefinition,
};

const TABLE_A: TableDefinition<&str, u64> = TableDefinition::new("table_a");
const TABLE_B: TableDefinition<&str, &str> = TableDefinition::new("table_b");
const MULTIMAP: MultimapTableDefinition<&str, u64> = MultimapTableDefinition::new("mmap");

fn create_cdc_db() -> (tempfile::NamedTempFile, Database) {
    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let db = Database::builder()
        .set_cdc(CdcConfig {
            enabled: true,
            retention_max_txns: 0,
        })
        .create(tmpfile.path())
        .unwrap();
    (tmpfile, db)
}

#[test]
fn cdc_disabled_by_default() {
    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE_A).unwrap();
        table.insert("key1", &100u64).unwrap();
    }
    txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let changes = read_txn.read_cdc_since(0).unwrap();
    assert!(changes.is_empty(), "CDC should produce no events when disabled");
}

#[test]
fn cdc_insert_single() {
    let (_tmp, db) = create_cdc_db();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE_A).unwrap();
        table.insert("hello", &42u64).unwrap();
    }
    txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let changes = read_txn.read_cdc_since(0).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].op, ChangeOp::Insert);
    assert_eq!(changes[0].table_name, "table_a");
    assert_eq!(changes[0].key, b"hello");
    assert!(changes[0].new_value.is_some());
    assert!(changes[0].old_value.is_none());
}

#[test]
fn cdc_update() {
    let (_tmp, db) = create_cdc_db();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE_A).unwrap();
        table.insert("key", &1u64).unwrap();
    }
    txn.commit().unwrap();

    let txn2 = db.begin_write().unwrap();
    {
        let mut table = txn2.open_table(TABLE_A).unwrap();
        table.insert("key", &2u64).unwrap();
    }
    txn2.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let changes = read_txn.read_cdc_since(0).unwrap();
    // First txn: Insert; Second txn: Update
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].op, ChangeOp::Insert);
    assert_eq!(changes[1].op, ChangeOp::Update);
    assert_eq!(changes[1].table_name, "table_a");
    // Old value should be the original value (1u64 as LE bytes)
    assert!(changes[1].old_value.is_some());
    let old_bytes = changes[1].old_value.as_ref().unwrap();
    assert_eq!(u64::from_le_bytes(old_bytes[..8].try_into().unwrap()), 1);
    // New value should be 2u64
    let new_bytes = changes[1].new_value.as_ref().unwrap();
    assert_eq!(u64::from_le_bytes(new_bytes[..8].try_into().unwrap()), 2);
}

#[test]
fn cdc_delete() {
    let (_tmp, db) = create_cdc_db();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE_A).unwrap();
        table.insert("bye", &99u64).unwrap();
    }
    txn.commit().unwrap();

    let txn2 = db.begin_write().unwrap();
    {
        let mut table = txn2.open_table(TABLE_A).unwrap();
        table.remove("bye").unwrap();
    }
    txn2.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let changes = read_txn.read_cdc_since(0).unwrap();
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[1].op, ChangeOp::Delete);
    assert!(changes[1].new_value.is_none());
    assert!(changes[1].old_value.is_some());
    let old_bytes = changes[1].old_value.as_ref().unwrap();
    assert_eq!(u64::from_le_bytes(old_bytes[..8].try_into().unwrap()), 99);
}

#[test]
fn cdc_multiple_tables() {
    let (_tmp, db) = create_cdc_db();

    let txn = db.begin_write().unwrap();
    {
        let mut a = txn.open_table(TABLE_A).unwrap();
        a.insert("a_key", &10u64).unwrap();
    }
    {
        let mut b = txn.open_table(TABLE_B).unwrap();
        b.insert("b_key", "b_val").unwrap();
    }
    txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let changes = read_txn.read_cdc_since(0).unwrap();
    assert_eq!(changes.len(), 2);
    let table_names: Vec<&str> = changes.iter().map(|c| c.table_name.as_str()).collect();
    assert!(table_names.contains(&"table_a"));
    assert!(table_names.contains(&"table_b"));
    // All from the same transaction
    assert_eq!(changes[0].transaction_id, changes[1].transaction_id);
}

#[test]
fn cdc_transaction_ordering() {
    let (_tmp, db) = create_cdc_db();

    // Transaction 1
    let txn1 = db.begin_write().unwrap();
    {
        let mut table = txn1.open_table(TABLE_A).unwrap();
        table.insert("first", &1u64).unwrap();
    }
    txn1.commit().unwrap();

    // Transaction 2
    let txn2 = db.begin_write().unwrap();
    {
        let mut table = txn2.open_table(TABLE_A).unwrap();
        table.insert("second", &2u64).unwrap();
        table.insert("third", &3u64).unwrap();
    }
    txn2.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let changes = read_txn.read_cdc_since(0).unwrap();
    assert_eq!(changes.len(), 3);

    // First change from txn1, next two from txn2
    let txn1_id = changes[0].transaction_id;
    let txn2_id = changes[1].transaction_id;
    assert!(txn2_id > txn1_id);
    assert_eq!(changes[2].transaction_id, txn2_id);

    // Sequence within txn2
    assert_eq!(changes[1].sequence, 0);
    assert_eq!(changes[2].sequence, 1);
}

#[test]
fn cdc_read_since() {
    let (_tmp, db) = create_cdc_db();

    let txn1 = db.begin_write().unwrap();
    {
        let mut table = txn1.open_table(TABLE_A).unwrap();
        table.insert("old", &1u64).unwrap();
    }
    txn1.commit().unwrap();

    // Get txn1's id
    let read_txn = db.begin_read().unwrap();
    let all = read_txn.read_cdc_since(0).unwrap();
    let txn1_id = all[0].transaction_id;
    drop(read_txn);

    let txn2 = db.begin_write().unwrap();
    {
        let mut table = txn2.open_table(TABLE_A).unwrap();
        table.insert("new", &2u64).unwrap();
    }
    txn2.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let since = read_txn.read_cdc_since(txn1_id).unwrap();
    assert_eq!(since.len(), 1);
    assert_eq!(since[0].key, b"new");
}

#[test]
fn cdc_read_range() {
    let (_tmp, db) = create_cdc_db();

    // Create 3 transactions
    let mut txn_ids = Vec::new();
    for i in 0u64..3 {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE_A).unwrap();
            table.insert(format!("k{i}").as_str(), &i).unwrap();
        }
        txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let latest = read_txn.latest_cdc_transaction_id().unwrap().unwrap();
        txn_ids.push(latest);
        drop(read_txn);
    }

    let read_txn = db.begin_read().unwrap();
    // Only get changes from txn 1 and 2 (indices 1..2)
    let range = read_txn.read_cdc_range(txn_ids[1], txn_ids[1]).unwrap();
    assert_eq!(range.len(), 1);
    assert_eq!(range[0].key, b"k1");
}

#[test]
fn cdc_cursor_advance() {
    let (_tmp, db) = create_cdc_db();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE_A).unwrap();
        table.insert("x", &1u64).unwrap();
    }
    txn.commit().unwrap();

    // Read cursor before setting
    let read_txn = db.begin_read().unwrap();
    assert!(read_txn.cdc_cursor("consumer-1").unwrap().is_none());
    let latest_id = read_txn.latest_cdc_transaction_id().unwrap().unwrap();
    drop(read_txn);

    // Set cursor
    let txn2 = db.begin_write().unwrap();
    txn2.advance_cdc_cursor("consumer-1", latest_id).unwrap();
    txn2.commit().unwrap();

    // Verify cursor is persisted
    let read_txn = db.begin_read().unwrap();
    let cursor = read_txn.cdc_cursor("consumer-1").unwrap();
    assert_eq!(cursor, Some(latest_id));
}

#[test]
fn cdc_drain_events() {
    let (_tmp, db) = create_cdc_db();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE_A).unwrap();
        table.insert("a", &1u64).unwrap();
        table.insert("b", &2u64).unwrap();
        table.insert("c", &3u64).unwrap();
    }
    txn.commit().unwrap();

    let txn2 = db.begin_write().unwrap();
    {
        let mut table = txn2.open_table(TABLE_A).unwrap();
        let drained = table.drain::<&str>(..).unwrap();
        assert_eq!(drained, 3);
    }
    txn2.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let changes = read_txn.read_cdc_since(0).unwrap();
    // 3 inserts + 3 deletes
    assert_eq!(changes.len(), 6);
    let deletes: Vec<_> = changes.iter().filter(|c| c.op == ChangeOp::Delete).collect();
    assert_eq!(deletes.len(), 3);
    for d in &deletes {
        assert!(d.old_value.is_some());
        assert!(d.new_value.is_none());
    }
}

#[test]
fn cdc_multimap() {
    let (_tmp, db) = create_cdc_db();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_multimap_table(MULTIMAP).unwrap();
        table.insert("key", &10u64).unwrap();
        table.insert("key", &20u64).unwrap();
    }
    txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let changes = read_txn.read_cdc_since(0).unwrap();
    // Two inserts (multimap allows multiple values per key)
    assert_eq!(changes.len(), 2);
    assert!(changes.iter().all(|c| c.op == ChangeOp::Insert));

    // Verify the values were actually inserted
    let table = read_txn.open_multimap_table(MULTIMAP).unwrap();
    let vals: Vec<u64> = table
        .get("key")
        .unwrap()
        .map(|r| r.unwrap().value())
        .collect();
    assert_eq!(vals.len(), 2);
    drop(read_txn);

    // Remove one value
    let txn2 = db.begin_write().unwrap();
    {
        let mut table = txn2.open_multimap_table(MULTIMAP).unwrap();
        assert!(table.remove("key", &10u64).unwrap());
    }
    txn2.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let changes = read_txn.read_cdc_since(0).unwrap();
    assert_eq!(changes.len(), 3); // 2 inserts + 1 delete
    assert_eq!(changes[2].op, ChangeOp::Delete);
}

#[test]
fn cdc_retention_pruning() {
    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let db = Database::builder()
        .set_cdc(CdcConfig {
            enabled: true,
            retention_max_txns: 2,
        })
        .create(tmpfile.path())
        .unwrap();

    // Create 5 transactions
    for i in 0u64..5 {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE_A).unwrap();
            table.insert(format!("k{i}").as_str(), &i).unwrap();
        }
        txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let all = read_txn.read_cdc_since(0).unwrap();
    // With retention of 2 transactions, older entries should have been pruned.
    // Only the last ~2 transactions' events should remain.
    assert!(
        all.len() <= 3,
        "Expected at most 3 events with retention_max_txns=2, got {}",
        all.len()
    );
    assert!(!all.is_empty());
}
