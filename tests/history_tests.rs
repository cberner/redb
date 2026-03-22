use shodh_redb::{Database, StorageError, TableDefinition, TransactionError};

const TABLE: TableDefinition<u64, &str> = TableDefinition::new("data");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

fn create_history_db(retention: u64) -> (tempfile::NamedTempFile, Database) {
    let tmpfile = create_tempfile();
    let db = Database::builder()
        .set_history_retention(retention)
        .create(tmpfile.path())
        .unwrap();
    (tmpfile, db)
}

#[test]
fn history_disabled_by_default() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        t.insert(1, "hello").unwrap();
    }
    txn.commit().unwrap();

    assert!(db.transaction_history().unwrap().is_empty());

    let result = db.begin_read_at(1);
    assert!(result.is_err());
    match result.unwrap_err() {
        TransactionError::Storage(StorageError::HistorySnapshotNotFound(_)) => {}
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn history_basic_time_travel() {
    let (_tmpfile, db) = create_history_db(10);

    // Write 5 transactions with different values
    let mut txn_ids = Vec::new();
    for i in 0u64..5 {
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            t.insert(1, format!("v{i}").as_str()).unwrap();
        }
        txn.commit().unwrap();

        let history = db.transaction_history().unwrap();
        txn_ids.push(history.last().unwrap().transaction_id);
    }

    // Read back at each historical transaction
    for (i, tid) in txn_ids.iter().enumerate() {
        let rtxn = db.begin_read_at(*tid).unwrap();
        let t = rtxn.open_table(TABLE).unwrap();
        let val = t.get(1).unwrap().unwrap();
        assert_eq!(val.value(), format!("v{i}").as_str());
    }
}

#[test]
fn history_retention_pruning() {
    let (_tmpfile, db) = create_history_db(3);

    // Write 10 transactions
    for i in 0u64..10 {
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            t.insert(1, format!("v{i}").as_str()).unwrap();
        }
        txn.commit().unwrap();
    }

    let history = db.transaction_history().unwrap();
    assert_eq!(history.len(), 3);

    // The oldest retained should be transaction 8 (0-indexed: v7, v8, v9)
    let rtxn = db.begin_read_at(history[0].transaction_id).unwrap();
    let t = rtxn.open_table(TABLE).unwrap();
    let val = t.get(1).unwrap().unwrap();
    assert_eq!(val.value(), "v7");
}

#[test]
fn history_page_safety() {
    // Retention must be large enough to keep the first snapshot through all writes
    let (_tmpfile, db) = create_history_db(50);

    // Create initial data
    let txn = db.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        t.insert(1, "original").unwrap();
        t.insert(2, "original").unwrap();
    }
    txn.commit().unwrap();

    let history = db.transaction_history().unwrap();
    let first_tid = history[0].transaction_id;

    // Overwrite and add more data to trigger page splits/frees
    for i in 0u64..20 {
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            t.insert(1, format!("updated-{i}").as_str()).unwrap();
            t.insert(i + 100, "padding").unwrap();
        }
        txn.commit().unwrap();
    }

    // The first snapshot should still be readable since pages are held
    let rtxn = db.begin_read_at(first_tid).unwrap();
    let t = rtxn.open_table(TABLE).unwrap();
    let val = t.get(1).unwrap().unwrap();
    assert_eq!(val.value(), "original");
    let val2 = t.get(2).unwrap().unwrap();
    assert_eq!(val2.value(), "original");
}

#[test]
fn history_timestamp_lookup() {
    let (_tmpfile, db) = create_history_db(10);

    let txn = db.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        t.insert(1, "first").unwrap();
    }
    txn.commit().unwrap();

    let history = db.transaction_history().unwrap();
    let ts = history[0].timestamp_ms;

    // Lookup by a timestamp >= the snapshot's timestamp should find it
    let rtxn = db.begin_read_at_time(ts).unwrap();
    let t = rtxn.open_table(TABLE).unwrap();
    let val = t.get(1).unwrap().unwrap();
    assert_eq!(val.value(), "first");

    // A far-future timestamp should still return the latest snapshot
    let rtxn = db.begin_read_at_time(u64::MAX).unwrap();
    let t = rtxn.open_table(TABLE).unwrap();
    let val = t.get(1).unwrap().unwrap();
    assert_eq!(val.value(), "first");

    // A timestamp before the snapshot should fail
    let result = db.begin_read_at_time(0);
    assert!(result.is_err());
}

#[test]
fn history_reopen_persistence() {
    let tmpfile = create_tempfile();

    // Write with history enabled, then close
    {
        let db = Database::builder()
            .set_history_retention(10)
            .create(tmpfile.path())
            .unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            t.insert(1, "persisted").unwrap();
        }
        txn.commit().unwrap();
    }

    // Reopen with history enabled — snapshots should be restored
    {
        let db = Database::builder()
            .set_history_retention(10)
            .open(tmpfile.path())
            .unwrap();

        let history = db.transaction_history().unwrap();
        assert_eq!(history.len(), 1);

        let rtxn = db.begin_read_at(history[0].transaction_id).unwrap();
        let t = rtxn.open_table(TABLE).unwrap();
        let val = t.get(1).unwrap().unwrap();
        assert_eq!(val.value(), "persisted");
    }
}

#[test]
fn history_concurrent_reads() {
    let (_tmpfile, db) = create_history_db(10);

    let txn = db.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        t.insert(1, "v1").unwrap();
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut t = txn.open_table(TABLE).unwrap();
        t.insert(1, "v2").unwrap();
    }
    txn.commit().unwrap();

    let history = db.transaction_history().unwrap();
    assert_eq!(history.len(), 2);

    // Open two historical read transactions concurrently
    let rtxn1 = db.begin_read_at(history[0].transaction_id).unwrap();
    let rtxn2 = db.begin_read_at(history[1].transaction_id).unwrap();

    let t1 = rtxn1.open_table(TABLE).unwrap();
    let t2 = rtxn2.open_table(TABLE).unwrap();

    assert_eq!(t1.get(1).unwrap().unwrap().value(), "v1");
    assert_eq!(t2.get(1).unwrap().unwrap().value(), "v2");
}

#[test]
fn history_empty_on_fresh_db() {
    let (_tmpfile, db) = create_history_db(10);
    assert!(db.transaction_history().unwrap().is_empty());
}

#[test]
fn history_retention_zero_purges() {
    let tmpfile = create_tempfile();

    // Write with history enabled
    {
        let db = Database::builder()
            .set_history_retention(10)
            .create(tmpfile.path())
            .unwrap();

        for i in 0u64..5 {
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(TABLE).unwrap();
                t.insert(i, "data").unwrap();
            }
            txn.commit().unwrap();
        }

        let history = db.transaction_history().unwrap();
        assert_eq!(history.len(), 5);
    }

    // Reopen with retention=0 — all history should be purged
    {
        let db = Database::builder()
            .set_history_retention(0)
            .open(tmpfile.path())
            .unwrap();

        assert!(db.transaction_history().unwrap().is_empty());
    }
}
