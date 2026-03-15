use redb::{
    Database, Error, GroupCommitError, ReadableDatabase, ReadableTableMetadata, TableDefinition,
    WriteBatch,
};
use std::sync::Arc;
use std::thread;

const TABLE_A: TableDefinition<&str, u64> = TableDefinition::new("table_a");
const TABLE_B: TableDefinition<&str, &str> = TableDefinition::new("table_b");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

#[test]
fn group_commit_single_batch() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let batch = WriteBatch::new(|txn| {
        let mut table = txn.open_table(TABLE_A)?;
        table.insert("hello", &42)?;
        Ok(())
    });
    db.submit_write_batch(batch).unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_A).unwrap();
    assert_eq!(table.get("hello").unwrap().unwrap().value(), 42);
}

#[test]
fn group_commit_concurrent_batches() {
    let tmpfile = create_tempfile();
    let db = Arc::new(Database::create(tmpfile.path()).unwrap());

    let mut handles = Vec::new();
    for i in 0..10u64 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            let key = format!("key_{i}");
            let batch = WriteBatch::new(move |txn| {
                let mut table = txn.open_table(TABLE_A)?;
                table.insert(key.as_str(), &i)?;
                Ok(())
            });
            db.submit_write_batch(batch).unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_A).unwrap();
    for i in 0..10u64 {
        let key = format!("key_{i}");
        let val = table.get(key.as_str()).unwrap().unwrap().value();
        assert_eq!(val, i);
    }
}

#[test]
fn group_commit_batch_failure() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // First store a valid value so we can detect rollback
    {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE_A).unwrap();
            table.insert("existing", &100).unwrap();
        }
        txn.commit().unwrap();
    }

    // Submit a batch that triggers an error
    let batch = WriteBatch::new(|txn| {
        let mut table = txn.open_table(TABLE_A)?;
        table.insert("new_key", &200)?;
        // Simulate failure by returning an error
        Err(Error::Corrupted("intentional test failure".to_string()))
    });

    let result = db.submit_write_batch(batch);
    assert!(result.is_err());
    match result.unwrap_err() {
        GroupCommitError::BatchFailed(_) => {}
        other => panic!("Expected BatchFailed, got: {other}"),
    }

    // Verify the batch's insert was rolled back
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_A).unwrap();
    assert_eq!(table.get("existing").unwrap().unwrap().value(), 100);
    assert!(table.get("new_key").unwrap().is_none());
}

#[test]
fn group_commit_interleaved_with_direct_write() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Direct write
    {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE_A).unwrap();
            table.insert("direct_1", &1).unwrap();
        }
        txn.commit().unwrap();
    }

    // Group commit
    let batch = WriteBatch::new(|txn| {
        let mut table = txn.open_table(TABLE_A)?;
        table.insert("batch_1", &2)?;
        Ok(())
    });
    db.submit_write_batch(batch).unwrap();

    // Another direct write
    {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE_A).unwrap();
            table.insert("direct_2", &3).unwrap();
        }
        txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_A).unwrap();
    assert_eq!(table.get("direct_1").unwrap().unwrap().value(), 1);
    assert_eq!(table.get("batch_1").unwrap().unwrap().value(), 2);
    assert_eq!(table.get("direct_2").unwrap().unwrap().value(), 3);
}

#[test]
fn group_commit_read_isolation() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Insert initial data
    {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE_A).unwrap();
            table.insert("before", &1).unwrap();
        }
        txn.commit().unwrap();
    }

    // Start a read transaction BEFORE group commit
    let read_txn = db.begin_read().unwrap();

    // Group commit adds new data
    let batch = WriteBatch::new(|txn| {
        let mut table = txn.open_table(TABLE_A)?;
        table.insert("after", &2)?;
        Ok(())
    });
    db.submit_write_batch(batch).unwrap();

    // Read transaction should NOT see the new data (snapshot isolation)
    let table = read_txn.open_table(TABLE_A).unwrap();
    assert_eq!(table.get("before").unwrap().unwrap().value(), 1);
    assert!(table.get("after").unwrap().is_none());

    // New read transaction SHOULD see it
    let read_txn2 = db.begin_read().unwrap();
    let table2 = read_txn2.open_table(TABLE_A).unwrap();
    assert_eq!(table2.get("after").unwrap().unwrap().value(), 2);
}

#[test]
fn group_commit_multiple_tables() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let batch = WriteBatch::new(|txn| {
        {
            let mut table_a = txn.open_table(TABLE_A)?;
            table_a.insert("num", &42)?;
        }
        {
            let mut table_b = txn.open_table(TABLE_B)?;
            table_b.insert("greeting", "hello")?;
        }
        Ok(())
    });
    db.submit_write_batch(batch).unwrap();

    let read_txn = db.begin_read().unwrap();
    let table_a = read_txn.open_table(TABLE_A).unwrap();
    assert_eq!(table_a.get("num").unwrap().unwrap().value(), 42);
    let table_b = read_txn.open_table(TABLE_B).unwrap();
    assert_eq!(table_b.get("greeting").unwrap().unwrap().value(), "hello");
}

#[test]
fn group_commit_throughput() {
    let tmpfile = create_tempfile();
    let db = Arc::new(Database::create(tmpfile.path()).unwrap());

    let mut handles = Vec::new();
    for i in 0..100u64 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            let key = format!("stress_{i}");
            let batch = WriteBatch::new(move |txn| {
                let mut table = txn.open_table(TABLE_A)?;
                table.insert(key.as_str(), &i)?;
                Ok(())
            });
            db.submit_write_batch(batch).unwrap();
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_A).unwrap();
    let count = table.len().unwrap();
    assert_eq!(count, 100);
}

#[test]
fn group_commit_sequential_groups() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    for round in 0..5u64 {
        let batch = WriteBatch::new(move |txn| {
            let mut table = txn.open_table(TABLE_A)?;
            let key = format!("round_{round}");
            table.insert(key.as_str(), &round)?;
            Ok(())
        });
        db.submit_write_batch(batch).unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_A).unwrap();
    for round in 0..5u64 {
        let key = format!("round_{round}");
        assert_eq!(table.get(key.as_str()).unwrap().unwrap().value(), round);
    }
    assert_eq!(table.len().unwrap(), 5);
}
