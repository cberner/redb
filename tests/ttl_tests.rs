use redb::{Database, ReadableDatabase, ReadableTableMetadata, TtlTableDefinition};
use std::thread;
use std::time::Duration;

const TTL_TABLE: TtlTableDefinition<&str, u64> = TtlTableDefinition::new("ttl_data");
const TTL_TABLE_B: TtlTableDefinition<&str, &str> = TtlTableDefinition::new("ttl_data_b");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

#[test]
fn ttl_insert_and_get() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_ttl_table(TTL_TABLE).unwrap();
        table
            .insert_with_ttl("key1", &42, Duration::from_secs(60))
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_ttl_table(TTL_TABLE).unwrap();
    let guard = table.get("key1").unwrap().unwrap();
    assert_eq!(guard.value(), 42);
    assert!(guard.expires_at_ms() > 0);
}

#[test]
fn ttl_expired_returns_none() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_ttl_table(TTL_TABLE).unwrap();
        table
            .insert_with_ttl("ephemeral", &99, Duration::from_millis(1))
            .unwrap();
    }
    write_txn.commit().unwrap();

    thread::sleep(Duration::from_millis(10));

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_ttl_table(TTL_TABLE).unwrap();
    assert!(table.get("ephemeral").unwrap().is_none());
}

#[test]
fn ttl_no_expiry() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_ttl_table(TTL_TABLE).unwrap();
        table.insert("permanent", &77).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_ttl_table(TTL_TABLE).unwrap();
    let guard = table.get("permanent").unwrap().unwrap();
    assert_eq!(guard.value(), 77);
    assert_eq!(guard.expires_at_ms(), 0);
}

#[test]
fn ttl_purge_expired() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Insert one permanent and one short-lived
    {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_ttl_table(TTL_TABLE).unwrap();
            table.insert("alive", &1).unwrap();
            table
                .insert_with_ttl("dead", &2, Duration::from_millis(1))
                .unwrap();
        }
        write_txn.commit().unwrap();
    }

    thread::sleep(Duration::from_millis(10));

    // Purge
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_ttl_table(TTL_TABLE).unwrap();
        let purged = table.purge_expired().unwrap();
        assert_eq!(purged, 1);
    }
    write_txn.commit().unwrap();

    // Verify
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_ttl_table(TTL_TABLE).unwrap();
    assert!(table.get("alive").unwrap().is_some());
    assert!(table.get("dead").unwrap().is_none());
    assert_eq!(table.len().unwrap(), 1);
}

#[test]
fn ttl_range_skips_expired() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_ttl_table(TTL_TABLE).unwrap();
            table.insert("a", &1).unwrap();
            table
                .insert_with_ttl("b", &2, Duration::from_millis(1))
                .unwrap();
            table.insert("c", &3).unwrap();
        }
        write_txn.commit().unwrap();
    }

    thread::sleep(Duration::from_millis(10));

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_ttl_table(TTL_TABLE).unwrap();
    let entries: Vec<_> = table
        .iter()
        .unwrap()
        .map(|r| {
            let (k, v) = r.unwrap();
            (k.value().to_string(), v.value())
        })
        .collect();
    assert_eq!(entries, vec![("a".to_string(), 1), ("c".to_string(), 3)]);
}

#[test]
fn ttl_insert_overwrites() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_ttl_table(TTL_TABLE).unwrap();
        table.insert("key", &10).unwrap();
        let old = table.insert("key", &20).unwrap();
        assert_eq!(old.unwrap().value(), 10);
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_ttl_table(TTL_TABLE).unwrap();
    assert_eq!(table.get("key").unwrap().unwrap().value(), 20);
}

#[test]
fn ttl_remove() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_ttl_table(TTL_TABLE).unwrap();
        table.insert("to_delete", &42).unwrap();
        let old = table.remove("to_delete").unwrap();
        assert_eq!(old.unwrap().value(), 42);
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_ttl_table(TTL_TABLE).unwrap();
    assert!(table.get("to_delete").unwrap().is_none());
}

#[test]
fn ttl_read_isolation() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Insert initial data
    {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_ttl_table(TTL_TABLE).unwrap();
            table.insert("before", &1).unwrap();
        }
        write_txn.commit().unwrap();
    }

    // Start a read transaction BEFORE the next write
    let read_txn = db.begin_read().unwrap();

    // Write new data
    {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_ttl_table(TTL_TABLE).unwrap();
            table.insert("after", &2).unwrap();
        }
        write_txn.commit().unwrap();
    }

    // Old read should NOT see "after"
    let table = read_txn.open_ttl_table(TTL_TABLE).unwrap();
    assert!(table.get("before").unwrap().is_some());
    assert!(table.get("after").unwrap().is_none());

    // New read SHOULD see it
    let read_txn2 = db.begin_read().unwrap();
    let table2 = read_txn2.open_ttl_table(TTL_TABLE).unwrap();
    assert_eq!(table2.get("after").unwrap().unwrap().value(), 2);
}

#[test]
fn ttl_multiple_tables() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut t1 = write_txn.open_ttl_table(TTL_TABLE).unwrap();
        t1.insert("num", &42).unwrap();
    }
    {
        let mut t2 = write_txn.open_ttl_table(TTL_TABLE_B).unwrap();
        t2.insert("greeting", "hello").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let t1 = read_txn.open_ttl_table(TTL_TABLE).unwrap();
    assert_eq!(t1.get("num").unwrap().unwrap().value(), 42);
    let t2 = read_txn.open_ttl_table(TTL_TABLE_B).unwrap();
    assert_eq!(t2.get("greeting").unwrap().unwrap().value(), "hello");
}

#[test]
fn ttl_purge_returns_count() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_ttl_table(TTL_TABLE).unwrap();
            for i in 0..5u64 {
                let key = format!("expire_{i}");
                table
                    .insert_with_ttl(key.as_str(), &i, Duration::from_millis(1))
                    .unwrap();
            }
            for i in 0..3u64 {
                let key = format!("keep_{i}");
                table.insert(key.as_str(), &(i + 100)).unwrap();
            }
        }
        write_txn.commit().unwrap();
    }

    thread::sleep(Duration::from_millis(10));

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_ttl_table(TTL_TABLE).unwrap();
        let count = table.purge_expired().unwrap();
        assert_eq!(count, 5);
        assert_eq!(table.len().unwrap(), 3);
    }
    write_txn.commit().unwrap();
}
