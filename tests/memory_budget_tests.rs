use redb::{Database, ReadableDatabase, TableDefinition};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("budget_test");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

#[test]
fn budget_basic_read_write() {
    let tmpfile = create_tempfile();
    let budget = 10 * 1024 * 1024; // 10 MiB
    let db = Database::builder()
        .set_memory_budget(budget)
        .create(tmpfile.path())
        .unwrap();

    let value = vec![0u8; 4096];

    // Insert some data
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        for i in 0..100u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Read it back
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    for i in 0..100u64 {
        let val = table.get(&i).unwrap().unwrap();
        assert_eq!(val.value().len(), 4096);
    }
    drop(table);
    drop(read_txn);

    // Verify used_bytes is reported and within budget
    let stats = db.cache_stats();
    assert!(
        stats.used_bytes() <= budget,
        "used_bytes {} exceeds budget {}",
        stats.used_bytes(),
        budget
    );
}

#[test]
fn budget_eviction_under_pressure() {
    let tmpfile = create_tempfile();
    // Very small budget: 512 KiB
    let budget = 512 * 1024;
    let db = Database::builder()
        .set_memory_budget(budget)
        .create(tmpfile.path())
        .unwrap();

    let value = vec![42u8; 4096];

    // Insert enough data to exceed the budget many times over
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        for i in 0..500u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Read all data back -- this will repeatedly fill the cache
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    for i in 0..500u64 {
        let val = table.get(&i).unwrap().unwrap();
        assert_eq!(val.value()[0], 42);
    }
    drop(table);
    drop(read_txn);

    // Cache usage should be bounded by the budget
    let stats = db.cache_stats();
    assert!(
        stats.used_bytes() <= budget,
        "used_bytes {} exceeds budget {}",
        stats.used_bytes(),
        budget
    );
}

#[test]
fn budget_write_buffer_auto_flush() {
    let tmpfile = create_tempfile();
    // Small budget -- write buffer gets 20% = 200 KiB
    let budget = 1024 * 1024;
    let db = Database::builder()
        .set_memory_budget(budget)
        .create(tmpfile.path())
        .unwrap();

    let value = vec![0xFFu8; 8192];

    // Write many pages within a single transaction -- this should trigger auto-flush
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        for i in 0..200u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Verify data integrity after auto-flushes
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    for i in 0..200u64 {
        let val = table.get(&i).unwrap().unwrap();
        assert_eq!(val.value().len(), 8192);
        assert_eq!(val.value()[0], 0xFF);
    }
}

#[test]
fn budget_graceful_degradation() {
    let tmpfile = create_tempfile();
    // Extremely tight budget: 128 KiB
    let budget = 128 * 1024;
    let db = Database::builder()
        .set_memory_budget(budget)
        .create(tmpfile.path())
        .unwrap();

    let value = vec![0xABu8; 4096];

    // Insert data well beyond the budget
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        for i in 0..1000u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Read everything back -- should still work correctly, just slower
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    for i in 0..1000u64 {
        let val = table.get(&i).unwrap().unwrap();
        assert_eq!(val.value().len(), 4096);
        assert_eq!(val.value()[0], 0xAB);
    }
}

#[test]
fn budget_coexists_with_set_cache_size() {
    let tmpfile = create_tempfile();

    // set_memory_budget should override set_cache_size
    let db = Database::builder()
        .set_cache_size(1024 * 1024 * 1024) // 1 GiB
        .set_memory_budget(256 * 1024) // 256 KiB -- this takes precedence
        .create(tmpfile.path())
        .unwrap();

    let value = vec![0u8; 4096];

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        for i in 0..200u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    for i in 0..200u64 {
        let val = table.get(&i).unwrap().unwrap();
        assert_eq!(val.value().len(), 4096);
    }
    drop(table);
    drop(read_txn);

    let stats = db.cache_stats();
    // Should be bounded by 256 KiB, not 1 GiB
    assert!(
        stats.used_bytes() <= 256 * 1024,
        "used_bytes {} exceeds memory budget 256 KiB (set_cache_size should have been overridden)",
        stats.used_bytes()
    );
}

#[test]
fn budget_used_bytes_always_available() {
    let tmpfile = create_tempfile();
    let db = Database::builder()
        .set_memory_budget(10 * 1024 * 1024)
        .create(tmpfile.path())
        .unwrap();

    let value = vec![0u8; 4096];

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        for i in 0..50u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Force cache population by reading
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    for i in 0..50u64 {
        let _ = table.get(&i).unwrap().unwrap();
    }
    drop(table);
    drop(read_txn);

    // used_bytes should be non-zero regardless of cache_metrics feature
    let stats = db.cache_stats();
    assert!(
        stats.used_bytes() > 0,
        "used_bytes should be non-zero after populating cache"
    );
}

#[test]
fn budget_cross_stripe_eviction() {
    let tmpfile = create_tempfile();
    // Small budget forces cross-stripe eviction
    let budget = 256 * 1024;
    let db = Database::builder()
        .set_memory_budget(budget)
        .create(tmpfile.path())
        .unwrap();

    let value = vec![0u8; 4096];

    // Insert data that will hash to many different stripes
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        for i in 0..500u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Read to populate cache across stripes
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    for i in 0..500u64 {
        let _ = table.get(&i).unwrap().unwrap();
    }
    drop(table);
    drop(read_txn);

    let stats = db.cache_stats();
    assert!(
        stats.used_bytes() <= budget,
        "used_bytes {} exceeds budget {} -- cross-stripe eviction may not be working",
        stats.used_bytes(),
        budget
    );
}

#[test]
fn budget_none_default() {
    let tmpfile = create_tempfile();
    // Default Builder has no memory budget (unlimited, defaults to 1 GiB cache)
    let db = Database::create(tmpfile.path()).unwrap();

    let value = vec![0u8; 4096];

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        for i in 0..100u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Read to populate cache
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    for i in 0..100u64 {
        let _ = table.get(&i).unwrap().unwrap();
    }
    drop(table);
    drop(read_txn);

    // Without a budget, used_bytes can be anything -- just verify it doesn't crash
    // and that the database works correctly
    let stats = db.cache_stats();
    // Default cache is 1 GiB, so 100 * 4096 bytes should fit easily
    let _ = stats.used_bytes();
    // No budget set, so budget_bytes should be None
    assert!(stats.budget_bytes().is_none());
}

#[test]
fn budget_stats_report_configured_budget() {
    let tmpfile = create_tempfile();
    let budget = 2 * 1024 * 1024; // 2 MiB
    let db = Database::builder()
        .set_memory_budget(budget)
        .create(tmpfile.path())
        .unwrap();

    let stats = db.cache_stats();
    assert_eq!(stats.budget_bytes(), Some(budget));
}

#[test]
#[should_panic(expected = "Memory budget must be at least 16 KiB")]
fn budget_rejects_too_small() {
    let mut builder = Database::builder();
    builder.set_memory_budget(100); // Way too small
}

#[test]
fn budget_mid_transaction_enforcement() {
    let tmpfile = create_tempfile();
    // Small budget to force mid-transaction eviction
    let budget = 256 * 1024;
    let db = Database::builder()
        .set_memory_budget(budget)
        .create(tmpfile.path())
        .unwrap();

    let value = vec![0xCDu8; 8192];

    // Write many pages in a single transaction -- triggers write buffer eviction
    // and mid-transaction cross-stripe read cache eviction
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        for i in 0..300u64 {
            table.insert(&i, value.as_slice()).unwrap();
        }
    }
    write_txn.commit().unwrap();

    // Verify data integrity
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    for i in 0..300u64 {
        let val = table.get(&i).unwrap().unwrap();
        assert_eq!(val.value()[0], 0xCD);
    }
    drop(table);
    drop(read_txn);

    let stats = db.cache_stats();
    assert!(
        stats.used_bytes() <= budget,
        "used_bytes {} exceeds budget {} after mid-transaction enforcement",
        stats.used_bytes(),
        budget
    );
}
