// Concurrency audit tests. Each test asserts CORRECT behavior; a failure
// indicates a bug in redb.
#[cfg(not(target_os = "wasi"))]
mod audit {
    use redb::{
        CommitError, Database, Durability, ReadableDatabase, ReadableTable, TableDefinition,
    };
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn create_tempfile() -> tempfile::NamedTempFile {
        tempfile::NamedTempFile::new().unwrap()
    }

    const TABLE: TableDefinition<u64, u64> = TableDefinition::new("audit");
    const TABLE2: TableDefinition<u64, &[u8]> = TableDefinition::new("audit2");

    // A savepoint taken on a non-durable commit must either remain restorable
    // after check_integrity(), or check_integrity()/restore_savepoint() must
    // refuse to run. Restoring it must never corrupt the database.
    #[test]
    fn check_integrity_with_live_savepoint_on_nondurable_commit() {
        let tmpfile = create_tempfile();
        let mut db = Database::create(tmpfile.path()).unwrap();

        // Durable baseline
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            t.insert(0, 0).unwrap();
        }
        txn.commit().unwrap();

        // Non-durable commit with real data
        let mut txn = db.begin_write().unwrap();
        txn.set_durability(Durability::None).unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for k in 1..100u64 {
                t.insert(k, k * 10).unwrap();
            }
        }
        txn.commit().unwrap();

        // Ephemeral savepoint referencing the non-durable root
        let txn = db.begin_write().unwrap();
        let savepoint = txn.ephemeral_savepoint().unwrap();
        txn.abort().unwrap();

        // The savepoint holds no Arc<TransactionalMemory>, so the exclusivity
        // guard lets this run.
        let clean = db.check_integrity().unwrap();
        assert!(clean, "freshly written database reported corruption");

        // Restore the savepoint
        let mut txn = db.begin_write().unwrap();
        txn.restore_savepoint(&savepoint).unwrap();
        txn.commit().unwrap();
        drop(savepoint);

        // Write a bunch of unrelated data, to reuse any pages the allocator
        // thinks are free
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE2).unwrap();
            let big = vec![0xABu8; 4096];
            for k in 0..100u64 {
                t.insert(k, big.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        // The restored data must be intact
        let read = db.begin_read().unwrap();
        let t = read.open_table(TABLE).unwrap();
        for k in 1..100u64 {
            let v = t.get(k).unwrap();
            assert_eq!(v.map(|x| x.value()), Some(k * 10), "key {k} corrupted");
        }
        drop(t);
        drop(read);

        assert!(db.check_integrity().unwrap());
    }

    // check_integrity() must not silently roll back a successfully committed
    // (but not yet durable) transaction while the process is alive: the data
    // was visible to readers before the call and the call reports "clean".
    #[test]
    fn check_integrity_preserves_nondurable_commits() {
        let tmpfile = create_tempfile();
        let mut db = Database::create(tmpfile.path()).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            t.insert(0, 0).unwrap();
        }
        txn.commit().unwrap();
        let mut txn = db.begin_write().unwrap();
        txn.set_durability(Durability::None).unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for k in 1..100u64 {
                t.insert(k, k * 10).unwrap();
            }
        }
        txn.commit().unwrap();
        assert!(db.check_integrity().unwrap());
        let read = db.begin_read().unwrap();
        let t = read.open_table(TABLE).unwrap();
        let mut missing = 0;
        for k in 1..100u64 {
            if t.get(k).unwrap().is_none() {
                missing += 1;
            }
        }
        assert_eq!(missing, 0, "{missing}/99 committed keys lost");
    }

    // A panicking retain() predicate poisons the transaction, but the
    // Database must stay fully usable afterwards.
    #[test]
    fn predicate_panic_leaves_database_usable() {
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for k in 0..100u64 {
                t.insert(k, k).unwrap();
            }
        }
        txn.commit().unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            let result = catch_unwind(AssertUnwindSafe(|| {
                t.retain(|k, _| if k == 50 { panic!("user panic") } else { true })
                    .unwrap();
            }));
            assert!(result.is_err());
        }
        match txn.commit() {
            Err(CommitError::TransactionPoisoned) => {}
            other => panic!("expected TransactionPoisoned, got {other:?}"),
        }

        // Same for extract_if
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            let result = catch_unwind(AssertUnwindSafe(|| {
                for entry in t
                    .extract_if(|k, _| {
                        if k == 10 {
                            panic!("user panic")
                        } else {
                            k < 20
                        }
                    })
                    .unwrap()
                {
                    entry.unwrap();
                }
            }));
            assert!(result.is_err());
        }
        let _ = txn.commit();

        // The database must still work: write, commit, and read back
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            t.insert(1000, 1000).unwrap();
        }
        txn.commit().unwrap();
        let read = db.begin_read().unwrap();
        let t = read.open_table(TABLE).unwrap();
        assert_eq!(t.get(1000).unwrap().unwrap().value(), 1000);
        for k in 0..100u64 {
            assert_eq!(t.get(k).unwrap().unwrap().value(), k, "key {k} lost");
        }
    }

    // Readers concurrent with committing writers must always see a consistent
    // snapshot: every key has the same version within one read transaction.
    #[test]
    fn snapshot_consistency_under_concurrent_commits() {
        let tmpfile = create_tempfile();
        let db = Arc::new(Database::create(tmpfile.path()).unwrap());
        const KEYS: u64 = 16;
        const VERSIONS: u64 = 300;

        // Version 0 baseline so the table always exists
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for k in 0..KEYS {
                t.insert(k, 0).unwrap();
            }
        }
        txn.commit().unwrap();

        let done = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(3));

        let writer = {
            let db = db.clone();
            let done = done.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                for v in 1..=VERSIONS {
                    let mut txn = db.begin_write().unwrap();
                    if v % 2 == 0 {
                        txn.set_durability(Durability::None).unwrap();
                    }
                    {
                        let mut t = txn.open_table(TABLE).unwrap();
                        for k in 0..KEYS {
                            t.insert(k, v).unwrap();
                        }
                    }
                    txn.commit().unwrap();
                }
                done.store(true, Ordering::Release);
            })
        };

        let mut readers = vec![];
        for _ in 0..2 {
            let db = db.clone();
            let done = done.clone();
            let barrier = barrier.clone();
            readers.push(thread::spawn(move || {
                barrier.wait();
                let mut last_seen = 0u64;
                while !done.load(Ordering::Acquire) {
                    let read = db.begin_read().unwrap();
                    let t = read.open_table(TABLE).unwrap();
                    let first = t.get(0).unwrap().unwrap().value();
                    for k in 1..KEYS {
                        let v = t.get(k).unwrap().unwrap().value();
                        assert_eq!(v, first, "torn snapshot: key {k}={v}, key 0={first}");
                    }
                    assert!(first >= last_seen, "snapshot went backwards");
                    last_seen = first;
                }
            }));
        }

        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }
    }

    // Multiple threads contending on begin_write must serialize cleanly with
    // no lost updates.
    #[test]
    fn begin_write_contention() {
        let tmpfile = create_tempfile();
        let db = Arc::new(Database::create(tmpfile.path()).unwrap());

        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            t.insert(0, 0).unwrap();
        }
        txn.commit().unwrap();

        const THREADS: usize = 4;
        const INCREMENTS: u64 = 50;
        let barrier = Arc::new(Barrier::new(THREADS));
        let mut handles = vec![];
        for i in 0..THREADS {
            let db = db.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                for j in 0..INCREMENTS {
                    let mut txn = db.begin_write().unwrap();
                    if (i + j as usize) % 3 == 0 {
                        txn.set_durability(Durability::None).unwrap();
                    }
                    {
                        let mut t = txn.open_table(TABLE).unwrap();
                        let old = t.get(0).unwrap().unwrap().value();
                        t.insert(0, old + 1).unwrap();
                    }
                    txn.commit().unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let read = db.begin_read().unwrap();
        let t = read.open_table(TABLE).unwrap();
        assert_eq!(
            t.get(0).unwrap().unwrap().value(),
            (THREADS as u64) * INCREMENTS
        );
    }

    // Ephemeral savepoints dropped on other threads while the writer churns
    // must not corrupt tracker state.
    #[test]
    fn savepoint_drop_races_commit() {
        let tmpfile = create_tempfile();
        let db = Arc::new(Database::create(tmpfile.path()).unwrap());

        for i in 0..100u64 {
            let txn = db.begin_write().unwrap();
            let savepoint = txn.ephemeral_savepoint().unwrap();
            txn.commit().unwrap();

            let barrier = Arc::new(Barrier::new(2));
            let dropper = {
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    drop(savepoint);
                })
            };

            barrier.wait();
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(TABLE).unwrap();
                t.insert(i, i).unwrap();
                t.remove(i.wrapping_sub(1)).unwrap();
            }
            txn.commit().unwrap();
            dropper.join().unwrap();
        }

        let read = db.begin_read().unwrap();
        let t = read.open_table(TABLE).unwrap();
        assert!(t.get(99).unwrap().is_some());
    }

    // A reader's snapshot pages must never be freed and reused while the
    // reader is live. Tiny cache forces reads from disk, so reuse would be
    // visible as torn values or checksum errors.
    #[test]
    fn page_reuse_under_live_reader() {
        let tmpfile = create_tempfile();
        let db = Arc::new(
            Database::builder()
                .set_cache_size(50 * 1024)
                .create(tmpfile.path())
                .unwrap(),
        );
        const KEYS: u64 = 100;
        const VAL_LEN: usize = 2000;

        let make_val = |version: u8| vec![version; VAL_LEN];

        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE2).unwrap();
            for k in 0..KEYS {
                t.insert(k, make_val(0).as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        const ITERS: u8 = 40;
        let barrier = Arc::new(Barrier::new(2));
        let reader = {
            let db = db.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                for _ in 0..ITERS {
                    barrier.wait();
                    let read = db.begin_read().unwrap();
                    let t = read.open_table(TABLE2).unwrap();
                    // Snapshot is open; writer is now overwriting + committing
                    barrier.wait();
                    let mut version = None;
                    for k in 0..KEYS {
                        let v = t.get(k).unwrap().unwrap();
                        let bytes = v.value();
                        assert_eq!(bytes.len(), VAL_LEN);
                        let first = bytes[0];
                        assert!(bytes.iter().all(|b| *b == first), "torn value for key {k}");
                        match version {
                            None => version = Some(first),
                            Some(expected) => {
                                assert_eq!(first, expected, "inconsistent snapshot at key {k}");
                            }
                        }
                    }
                }
            })
        };

        for version in 1..=ITERS {
            barrier.wait(); // reader opens snapshot
            // Two commits: the second processes the freed pages of the first
            for _ in 0..2 {
                let txn = db.begin_write().unwrap();
                {
                    let mut t = txn.open_table(TABLE2).unwrap();
                    for k in 0..KEYS {
                        t.insert(k, make_val(version).as_slice()).unwrap();
                    }
                }
                txn.commit().unwrap();
            }
            barrier.wait(); // reader verifies while more commits could run
        }
        reader.join().unwrap();
    }

    // Rapid read-transaction churn while a writer commits must not leak
    // pinned transactions (which would prevent page reuse forever).
    #[test]
    fn reader_churn_does_not_leak_pages() {
        fn workload(db: &Database) {
            let val = vec![1u8; 1024];
            for i in 0..150u64 {
                let txn = db.begin_write().unwrap();
                {
                    let mut t = txn.open_table(TABLE2).unwrap();
                    for k in 0..50u64 {
                        t.insert(i * 50 + k, val.as_slice()).unwrap();
                        if i > 0 {
                            t.remove((i - 1) * 50 + k).unwrap();
                        }
                    }
                }
                txn.commit().unwrap();
            }
            // Drain pending frees
            for _ in 0..3 {
                let txn = db.begin_write().unwrap();
                txn.commit().unwrap();
            }
        }

        fn allocated_pages(db: &Database) -> u64 {
            let txn = db.begin_write().unwrap();
            let stats = txn.stats().unwrap();
            txn.abort().unwrap();
            stats.allocated_pages()
        }

        // Baseline without readers
        let tmpfile1 = create_tempfile();
        let db1 = Database::create(tmpfile1.path()).unwrap();
        workload(&db1);
        let baseline = allocated_pages(&db1);

        // Same workload with rapid reader churn
        let tmpfile2 = create_tempfile();
        let db2 = Arc::new(Database::create(tmpfile2.path()).unwrap());
        let done = Arc::new(AtomicBool::new(false));
        let mut churners = vec![];
        for _ in 0..3 {
            let db = db2.clone();
            let done = done.clone();
            churners.push(thread::spawn(move || {
                while !done.load(Ordering::Acquire) {
                    let read = db.begin_read().unwrap();
                    if let Ok(t) = read.open_table(TABLE2) {
                        let _ = t.first();
                    }
                }
            }));
        }
        workload(&db2);
        done.store(true, Ordering::Release);
        for c in churners {
            c.join().unwrap();
        }
        // After churn has stopped, pending frees must drain
        for _ in 0..3 {
            let txn = db2.begin_write().unwrap();
            txn.commit().unwrap();
        }
        let with_churn = allocated_pages(&db2);

        assert!(
            with_churn <= baseline * 3 + 100,
            "page leak: baseline={baseline}, with churn={with_churn}"
        );
    }

    // Dropping the Database while a write transaction is in flight on another
    // thread must not deadlock or lose the committed data.
    #[test]
    fn database_drop_with_inflight_writer() {
        let tmpfile = create_tempfile();
        let path = tmpfile.path().to_path_buf();
        let db = Database::create(&path).unwrap();

        let barrier = Arc::new(Barrier::new(2));
        let writer = {
            let txn = db.begin_write().unwrap();
            let barrier = barrier.clone();
            thread::spawn(move || {
                {
                    let mut t = txn.open_table(TABLE).unwrap();
                    t.insert(1, 1).unwrap();
                    barrier.wait();
                    // Keep writing while the main thread drops the Database
                    for k in 2..2000u64 {
                        t.insert(k, k).unwrap();
                    }
                }
                txn.commit().unwrap();
            })
        };

        barrier.wait();
        drop(db); // must block until the writer's transaction completes
        writer.join().unwrap();

        let db = Database::open(&path).unwrap();
        let read = db.begin_read().unwrap();
        let t = read.open_table(TABLE).unwrap();
        assert_eq!(t.get(1999).unwrap().unwrap().value(), 1999);
    }

    // Dropping the Database while read transactions are alive on other
    // threads must not panic or deadlock; in-flight reads may error but the
    // process must stay sound.
    #[test]
    fn database_drop_with_inflight_readers() {
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE2).unwrap();
            let big = vec![7u8; 4096];
            for k in 0..500u64 {
                t.insert(k, big.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();

        let barrier = Arc::new(Barrier::new(3));
        let mut handles = vec![];
        for _ in 0..2 {
            let read = db.begin_read().unwrap();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                let t = read.open_table(TABLE2).unwrap();
                barrier.wait();
                let mut errors = 0;
                for _ in 0..50 {
                    for k in 0..500u64 {
                        match t.get(k) {
                            Ok(Some(v)) => assert_eq!(v.value()[0], 7),
                            Ok(None) => panic!("missing key {k}"),
                            Err(_) => {
                                errors += 1;
                            }
                        }
                    }
                }
                errors
            }));
        }

        barrier.wait();
        drop(db);
        for h in handles {
            // Errors are acceptable; panics or deadlock are not
            let _ = h.join().unwrap();
        }
    }
}
