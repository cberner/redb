// Hostile regression tests for the three bug fixes merged at b8392e2:
//   Fix 1 (80d4d31): extract_if direction-switch panic / allow_in_place plumbing
//   Fix 2 (e1ed56d): leaf num_pairs u16 overflow + build_split clamp
//   Fix 3 (b8392e2): check_integrity rework
use redb::{
    Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
};
use std::collections::BTreeMap;

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("audit");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

// Deterministic RNG so failures are reproducible
struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self {
        Self(
            seed.wrapping_mul(2862933555777941757)
                .wrapping_add(3037000493),
        )
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
    fn next_bool(&mut self) -> bool {
        self.next() & 1 == 1
    }
}

fn value_for(key: u64, size: usize) -> Vec<u8> {
    let mut v = vec![0u8; size];
    let bytes = key.to_le_bytes();
    for (i, b) in v.iter_mut().enumerate() {
        *b = bytes[i % 8].wrapping_add(i as u8);
    }
    v
}

fn matches_predicate(key: u64) -> bool {
    key.wrapping_mul(2654435761) % 100 < 60
}

// Runs one extract_if scan with a pseudo-random direction sequence against a
// model, holding all yielded guards until the iterator is finished. Verifies
// the extracted set, the remaining table contents, commit, page accounting
// (check_integrity rebuilds the allocator from the live roots), and the
// persisted state after reopen.
fn extract_if_model_case(seed: u64, n: u64, size_fn: fn(u64) -> usize, stop_after: Option<usize>) {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();

    let mut model: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..n {
            let v = value_for(k, size_fn(k));
            table.insert(&k, v.as_slice()).unwrap();
            model.insert(k, v);
        }
    }
    txn.commit().unwrap();

    let mut expected_extracted: BTreeMap<u64, Vec<u8>> = BTreeMap::new();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        let mut held = vec![];
        {
            let mut rng = Lcg::new(seed);
            let mut iter = table.extract_if(|k, _| matches_predicate(k)).unwrap();
            let mut steps = 0usize;
            loop {
                if let Some(limit) = stop_after
                    && steps >= limit
                {
                    break;
                }
                let entry = if rng.next_bool() {
                    iter.next()
                } else {
                    iter.next_back()
                };
                steps += 1;
                match entry {
                    Some(item) => held.push(item.unwrap()),
                    None => break,
                }
            }
            // iterator dropped here, possibly with pending batches on both ends
        }
        for (k, v) in &held {
            let removed = model.remove(&k.value());
            assert_eq!(
                removed.as_deref(),
                Some(v.value()),
                "extracted guard mismatch for key {} (seed {seed}, n {n})",
                k.value()
            );
            expected_extracted.insert(k.value(), removed.unwrap());
        }
        drop(held);

        // If the scan ran to completion, every matching key must be extracted
        if stop_after.is_none() {
            for k in model.keys() {
                assert!(
                    !matches_predicate(*k),
                    "key {k} matched the predicate but was not extracted (seed {seed}, n {n})"
                );
            }
        }

        assert_eq!(table.len().unwrap(), model.len() as u64);
        for (k, v) in &model {
            let got = table.get(k).unwrap().unwrap();
            assert_eq!(got.value(), v.as_slice(), "key {k} (seed {seed}, n {n})");
        }
        // Confirm extracted keys are gone
        for k in expected_extracted.keys() {
            assert!(table.get(k).unwrap().is_none());
        }

        // Exercise further mutations through other code paths in the same txn
        let mut rng = Lcg::new(seed ^ 0xdead_beef);
        for _ in 0..20.min(n) {
            match rng.next() % 3 {
                0 => {
                    if let Some((k, v)) = table.pop_first().unwrap().map(|(k, v)| {
                        let k = k.value();
                        let v = v.value().to_vec();
                        (k, v)
                    }) {
                        assert_eq!(model.remove(&k).as_deref(), Some(v.as_slice()));
                    } else {
                        assert!(model.is_empty());
                    }
                }
                1 => {
                    if let Some((k, v)) = table.pop_last().unwrap().map(|(k, v)| {
                        let k = k.value();
                        let v = v.value().to_vec();
                        (k, v)
                    }) {
                        assert_eq!(model.remove(&k).as_deref(), Some(v.as_slice()));
                    } else {
                        assert!(model.is_empty());
                    }
                }
                _ => {
                    let k = rng.next() % (2 * n.max(1));
                    let v = value_for(k, size_fn(k));
                    table.insert(&k, v.as_slice()).unwrap();
                    model.insert(k, v);
                }
            }
        }
        assert_eq!(table.len().unwrap(), model.len() as u64);
    }
    txn.commit().unwrap();

    // Page-leak / allocator consistency check on the live handle
    assert!(
        db.check_integrity().unwrap(),
        "check_integrity reported corruption after extract_if (seed {seed}, n {n})"
    );

    // Verify persisted state after reopen
    drop(db);
    let mut db = Database::create(tmpfile.path()).unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), model.len() as u64);
    for (k, v) in &model {
        assert_eq!(table.get(k).unwrap().unwrap().value(), v.as_slice());
    }
    drop(table);
    drop(read);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn extract_if_alternating_model_small_values() {
    for seed in 0..6 {
        extract_if_model_case(seed, 400, |_| 8, None);
    }
}

#[test]
fn extract_if_alternating_model_mixed_values() {
    // Mixed sizes force splits and merges (PartialLeaf paths) during batch
    // resolution and flushes
    fn size(k: u64) -> usize {
        match k % 7 {
            0 => 1900,
            1 => 700,
            2 => 150,
            _ => 10,
        }
    }
    for seed in 0..6 {
        extract_if_model_case(seed, 300, size, None);
    }
}

#[test]
fn extract_if_alternating_model_partial_consumption() {
    // Drop the iterator at many different points, with pending batches on
    // either or both ends
    for cut in [1usize, 2, 3, 5, 7, 10, 20, 40, 70, 100, 150] {
        extract_if_model_case(cut as u64, 200, |_| 8, Some(cut));
        extract_if_model_case(
            cut as u64 ^ 0x55,
            150,
            |k| if k % 5 == 0 { 1500 } else { 12 },
            Some(cut),
        );
    }
}

// Both ends ping-pong within a single leaf (and across two leaves), removing
// everything while holding guards: the Pending batch snapshot and the live
// leaf share buffers
#[test]
fn extract_if_single_leaf_ping_pong() {
    for n in [1u64, 2, 3, 5, 10, 20, 50, 120] {
        for seed in 0..8u64 {
            extract_if_model_case(seed.wrapping_add(n), n, |_| 8, None);
        }
    }
}

// extract_from_if over a sub-range with alternation must not disturb
// out-of-range entries
#[test]
fn extract_from_if_subrange_alternating() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let n = 500u64;

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..n {
            table.insert(&k, value_for(k, 20).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        let mut extracted = vec![];
        {
            let mut iter = table
                .extract_from_if(100..400u64, |k, _| k % 3 != 0)
                .unwrap();
            let mut rng = Lcg::new(42);
            loop {
                let entry = if rng.next_bool() {
                    iter.next()
                } else {
                    iter.next_back()
                };
                match entry {
                    Some(item) => extracted.push(item.unwrap().0.value()),
                    None => break,
                }
            }
        }
        extracted.sort_unstable();
        let expected: Vec<u64> = (100..400).filter(|k| k % 3 != 0).collect();
        assert_eq!(extracted, expected);
        for k in 0..n {
            let in_table = table.get(&k).unwrap().is_some();
            let expected_in_table = !(100..400).contains(&k) || k % 3 == 0;
            assert_eq!(in_table, expected_in_table, "key {k}");
        }
    }
    txn.commit().unwrap();
    assert!(db.check_integrity().unwrap());
}

// ------------------------- Fix 2: num_pairs u16 overflow -------------------------

// Drives a leaf to exactly u16::MAX pairs via the public API (in-place appends
// into a large page), then continues inserting so the leaf must split. The
// split halves are at the 65535/65536 boundary.
#[test]
fn huge_leaf_append_cap_public_api() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let big = vec![7u8; 2 * 1024 * 1024];
    let max_pairs: u64 = u16::MAX as u64;

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        // Creates a large (order > 0) page
        table.insert(&500u64, big.as_slice()).unwrap();
        // Shrink the value in place; the leaf keeps its large page with free space
        table.insert(&500u64, [1u8].as_slice()).unwrap();
        // In-place appends up to the u16::MAX cap, then through the split
        let extra = 1000u64;
        for i in 0..(max_pairs - 1 + extra) {
            table.insert(&(1000 + i), [].as_slice()).unwrap();
        }
        assert_eq!(table.len().unwrap(), max_pairs + extra);
        // Spot checks
        assert_eq!(table.get(&500u64).unwrap().unwrap().value(), &[1u8]);
        assert!(table.get(&1000u64).unwrap().is_some());
        assert!(
            table
                .get(&(1000 + max_pairs - 2 + extra))
                .unwrap()
                .is_some()
        );
    }
    txn.commit().unwrap();

    assert!(db.check_integrity().unwrap());

    drop(db);
    let mut db = Database::create(tmpfile.path()).unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), max_pairs + 1000);
    let count = table.iter().unwrap().count();
    assert_eq!(count as u64, max_pairs + 1000);
    drop(table);
    drop(read);
    assert!(db.check_integrity().unwrap());
}

// Forces a leaf merge whose combined pair count exceeds u16::MAX with the
// huge value at the END of the merged ordering, so the byte-balanced division
// exceeds u16::MAX and must be clamped. Verifies the clamped split is built
// correctly.
#[test]
fn leaf_merge_exceeding_max_pairs_with_trailing_huge_value() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let big = vec![3u8; 2 * 1024 * 1024];
    let big600 = vec![9u8; 600 * 1024];
    let max_pairs: u64 = u16::MAX as u64;

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        // Single huge leaf [500 -> 2MiB]
        table.insert(&500u64, big.as_slice()).unwrap();
        // Inserting a smaller key triggers the single-large-value fast path:
        // branch [A=[5], C=[500 -> 2MiB]] where C keeps the huge page
        table.insert(&5u64, [0u8].as_slice()).unwrap();
        // Grow A (order-0 page, in-place inserts at any position)
        for k in [1u64, 2, 3, 4] {
            table.insert(&k, [0u8].as_slice()).unwrap();
        }
        // Shrink C's huge value in place; C keeps the huge page and free space
        table.insert(&500u64, [].as_slice()).unwrap();
        // In-place appends into C's huge page up to the cap
        for i in 0..(max_pairs - 1) {
            table.insert(&(1000 + i), [].as_slice()).unwrap();
        }
        // C now has u16::MAX pairs. Re-inflate its LAST value in place.
        let last_key = 1000 + max_pairs - 2;
        table.insert(&last_key, big600.as_slice()).unwrap();
        // Deleting from A makes it partial -> merged with C ->
        // 4 + 65535 = 65539 pairs with the 600KiB value last. The byte-balanced
        // division would be 65538 and must be clamped to 65535.
        assert!(table.remove(&1u64).unwrap().is_some());

        assert_eq!(table.len().unwrap(), 4 + max_pairs);
        assert_eq!(
            table.get(&last_key).unwrap().unwrap().value(),
            big600.as_slice()
        );
        assert_eq!(table.get(&5u64).unwrap().unwrap().value(), &[0u8]);
        assert!(table.get(&1u64).unwrap().is_none());
        assert!(table.get(&1000u64).unwrap().is_some());
    }
    txn.commit().unwrap();

    assert!(db.check_integrity().unwrap());

    drop(db);
    let mut db = Database::create(tmpfile.path()).unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 4 + max_pairs);
    assert_eq!(table.iter().unwrap().count() as u64, 4 + max_pairs);
    drop(table);
    drop(read);
    assert!(db.check_integrity().unwrap());
}

// ------------------------- Fix 3: check_integrity rework -------------------------

#[test]
fn check_integrity_fresh_empty_database() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    assert!(db.check_integrity().unwrap());
    assert!(db.check_integrity().unwrap());
}

#[test]
fn check_integrity_in_memory_backend() {
    let mut db = Database::builder()
        .create_with_backend(redb::backends::InMemoryBackend::new())
        .unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..500u64 {
            table.insert(&k, value_for(k, 30).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..250u64 {
            table.remove(&k).unwrap();
        }
    }
    txn.commit().unwrap();
    assert!(db.check_integrity().unwrap());
    assert!(db.check_integrity().unwrap());
}

#[test]
fn check_integrity_after_compact() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..2000u64 {
            table.insert(&k, value_for(k, 100).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 500..2000u64 {
            table.remove(&k).unwrap();
        }
    }
    txn.commit().unwrap();
    db.compact().unwrap();
    assert!(db.check_integrity().unwrap());
    assert!(db.check_integrity().unwrap());
}

#[test]
fn check_integrity_after_aborted_transaction() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..500u64 {
            table.insert(&k, value_for(k, 50).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..200u64 {
            table.remove(&k).unwrap();
        }
        for k in 1000..1300u64 {
            table.insert(&k, value_for(k, 50).as_slice()).unwrap();
        }
    }
    txn.abort().unwrap();
    assert!(db.check_integrity().unwrap());
    // The aborted changes must not be visible
    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 500);
}

// check_integrity on a healthy database must not modify the file (no spurious
// repair commit)
#[test]
fn check_integrity_clean_db_file_unchanged() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..1000u64 {
            table.insert(&k, value_for(k, 40).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        table.remove(&0u64).unwrap();
    }
    txn.commit().unwrap();

    let before = std::fs::read(tmpfile.path()).unwrap();
    assert!(db.check_integrity().unwrap());
    let after = std::fs::read(tmpfile.path()).unwrap();
    assert_eq!(
        before.len(),
        after.len(),
        "file length changed by check_integrity on a clean database"
    );
    assert!(
        before == after,
        "file bytes changed by check_integrity on a clean database"
    );
}

// An ephemeral savepoint holds references to old pages without holding a
// transaction. check_integrity must neither report a false positive nor
// invalidate the savepoint.
#[test]
fn check_integrity_with_ephemeral_savepoint_then_restore() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..1000u64 {
            table.insert(&k, value_for(k, 60).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let savepoint = txn.ephemeral_savepoint().unwrap();
    txn.abort().unwrap();

    // Mutate heavily after the savepoint, freeing many of its pages
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..900u64 {
            table.remove(&k).unwrap();
        }
        for k in 5000..5500u64 {
            table.insert(&k, value_for(k, 60).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    assert!(
        db.check_integrity().unwrap(),
        "false corruption report while an ephemeral savepoint exists"
    );

    // The savepoint must still be restorable, with intact contents
    let mut txn = db.begin_write().unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    txn.commit().unwrap();
    drop(savepoint);

    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 1000);
    for k in 0..1000u64 {
        assert_eq!(
            table.get(&k).unwrap().unwrap().value(),
            value_for(k, 60).as_slice(),
            "key {k} corrupted after savepoint restore following check_integrity"
        );
    }
    drop(table);
    drop(read);
    assert!(db.check_integrity().unwrap());
}

#[test]
fn check_integrity_with_persistent_savepoint_then_restore() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..1000u64 {
            table.insert(&k, value_for(k, 60).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let savepoint_id = txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..900u64 {
            table.remove(&k).unwrap();
        }
    }
    txn.commit().unwrap();

    assert!(
        db.check_integrity().unwrap(),
        "false corruption report while a persistent savepoint exists"
    );

    let mut txn = db.begin_write().unwrap();
    let savepoint = txn.get_persistent_savepoint(savepoint_id).unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    drop(savepoint);
    txn.commit().unwrap();

    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 1000);
    drop(table);
    drop(read);

    let txn = db.begin_write().unwrap();
    txn.delete_persistent_savepoint(savepoint_id).unwrap();
    txn.commit().unwrap();
    assert!(db.check_integrity().unwrap());
}

// Minimal reproduction: a savepoint restore leaves stale DATA_ALLOCATED_TABLE
// entries behind (pages it queued for freeing stay listed as allocated), and
// check_integrity afterwards must not panic or report corruption.
#[test]
fn check_integrity_after_savepoint_restore() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..1000u64 {
            table.insert(&k, value_for(k, 60).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let savepoint = txn.ephemeral_savepoint().unwrap();
    txn.abort().unwrap();

    // Allocate pages after the savepoint, so they're recorded in the
    // allocated-pages table
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 2000..2500u64 {
            table.insert(&k, value_for(k, 60).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let mut txn = db.begin_write().unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    txn.commit().unwrap();
    drop(savepoint);

    // A couple of commits to drain pending frees
    for _ in 0..3 {
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            t.insert(&1u64, [1u8].as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }

    assert!(
        db.check_integrity().unwrap(),
        "false corruption report after savepoint restore"
    );
}

// Restoring the same savepoint twice must not double-free the pages recorded
// in the allocated-pages table by transactions between the savepoint and the
// first restore.
#[test]
fn restore_same_savepoint_twice() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..1000u64 {
            table.insert(&k, value_for(k, 60).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let savepoint = txn.ephemeral_savepoint().unwrap();
    txn.abort().unwrap();

    // Allocate pages after the savepoint (recorded in the allocated table)
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 2000..2500u64 {
            table.insert(&k, value_for(k, 60).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let mut txn = db.begin_write().unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    txn.commit().unwrap();

    // Let the freed pages get processed and reused by new allocations
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 3000..3500u64 {
            table.insert(&k, value_for(k, 60).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    // Restore the same savepoint again
    let mut txn = db.begin_write().unwrap();
    txn.restore_savepoint(&savepoint).unwrap();
    txn.commit().unwrap();
    drop(savepoint);

    // Drain pending frees and keep using the database
    for i in 0..5u64 {
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(TABLE).unwrap();
            for k in 0..200u64 {
                t.insert(&(10_000 + i * 1000 + k), value_for(k, 60).as_slice())
                    .unwrap();
            }
        }
        txn.commit().unwrap();
    }

    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 1000 + 5 * 200);
    for k in 0..1000u64 {
        assert_eq!(
            table.get(&k).unwrap().unwrap().value(),
            value_for(k, 60).as_slice(),
            "key {k} corrupted after double savepoint restore"
        );
    }
    drop(table);
    drop(read);
    drop(db);

    let mut db = Database::create(tmpfile.path()).unwrap();
    assert!(db.check_integrity().unwrap());
}

// Simplest trigger: create a persistent savepoint, churn pages, delete the
// savepoint, then run check_integrity. No restore involved.
#[test]
fn check_integrity_after_persistent_savepoint_delete() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..1000u64 {
            table.insert(&k, value_for(k, 60).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    let savepoint_id = txn.persistent_savepoint().unwrap();
    txn.commit().unwrap();

    // Allocate pages (recorded in the allocated-pages table because a
    // savepoint exists) ...
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 2000..2500u64 {
            table.insert(&k, value_for(k, 60).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
    // ... and free them again
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 2000..2500u64 {
            table.remove(&k).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_write().unwrap();
    txn.delete_persistent_savepoint(savepoint_id).unwrap();
    txn.commit().unwrap();

    assert!(
        db.check_integrity().unwrap(),
        "false corruption report after deleting a persistent savepoint"
    );
}

// A non-durable commit is part of the live state of the database. A
// healthy-database integrity check must not silently roll it back.
#[test]
fn check_integrity_preserves_non_durable_commit() {
    let tmpfile = create_tempfile();
    let mut db = Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..100u64 {
            table.insert(&k, value_for(k, 20).as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    let mut txn = db.begin_write().unwrap();
    txn.set_durability(Durability::None).unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        table.insert(&7777u64, [42u8].as_slice()).unwrap();
        table.remove(&0u64).unwrap();
    }
    txn.commit().unwrap();

    // The non-durable commit is visible before the check
    {
        let read = db.begin_read().unwrap();
        let table = read.open_table(TABLE).unwrap();
        assert_eq!(table.get(&7777u64).unwrap().unwrap().value(), &[42u8]);
        assert!(table.get(&0u64).unwrap().is_none());
    }

    let clean = db.check_integrity().unwrap();
    assert!(clean, "healthy db reported as not clean");

    // ... and must still be visible afterwards
    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert!(
        table.get(&7777u64).unwrap().is_some(),
        "check_integrity silently rolled back a non-durable commit (insert lost)"
    );
    assert!(
        table.get(&0u64).unwrap().is_none(),
        "check_integrity silently rolled back a non-durable commit (remove undone)"
    );
}
