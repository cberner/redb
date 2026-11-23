//! A corrupted branch pointer must not send the btree descent into unbounded recursion. That
//! overflows the stack, which aborts the process rather than unwinding, so it cannot be reported
//! as an error or caught by the caller. See https://github.com/cberner/redb/issues/1332

use redb::{ReadOnlyDatabase, ReadableDatabase, ReadableTable, StorageError, TableDefinition};
use std::ops::Bound;
use std::path::Path;

const T: TableDefinition<u64, u64> = TableDefinition::new("t");
const BYTES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bytes");
const LEAF: u8 = 1;
const BRANCH: u8 = 2;
const PAGE_SIZE: usize = 4096;

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

// In u64 because a full region is 4GiB, which overflows a 32 bit usize (wasm32)
fn region_size(data: &[u8]) -> u64 {
    let region_header_pages = u64::from(u32::from_le_bytes(data[16..20].try_into().unwrap()));
    let region_max_data_pages = u64::from(u32::from_le_bytes(data[20..24].try_into().unwrap()));
    (region_header_pages + region_max_data_pages) * PAGE_SIZE as u64
}

// Offsets of every page that looks like a branch. Some belong to the system tree, and some are
// stale pages left by earlier commits; the caller sorts out which are usable.
fn branch_page_offsets(data: &[u8]) -> Vec<usize> {
    let mut offsets = vec![];
    // The super-header occupies the first page
    let mut offset = PAGE_SIZE;
    while offset + PAGE_SIZE <= data.len() {
        let page = &data[offset..offset + PAGE_SIZE];
        let num_keys = u16::from_le_bytes([page[2], page[3]]) as usize;
        if page[0] == BRANCH && num_keys > 0 && 8 + 24 * (num_keys + 1) <= PAGE_SIZE {
            offsets.push(offset);
        }
        offset += PAGE_SIZE;
    }
    offsets
}

// Points every child pointer of a branch page at the branch page itself, so that a descent in any
// direction hits the cycle. Reads do not verify checksums, so the stale checksum does not matter.
fn make_self_referential(data: &mut [u8], offset: usize) {
    let region_size = region_size(data);
    let relative = (offset - PAGE_SIZE) as u64;
    let region = relative / region_size;
    let index = (relative % region_size) / PAGE_SIZE as u64;
    // order 0, so the page number is just the region and index
    let self_reference = (region << 20) | index;

    let page = &mut data[offset..offset + PAGE_SIZE];
    let num_keys = u16::from_le_bytes([page[2], page[3]]) as usize;
    // Child pointers follow the per-child checksums, which follow an 8 byte header
    let children = 8 + 16 * (num_keys + 1);
    for i in 0..=num_keys {
        let child = children + 8 * i;
        page[child..child + 8].copy_from_slice(&self_reference.to_le_bytes());
    }
}

// Makes one child pointer unreadable by corrupting its page-order bits: descending into it
// reports Corrupted, without latching an I/O failure
fn corrupt_child_pointer(data: &mut [u8], offset: usize, child: usize) {
    let page = &mut data[offset..offset + PAGE_SIZE];
    let num_keys = u16::from_le_bytes([page[2], page[3]]) as usize;
    assert!(child <= num_keys);
    // Child pointers follow the per-child checksums, which follow an 8 byte header
    let children = 8 + 16 * (num_keys + 1);
    let pointer = children + 8 * child;
    // The page order lives in the top bits of the 64 bit page number
    page[pointer + 7] |= 0xF8;
}

fn opens_cleanly(path: &Path) -> bool {
    let Ok(db) = ReadOnlyDatabase::open(path) else {
        return false;
    };
    let Ok(txn) = db.begin_read() else {
        return false;
    };
    txn.open_table(T).is_ok()
}

// Opened read-only, because a debug-assertions build of the writable open walks every page while
// seeding its allocator assertions, and rejects the cycle there. This test is about the descent
// itself, which the read-only open reaches without that walk.
#[test]
fn cyclic_branch_pointer_is_reported_rather_than_overflowing_the_stack() {
    let tmpfile = create_tempfile();
    {
        let db = redb::Database::create(tmpfile.path()).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(T).unwrap();
            // Enough entries that the table's tree has branch pages
            for i in 0..5_000u64 {
                table.insert(&i, &i).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    let pristine = std::fs::read(tmpfile.path()).unwrap();
    let candidates = branch_page_offsets(&pristine);
    assert!(
        !candidates.is_empty(),
        "no branch page was found to corrupt"
    );

    // Keep the branch pages that are not in the system tree, which opening the database walks
    let mut usable = vec![];
    for &offset in &candidates {
        let mut data = pristine.clone();
        make_self_referential(&mut data, offset);
        std::fs::write(tmpfile.path(), &data).unwrap();
        if opens_cleanly(tmpfile.path()) {
            usable.push(offset);
        }
    }
    assert!(
        !usable.is_empty(),
        "every branch page was in the system tree"
    );

    // Corrupt all of them at once, so any descent into the table hits a cycle immediately
    let mut data = pristine.clone();
    for &offset in &usable {
        make_self_referential(&mut data, offset);
    }
    std::fs::write(tmpfile.path(), &data).unwrap();

    let db = ReadOnlyDatabase::open(tmpfile.path()).unwrap();
    let txn = db.begin_read().unwrap();
    let table = txn.open_table(T).unwrap();

    // Each of these descends from the root, and must give up rather than recurse forever
    assert!(matches!(table.get(&0u64), Err(StorageError::Corrupted(_))));
    assert!(matches!(table.first(), Err(StorageError::Corrupted(_))));
    assert!(matches!(table.last(), Err(StorageError::Corrupted(_))));
    // The cursors build lazily, so the descent may be reported by the first step instead
    match table.range(0u64..) {
        Err(StorageError::Corrupted(_)) => {}
        Ok(mut range) => assert!(matches!(
            range.next(),
            Some(Err(StorageError::Corrupted(_)))
        )),
        Err(err) => panic!("expected Corrupted, got {err:?}"),
    }
    match table.iter() {
        Err(StorageError::Corrupted(_)) => {}
        Ok(mut iter) => assert!(matches!(iter.next(), Some(Err(StorageError::Corrupted(_))))),
        Err(err) => panic!("expected Corrupted, got {err:?}"),
    }
}

// Resuming iteration after an error must keep reporting the error, not skip the unreadable
// subtree and yield the rest of the table as if nothing were missing
#[test]
fn iteration_does_not_resume_past_corruption() {
    let tmpfile = create_tempfile();
    {
        let db = redb::Database::create(tmpfile.path()).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(T).unwrap();
            // Enough entries that the table's tree has branch pages
            for i in 0..5_000u64 {
                table.insert(&i, &i).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    let pristine = std::fs::read(tmpfile.path()).unwrap();
    let candidates = branch_page_offsets(&pristine);
    assert!(
        !candidates.is_empty(),
        "no branch page was found to corrupt"
    );

    // Corrupt the second child of one branch at a time; candidates may be stale or unreachable
    // pages, so use the first that makes the iteration fail part way through
    for &offset in &candidates {
        let mut data = pristine.clone();
        corrupt_child_pointer(&mut data, offset, 1);
        std::fs::write(tmpfile.path(), &data).unwrap();
        if !opens_cleanly(tmpfile.path()) {
            continue;
        }

        let db = ReadOnlyDatabase::open(tmpfile.path()).unwrap();
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(T).unwrap();

        let mut iter = table.iter().unwrap();
        let mut yielded = 0u64;
        let mut saw_error = false;
        for entry in &mut iter {
            match entry {
                Ok(_) => yielded += 1,
                Err(_) => {
                    saw_error = true;
                    break;
                }
            }
        }
        if !saw_error {
            // This branch page was not reachable by the iteration; try another
            assert_eq!(yielded, 5_000);
            continue;
        }

        assert!(yielded > 0 && yielded < 5_000, "yielded {yielded}");
        // Yielding entries again would mean the unreadable subtree was skipped, and a bare end
        // would claim the table is complete
        for _ in 0..10 {
            match iter.next() {
                Some(Err(_)) => {}
                Some(Ok(entry)) => panic!(
                    "iterator resumed past the corruption at key {}",
                    entry.0.value()
                ),
                None => panic!("iterator ended as if complete after {yielded} of 5000 entries"),
            }
        }
        return;
    }
    panic!("no corruptible branch page was reachable by iteration");
}

// An error on one iteration end must latch the whole range: the other end runs on an
// independent cursor and would otherwise keep yielding entries
#[test]
fn iteration_from_both_ends_latches_after_error() {
    let tmpfile = create_tempfile();
    {
        let db = redb::Database::create(tmpfile.path()).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(T).unwrap();
            for i in 0..5_000u64 {
                table.insert(&i, &i).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    let pristine = std::fs::read(tmpfile.path()).unwrap();
    let candidates = branch_page_offsets(&pristine);
    assert!(
        !candidates.is_empty(),
        "no branch page was found to corrupt"
    );

    // Iterate backward into the corruption, then continue forward: the front cursor is
    // untouched by the back cursor's error, so only the range-level latch stops it.
    for &offset in &candidates {
        let mut data = pristine.clone();
        corrupt_child_pointer(&mut data, offset, 1);
        std::fs::write(tmpfile.path(), &data).unwrap();
        if !opens_cleanly(tmpfile.path()) {
            continue;
        }

        let db = ReadOnlyDatabase::open(tmpfile.path()).unwrap();
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(T).unwrap();

        let mut iter = table.iter().unwrap();
        let mut yielded = 0u64;
        let mut saw_error = false;
        while let Some(entry) = iter.next_back() {
            match entry {
                Ok(_) => yielded += 1,
                Err(_) => {
                    saw_error = true;
                    break;
                }
            }
        }
        if !saw_error {
            // This branch page was not reachable by the iteration; try another
            assert_eq!(yielded, 5_000);
            continue;
        }

        assert!(yielded > 0 && yielded < 5_000, "yielded {yielded}");
        // Both ends must report the error from here on
        for _ in 0..3 {
            match iter.next() {
                Some(Err(_)) => {}
                Some(Ok(entry)) => panic!(
                    "front end continued after the back end's error, at key {}",
                    entry.0.value()
                ),
                None => panic!("iterator ended as if complete after {yielded} of 5000 entries"),
            }
            assert!(matches!(iter.next_back(), Some(Err(_))));
        }
        return;
    }
    panic!("no corruptible branch page was reachable by iteration");
}

fn page_offset(data: &[u8], page_number: u64) -> usize {
    let index = page_number & 0xF_FFFF;
    let region = (page_number >> 20) & 0xF_FFFF;
    usize::try_from(PAGE_SIZE as u64 + region * region_size(data) + index * PAGE_SIZE as u64)
        .unwrap()
}

fn page_number_at(data: &[u8], offset: usize) -> u64 {
    let relative = (offset - PAGE_SIZE) as u64;
    let region_size = region_size(data);
    // order 0, so the page number is just the region and index
    ((relative / region_size) << 20) | ((relative % region_size) / PAGE_SIZE as u64)
}

// The child page numbers of the branch page at `offset`
fn branch_children(data: &[u8], offset: usize) -> Vec<u64> {
    let page = &data[offset..offset + PAGE_SIZE];
    let num_keys = u16::from_le_bytes([page[2], page[3]]) as usize;
    // Child pointers follow the per-child checksums, which follow an 8 byte header
    let children = 8 + 16 * (num_keys + 1);
    (0..=num_keys)
        .map(|i| {
            let child = children + 8 * i;
            u64::from_le_bytes(page[child..child + 8].try_into().unwrap())
        })
        .collect()
}

// A branch page with one key and both children `child`. Child checksums are left zero, since
// reads do not verify them.
fn write_doubled_branch(data: &mut [u8], offset: usize, child: u64) {
    let page = &mut data[offset..offset + PAGE_SIZE];
    page.fill(0);
    page[0] = BRANCH;
    page[2..4].copy_from_slice(&1u16.to_le_bytes());
    page[40..48].copy_from_slice(&child.to_le_bytes());
    page[48..56].copy_from_slice(&child.to_le_bytes());
    // The end offset of the only key, which is one byte long
    page[56..60].copy_from_slice(&61u32.to_le_bytes());
    page[60] = b'k';
}

fn write_empty_leaf(data: &mut [u8], offset: usize) {
    let page = &mut data[offset..offset + PAGE_SIZE];
    page.fill(0);
    page[0] = LEAF;
}

fn expect_corrupted<T>(entry: Option<Result<T, StorageError>>) {
    match entry {
        Some(Err(StorageError::Corrupted(_))) => {}
        Some(Err(err)) => panic!("expected Corrupted, got {err:?}"),
        Some(Ok(_)) => panic!("expected Corrupted, got an entry"),
        None => panic!("expected Corrupted, got the end of the table"),
    }
}

// Branch pages that share their children make a tree into a DAG, which a scan walks once per path
// through it: 2^levels empty leaves for this chain. The scan must give up once it has visited more
// pages than the file holds, rather than run for an exponential time without returning.
#[test]
fn shared_branch_children_do_not_make_scans_exponential() {
    const LEVELS: usize = 40;
    let tmpfile = create_tempfile();
    {
        let db = redb::Database::create(tmpfile.path()).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(BYTES).unwrap();
            // Large values, so the table's branches have many leaf children
            for i in 0..1_000u32 {
                table
                    .insert(format!("k{i:06}").as_bytes(), &[7u8; 1000][..])
                    .unwrap();
            }
        }
        txn.commit().unwrap();
    }

    let pristine = std::fs::read(tmpfile.path()).unwrap();
    // Candidates may be stale pages, so try each until one is reached by the scan
    for offset in branch_page_offsets(&pristine) {
        let children = branch_children(&pristine, offset);
        let child_offsets: Vec<usize> = children
            .iter()
            .map(|&child| page_offset(&pristine, child))
            .collect();
        if children.len() <= LEVELS
            || children.iter().any(|&child| child >> 59 != 0)
            || child_offsets
                .iter()
                .any(|&o| o + PAGE_SIZE > pristine.len() || pristine[o] != LEAF)
        {
            continue;
        }

        // The branch, then its first LEVELS children, each a branch whose two children are the
        // next page, ending in an empty leaf
        let mut data = pristine.clone();
        let mut chain = vec![offset];
        chain.extend_from_slice(&child_offsets[..LEVELS]);
        for pair in chain.windows(2) {
            let next = page_number_at(&data, pair[1]);
            write_doubled_branch(&mut data, pair[0], next);
        }
        write_empty_leaf(&mut data, chain[LEVELS]);
        std::fs::write(tmpfile.path(), &data).unwrap();
        // The page may be in the system tree, which opening the table walks
        let Ok(db) = ReadOnlyDatabase::open(tmpfile.path()) else {
            continue;
        };
        let txn = db.begin_read().unwrap();
        let Ok(table) = txn.open_table(BYTES) else {
            continue;
        };
        let mut iter = table.iter().unwrap();
        let outcome = iter.by_ref().find(Result::is_err);
        let Some(outcome) = outcome else {
            // This branch page was not reachable by the scan; try another
            continue;
        };
        expect_corrupted(Some(outcome));
        expect_corrupted(table.iter().unwrap().rev().find(Result::is_err));
        return;
    }
    panic!("no branch page reachable by the scan had enough leaf children");
}

// Moving a cursor back and forth across a leaf boundary loads the same pages again and again. That
// is the caller's doing, not a sign of shared pages, so it must not exhaust the scan's page budget.
#[test]
fn cursor_reversals_do_not_exhaust_the_page_budget() {
    let tmpfile = create_tempfile();
    let db = redb::Database::create(tmpfile.path()).unwrap();
    let txn = db.begin_write().unwrap();
    let keys: Vec<String> = (0..1_000u32).map(|i| format!("k{i:06}")).collect();
    {
        let mut table = txn.open_table(BYTES).unwrap();
        // Large values, so there are few entries per leaf and many leaf boundaries
        for key in &keys {
            table.insert(key.as_bytes(), &[7u8; 1000][..]).unwrap();
        }
    }
    txn.commit().unwrap();

    let txn = db.begin_read().unwrap();
    let table = txn.open_table(BYTES).unwrap();
    let mut cursor = table.lower_bound(Bound::<&[u8]>::Unbounded).unwrap();
    assert_eq!(
        cursor.next().unwrap().unwrap().0.value(),
        keys[0].as_bytes()
    );
    for pair in keys.windows(2) {
        let (previous, key) = (pair[0].as_bytes(), pair[1].as_bytes());
        assert_eq!(cursor.next().unwrap().unwrap().0.value(), key);
        // When `previous` and `key` are in different leaves, each round trip loads both again
        for _ in 0..50 {
            assert_eq!(cursor.prev().unwrap().unwrap().0.value(), key);
            assert_eq!(cursor.prev().unwrap().unwrap().0.value(), previous);
            assert_eq!(cursor.next().unwrap().unwrap().0.value(), previous);
            assert_eq!(cursor.next().unwrap().unwrap().0.value(), key);
        }
    }
    assert!(cursor.next().unwrap().is_none());
}
