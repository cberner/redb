//! A corrupted branch pointer must not send the btree descent into unbounded recursion. That
//! overflows the stack, which aborts the process rather than unwinding, so it cannot be reported
//! as an error or caught by the caller. See https://github.com/cberner/redb/issues/1332

use redb::{ReadOnlyDatabase, ReadableDatabase, ReadableTable, StorageError, TableDefinition};
use std::path::Path;

const T: TableDefinition<u64, u64> = TableDefinition::new("t");
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
