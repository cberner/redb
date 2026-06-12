//! Regression test: `check_integrity()` must not panic when repairing a corrupt database that has
//! a pending `Durability::None` commit. The non-durable flush can drive the repair's allocator
//! rebuild to walk a freed tree containing an out-of-range page number; that must be reported as
//! corruption (a graceful `Err`/`Ok(false)`), not panic in the allocator bitmap.

use redb::{
    Database, Durability, ReadableDatabase, ReadableTableMetadata, StorageBackend, TableDefinition,
};
use std::sync::{Arc, RwLock};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("t");

// Offset of the trailing-region page count (a layout field) in the database header.
const TRAILING_REGION_DATA_PAGES_OFFSET: usize = 28;

// A backend backed by a shared Vec<u8> so the test can patch arbitrary on-disk bytes, simulating
// external file modification / corruption.
#[derive(Clone, Debug, Default)]
struct PatchBackend {
    inner: Arc<RwLock<Vec<u8>>>,
}

impl PatchBackend {
    fn read_u32(&self, offset: usize) -> u32 {
        let guard = self.inner.read().unwrap();
        u32::from_le_bytes(guard[offset..offset + 4].try_into().unwrap())
    }
    fn write_u32(&self, offset: usize, value: u32) {
        self.inner.write().unwrap()[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }
    fn file_len(&self) -> usize {
        self.inner.read().unwrap().len()
    }
    fn truncate(&self, len: usize) {
        self.inner.write().unwrap().truncate(len);
    }
    // Flip all bits in the first occurrence of `needle`. Returns whether anything was patched.
    fn corrupt_first_occurrence(&self, needle: &[u8]) -> bool {
        let mut guard = self.inner.write().unwrap();
        let mut i = 0;
        while i + needle.len() <= guard.len() {
            if &guard[i..i + needle.len()] == needle {
                for b in &mut guard[i..i + needle.len()] {
                    *b ^= 0xFF;
                }
                return true;
            }
            i += 1;
        }
        false
    }
}

impl StorageBackend for PatchBackend {
    fn len(&self) -> Result<u64, std::io::Error> {
        Ok(self.inner.read().unwrap().len() as u64)
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
        let offset = usize::try_from(offset).unwrap();
        let guard = self.inner.read().unwrap();
        if offset + out.len() > guard.len() {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        out.copy_from_slice(&guard[offset..offset + out.len()]);
        Ok(())
    }
    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.inner
            .write()
            .unwrap()
            .resize(len.try_into().unwrap(), 0);
        Ok(())
    }
    fn sync_data(&self) -> Result<(), std::io::Error> {
        Ok(())
    }
    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        let offset = usize::try_from(offset).unwrap();
        let mut guard = self.inner.write().unwrap();
        if offset + data.len() > guard.len() {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        guard[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }
}

fn make_db(backend: PatchBackend, n: u64) -> Database {
    let db = Database::builder().create_with_backend(backend).unwrap();
    let txn = db.begin_write().unwrap();
    {
        let mut table = txn.open_table(TABLE).unwrap();
        for k in 0..n {
            let v = vec![(k as u8).wrapping_add(0xA0); 64];
            table.insert(&k, v.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();
    db
}

#[test]
fn check_integrity_corrupt_repair_with_pending_nondurable_commit_does_not_panic() {
    let backend = PatchBackend::default();
    let db = make_db(backend.clone(), 10);

    let durable_val = vec![0xC2u8; 64];
    {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            table.insert(&2u64, durable_val.as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }
    // A pending non-durable commit on a different key, so the durable value's leaf stays reachable
    // from the live root and the non-durable state is what check_integrity() flushes.
    {
        let mut txn = db.begin_write().unwrap();
        txn.set_durability(Durability::None).unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            table.insert(&8u64, [0x88u8; 64].as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }

    assert!(
        backend.corrupt_first_occurrence(&durable_val),
        "could not locate the value on disk to corrupt"
    );

    // check_integrity() must report the corruption gracefully, never panic and never falsely
    // report the corrupt database as clean.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut db = db;
        let r = db.check_integrity();
        // The database is corrupt; its Drop may itself fail, so leak it rather than risk a
        // double panic obscuring the result.
        std::mem::forget(db);
        r
    }));

    match result {
        Err(_) => panic!("check_integrity() panicked instead of reporting corruption"),
        Ok(Ok(true)) => panic!("check_integrity() reported a corrupt database as clean"),
        Ok(Ok(false)) | Ok(Err(_)) => {}
    }
}

// When a non-durable commit is pending, check_integrity() makes it durable by writing the
// in-memory header before reloading. The unchecksummed layout fields (region count / trailing
// region size) must still be validated against the durable header, so external corruption of
// those bytes is reported rather than overwritten and masked as clean.
fn check_integrity_reports_corrupt_layout_field(delta: i64) {
    let backend = PatchBackend::default();
    let db = make_db(backend.clone(), 50);
    // A small pending non-durable commit that does not grow the file, so the in-memory layout
    // still matches the durable header.
    {
        let mut txn = db.begin_write().unwrap();
        txn.set_durability(Durability::None).unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            table.insert(&0u64, [0x99u8; 100].as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }

    let original = backend.read_u32(TRAILING_REGION_DATA_PAGES_OFFSET);
    let corrupted = u32::try_from((i64::from(original) + delta).max(0)).unwrap();
    assert_ne!(original, corrupted, "test did not change the layout field");
    backend.write_u32(TRAILING_REGION_DATA_PAGES_OFFSET, corrupted);

    let mut db = db;
    if let Ok(true) = db.check_integrity() {
        panic!("externally corrupted layout field reported as clean");
    }
}

#[test]
fn check_integrity_reports_layout_field_corrupted_larger() {
    check_integrity_reports_corrupt_layout_field(1);
}

#[test]
fn check_integrity_reports_layout_field_corrupted_smaller() {
    check_integrity_reports_corrupt_layout_field(-1);
}

// A non-durable commit that grows the file moves the in-memory layout legitimately ahead of the
// durable header; check_integrity() must still flush and preserve it, not mistake it for
// corruption.
#[test]
fn check_integrity_preserves_growing_nondurable_commit() {
    let backend = PatchBackend::default();
    let mut db = make_db(backend.clone(), 1);

    let len_before = backend.inner.read().unwrap().len();
    {
        let mut txn = db.begin_write().unwrap();
        txn.set_durability(Durability::None).unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for k in 1000..3000u64 {
                table.insert(&k, vec![0xEEu8; 2000].as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }
    assert!(
        backend.inner.read().unwrap().len() > len_before,
        "the non-durable commit was expected to grow the file"
    );

    assert!(db.check_integrity().unwrap());

    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 2001);
    assert_eq!(
        table.get(&2500u64).unwrap().unwrap().value(),
        [0xEEu8; 2000]
    );
}

// If an external writer truncates the file after a non-durable commit grew it, the in-memory
// (grown) layout is ahead of the file. check_integrity() must not flush that grown layout over the
// durable header -- doing so would commit a layout pointing past the end of the file, destroying
// the recoverable durable state. It must instead detect the mismatch and leave the durable state
// recoverable.
#[test]
fn check_integrity_detects_truncation_after_nondurable_growth() {
    let backend = PatchBackend::default();
    let mut db = make_db(backend.clone(), 1);
    let durable_len = backend.file_len();

    {
        let mut txn = db.begin_write().unwrap();
        txn.set_durability(Durability::None).unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for k in 1000..3000u64 {
                table.insert(&k, vec![0xEEu8; 2000].as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }
    assert!(
        backend.file_len() > durable_len,
        "the non-durable commit was expected to grow the file"
    );

    // External truncation back to the last durable length (the grown pages remain cached, so the
    // live checksum pass still succeeds).
    backend.truncate(durable_len);

    if let Ok(true) = db.check_integrity() {
        panic!("truncation after non-durable growth reported as clean");
    }
    drop(db);

    // The durable state must still be recoverable on reopen.
    let mut reopened = Database::builder().create_with_backend(backend).unwrap();
    assert!(reopened.check_integrity().unwrap());
    let read = reopened.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 1);
}
