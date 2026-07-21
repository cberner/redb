// The deprecated ReadOnlyTable and ReadOnlyMultimapTable accessors are exercised throughout
// these tests; they remain covered until they are removed.
#![allow(deprecated)]

//! Tests for `check_integrity()` with a pending `Durability::None` commit. The check promotes such
//! a commit to durable when the live state verifies (so acknowledged data is not lost), verifying
//! the live state from disk; it refuses to promote when the backing file was externally truncated
//! or extended, falling back to repairing the durable state instead.

use redb::{
    Database, Durability, ReadableDatabase, ReadableTableMetadata, StorageBackend, TableDefinition,
};
use std::sync::{Arc, RwLock};

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("t");

// A backend backed by a shared Vec<u8> so the test can patch arbitrary on-disk bytes, simulating
// external file modification / corruption.
#[derive(Clone, Debug, Default)]
struct PatchBackend {
    inner: Arc<RwLock<Vec<u8>>>,
}

impl PatchBackend {
    fn file_len(&self) -> usize {
        self.inner.read().unwrap().len()
    }
    fn truncate(&self, len: usize) {
        self.inner.write().unwrap().truncate(len);
    }
    fn extend(&self, additional: usize) {
        let mut guard = self.inner.write().unwrap();
        let new_len = guard.len() + additional;
        guard.resize(new_len, 0);
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

// The live state must be verified from disk, not the page cache, before promoting a non-durable
// commit. Here a page reachable only from the non-durable commit is corrupt on disk but still
// cached as its good version; promoting it would make a state that is corrupt on disk durable. The
// check must detect it from disk and roll back to the durable state instead.
#[test]
fn check_integrity_does_not_promote_disk_corrupt_nondurable_commit() {
    let backend = PatchBackend::default();
    let mut db = make_db(backend.clone(), 5);

    // A pending non-durable commit that writes a uniquely-tagged page.
    let marker = vec![0xC7u8; 2000];
    {
        let mut txn = db.begin_write().unwrap();
        txn.set_durability(Durability::None).unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            table.insert(&999u64, marker.as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }

    // Corrupt that page on disk; redb's cache still holds the good copy.
    assert!(
        backend.corrupt_first_occurrence(&marker),
        "could not locate the value on disk to corrupt"
    );

    assert!(
        !matches!(db.check_integrity(), Ok(true)),
        "promoted a non-durable commit that is corrupt on disk"
    );

    // The durable baseline is intact and the non-durable commit was rolled back.
    drop(db);
    let mut reopened = Database::builder().create_with_backend(backend).unwrap();
    assert!(reopened.check_integrity().unwrap());
    let read = reopened.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 5);
    assert!(table.get(&999u64).unwrap().is_none());
}

// A non-durable commit that grows the file is acknowledged data; a passing check must promote it
// to durable, not lose it.
#[test]
fn check_integrity_preserves_growing_nondurable_commit() {
    let backend = PatchBackend::default();
    let mut db = make_db(backend.clone(), 1);

    let len_before = backend.file_len();
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
        backend.file_len() > len_before,
        "the non-durable commit was expected to grow the file"
    );

    assert!(db.check_integrity().unwrap());

    // The promoted commit must survive reopening, since the check made it durable.
    drop(db);
    let db = Database::builder().create_with_backend(backend).unwrap();
    let read = db.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 2001);
    assert_eq!(
        table.get(&2500u64).unwrap().unwrap().value(),
        [0xEEu8; 2000]
    );
}

// If an external writer extends the file while a non-durable commit is pending, the file is longer
// than the in-memory layout. check_integrity() must not promote the commit then (a layout
// inconsistent with the file would corrupt subsequent opens). It must detect the mismatch and leave
// the database usable and consistent.
#[test]
fn check_integrity_handles_external_extension_with_pending_commit() {
    let backend = PatchBackend::default();
    let mut db = make_db(backend.clone(), 5);

    {
        let mut txn = db.begin_write().unwrap();
        txn.set_durability(Durability::None).unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            table.insert(&7777u64, [0x99u8; 100].as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }

    // External extension to a longer file (one full region's worth of bytes).
    backend.extend(4 * 1024 * 1024);

    assert!(
        !matches!(db.check_integrity(), Ok(true)),
        "external file extension reported as clean"
    );

    // The durable data must still be intact, and the database usable for further writes.
    {
        let read = db.begin_read().unwrap();
        let table = read.open_table(TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 5);
    }
    {
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            table.insert(&1u64, [0x11u8; 64].as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }
    // A fresh check must now pass: recovery left a consistent state.
    assert!(db.check_integrity().unwrap());
}

// If an external writer truncates the file after a non-durable commit grew it, the file is shorter
// than the in-memory layout. check_integrity() must not promote the commit -- doing so would commit
// a layout pointing past the end of the file. It must detect the mismatch and leave the durable
// state recoverable.
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

    // External truncation back to the last durable length.
    backend.truncate(durable_len);

    assert!(
        !matches!(db.check_integrity(), Ok(true)),
        "truncation after non-durable growth reported as clean"
    );
    drop(db);

    // The durable state must still be recoverable on reopen.
    let mut reopened = Database::builder().create_with_backend(backend).unwrap();
    assert!(reopened.check_integrity().unwrap());
    let read = reopened.begin_read().unwrap();
    let table = read.open_table(TABLE).unwrap();
    assert_eq!(table.len().unwrap(), 1);
}
