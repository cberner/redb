//! Cross-process coordination over the byte-range locks of docs/design.md's multi-process
//! locking protocol. Apart from the header lock, every offset here lies far past any possible
//! end of file: a pure namespace that never touches data.
//!
//! The locks are taken through the open file description the participant's storage I/O goes
//! through. That is what makes distinct participants contend, in one process or in many, and
//! is load-bearing on Windows, where the locks are mandatory and only the description holding
//! the header lock can perform the header I/O it serializes.

use crate::db::ConcurrencyMode;
use crate::transaction_tracker::TransactionId;
use crate::tree_store::TransactionalMemory;
use crate::{DatabaseError, Result, StorageError};
use std::sync::Arc;

// From inside a running handle, a reload's DatabaseError variants are all storage failures
fn reload_error(error: DatabaseError) -> StorageError {
    match error {
        DatabaseError::Storage(storage) => storage,
        other => StorageError::Corrupted(other.to_string()),
    }
}

/// Coordinates this handle with the other processes using the database file. One per
/// multi-process handle open for writing; the storage layer knows nothing of it.
pub(crate) struct Coordinator {
    mode: ConcurrencyMode,
}

impl Coordinator {
    pub(crate) fn new(mode: ConcurrencyMode) -> Self {
        assert!(!matches!(mode, ConcurrencyMode::SingleProcess));
        Self { mode }
    }

    /// True when this process holds the writer byte for as long as it is open, so nothing can
    /// change the file behind its back and its own reads need no pins.
    fn sole_lifetime_writer(&self) -> bool {
        matches!(self.mode, ConcurrencyMode::SingleWriterProcess)
    }

    /// True when the writer byte is taken per transaction, so another process can take over
    /// between this one's transactions and everything left in memory has to be assumed stale.
    pub(crate) fn multi_writer(&self) -> bool {
        matches!(self.mode, ConcurrencyMode::MultiWriterProcess)
    }

    /// Makes this handle's state current so a write transaction can start, and returns the last
    /// transaction any process has committed -- the id the new transaction is numbered from.
    ///
    /// The caller must already hold the writer byte: what this loads is only stable while no
    /// other process can commit.
    pub(crate) fn refresh_for_write(
        &self,
        mem: &Arc<TransactionalMemory>,
    ) -> Result<TransactionId> {
        if self.sole_lifetime_writer() {
            return mem.get_last_committed_transaction_id();
        }
        // Not the in-memory header: a read transaction on this handle adopts a peer's header
        // without loading the allocator it describes
        let loaded = mem.loaded_transaction_id()?;
        // From the file, under a shared header hold: a peer may have committed behind this
        // process's copy
        let last_committed = {
            let header = mem.lock_header_shared()?;
            mem.reload_header(&header)?
        };
        // An unchanged id means the file is exactly the state this process already has;
        // otherwise the layout and the allocator are reloaded too
        if last_committed != loaded {
            mem.reload_for_write().map_err(reload_error)?;
            crate::Database::load_or_rebuild_allocator_state(mem).map_err(reload_error)?;
        }

        Ok(last_committed)
    }

    /// Whether finishing the close -- the allocator record and the clean-shutdown header -- is
    /// this handle's to do: only the last writer may, since another process may be mid-commit.
    /// Decided by claiming the mode byte exclusively, after dropping this handle's own shared
    /// claim. An open that finds the byte claimed is refused, as by a single-writer holder.
    pub(crate) fn take_sole_writer_for_close(&self, mem: &TransactionalMemory) -> bool {
        if !self.multi_writer() {
            // Single-writer already holds the byte exclusively, for this handle's whole life
            return true;
        }
        mem.claim_sole_writer()
    }
}

/// Opens or creates a database writable in one of the multi-process modes.
pub(crate) fn open_writable(
    path: &std::path::Path,
    create: bool,
    params: &crate::db::OpenParams<'_>,
) -> Result<crate::Database, DatabaseError> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .truncate(false)
        .open(path)?;
    let coordinator = Arc::new(Coordinator::new(params.concurrency_mode));
    let backend = crate::tree_store::file_backend::FileBackend::new(file)?;

    // The emptiness check happens inside, under the mode bytes and the writer byte, since out
    // here a file a peer is creating looks uninitialized
    let db = crate::Database::new_multiprocess(Box::new(backend), create, params, &coordinator)?;
    if coordinator.multi_writer() {
        db.get_memory().release_open_writer_hold()?;
    }
    Ok(db)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::{SHARED_READER_BYTE, SHARED_WRITER_BYTE, TXN_BASE, WRITER_BYTE, byte_range};
    use crate::tree_store::file_backend::range_lock::RangeLock;
    use std::fs::{File, OpenOptions};
    use std::path::Path;

    fn reopen(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
    }

    #[test]
    fn the_shared_writer_byte_negotiates_the_mode() {
        let tmpfile = crate::create_tempfile();
        let sole = reopen(tmpfile.path());
        let other = reopen(tmpfile.path());

        // A sole writer holds the byte exclusively, so a would-be co-writer is refused ...
        assert!(sole.try_lock_range(byte_range(SHARED_WRITER_BYTE)).unwrap());
        assert!(
            !other
                .try_lock_shared_range(byte_range(SHARED_WRITER_BYTE))
                .unwrap()
        );
        sole.unlock_range(byte_range(SHARED_WRITER_BYTE)).unwrap();

        // ... and a multi-writer cohort holds it shared, refusing a sole writer
        assert!(
            other
                .try_lock_shared_range(byte_range(SHARED_WRITER_BYTE))
                .unwrap()
        );
        let third = reopen(tmpfile.path());
        assert!(
            third
                .try_lock_shared_range(byte_range(SHARED_WRITER_BYTE))
                .unwrap()
        );
        assert!(!sole.try_lock_range(byte_range(SHARED_WRITER_BYTE)).unwrap());
        other.unlock_range(byte_range(SHARED_WRITER_BYTE)).unwrap();
        third.unlock_range(byte_range(SHARED_WRITER_BYTE)).unwrap();

        // Read-only handles announce themselves the same way: shared with each other, and
        // conflicting with the exclusive whole-range lock a single-process open takes
        assert!(
            other
                .try_lock_shared_range(byte_range(SHARED_READER_BYTE))
                .unwrap()
        );
        assert!(
            third
                .try_lock_shared_range(byte_range(SHARED_READER_BYTE))
                .unwrap()
        );
        assert!(!sole.try_lock_range(byte_range(SHARED_READER_BYTE)).unwrap());
        other.unlock_range(byte_range(SHARED_READER_BYTE)).unwrap();
        third.unlock_range(byte_range(SHARED_READER_BYTE)).unwrap();
    }

    #[test]
    fn the_writer_byte_hands_over_to_a_blocked_waiter() {
        let tmpfile = crate::create_tempfile();
        let holder = reopen(tmpfile.path());
        assert!(holder.try_lock_range(byte_range(WRITER_BYTE)).unwrap());

        let path = tmpfile.path().to_path_buf();
        let waiter = std::thread::spawn(move || {
            let waiter = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            waiter.lock_range(byte_range(WRITER_BYTE)).unwrap();
            waiter.unlock_range(byte_range(WRITER_BYTE)).unwrap();
        });

        std::thread::sleep(std::time::Duration::from_millis(50));
        holder.unlock_range(byte_range(WRITER_BYTE)).unwrap();
        waiter.join().unwrap();
    }

    #[test]
    fn an_id_past_the_lock_range_is_refused() {
        // The last id with a byte is scannable; the first one past the range is corruption,
        // since no database can commit often enough to reach it
        let last_in_range = TransactionId::new((1u64 << 63) - 1 - TXN_BASE);
        let past_range = TransactionId::new((1u64 << 63) - TXN_BASE);
        assert_eq!(
            TransactionalMemory::active_transaction_byte(last_in_range).unwrap(),
            (1u64 << 63) - 1
        );
        assert!(matches!(
            TransactionalMemory::active_transaction_byte(past_range).unwrap_err(),
            StorageError::Corrupted(_)
        ));
        // An id large enough to wrap the offset arithmetic is the same refusal
        assert!(matches!(
            TransactionalMemory::active_transaction_byte(TransactionId::new(u64::MAX)).unwrap_err(),
            StorageError::Corrupted(_)
        ));
    }
}
