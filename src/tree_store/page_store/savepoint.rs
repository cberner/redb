use crate::transaction_tracker::{SavepointId, TransactionId, TransactionTracker};
use crate::tree_store::page_store::ChecksumType;
use crate::tree_store::{Checksum, PageNumber};
use crate::Database;
use std::sync::{Arc, Mutex};

pub struct Savepoint {
    id: SavepointId,
    // Each savepoint has an associated read transaction id to ensure that any pages it references
    // are not freed
    transaction_id: TransactionId,
    version: u8,
    checksum_type: ChecksumType,
    root: Option<(PageNumber, Checksum)>,
    freed_root: Option<(PageNumber, Checksum)>,
    regional_allocators: Vec<Vec<u8>>,
    transaction_tracker: Arc<Mutex<TransactionTracker>>,
}

impl Savepoint {
    pub(crate) fn new(
        db: &Database,
        id: SavepointId,
        transaction_id: TransactionId,
        root: Option<(PageNumber, Checksum)>,
        freed_root: Option<(PageNumber, Checksum)>,
        regional_allocators: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            id,
            transaction_id,
            version: db.get_memory().get_version(),
            checksum_type: db.get_memory().checksum_type(),
            root,
            freed_root,
            regional_allocators,
            transaction_tracker: db.transaction_tracker(),
        }
    }

    pub(crate) fn get_version(&self) -> u8 {
        self.version
    }

    pub(crate) fn get_checksum_type(&self) -> ChecksumType {
        self.checksum_type
    }

    pub(crate) fn get_id(&self) -> SavepointId {
        self.id
    }

    pub(crate) fn get_transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    pub(crate) fn get_root(&self) -> Option<(PageNumber, Checksum)> {
        self.root
    }

    pub(crate) fn get_freed_root(&self) -> Option<(PageNumber, Checksum)> {
        self.freed_root
    }

    pub(crate) fn get_regional_allocator_states(&self) -> &[Vec<u8>] {
        &self.regional_allocators
    }

    pub(crate) fn db_address(&self) -> *const Mutex<TransactionTracker> {
        self.transaction_tracker.as_ref() as *const _
    }
}

impl Drop for Savepoint {
    fn drop(&mut self) {
        self.transaction_tracker
            .lock()
            .unwrap()
            .deallocate_savepoint(self);
    }
}
