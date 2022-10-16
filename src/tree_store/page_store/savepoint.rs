use crate::db::{SavepointId, TransactionId};
use crate::tree_store::page_store::ChecksumType;
use crate::tree_store::{Checksum, PageNumber};
use crate::Database;

pub struct Savepoint<'a> {
    db: &'a Database,
    id: SavepointId,
    // Each savepoint has an associated read transaction id to ensure that any pages it references
    // are not freed
    transaction_id: TransactionId,
    version: u8,
    checksum_type: ChecksumType,
    root: Option<(PageNumber, Checksum)>,
    freed_root: Option<(PageNumber, Checksum)>,
    regional_allocators: Vec<Vec<u8>>,
}

impl<'a> Savepoint<'a> {
    pub(crate) fn new(
        db: &'a Database,
        id: SavepointId,
        transaction_id: TransactionId,
        root: Option<(PageNumber, Checksum)>,
        freed_root: Option<(PageNumber, Checksum)>,
        regional_allocators: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            db,
            id,
            transaction_id,
            version: db.get_memory().get_version(),
            checksum_type: db.get_memory().checksum_type(),
            root,
            freed_root,
            regional_allocators,
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
}

impl Drop for Savepoint<'_> {
    fn drop(&mut self) {
        self.db.deallocate_savepoint(self);
    }
}
