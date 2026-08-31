use crate::db::TransactionGuard;
use crate::transaction_tracker::{SavepointId, TransactionId, TransactionTracker};
use crate::tree_store::page_store::page_manager::FILE_FORMAT_VERSION3;
use crate::tree_store::{BtreeHeader, TransactionalMemory};
use crate::{Result, StorageError, TypeName, Value};
use alloc::format;
use alloc::string::ToString;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::fmt::Debug;
use core::mem::size_of;

// on-disk format:
// * 1 byte: version
// * 8 bytes: savepoint id
// * 8 bytes: transaction id
// * 1 byte: user root not-null
// * 8 bytes: user root page
// * 8 bytes: user root checksum
/// A database savepoint
///
/// May be used with [`WriteTransaction::restore_savepoint`] to restore the database to the state
/// when this savepoint was created
///
/// [`WriteTransaction::restore_savepoint`]: crate::WriteTransaction::restore_savepoint
pub struct Savepoint {
    version: u8,
    id: SavepointId,
    // Each savepoint has an associated read transaction id to ensure that any pages it references
    // are not freed
    transaction: TransactionGuard,
    user_root: Option<BtreeHeader>,
}

impl Savepoint {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_ephemeral(
        mem: &TransactionalMemory,
        transaction_tracker: Arc<TransactionTracker>,
        id: SavepointId,
        transaction_id: TransactionId,
        user_root: Option<BtreeHeader>,
    ) -> Self {
        Self {
            id,
            version: mem.get_version(),
            user_root,
            transaction: TransactionGuard::new_read(transaction_id, transaction_tracker),
        }
    }

    pub(crate) fn get_version(&self) -> u8 {
        self.version
    }

    pub(crate) fn get_id(&self) -> SavepointId {
        self.id
    }

    pub(crate) fn get_transaction_id(&self) -> TransactionId {
        self.transaction.id()
    }

    pub(crate) fn get_user_root(&self) -> Option<BtreeHeader> {
        self.user_root
    }

    pub(crate) fn db_address(&self) -> *const TransactionTracker {
        core::ptr::from_ref(self.transaction.tracker().as_ref())
    }

    pub(crate) fn set_persistent(&mut self) {
        self.transaction.release_to_database();
    }
}

impl Drop for Savepoint {
    fn drop(&mut self) {
        // A persistent savepoint outlives its handle: the database record owns both its entry
        // and the transaction's reference until the savepoint is deleted
        if self.transaction.owns_reference() {
            self.transaction.tracker().remove_savepoint(self.id);
        }
        // The guard releases the transaction as it drops
    }
}

#[derive(Debug)]
pub(crate) enum SerializedSavepoint<'a> {
    Ref(&'a [u8]),
    Owned(Vec<u8>),
}

impl SerializedSavepoint<'_> {
    pub(crate) fn from_savepoint(savepoint: &Savepoint) -> Self {
        assert_eq!(savepoint.version, FILE_FORMAT_VERSION3);
        let mut result = vec![savepoint.version];
        result.extend(savepoint.id.0.to_le_bytes());
        result.extend(savepoint.get_transaction_id().raw_id().to_le_bytes());

        if let Some(header) = savepoint.user_root {
            result.push(1);
            result.extend(header.to_le_bytes());
        } else {
            result.push(0);
            result.extend([0; BtreeHeader::serialized_size()]);
        }

        Self::Owned(result)
    }

    fn data(&self) -> &[u8] {
        match self {
            SerializedSavepoint::Ref(x) => x,
            SerializedSavepoint::Owned(x) => x.as_slice(),
        }
    }

    pub(crate) fn to_savepoint(
        &self,
        transaction_tracker: Arc<TransactionTracker>,
    ) -> Result<Savepoint> {
        let data = self.data();
        let serialized_len =
            2 * size_of::<u8>() + 2 * size_of::<u64>() + BtreeHeader::serialized_size();
        if data.len() != serialized_len {
            return Err(StorageError::Corrupted(
                "Corrupted savepoint record".to_string(),
            ));
        }
        let mut offset = 0;
        let version = data[offset];
        if version != FILE_FORMAT_VERSION3 {
            return Err(StorageError::Corrupted(format!(
                "Unsupported savepoint version: {version}"
            )));
        }
        offset += size_of::<u8>();

        let id = u64::from_le_bytes(
            data[offset..(offset + size_of::<u64>())]
                .try_into()
                .unwrap(),
        );
        offset += size_of::<u64>();

        let transaction_id = u64::from_le_bytes(
            data[offset..(offset + size_of::<u64>())]
                .try_into()
                .unwrap(),
        );
        offset += size_of::<u64>();

        let not_null = data[offset];
        if not_null > 1 {
            return Err(StorageError::Corrupted(
                "Corrupted savepoint record".to_string(),
            ));
        }
        offset += 1;
        let user_root = if not_null == 1 {
            Some(BtreeHeader::from_le_bytes(
                data[offset..(offset + BtreeHeader::serialized_size())]
                    .try_into()
                    .unwrap(),
            ))
        } else {
            None
        };
        offset += BtreeHeader::serialized_size();
        debug_assert_eq!(offset, data.len());

        Ok(Savepoint {
            version,
            id: SavepointId(id),
            user_root,
            transaction: TransactionGuard::new_read_unowned(
                TransactionId::new(transaction_id),
                transaction_tracker,
            ),
        })
    }
}

impl Value for SerializedSavepoint<'_> {
    type SelfType<'a>
        = SerializedSavepoint<'a>
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        SerializedSavepoint::Ref(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.data()
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::SerializedSavepoint")
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::transaction_tracker::{TransactionId, TransactionTracker};
    use alloc::sync::Arc;

    #[test]
    fn corrupted_record_errors() {
        let tracker = Arc::new(TransactionTracker::new(TransactionId::new(1)));

        let mut record = vec![FILE_FORMAT_VERSION3];
        record.extend(1u64.to_le_bytes());
        record.extend(1u64.to_le_bytes());
        record.push(0);
        record.extend([0; BtreeHeader::serialized_size()]);
        assert!(
            SerializedSavepoint::Ref(&record)
                .to_savepoint(tracker.clone())
                .is_ok()
        );

        let truncated = &record[..record.len() - 1];
        assert!(matches!(
            SerializedSavepoint::Ref(truncated).to_savepoint(tracker.clone()),
            Err(StorageError::Corrupted(_))
        ));

        let mut bad_version = record.clone();
        bad_version[0] = 9;
        assert!(matches!(
            SerializedSavepoint::Ref(&bad_version).to_savepoint(tracker.clone()),
            Err(StorageError::Corrupted(_))
        ));

        let mut bad_null_marker = record.clone();
        bad_null_marker[17] = 2;
        assert!(matches!(
            SerializedSavepoint::Ref(&bad_null_marker).to_savepoint(tracker),
            Err(StorageError::Corrupted(_))
        ));
    }
}
