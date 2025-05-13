use crate::transaction_tracker::{SavepointId, TransactionId, TransactionTracker};
use crate::tree_store::page_store::page_manager::{FILE_FORMAT_VERSION2, FILE_FORMAT_VERSION3};
use crate::tree_store::{BtreeHeader, TransactionalMemory};
use crate::{TypeName, Value};
use std::fmt::Debug;
use std::mem::size_of;
use std::sync::Arc;

// on-disk format:
// * 1 byte: version
// * 8 bytes: savepoint id
// * 8 bytes: transaction id
// * 1 byte: user root not-null
// * 8 bytes: user root page
// * 8 bytes: user root checksum
// * 1 byte: system root not-null
// * 8 bytes: system root page
// * 8 bytes: system root checksum
// * 1 byte: freed root not-null
// * 8 bytes: freed root page
// * 8 bytes: freed root checksum
// * 4 bytes: number of regions
// * 4 bytes: length of each regional tracker
// * n = (number of regions * length of each) bytes: regional tracker data
pub struct Savepoint {
    version: u8,
    id: SavepointId,
    // Each savepoint has an associated read transaction id to ensure that any pages it references
    // are not freed
    transaction_id: TransactionId,
    user_root: Option<BtreeHeader>,
    system_root: Option<BtreeHeader>,
    freed_root: Option<BtreeHeader>,
    regional_allocators: Vec<Vec<u8>>,
    transaction_tracker: Arc<TransactionTracker>,
    ephemeral: bool,
}

impl Savepoint {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_ephemeral(
        mem: &TransactionalMemory,
        transaction_tracker: Arc<TransactionTracker>,
        id: SavepointId,
        transaction_id: TransactionId,
        user_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
        freed_root: Option<BtreeHeader>,
        regional_allocators: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            id,
            transaction_id,
            version: mem.get_version(),
            user_root,
            system_root,
            freed_root,
            regional_allocators,
            transaction_tracker,
            ephemeral: true,
        }
    }

    pub(crate) fn get_version(&self) -> u8 {
        self.version
    }

    pub(crate) fn get_id(&self) -> SavepointId {
        self.id
    }

    pub(crate) fn get_transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    pub(crate) fn get_user_root(&self) -> Option<BtreeHeader> {
        self.user_root
    }

    pub(crate) fn get_system_root(&self) -> Option<BtreeHeader> {
        self.system_root
    }

    pub(crate) fn get_freed_root(&self) -> Option<BtreeHeader> {
        self.freed_root
    }

    pub(crate) fn get_regional_allocators(&self) -> &Vec<Vec<u8>> {
        &self.regional_allocators
    }

    pub(crate) fn db_address(&self) -> *const TransactionTracker {
        std::ptr::from_ref(self.transaction_tracker.as_ref())
    }

    pub(crate) fn set_persistent(&mut self) {
        self.ephemeral = false;
    }
}

impl Drop for Savepoint {
    fn drop(&mut self) {
        if self.ephemeral {
            self.transaction_tracker
                .deallocate_savepoint(self.get_id(), self.get_transaction_id());
        }
    }
}

#[derive(Debug)]
pub(crate) enum SerializedSavepoint<'a> {
    Ref(&'a [u8]),
    Owned(Vec<u8>),
}

impl SerializedSavepoint<'_> {
    pub(crate) fn from_savepoint(savepoint: &Savepoint, enable_file_format_v3: bool) -> Self {
        if enable_file_format_v3 {
            assert_eq!(savepoint.version, FILE_FORMAT_VERSION3);
        } else {
            assert_eq!(savepoint.version, FILE_FORMAT_VERSION2);
        }
        let mut result = vec![savepoint.version];
        result.extend(savepoint.id.0.to_le_bytes());
        result.extend(savepoint.transaction_id.raw_id().to_le_bytes());

        if let Some(header) = savepoint.user_root {
            result.push(1);
            result.extend(header.to_le_bytes());
        } else {
            result.push(0);
            result.extend([0; BtreeHeader::serialized_size()]);
        }

        if !enable_file_format_v3 {
            if let Some(header) = savepoint.system_root {
                result.push(1);
                result.extend(header.to_le_bytes());
            } else {
                result.push(0);
                result.extend([0; BtreeHeader::serialized_size()]);
            }

            if let Some(header) = savepoint.freed_root {
                result.push(1);
                result.extend(header.to_le_bytes());
            } else {
                result.push(0);
                result.extend([0; BtreeHeader::serialized_size()]);
            }

            result.extend(
                u32::try_from(savepoint.regional_allocators.len())
                    .unwrap()
                    .to_le_bytes(),
            );
            for region in &savepoint.regional_allocators {
                assert_eq!(savepoint.regional_allocators[0].len(), region.len());
            }
            result.extend(
                u32::try_from(savepoint.regional_allocators[0].len())
                    .unwrap()
                    .to_le_bytes(),
            );

            // TODO: this seems like it is going to fail on databases with > 3GiB of regional allocators
            // since that is the max size of a single stored value
            for region in &savepoint.regional_allocators {
                result.extend(region);
            }
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
        enable_file_format_v3: bool,
    ) -> Savepoint {
        let data = self.data();
        let mut offset = 0;
        let version = data[offset];
        if enable_file_format_v3 {
            assert_eq!(version, FILE_FORMAT_VERSION3);
        } else {
            assert_eq!(version, FILE_FORMAT_VERSION2);
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
        assert!(not_null == 0 || not_null == 1);
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

        let (system_root, freed_root, regional_allocators) = if enable_file_format_v3 {
            (None, None, vec![])
        } else {
            let not_null = data[offset];
            assert!(not_null == 0 || not_null == 1);
            offset += 1;
            let system_root = if not_null == 1 {
                Some(BtreeHeader::from_le_bytes(
                    data[offset..(offset + BtreeHeader::serialized_size())]
                        .try_into()
                        .unwrap(),
                ))
            } else {
                None
            };
            offset += BtreeHeader::serialized_size();

            let not_null = data[offset];
            assert!(not_null == 0 || not_null == 1);
            offset += 1;
            let freed_root = if not_null == 1 {
                Some(BtreeHeader::from_le_bytes(
                    data[offset..(offset + BtreeHeader::serialized_size())]
                        .try_into()
                        .unwrap(),
                ))
            } else {
                None
            };
            offset += BtreeHeader::serialized_size();

            let regions = u32::from_le_bytes(
                data[offset..(offset + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            offset += size_of::<u32>();
            let allocator_len = u32::from_le_bytes(
                data[offset..(offset + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            offset += size_of::<u32>();

            let mut regional_allocators = vec![];

            for _ in 0..regions {
                regional_allocators.push(data[offset..(offset + allocator_len)].to_vec());
                offset += allocator_len;
            }

            (system_root, freed_root, regional_allocators)
        };

        assert_eq!(offset, data.len());

        Savepoint {
            version,
            id: SavepointId(id),
            transaction_id: TransactionId::new(transaction_id),
            user_root,
            system_root,
            freed_root,
            regional_allocators,
            transaction_tracker,
            ephemeral: false,
        }
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
