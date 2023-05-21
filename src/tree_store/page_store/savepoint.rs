use crate::transaction_tracker::{SavepointId, TransactionId, TransactionTracker};
use crate::tree_store::{Checksum, PageNumber, TransactionalMemory};
use std::mem::size_of;
use std::sync::{Arc, Mutex};

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
    user_root: Option<(PageNumber, Checksum)>,
    system_root: Option<(PageNumber, Checksum)>,
    freed_root: Option<(PageNumber, Checksum)>,
    regional_allocators: Vec<Vec<u8>>,
    transaction_tracker: Arc<Mutex<TransactionTracker>>,
    ephemeral: bool,
}

impl Savepoint {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_ephemeral(
        mem: &TransactionalMemory,
        transaction_tracker: Arc<Mutex<TransactionTracker>>,
        id: SavepointId,
        transaction_id: TransactionId,
        user_root: Option<(PageNumber, Checksum)>,
        system_root: Option<(PageNumber, Checksum)>,
        freed_root: Option<(PageNumber, Checksum)>,
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

    pub(crate) fn get_user_root(&self) -> Option<(PageNumber, Checksum)> {
        self.user_root
    }

    pub(crate) fn get_system_root(&self) -> Option<(PageNumber, Checksum)> {
        self.system_root
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

    pub(crate) fn set_persistent(&mut self) {
        self.ephemeral = false;
    }

    pub(crate) fn from_bytes(
        data: &[u8],
        transaction_tracker: Arc<Mutex<TransactionTracker>>,
        ephemeral: bool,
    ) -> Self {
        let mut offset = 0;
        let version = data[offset];
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
            let page_number = PageNumber::from_le_bytes(
                data[offset..(offset + PageNumber::serialized_size())]
                    .try_into()
                    .unwrap(),
            );
            offset += PageNumber::serialized_size();
            let checksum = Checksum::from_le_bytes(
                data[offset..(offset + size_of::<Checksum>())]
                    .try_into()
                    .unwrap(),
            );
            offset += size_of::<Checksum>();
            Some((page_number, checksum))
        } else {
            offset += PageNumber::serialized_size();
            offset += size_of::<Checksum>();
            None
        };

        let not_null = data[offset];
        assert!(not_null == 0 || not_null == 1);
        offset += 1;
        let system_root = if not_null == 1 {
            let page_number = PageNumber::from_le_bytes(
                data[offset..(offset + PageNumber::serialized_size())]
                    .try_into()
                    .unwrap(),
            );
            offset += PageNumber::serialized_size();
            let checksum = Checksum::from_le_bytes(
                data[offset..(offset + size_of::<Checksum>())]
                    .try_into()
                    .unwrap(),
            );
            offset += size_of::<Checksum>();
            Some((page_number, checksum))
        } else {
            offset += PageNumber::serialized_size();
            offset += size_of::<Checksum>();
            None
        };

        let not_null = data[offset];
        assert!(not_null == 0 || not_null == 1);
        offset += 1;
        let freed_root = if not_null == 1 {
            let page_number = PageNumber::from_le_bytes(
                data[offset..(offset + PageNumber::serialized_size())]
                    .try_into()
                    .unwrap(),
            );
            offset += PageNumber::serialized_size();
            let checksum = Checksum::from_le_bytes(
                data[offset..(offset + size_of::<Checksum>())]
                    .try_into()
                    .unwrap(),
            );
            offset += size_of::<Checksum>();
            Some((page_number, checksum))
        } else {
            offset += PageNumber::serialized_size();
            offset += size_of::<Checksum>();
            None
        };

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

        assert_eq!(offset, data.len());

        Self {
            version,
            id: SavepointId(id),
            transaction_id: TransactionId(transaction_id),
            user_root,
            system_root,
            freed_root,
            regional_allocators,
            transaction_tracker,
            ephemeral,
        }
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut result = vec![self.version];
        result.extend(self.id.0.to_le_bytes());
        result.extend(self.transaction_id.0.to_le_bytes());

        if let Some((root, checksum)) = self.user_root {
            result.push(1);
            result.extend(root.to_le_bytes());
            result.extend(checksum.to_le_bytes());
        } else {
            result.push(0);
            result.extend([0; PageNumber::serialized_size()]);
            result.extend((0 as Checksum).to_le_bytes());
        }

        if let Some((root, checksum)) = self.system_root {
            result.push(1);
            result.extend(root.to_le_bytes());
            result.extend(checksum.to_le_bytes());
        } else {
            result.push(0);
            result.extend([0; PageNumber::serialized_size()]);
            result.extend((0 as Checksum).to_le_bytes());
        }

        if let Some((root, checksum)) = self.freed_root {
            result.push(1);
            result.extend(root.to_le_bytes());
            result.extend(checksum.to_le_bytes());
        } else {
            result.push(0);
            result.extend([0; PageNumber::serialized_size()]);
            result.extend((0 as Checksum).to_le_bytes());
        }

        result.extend(
            u32::try_from(self.regional_allocators.len())
                .unwrap()
                .to_le_bytes(),
        );
        for region in self.regional_allocators.iter() {
            assert_eq!(self.regional_allocators[0].len(), region.len());
        }
        result.extend(
            u32::try_from(self.regional_allocators[0].len())
                .unwrap()
                .to_le_bytes(),
        );

        for region in self.regional_allocators.iter() {
            result.extend(region);
        }
        result
    }
}

impl Drop for Savepoint {
    fn drop(&mut self) {
        if self.ephemeral {
            self.transaction_tracker
                .lock()
                .unwrap()
                .deallocate_savepoint(self);
        }
    }
}
