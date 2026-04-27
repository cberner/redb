use crate::transaction_tracker::TransactionId;
use crate::tree_store::btree_base::{BtreeHeader, Checksum};
use crate::tree_store::page_store::layout::{DatabaseLayout, RegionLayout};
use crate::tree_store::page_store::page_manager::{
    FILE_FORMAT_VERSION1, FILE_FORMAT_VERSION2, FILE_FORMAT_VERSION3, xxh3_checksum,
};
use crate::{DatabaseError, Result, StorageError};
use std::mem::size_of;

// Database layout:
//
// Super-header (header + commit slots)
// The super-header length is rounded up to the nearest full page size
//
// Header (first 64 bytes):
// 9 bytes: magic number
// 1 byte: god byte
// 2 byte: padding
// 4 bytes: page size
// Definition of region
// 4 bytes: region header pages
// 4 bytes: region max data pages
//
// Commit slot 0 (next 128 bytes):
// 1 byte: version
// 1 byte: != 0 if root page is non-null
// 1 byte: != 0 if freed table root page is non-null
// 5 bytes: padding
// 8 bytes: root page
// 16 bytes: root checksum
// 8 bytes: unused: formerly freed table root page
// 16 bytes: unused: formerly freed table root checksum
// 8 bytes: last committed transaction id
// 4 bytes: number of full regions
// 4 bytes: data pages in partial trailing region
// 8 bytes: unused: formerly region tracker page number
// 16 bytes: slot checksum
//
// Commit slot 1 (next 128 bytes):
// Same layout as slot 0

// Inspired by PNG's magic number
pub(super) const MAGICNUMBER: [u8; 9] = [b'r', b'e', b'd', b'b', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A];
const GOD_BYTE_OFFSET: usize = MAGICNUMBER.len();
const PAGE_SIZE_OFFSET: usize = GOD_BYTE_OFFSET + size_of::<u8>() + 2; // +2 for padding
const REGION_HEADER_PAGES_OFFSET: usize = PAGE_SIZE_OFFSET + size_of::<u32>();
const REGION_MAX_DATA_PAGES_OFFSET: usize = REGION_HEADER_PAGES_OFFSET + size_of::<u32>();
const NUM_FULL_REGIONS_OFFSET: usize = REGION_MAX_DATA_PAGES_OFFSET + size_of::<u32>();
const TRAILING_REGION_DATA_PAGES_OFFSET: usize = NUM_FULL_REGIONS_OFFSET + size_of::<u32>();
// Formerly the region tracker page
const _UNUSED3_OFFSET: usize = TRAILING_REGION_DATA_PAGES_OFFSET + size_of::<u32>();
const TRANSACTION_SIZE: usize = 128;
const TRANSACTION_0_OFFSET: usize = 64;
const TRANSACTION_1_OFFSET: usize = TRANSACTION_0_OFFSET + TRANSACTION_SIZE;
pub(super) const DB_HEADER_SIZE: usize = TRANSACTION_1_OFFSET + TRANSACTION_SIZE;

// God byte flags
const PRIMARY_BIT: u8 = 1;
const RECOVERY_REQUIRED: u8 = 2;
const TWO_PHASE_COMMIT: u8 = 4;

// Structure of each commit slot
const VERSION_OFFSET: usize = 0;
const USER_ROOT_NON_NULL_OFFSET: usize = size_of::<u8>();
const SYSTEM_ROOT_NON_NULL_OFFSET: usize = USER_ROOT_NON_NULL_OFFSET + size_of::<u8>();
const _UNUSED_OFFSET: usize = SYSTEM_ROOT_NON_NULL_OFFSET + size_of::<u8>();
const PADDING: usize = 4;

const USER_ROOT_OFFSET: usize = _UNUSED_OFFSET + size_of::<u8>() + PADDING;
const SYSTEM_ROOT_OFFSET: usize = USER_ROOT_OFFSET + BtreeHeader::serialized_size();
const _UNUSED2_OFFSET: usize = SYSTEM_ROOT_OFFSET + BtreeHeader::serialized_size();
const TRANSACTION_ID_OFFSET: usize = _UNUSED2_OFFSET + BtreeHeader::serialized_size();
const TRANSACTION_LAST_FIELD: usize = TRANSACTION_ID_OFFSET + size_of::<u64>();

const SLOT_CHECKSUM_OFFSET: usize = TRANSACTION_SIZE - size_of::<Checksum>();

pub(crate) const PAGE_SIZE: usize = 4096;

fn get_u32(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[..size_of::<u32>()].try_into().unwrap())
}

fn get_u64(data: &[u8]) -> u64 {
    u64::from_le_bytes(data[..size_of::<u64>()].try_into().unwrap())
}

// A header parsed from disk that has not yet committed to a primary slot.
pub(super) struct UnrepairedDatabaseHeader {
    inner: DatabaseHeader,
    primary_corrupted: bool,
    secondary_corrupted: bool,
}

#[derive(Clone)]
pub(super) struct DatabaseHeader {
    primary_slot: usize,
    pub(super) recovery_required: bool,
    pub(super) two_phase_commit: bool,
    page_size: u32,
    region_header_pages: u32,
    region_max_data_pages: u32,
    full_regions: u32,
    trailing_partial_region_pages: u32,
    transaction_slots: [TransactionHeader; 2],
}

impl UnrepairedDatabaseHeader {
    pub(super) fn from_bytes(data: &[u8]) -> Result<Self, DatabaseError> {
        if data[..MAGICNUMBER.len()] != MAGICNUMBER {
            return Err(StorageError::Corrupted("Invalid magic number".to_string()).into());
        }

        let primary_slot = usize::from(data[GOD_BYTE_OFFSET] & PRIMARY_BIT != 0);
        let recovery_required = (data[GOD_BYTE_OFFSET] & RECOVERY_REQUIRED) != 0;
        let two_phase_commit = (data[GOD_BYTE_OFFSET] & TWO_PHASE_COMMIT) != 0;
        let page_size = get_u32(&data[PAGE_SIZE_OFFSET..]);
        let region_header_pages = get_u32(&data[REGION_HEADER_PAGES_OFFSET..]);
        let region_max_data_pages = get_u32(&data[REGION_MAX_DATA_PAGES_OFFSET..]);
        let full_regions = get_u32(&data[NUM_FULL_REGIONS_OFFSET..]);
        let trailing_data_pages = get_u32(&data[TRAILING_REGION_DATA_PAGES_OFFSET..]);
        let (slot0, slot0_corrupted) = TransactionHeader::from_bytes(
            &data[TRANSACTION_0_OFFSET..(TRANSACTION_0_OFFSET + TRANSACTION_SIZE)],
        )?;
        let (slot1, slot1_corrupted) = TransactionHeader::from_bytes(
            &data[TRANSACTION_1_OFFSET..(TRANSACTION_1_OFFSET + TRANSACTION_SIZE)],
        )?;
        let (primary_corrupted, secondary_corrupted) = if primary_slot == 0 {
            (slot0_corrupted, slot1_corrupted)
        } else {
            (slot1_corrupted, slot0_corrupted)
        };

        Ok(Self {
            inner: DatabaseHeader {
                primary_slot,
                recovery_required,
                two_phase_commit,
                page_size,
                region_header_pages,
                region_max_data_pages,
                full_regions,
                trailing_partial_region_pages: trailing_data_pages,
                transaction_slots: [slot0, slot1],
            },
            primary_corrupted,
            secondary_corrupted,
        })
    }

    pub(super) fn page_size(&self) -> u32 {
        self.inner.page_size
    }

    // Returns true if the header needs to be repaired before use: either the recovery_required
    // flag is set on disk, or the stored layout no longer matches the current file length (e.g.
    // the file was truncated or extended externally). Callers must pass the actual file length
    // so both conditions are always checked together.
    pub(super) fn recovery_required(&self, file_len: u64) -> bool {
        self.inner.recovery_required || self.inner.layout().len() != file_len
    }

    // Consume self, reconcile the layout against the actual file length, and select a primary slot
    // (repairing if necessary). Returns the usable DatabaseHeader along with a `clean` flag that is
    // true only when nothing had to be reconciled: the primary was kept and the stored layout
    // already matched `file_len`.
    pub(super) fn finalize(mut self, file_len: u64) -> Result<(DatabaseHeader, bool)> {
        // The backing file may have been truncated or extended since the header was last written
        // (e.g. externally, or by a prior crashed resize). Re-derive the layout from the actual
        // file length so callers always see a layout consistent with the file. Truncation below
        // the stored layout is always corruption -- redb shrinks the file only after writing a
        // smaller layout -- and must be rejected before `recalculate` runs, since its arithmetic
        // assumes at least one page of file length.
        let stored_len = self.inner.layout().len();
        if file_len < stored_len {
            return Err(StorageError::Corrupted(format!(
                "File truncated below stored layout: file_len={file_len}, layout_len={stored_len}"
            )));
        }
        let layout_stale = stored_len != file_len;
        if layout_stale {
            let layout = self.inner.layout();
            let region_max_pages = layout.full_region_layout().num_pages();
            let region_header_pages = layout.full_region_layout().get_header_pages();
            self.inner.set_layout(DatabaseLayout::recalculate(
                file_len,
                region_header_pages,
                region_max_pages,
                self.inner.page_size,
            ));
        }
        let kept_primary = self.select_primary_slot()?;
        Ok((self.inner, kept_primary && !layout_stale))
    }

    fn select_primary_slot(&mut self) -> Result<bool> {
        // If the primary was written using 2-phase commit, it's guaranteed to be valid. Don't look
        // at the secondary; even if it happens to have a valid checksum, Durability::Paranoid means
        // we can't trust it
        if self.inner.two_phase_commit {
            if self.primary_corrupted {
                return Err(StorageError::Corrupted(
                    "Primary is corrupted despite 2-phase commit".to_string(),
                ));
            }
            return Ok(true);
        }

        // Pick whichever slot is newer, assuming it has a valid checksum. This handles an edge case
        // where we crash during fsync(), and the only data that got written to disk was the god byte
        // update swapping the primary -- in that case, the primary contains a valid but out-of-date
        // transaction, so we need to load from the secondary instead
        if self.primary_corrupted {
            if self.secondary_corrupted {
                return Err(StorageError::Corrupted(
                    "Both commit slots are corrupted".to_string(),
                ));
            }
            self.inner.swap_primary_slot();
            return Ok(false);
        }

        let secondary_newer =
            self.inner.secondary_slot().transaction_id > self.inner.primary_slot().transaction_id;
        if secondary_newer && !self.secondary_corrupted {
            self.inner.swap_primary_slot();
            return Ok(false);
        }

        Ok(true)
    }
}

impl DatabaseHeader {
    pub(super) fn new(layout: DatabaseLayout, transaction_id: TransactionId) -> Self {
        #[allow(clippy::assertions_on_constants)]
        {
            assert!(TRANSACTION_LAST_FIELD <= SLOT_CHECKSUM_OFFSET);
        }

        let slot = TransactionHeader::new(transaction_id);
        Self {
            primary_slot: 0,
            recovery_required: true,
            two_phase_commit: false,
            page_size: layout.full_region_layout().page_size(),
            region_header_pages: layout.full_region_layout().get_header_pages(),
            region_max_data_pages: layout.full_region_layout().num_pages(),
            full_regions: layout.num_full_regions(),
            trailing_partial_region_pages: layout
                .trailing_region_layout()
                .map(|x| x.num_pages())
                .unwrap_or_default(),
            transaction_slots: [slot.clone(), slot],
        }
    }

    pub(super) fn page_size(&self) -> u32 {
        self.page_size
    }

    pub(super) fn layout(&self) -> DatabaseLayout {
        let full_layout = RegionLayout::new(
            self.region_max_data_pages,
            self.region_header_pages,
            self.page_size,
        );
        let trailing = if self.trailing_partial_region_pages > 0 {
            Some(RegionLayout::new(
                self.trailing_partial_region_pages,
                self.region_header_pages,
                self.page_size,
            ))
        } else {
            None
        };
        DatabaseLayout::new(self.full_regions, full_layout, trailing)
    }

    pub(super) fn set_layout(&mut self, layout: DatabaseLayout) {
        assert_eq!(
            self.layout().full_region_layout(),
            layout.full_region_layout()
        );
        if let Some(trailing) = layout.trailing_region_layout() {
            assert_eq!(trailing.get_header_pages(), self.region_header_pages);
            assert_eq!(trailing.page_size(), self.page_size);
            self.trailing_partial_region_pages = trailing.num_pages();
        } else {
            self.trailing_partial_region_pages = 0;
        }
        self.full_regions = layout.num_full_regions();
    }

    pub(super) fn primary_slot(&self) -> &TransactionHeader {
        &self.transaction_slots[self.primary_slot]
    }

    pub(super) fn secondary_slot(&self) -> &TransactionHeader {
        &self.transaction_slots[self.primary_slot ^ 1]
    }

    pub(super) fn secondary_slot_mut(&mut self) -> &mut TransactionHeader {
        &mut self.transaction_slots[self.primary_slot ^ 1]
    }

    pub(super) fn swap_primary_slot(&mut self) {
        self.primary_slot ^= 1;
    }

    pub(super) fn to_bytes(&self, include_magic_number: bool) -> [u8; DB_HEADER_SIZE] {
        let mut result = [0; DB_HEADER_SIZE];
        if include_magic_number {
            result[..MAGICNUMBER.len()].copy_from_slice(&MAGICNUMBER);
        }
        result[GOD_BYTE_OFFSET] = self.primary_slot.try_into().unwrap();
        if self.recovery_required {
            result[GOD_BYTE_OFFSET] |= RECOVERY_REQUIRED;
        }
        if self.two_phase_commit {
            result[GOD_BYTE_OFFSET] |= TWO_PHASE_COMMIT;
        }
        result[PAGE_SIZE_OFFSET..(PAGE_SIZE_OFFSET + size_of::<u32>())]
            .copy_from_slice(&self.page_size.to_le_bytes());
        result[REGION_HEADER_PAGES_OFFSET..(REGION_HEADER_PAGES_OFFSET + size_of::<u32>())]
            .copy_from_slice(&self.region_header_pages.to_le_bytes());
        result[REGION_MAX_DATA_PAGES_OFFSET..(REGION_MAX_DATA_PAGES_OFFSET + size_of::<u32>())]
            .copy_from_slice(&self.region_max_data_pages.to_le_bytes());
        result[NUM_FULL_REGIONS_OFFSET..(NUM_FULL_REGIONS_OFFSET + size_of::<u32>())]
            .copy_from_slice(&self.full_regions.to_le_bytes());
        result[TRAILING_REGION_DATA_PAGES_OFFSET
            ..(TRAILING_REGION_DATA_PAGES_OFFSET + size_of::<u32>())]
            .copy_from_slice(&self.trailing_partial_region_pages.to_le_bytes());
        let slot0 = self.transaction_slots[0].to_bytes();
        result[TRANSACTION_0_OFFSET..(TRANSACTION_0_OFFSET + slot0.len())].copy_from_slice(&slot0);
        let slot1 = self.transaction_slots[1].to_bytes();
        result[TRANSACTION_1_OFFSET..(TRANSACTION_1_OFFSET + slot1.len())].copy_from_slice(&slot1);

        result
    }
}

#[derive(Clone)]
pub(super) struct TransactionHeader {
    pub(super) version: u8,
    pub(super) user_root: Option<BtreeHeader>,
    pub(super) system_root: Option<BtreeHeader>,
    pub(super) transaction_id: TransactionId,
}

impl TransactionHeader {
    fn new(transaction_id: TransactionId) -> Self {
        Self {
            version: FILE_FORMAT_VERSION3,
            user_root: None,
            system_root: None,
            transaction_id,
        }
    }

    // Returned bool indicates whether the checksum was corrupted
    pub(super) fn from_bytes(data: &[u8]) -> Result<(Self, bool), DatabaseError> {
        let version = data[VERSION_OFFSET];
        match version {
            FILE_FORMAT_VERSION1 | FILE_FORMAT_VERSION2 => {
                return Err(DatabaseError::UpgradeRequired(version));
            }
            FILE_FORMAT_VERSION3 => {}
            _ => {
                return Err(StorageError::Corrupted(format!(
                    "Expected file format version <= {FILE_FORMAT_VERSION3}, found {version}",
                ))
                .into());
            }
        }
        let checksum = Checksum::from_le_bytes(
            data[SLOT_CHECKSUM_OFFSET..(SLOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
                .try_into()
                .unwrap(),
        );
        let corrupted = checksum != xxh3_checksum(&data[..SLOT_CHECKSUM_OFFSET]);

        let user_root = if data[USER_ROOT_NON_NULL_OFFSET] != 0 {
            Some(BtreeHeader::from_le_bytes(
                data[USER_ROOT_OFFSET..(USER_ROOT_OFFSET + BtreeHeader::serialized_size())]
                    .try_into()
                    .unwrap(),
            ))
        } else {
            None
        };
        let system_root = if data[SYSTEM_ROOT_NON_NULL_OFFSET] != 0 {
            Some(BtreeHeader::from_le_bytes(
                data[SYSTEM_ROOT_OFFSET..(SYSTEM_ROOT_OFFSET + BtreeHeader::serialized_size())]
                    .try_into()
                    .unwrap(),
            ))
        } else {
            None
        };
        let transaction_id = TransactionId::new(get_u64(&data[TRANSACTION_ID_OFFSET..]));

        let result = Self {
            version,
            user_root,
            system_root,
            transaction_id,
        };

        Ok((result, corrupted))
    }

    pub(super) fn to_bytes(&self) -> [u8; TRANSACTION_SIZE] {
        assert_eq!(self.version, FILE_FORMAT_VERSION3);
        let mut result = [0; TRANSACTION_SIZE];
        result[VERSION_OFFSET] = self.version;
        if let Some(header) = self.user_root {
            result[USER_ROOT_NON_NULL_OFFSET] = 1;
            result[USER_ROOT_OFFSET..(USER_ROOT_OFFSET + BtreeHeader::serialized_size())]
                .copy_from_slice(&header.to_le_bytes());
        }
        if let Some(header) = self.system_root {
            result[SYSTEM_ROOT_NON_NULL_OFFSET] = 1;
            result[SYSTEM_ROOT_OFFSET..(SYSTEM_ROOT_OFFSET + BtreeHeader::serialized_size())]
                .copy_from_slice(&header.to_le_bytes());
        }
        result[TRANSACTION_ID_OFFSET..(TRANSACTION_ID_OFFSET + size_of::<u64>())]
            .copy_from_slice(&self.transaction_id.raw_id().to_le_bytes());
        let checksum = xxh3_checksum(&result[..SLOT_CHECKSUM_OFFSET]);
        result[SLOT_CHECKSUM_OFFSET..(SLOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
            .copy_from_slice(&checksum.to_le_bytes());

        result
    }
}

#[cfg(test)]
mod test {
    use crate::backends::FileBackend;
    use crate::db::TableDefinition;
    use crate::tree_store::page_store::header::{
        DB_HEADER_SIZE, GOD_BYTE_OFFSET, MAGICNUMBER, PRIMARY_BIT, RECOVERY_REQUIRED,
        TRANSACTION_0_OFFSET, TRANSACTION_1_OFFSET, TWO_PHASE_COMMIT,
    };
    use crate::{Database, DatabaseError, StorageBackend};
    use crate::{ReadableDatabase, StorageError};
    use std::fs::OpenOptions;
    use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

    const X: TableDefinition<&str, &str> = TableDefinition::new("x");

    fn primary_slot_offset(god_byte: u8) -> usize {
        if god_byte & PRIMARY_BIT == 0 {
            TRANSACTION_0_OFFSET
        } else {
            TRANSACTION_1_OFFSET
        }
    }

    fn corrupt_slot_checksum(header: &mut [u8; DB_HEADER_SIZE], slot_offset: usize) {
        let checksum_offset = slot_offset + super::SLOT_CHECKSUM_OFFSET;
        header[checksum_offset] ^= 0xFF;
    }

    fn corrupt_primary_slot_checksum(header: &mut [u8; DB_HEADER_SIZE]) {
        corrupt_slot_checksum(header, primary_slot_offset(header[GOD_BYTE_OFFSET]));
    }

    #[derive(Clone, Debug)]
    struct FailingBackend {
        inner: Arc<FileBackend>,
        operations_until_failure: Arc<AtomicU64>,
        primary_bit_before_failure: Arc<AtomicU8>,
    }

    impl FailingBackend {
        const DISABLED: u64 = u64::MAX;
        const PRIMARY_BIT_DISABLED: u8 = 2;

        fn new(backend: FileBackend) -> Self {
            Self {
                inner: Arc::new(backend),
                operations_until_failure: Arc::new(AtomicU64::new(Self::DISABLED)),
                primary_bit_before_failure: Arc::new(AtomicU8::new(Self::PRIMARY_BIT_DISABLED)),
            }
        }

        fn read_header_directly(&self) -> Result<[u8; DB_HEADER_SIZE], std::io::Error> {
            let mut header = [0; DB_HEADER_SIZE];
            self.inner.read(0, &mut header)?;
            Ok(header)
        }

        fn write_header_directly(
            &self,
            header: &[u8; DB_HEADER_SIZE],
        ) -> Result<(), std::io::Error> {
            self.inner.write(0, header)
        }

        fn fail_after_primary_swap(&self) -> Result<(), std::io::Error> {
            let header = self.read_header_directly()?;
            let primary_bit = header[GOD_BYTE_OFFSET] & PRIMARY_BIT;
            assert_eq!(header[GOD_BYTE_OFFSET] & TWO_PHASE_COMMIT, 0);
            self.primary_bit_before_failure
                .store(primary_bit, Ordering::SeqCst);
            Ok(())
        }

        fn arm_failure_if_primary_swapped(&self, offset: u64, data: &[u8]) {
            if offset != 0 || data.len() < DB_HEADER_SIZE {
                return;
            }

            let primary_bit_before = self.primary_bit_before_failure.load(Ordering::SeqCst);
            if primary_bit_before == Self::PRIMARY_BIT_DISABLED {
                return;
            }

            if data[GOD_BYTE_OFFSET] & PRIMARY_BIT != primary_bit_before {
                self.primary_bit_before_failure
                    .store(Self::PRIMARY_BIT_DISABLED, Ordering::SeqCst);
                // Fail before the next mutating operation or sync, so no cleanup
                // writes can run after the primary switch reaches storage.
                self.operations_until_failure.store(1, Ordering::SeqCst);
            }
        }

        fn fail_if_armed(&self) -> Result<(), std::io::Error> {
            let previous = self.operations_until_failure.fetch_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |x| {
                    if x == Self::DISABLED {
                        None
                    } else {
                        Some(x.saturating_sub(1))
                    }
                },
            );

            match previous {
                Ok(1) => {
                    self.corrupt_primary_slot_checksum()?;
                    Err(std::io::Error::from(ErrorKind::Other))
                }
                Ok(0) => Err(std::io::Error::from(ErrorKind::Other)),
                _ => Ok(()),
            }
        }

        fn corrupt_primary_slot_checksum(&self) -> Result<(), std::io::Error> {
            let mut header = self.read_header_directly()?;
            header[GOD_BYTE_OFFSET] |= RECOVERY_REQUIRED;
            corrupt_primary_slot_checksum(&mut header);
            self.write_header_directly(&header)?;

            Ok(())
        }

        fn corrupt_all_slot_checksums(&self) -> Result<(), std::io::Error> {
            let mut header = self.read_header_directly()?;
            corrupt_slot_checksum(&mut header, TRANSACTION_0_OFFSET);
            corrupt_slot_checksum(&mut header, TRANSACTION_1_OFFSET);
            self.write_header_directly(&header)?;

            Ok(())
        }
    }

    impl StorageBackend for FailingBackend {
        fn len(&self) -> Result<u64, std::io::Error> {
            self.inner.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
            self.inner.read(offset, out)
        }

        fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
            self.fail_if_armed()?;
            self.inner.set_len(len)
        }

        fn sync_data(&self) -> Result<(), std::io::Error> {
            self.fail_if_armed()?;
            self.inner.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
            self.fail_if_armed()?;
            self.inner.write(offset, data)?;
            self.arm_failure_if_primary_swapped(offset, data);
            Ok(())
        }

        fn close(&self) -> Result<(), Error> {
            self.inner.close()
        }
    }

    #[test]
    fn repair_allocator_checksums() {
        let tmpfile = crate::create_tempfile();
        let cloned = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmpfile.path())
            .unwrap();
        let backend = FailingBackend::new(FileBackend::new(cloned).unwrap());
        let backend_control = backend.clone();
        let db = Database::builder().create_with_backend(backend).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert("hello", "world").unwrap();
        }
        write_txn.commit().unwrap();

        // Start a read to be sure the previous write isn't garbage collected
        let read_txn = db.begin_read().unwrap();

        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert("hello", "world2").unwrap();
        }

        backend_control.fail_after_primary_swap().unwrap();
        write_txn.commit().unwrap_err();
        drop(read_txn);
        drop(db);

        let cloned = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmpfile.path())
            .unwrap();
        let db2_backend = FailingBackend::new(FileBackend::new(cloned).unwrap());
        let db2_backend_control = db2_backend.clone();
        let mut db2 = Database::builder()
            .create_with_backend(db2_backend)
            .unwrap();
        {
            let read_txn = db2.begin_read().unwrap();
            let table = read_txn.open_table(X).unwrap();
            assert_eq!(table.get("hello").unwrap().unwrap().value(), "world");
        }
        let write_txn = db2.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert("hello2", "world2").unwrap();
        }
        write_txn.commit().unwrap();

        let mut header = db2_backend_control.read_header_directly().unwrap();
        // Simulate a failed non-2PC commit where the primary bit reached disk, but
        // the new primary slot did not. The old primary is now the valid secondary.
        header[GOD_BYTE_OFFSET] ^= PRIMARY_BIT;
        header[GOD_BYTE_OFFSET] |= RECOVERY_REQUIRED;
        header[GOD_BYTE_OFFSET] &= !TWO_PHASE_COMMIT;
        corrupt_primary_slot_checksum(&mut header);
        db2_backend_control.write_header_directly(&header).unwrap();

        assert!(!db2.check_integrity().unwrap());
        {
            let read_txn = db2.begin_read().unwrap();
            let table = read_txn.open_table(X).unwrap();
            assert_eq!(table.get("hello").unwrap().unwrap().value(), "world");
            assert_eq!(table.get("hello2").unwrap().unwrap().value(), "world2");
        }

        db2_backend_control.corrupt_all_slot_checksums().unwrap();
        assert!(matches!(
            db2.check_integrity().unwrap_err(),
            DatabaseError::Storage(StorageError::Corrupted(_))
        ));
    }

    // If the file is externally truncated below the stored layout, both open and check_integrity
    // should report corruption rather than panicking in the layout recalculation.
    #[test]
    fn truncated_file_is_rejected() {
        let tmpfile = crate::create_tempfile();
        let mut db = Database::builder().create(tmpfile.path()).unwrap();
        assert!(db.check_integrity().unwrap());
        drop(db);

        let file = OpenOptions::new().write(true).open(tmpfile.path()).unwrap();
        // Truncate to just the header (no page data) -- this is less than one page on any
        // supported page size, so recalculate() would underflow without the guard.
        file.set_len(crate::tree_store::page_store::header::DB_HEADER_SIZE as u64)
            .unwrap();

        let err = Database::open(tmpfile.path()).unwrap_err();
        assert!(
            matches!(err, DatabaseError::Storage(StorageError::Corrupted(_))),
            "expected Corrupted, got {err:?}"
        );
    }

    #[test]
    fn repair_empty() {
        let tmpfile = crate::create_tempfile();
        let db = Database::builder().create(tmpfile.path()).unwrap();
        drop(db);

        let mut file = tmpfile.as_file();

        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        let mut buffer = [0u8; 1];
        file.read_exact(&mut buffer).unwrap();
        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        buffer[0] |= RECOVERY_REQUIRED;
        file.write_all(&buffer).unwrap();

        Database::open(tmpfile.path()).unwrap();
    }

    #[test]
    fn close_on_drop() {
        let tmpfile = crate::create_tempfile();
        let db = Database::builder()
            .set_cache_size(0)
            .create(tmpfile.path())
            .unwrap();
        let table_def: TableDefinition<u64, u64> = TableDefinition::new("x");
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_def).unwrap();
            table.insert(0, 0).unwrap();
        }
        txn.commit().unwrap();
        let txn = db.begin_read().unwrap();
        drop(db);
        assert!(matches!(
            txn.list_tables().err().unwrap(),
            StorageError::DatabaseClosed
        ));

        let mut file = tmpfile.as_file();

        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        let mut buffer = [0u8; 1];
        file.read_exact(&mut buffer).unwrap();
        assert_eq!(buffer[0] & RECOVERY_REQUIRED, 0);
        drop(txn);
    }

    #[test]
    fn abort_repair() {
        let tmpfile = crate::create_tempfile();
        let db = Database::builder().create(tmpfile.path()).unwrap();
        drop(db);

        let mut file = tmpfile.as_file();

        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        let mut buffer = [0u8; 1];
        file.read_exact(&mut buffer).unwrap();
        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        buffer[0] |= RECOVERY_REQUIRED;
        buffer[0] &= !TWO_PHASE_COMMIT;
        file.write_all(&buffer).unwrap();

        let err = Database::builder()
            .set_repair_callback(|handle| handle.abort())
            .open(tmpfile.path())
            .unwrap_err();
        assert!(matches!(err, DatabaseError::RepairAborted));
    }

    #[test]
    fn repair_insert_reserve_regression() {
        let tmpfile = crate::create_tempfile();
        let db = Database::builder().create(tmpfile.path()).unwrap();

        let def: TableDefinition<&str, &[u8]> = TableDefinition::new("x");

        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(def).unwrap();
            let mut value = table.insert_reserve("hello", 5).unwrap();
            value.as_mut().copy_from_slice(b"world");
        }
        write_txn.commit().unwrap();

        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(def).unwrap();
            let mut value = table.insert_reserve("hello2", 5).unwrap();
            value.as_mut().copy_from_slice(b"world");
        }
        write_txn.commit().unwrap();

        drop(db);

        let mut file = tmpfile.as_file();

        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        let mut buffer = [0u8; 1];
        file.read_exact(&mut buffer).unwrap();
        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        buffer[0] |= RECOVERY_REQUIRED;
        file.write_all(&buffer).unwrap();

        Database::open(tmpfile.path()).unwrap();
    }

    #[test]
    fn magic_number() {
        // Test compliance with some, but not all, provisions recommended by
        // IETF Memo "Care and Feeding of Magic Numbers"

        // Test that magic number is not valid utf-8
        #[allow(invalid_from_utf8)]
        {
            assert!(std::str::from_utf8(&MAGICNUMBER).is_err());
        }
        // Test there is a octet with high-bit set
        assert!(MAGICNUMBER.iter().any(|x| *x & 0x80 != 0));
        // Test there is a non-printable ASCII character
        assert!(MAGICNUMBER.iter().any(|x| *x < 0x20 || *x > 0x7E));
        // Test there is a printable ASCII character
        assert!(MAGICNUMBER.iter().any(|x| *x >= 0x20 && *x <= 0x7E));
        // Test there is a printable ISO-8859 that's non-ASCII printable
        assert!(MAGICNUMBER.iter().any(|x| *x >= 0xA0));
        // Test there is a ISO-8859 control character other than 0x09, 0x0A, 0x0C, 0x0D
        assert!(MAGICNUMBER.iter().any(|x| *x < 0x09
            || *x == 0x0B
            || (0x0E <= *x && *x <= 0x1F)
            || (0x7F <= *x && *x <= 0x9F)));
    }
}
