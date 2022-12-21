use crate::transaction_tracker::TransactionId;
use crate::tree_store::page_store::layout::{DatabaseLayout, RegionLayout};
use crate::tree_store::page_store::page_manager::FILE_FORMAT_VERSION;
use crate::tree_store::page_store::ChecksumType;
use crate::tree_store::{Checksum, PageNumber};
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
// 1 byte: checksum type
// 4 bytes: padding
// 8 bytes: root page
// 16 bytes: root checksum
// 8 bytes: freed table root page
// 16 bytes: freed table root checksum
// 8 bytes: last committed transaction id
// 4 bytes: number of full regions
// 4 bytes: data pages in partial trailing region
// 8 bytes: region tracker page number
// 16 bytes: slot checksum
//
// Commit slot 1 (next 128 bytes):
// Same layout as slot 0

// Inspired by PNG's magic number
const MAGICNUMBER: [u8; 9] = [b'r', b'e', b'd', b'b', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A];
const GOD_BYTE_OFFSET: usize = MAGICNUMBER.len();
const PAGE_SIZE_OFFSET: usize = GOD_BYTE_OFFSET + size_of::<u8>() + 2; // +2 for padding
const REGION_HEADER_PAGES_OFFSET: usize = PAGE_SIZE_OFFSET + size_of::<u32>();
const REGION_MAX_DATA_PAGES_OFFSET: usize = REGION_HEADER_PAGES_OFFSET + size_of::<u32>();
const TRANSACTION_SIZE: usize = 128;
const TRANSACTION_0_OFFSET: usize = 64;
const TRANSACTION_1_OFFSET: usize = TRANSACTION_0_OFFSET + TRANSACTION_SIZE;
pub(super) const DB_HEADER_SIZE: usize = TRANSACTION_1_OFFSET + TRANSACTION_SIZE;

// God byte flags
const PRIMARY_BIT: u8 = 1;
const RECOVERY_REQUIRED: u8 = 2;

// Structure of each commit slot
const VERSION_OFFSET: usize = 0;
const ROOT_NON_NULL_OFFSET: usize = size_of::<u8>();
const FREED_ROOT_NON_NULL_OFFSET: usize = ROOT_NON_NULL_OFFSET + size_of::<u8>();
const CHECKSUM_TYPE_OFFSET: usize = FREED_ROOT_NON_NULL_OFFSET + size_of::<u8>();
const PADDING: usize = 4;
const ROOT_PAGE_OFFSET: usize = CHECKSUM_TYPE_OFFSET + size_of::<u8>() + PADDING;
const ROOT_CHECKSUM_OFFSET: usize = ROOT_PAGE_OFFSET + size_of::<u64>();
const FREED_ROOT_OFFSET: usize = ROOT_CHECKSUM_OFFSET + size_of::<u128>();
const FREED_ROOT_CHECKSUM_OFFSET: usize = FREED_ROOT_OFFSET + size_of::<u64>();
const TRANSACTION_ID_OFFSET: usize = FREED_ROOT_CHECKSUM_OFFSET + size_of::<u128>();
const NUM_FULL_REGIONS_OFFSET: usize = TRANSACTION_ID_OFFSET + size_of::<u64>();
const TRAILING_REGION_DATA_PAGES_OFFSET: usize = NUM_FULL_REGIONS_OFFSET + size_of::<u32>();
const REGION_TRACKER_PAGE_NUMBER_OFFSET: usize =
    TRAILING_REGION_DATA_PAGES_OFFSET + size_of::<u32>();
const SLOT_CHECKSUM_OFFSET: usize =
    REGION_TRACKER_PAGE_NUMBER_OFFSET + PageNumber::serialized_size();
const TRANSACTION_LAST_FIELD: usize = SLOT_CHECKSUM_OFFSET + size_of::<u128>();

fn get_u32(data: &[u8]) -> u32 {
    u32::from_le_bytes(data[..size_of::<u32>()].try_into().unwrap())
}

fn get_u64(data: &[u8]) -> u64 {
    u64::from_le_bytes(data[..size_of::<u64>()].try_into().unwrap())
}

#[derive(Copy, Clone)]
pub(super) struct HeaderRepairInfo {
    pub(super) invalid_magic_number: bool,
    pub(super) primary_corrupted: bool,
    pub(super) secondary_corrupted: bool,
}

pub(super) struct DatabaseHeader {
    primary_slot: usize,
    pub(super) recovery_required: bool,
    page_size: u32,
    region_header_pages: u32,
    region_max_data_pages: u32,
    transaction_slots: [TransactionHeader; 2],
}

impl DatabaseHeader {
    pub(super) fn new(
        layout: DatabaseLayout,
        checksum_type: ChecksumType,
        transaction_id: TransactionId,
        region_tracker: PageNumber,
    ) -> Self {
        #[allow(clippy::assertions_on_constants)]
        {
            assert!(TRANSACTION_LAST_FIELD <= TRANSACTION_SIZE);
        }

        let slot = TransactionHeader::new(checksum_type, transaction_id, region_tracker, layout);
        Self {
            primary_slot: 0,
            recovery_required: true,
            page_size: layout.full_region_layout().page_size(),
            region_header_pages: layout.full_region_layout().get_header_pages(),
            region_max_data_pages: layout.full_region_layout().num_pages(),
            transaction_slots: [slot.clone(), slot],
        }
    }

    pub(super) fn page_size(&self) -> u32 {
        self.page_size
    }

    pub(super) fn region_header_pages(&self) -> u32 {
        self.region_header_pages
    }

    pub(super) fn region_max_data_pages(&self) -> u32 {
        self.region_max_data_pages
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

    // TODO: consider returning an Err with the repair info
    pub(super) fn from_bytes(data: &[u8]) -> (Self, HeaderRepairInfo) {
        let invalid_magic_number = data[..MAGICNUMBER.len()] != MAGICNUMBER;

        let primary_slot = usize::from(data[GOD_BYTE_OFFSET] & PRIMARY_BIT != 0);
        let recovery_required = (data[GOD_BYTE_OFFSET] & RECOVERY_REQUIRED) != 0;
        let page_size = get_u32(&data[PAGE_SIZE_OFFSET..]);
        let region_header_pages = get_u32(&data[REGION_HEADER_PAGES_OFFSET..]);
        let region_max_data_pages = get_u32(&data[REGION_MAX_DATA_PAGES_OFFSET..]);
        let full_region_layout =
            RegionLayout::new(region_max_data_pages, region_header_pages, page_size);
        let (slot0, slot0_corrupted) =
            TransactionHeader::from_bytes(&data[TRANSACTION_0_OFFSET..], full_region_layout);
        let (slot1, slot1_corrupted) =
            TransactionHeader::from_bytes(&data[TRANSACTION_1_OFFSET..], full_region_layout);
        let (primary_corrupted, secondary_corrupted) = if primary_slot == 0 {
            (slot0_corrupted, slot1_corrupted)
        } else {
            (slot1_corrupted, slot0_corrupted)
        };

        let result = Self {
            primary_slot,
            recovery_required,
            page_size,
            region_header_pages,
            region_max_data_pages,
            transaction_slots: [slot0, slot1],
        };
        let repair = HeaderRepairInfo {
            invalid_magic_number,
            primary_corrupted,
            secondary_corrupted,
        };
        (result, repair)
    }

    pub(super) fn to_bytes(
        &self,
        include_magic_number: bool,
        swap_primary: bool,
    ) -> [u8; DB_HEADER_SIZE] {
        let mut result = [0; DB_HEADER_SIZE];
        if include_magic_number {
            result[..MAGICNUMBER.len()].copy_from_slice(&MAGICNUMBER);
        }
        result[GOD_BYTE_OFFSET] = self.primary_slot.try_into().unwrap();
        if swap_primary {
            result[GOD_BYTE_OFFSET] ^= PRIMARY_BIT;
        }
        if self.recovery_required {
            result[GOD_BYTE_OFFSET] |= RECOVERY_REQUIRED;
        }
        result[PAGE_SIZE_OFFSET..(PAGE_SIZE_OFFSET + size_of::<u32>())]
            .copy_from_slice(&self.page_size.to_le_bytes());
        result[REGION_HEADER_PAGES_OFFSET..(REGION_HEADER_PAGES_OFFSET + size_of::<u32>())]
            .copy_from_slice(&self.region_header_pages.to_le_bytes());
        result[REGION_MAX_DATA_PAGES_OFFSET..(REGION_MAX_DATA_PAGES_OFFSET + size_of::<u32>())]
            .copy_from_slice(&self.region_max_data_pages.to_le_bytes());
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
    pub(super) checksum_type: ChecksumType,
    pub(super) root: Option<(PageNumber, Checksum)>,
    pub(super) freed_root: Option<(PageNumber, Checksum)>,
    pub(super) transaction_id: TransactionId,
    pub(super) region_tracker: PageNumber,
    pub(super) layout: DatabaseLayout,
}

impl TransactionHeader {
    fn new(
        checksum_type: ChecksumType,
        transaction_id: TransactionId,
        region_tracker: PageNumber,
        layout: DatabaseLayout,
    ) -> Self {
        Self {
            version: FILE_FORMAT_VERSION,
            checksum_type,
            root: None,
            freed_root: None,
            transaction_id,
            region_tracker,
            layout,
        }
    }

    // Returned bool indicates whether the checksum was corrupted
    pub(super) fn from_bytes(data: &[u8], full_region_layout: RegionLayout) -> (Self, bool) {
        let version = data[VERSION_OFFSET];
        let checksum_type = ChecksumType::from(data[CHECKSUM_TYPE_OFFSET]);
        let checksum = Checksum::from_le_bytes(
            data[SLOT_CHECKSUM_OFFSET..(SLOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
                .try_into()
                .unwrap(),
        );
        let corrupted = checksum != checksum_type.checksum(&data[..SLOT_CHECKSUM_OFFSET]);

        let root = if data[ROOT_NON_NULL_OFFSET] != 0 {
            let page = PageNumber::from_le_bytes(
                data[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + PageNumber::serialized_size())]
                    .try_into()
                    .unwrap(),
            );
            let checksum = Checksum::from_le_bytes(
                data[ROOT_CHECKSUM_OFFSET..(ROOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
                    .try_into()
                    .unwrap(),
            );
            Some((page, checksum))
        } else {
            None
        };
        let freed_root = if data[FREED_ROOT_NON_NULL_OFFSET] != 0 {
            let page = PageNumber::from_le_bytes(
                data[FREED_ROOT_OFFSET..(FREED_ROOT_OFFSET + PageNumber::serialized_size())]
                    .try_into()
                    .unwrap(),
            );
            let checksum = Checksum::from_le_bytes(
                data[FREED_ROOT_CHECKSUM_OFFSET
                    ..(FREED_ROOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
                    .try_into()
                    .unwrap(),
            );
            Some((page, checksum))
        } else {
            None
        };
        let transaction_id = TransactionId(get_u64(&data[TRANSACTION_ID_OFFSET..]));
        let region_tracker = PageNumber::from_le_bytes(
            data[REGION_TRACKER_PAGE_NUMBER_OFFSET
                ..(REGION_TRACKER_PAGE_NUMBER_OFFSET + PageNumber::serialized_size())]
                .try_into()
                .unwrap(),
        );
        let full_regions = get_u32(&data[NUM_FULL_REGIONS_OFFSET..]);
        let trailing_data_pages = get_u32(&data[TRAILING_REGION_DATA_PAGES_OFFSET..]);
        let trailing_region = if trailing_data_pages > 0 {
            Some(RegionLayout::new(
                trailing_data_pages,
                full_region_layout.get_header_pages(),
                full_region_layout.page_size(),
            ))
        } else {
            None
        };
        let layout = DatabaseLayout::new(full_regions, full_region_layout, trailing_region);

        let result = Self {
            version,
            checksum_type,
            root,
            freed_root,
            transaction_id,
            region_tracker,
            layout,
        };

        (result, corrupted)
    }

    pub(super) fn to_bytes(&self) -> [u8; TRANSACTION_SIZE] {
        let mut result = [0; TRANSACTION_SIZE];
        result[VERSION_OFFSET] = self.version;
        result[CHECKSUM_TYPE_OFFSET] = self.checksum_type.into();
        if let Some((page, checksum)) = self.root {
            result[ROOT_NON_NULL_OFFSET] = 1;
            result[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + PageNumber::serialized_size())]
                .copy_from_slice(&page.to_le_bytes());
            result[ROOT_CHECKSUM_OFFSET..(ROOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
                .copy_from_slice(&checksum.to_le_bytes());
        }
        if let Some((page, checksum)) = self.freed_root {
            result[FREED_ROOT_NON_NULL_OFFSET] = 1;
            result[FREED_ROOT_OFFSET..(FREED_ROOT_OFFSET + PageNumber::serialized_size())]
                .copy_from_slice(&page.to_le_bytes());
            result
                [FREED_ROOT_CHECKSUM_OFFSET..(FREED_ROOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
                .copy_from_slice(&checksum.to_le_bytes());
        }
        result[TRANSACTION_ID_OFFSET..(TRANSACTION_ID_OFFSET + size_of::<u64>())]
            .copy_from_slice(&self.transaction_id.0.to_le_bytes());
        result[NUM_FULL_REGIONS_OFFSET..(NUM_FULL_REGIONS_OFFSET + size_of::<u32>())]
            .copy_from_slice(&self.layout.num_full_regions().to_le_bytes());
        if let Some(trailing) = self.layout.trailing_region_layout() {
            result[TRAILING_REGION_DATA_PAGES_OFFSET
                ..(TRAILING_REGION_DATA_PAGES_OFFSET + size_of::<u32>())]
                .copy_from_slice(&trailing.num_pages().to_le_bytes());
        }
        result[REGION_TRACKER_PAGE_NUMBER_OFFSET
            ..(REGION_TRACKER_PAGE_NUMBER_OFFSET + PageNumber::serialized_size())]
            .copy_from_slice(&self.region_tracker.to_le_bytes());
        let checksum = self.checksum_type.checksum(&result[..SLOT_CHECKSUM_OFFSET]);
        result[SLOT_CHECKSUM_OFFSET..(SLOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
            .copy_from_slice(&checksum.to_le_bytes());

        result
    }
}

#[cfg(test)]
mod test {
    use crate::db::TableDefinition;
    use crate::tree_store::page_store::header::{
        GOD_BYTE_OFFSET, MAGICNUMBER, PRIMARY_BIT, RECOVERY_REQUIRED, ROOT_CHECKSUM_OFFSET,
        TRANSACTION_0_OFFSET, TRANSACTION_1_OFFSET,
    };
    use crate::tree_store::page_store::TransactionalMemory;
    use crate::{Database, ReadableTable, WriteStrategy};
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::mem::size_of;
    use tempfile::NamedTempFile;

    const X: TableDefinition<&[u8], &[u8]> = TableDefinition::new("x");

    #[test]
    fn repair_allocator_no_checksums() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe {
            Database::builder()
                .set_write_strategy(WriteStrategy::TwoPhase)
                .create(tmpfile.path())
                .unwrap()
        };
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert(b"hello", b"world").unwrap();
        }
        write_txn.commit().unwrap();
        let write_txn = db.begin_write().unwrap();
        let allocated_pages = write_txn.stats().unwrap().allocated_pages();
        write_txn.abort().unwrap();
        drop(db);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmpfile.path())
            .unwrap();

        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        let mut buffer = [0u8; 1];
        file.read_exact(&mut buffer).unwrap();
        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        buffer[0] |= RECOVERY_REQUIRED;
        file.write_all(&buffer).unwrap();

        assert!(TransactionalMemory::new(
            file,
            false,
            None,
            None,
            None,
            0,
            0,
            Some(WriteStrategy::TwoPhase)
        )
        .unwrap()
        .needs_repair()
        .unwrap());

        let db2 = unsafe {
            Database::builder()
                .set_write_strategy(WriteStrategy::TwoPhase)
                .create(tmpfile.path())
                .unwrap()
        };
        let write_txn = db2.begin_write().unwrap();
        assert_eq!(
            allocated_pages,
            write_txn.stats().unwrap().allocated_pages()
        );
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert(b"hello2", b"world2").unwrap();
        }
        write_txn.commit().unwrap();
    }

    #[test]
    fn repair_allocator_checksums() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe {
            Database::builder()
                .set_write_strategy(WriteStrategy::Checksum)
                .create(tmpfile.path())
                .unwrap()
        };
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert(b"hello", b"world").unwrap();
        }
        write_txn.commit().unwrap();

        // Start a read to be sure the previous write isn't garbage collected
        let read_txn = db.begin_read().unwrap();

        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert(b"hello", b"world2").unwrap();
        }
        write_txn.commit().unwrap();
        drop(read_txn);
        drop(db);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmpfile.path())
            .unwrap();

        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        let mut buffer = [0u8; 1];
        file.read_exact(&mut buffer).unwrap();
        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        buffer[0] |= RECOVERY_REQUIRED;
        file.write_all(&buffer).unwrap();

        // Overwrite the primary checksum to simulate a failure during commit
        let primary_slot_offset = if buffer[0] & PRIMARY_BIT == 0 {
            TRANSACTION_0_OFFSET
        } else {
            TRANSACTION_1_OFFSET
        };
        file.seek(SeekFrom::Start(
            (primary_slot_offset + ROOT_CHECKSUM_OFFSET) as u64,
        ))
        .unwrap();
        file.write_all(&[0; size_of::<u128>()]).unwrap();

        assert!(TransactionalMemory::new(
            file,
            false,
            None,
            None,
            None,
            0,
            0,
            Some(WriteStrategy::Checksum)
        )
        .unwrap()
        .needs_repair()
        .unwrap());

        let db2 = unsafe { Database::create(tmpfile.path()).unwrap() };
        let write_txn = db2.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            assert_eq!(table.get(b"hello").unwrap().unwrap().value(), b"world");
            table.insert(b"hello2", b"world2").unwrap();
        }
        write_txn.commit().unwrap();
    }

    #[test]
    fn change_write_strategy_to_2pc() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe {
            Database::builder()
                .set_write_strategy(WriteStrategy::Checksum)
                .create(tmpfile.path())
                .unwrap()
        };
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert(b"hello", b"world").unwrap();
        }
        write_txn.commit().unwrap();
        let write_txn = db.begin_write().unwrap();
        let allocated_pages = write_txn.stats().unwrap().allocated_pages();
        write_txn.abort().unwrap();
        db.set_write_strategy(WriteStrategy::TwoPhase).unwrap();
        drop(db);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmpfile.path())
            .unwrap();

        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        let mut buffer = [0u8; 1];
        file.read_exact(&mut buffer).unwrap();
        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        buffer[0] |= RECOVERY_REQUIRED;
        file.write_all(&buffer).unwrap();

        assert!(TransactionalMemory::new(
            file,
            false,
            None,
            None,
            None,
            0,
            0,
            Some(WriteStrategy::TwoPhase)
        )
        .unwrap()
        .needs_repair()
        .unwrap());

        let db2 = unsafe {
            Database::builder()
                .set_write_strategy(WriteStrategy::TwoPhase)
                .create(tmpfile.path())
                .unwrap()
        };
        let write_txn = db2.begin_write().unwrap();
        assert_eq!(
            allocated_pages,
            write_txn.stats().unwrap().allocated_pages()
        );
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert(b"hello2", b"world2").unwrap();
        }
        write_txn.commit().unwrap();
    }

    #[test]
    fn repair_insert_reserve_regression() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe {
            Database::builder()
                .set_write_strategy(WriteStrategy::Checksum)
                .create(tmpfile.path())
                .unwrap()
        };

        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            let mut value = table.insert_reserve(b"hello", 5).unwrap();
            value.as_mut().copy_from_slice(b"world");
        }
        write_txn.commit().unwrap();

        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            let mut value = table.insert_reserve(b"hello2", 5).unwrap();
            value.as_mut().copy_from_slice(b"world");
        }
        write_txn.commit().unwrap();

        drop(db);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmpfile.path())
            .unwrap();

        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        let mut buffer = [0u8; 1];
        file.read_exact(&mut buffer).unwrap();
        file.seek(SeekFrom::Start(GOD_BYTE_OFFSET as u64)).unwrap();
        buffer[0] |= RECOVERY_REQUIRED;
        file.write_all(&buffer).unwrap();

        assert!(TransactionalMemory::new(
            file,
            false,
            None,
            None,
            None,
            0,
            0,
            Some(WriteStrategy::Checksum),
        )
        .unwrap()
        .needs_repair()
        .unwrap());

        unsafe { Database::open(tmpfile.path()).unwrap() };
    }

    #[test]
    fn magic_number() {
        // Test compliance with some, but not all, provisions recommended by
        // IETF Memo "Care and Feeding of Magic Numbers"

        // Test that magic number is not valid utf-8
        assert!(std::str::from_utf8(&MAGICNUMBER).is_err());
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
