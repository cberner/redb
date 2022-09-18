use crate::db::WriteStrategy;
use crate::tree_store::btree_base::Checksum;
use crate::tree_store::page_store::bitmap::{BtreeBitmap, BtreeBitmapMut};
use crate::tree_store::page_store::layout::{DatabaseLayout, RegionLayout};
use crate::tree_store::page_store::mmap::Mmap;
use crate::tree_store::page_store::region::{RegionHeaderAccessor, RegionHeaderMutator};
use crate::tree_store::page_store::utils::get_page_size;
use crate::tree_store::page_store::{hash128_with_seed, PageImpl, PageMut};
use crate::tree_store::PageNumber;
use crate::Error;
use crate::Result;
use std::cmp::{max, min};
#[cfg(debug_assertions)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryInto;
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

// Database layout:
//
// Super-header (header + region tracker state)
// The super-header length is rounded up to the nearest full page size
//
// Header (first 64 bytes):
// 9 bytes: magic number
// 1 byte: god byte
// 1 byte: checksum type
// 1 byte: padding
// 4 bytes: super-header pages
// 4 bytes: page size
// 4 bytes: region tracker state length
// 8 bytes: database max size
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
// 8 bytes: freed table root page
// 16 bytes: freed table root checksum
// 8 bytes: last committed transaction id
// 4 bytes: number of full regions
// 4 bytes: data pages in partial trailing region
// 16 bytes: slot checksum
//
// Commit slot 1 (next 128 bytes):
// Same layout as slot 0
//
// Region tracker state (byte 512...(512 + n))

// Regions have a maximum size of 4GiB. A `4GiB - overhead` value is the largest that can be represented,
// because the leaf node format uses 32bit offsets
const MAX_USABLE_REGION_SPACE: u64 = 4 * 1024 * 1024 * 1024;
pub(crate) const MAX_MAX_PAGE_ORDER: usize = 20;
pub(super) const MIN_USABLE_PAGES: usize = 10;
const MIN_DESIRED_USABLE_BYTES: usize = 1024 * 1024;

// TODO: set to 1, when version 1.0 is released
const FILE_FORMAT_VERSION: u8 = 106;

// Inspired by PNG's magic number
const MAGICNUMBER: [u8; 9] = [b'r', b'e', b'd', b'b', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A];
const GOD_BYTE_OFFSET: usize = MAGICNUMBER.len();
const CHECKSUM_TYPE_OFFSET: usize = GOD_BYTE_OFFSET + size_of::<u8>();
const SUPERHEADER_PAGES_OFFSET: usize = CHECKSUM_TYPE_OFFSET + size_of::<u8>() + 1; // +1 for padding
const PAGE_SIZE_OFFSET: usize = SUPERHEADER_PAGES_OFFSET + size_of::<u32>();
const REGION_TRACKER_LENGTH_OFFSET: usize = PAGE_SIZE_OFFSET + size_of::<u32>();
const DB_SIZE_OFFSET: usize = REGION_TRACKER_LENGTH_OFFSET + size_of::<u32>();
const REGION_HEADER_PAGES_OFFSET: usize = DB_SIZE_OFFSET + size_of::<u64>();
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
const PADDING: usize = 5;
const ROOT_PAGE_OFFSET: usize = FREED_ROOT_NON_NULL_OFFSET + size_of::<u8>() + PADDING;
const ROOT_CHECKSUM_OFFSET: usize = ROOT_PAGE_OFFSET + size_of::<u64>();
const FREED_ROOT_OFFSET: usize = ROOT_CHECKSUM_OFFSET + size_of::<u128>();
const FREED_ROOT_CHECKSUM_OFFSET: usize = FREED_ROOT_OFFSET + size_of::<u64>();
const TRANSACTION_ID_OFFSET: usize = FREED_ROOT_CHECKSUM_OFFSET + size_of::<u128>();
const NUM_FULL_REGIONS_OFFSET: usize = TRANSACTION_ID_OFFSET + size_of::<u64>();
const TRAILING_REGION_DATA_PAGES_OFFSET: usize = NUM_FULL_REGIONS_OFFSET + size_of::<u32>();
const SLOT_CHECKSUM_OFFSET: usize = TRAILING_REGION_DATA_PAGES_OFFSET + size_of::<u32>();
const TRANSACTION_LAST_FIELD: usize = SLOT_CHECKSUM_OFFSET + size_of::<u128>();

fn ceil_log2(x: usize) -> usize {
    if x.is_power_of_two() {
        x.trailing_zeros() as usize
    } else {
        x.next_power_of_two().trailing_zeros() as usize
    }
}

pub(crate) fn get_db_size(path: impl AsRef<Path>) -> Result<u64, io::Error> {
    let mut db_size = [0u8; size_of::<u64>()];
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(DB_SIZE_OFFSET as u64))?;
    file.read_exact(&mut db_size)?;
    Ok(u64::from_le_bytes(db_size))
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub(crate) enum ChecksumType {
    Zero, // No checksum is calculated. Just stores zero
    XXH3_128,
}

impl ChecksumType {
    pub(crate) fn checksum(&self, data: &[u8]) -> Checksum {
        match self {
            ChecksumType::Zero => 0,
            ChecksumType::XXH3_128 => hash128_with_seed(data, 0),
        }
    }
}

impl From<u8> for ChecksumType {
    fn from(x: u8) -> Self {
        match x {
            1 => ChecksumType::Zero,
            2 => ChecksumType::XXH3_128,
            _ => unimplemented!(),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<u8> for ChecksumType {
    fn into(self) -> u8 {
        match self {
            ChecksumType::Zero => 1,
            ChecksumType::XXH3_128 => 2,
        }
    }
}

// Marker struct for the mutex guarding the metadata (header & allocators)
struct MetadataGuard;

// Safety: MetadataAccessor may only use self.mmap to access the allocator states
struct MetadataAccessor<'a> {
    header: &'a mut [u8],
    mmap: &'a Mmap,
    guard: MutexGuard<'a, MetadataGuard>,
}

impl<'a> MetadataAccessor<'a> {
    // Safety: Caller must ensure that no other references to metadata memory exist, or are created
    // during the lifetime 'a
    unsafe fn new(mmap: &'a Mmap, guard: MutexGuard<'a, MetadataGuard>) -> Self {
        let header = mmap.get_memory_mut(0..DB_HEADER_SIZE);
        Self {
            header,
            mmap,
            guard,
        }
    }

    fn make_region_layout(&self, num_pages: usize) -> RegionLayout {
        RegionLayout::new(
            num_pages,
            self.get_region_header_length(),
            self.get_page_size(),
        )
    }

    // TODO: refactor so that we're not passing DatabaseLayout objects all over the place
    fn get_primary_layout(&self) -> DatabaseLayout {
        let trailing_pages = self.primary_slot().get_trailing_region_data_pages();
        let num_full_regions = self.primary_slot().get_full_regions();

        let full_region = self.make_region_layout(self.get_region_max_data_pages());
        let trailing_region = trailing_pages.map(|x| self.make_region_layout(x));

        DatabaseLayout::new(
            self.get_superheader_length(),
            self.get_region_tracker_state_length(),
            num_full_regions,
            full_region,
            trailing_region,
        )
    }

    fn get_secondary_layout(&self) -> DatabaseLayout {
        let trailing_pages = self.secondary_slot().get_trailing_region_data_pages();
        let num_full_regions = self.secondary_slot().get_full_regions();

        let full_region = self.make_region_layout(self.get_region_max_data_pages());
        let trailing_region = trailing_pages.map(|x| self.make_region_layout(x));

        DatabaseLayout::new(
            self.get_superheader_length(),
            self.get_region_tracker_state_length(),
            num_full_regions,
            full_region,
            trailing_region,
        )
    }

    fn primary_slot(&self) -> TransactionAccessor {
        let start = if self.header[GOD_BYTE_OFFSET] & PRIMARY_BIT == 0 {
            TRANSACTION_0_OFFSET
        } else {
            TRANSACTION_1_OFFSET
        };
        let end = start + TRANSACTION_SIZE;

        let mem = &self.header[start..end];
        TransactionAccessor::new(mem, &self.guard)
    }

    fn secondary_slot(&self) -> TransactionAccessor {
        let start = if self.header[GOD_BYTE_OFFSET] & PRIMARY_BIT == 0 {
            TRANSACTION_1_OFFSET
        } else {
            TRANSACTION_0_OFFSET
        };
        let end = start + TRANSACTION_SIZE;

        let mem = &self.header[start..end];
        TransactionAccessor::new(mem, &self.guard)
    }

    fn secondary_slot_mut(&mut self) -> TransactionMutator {
        let start = if self.header[GOD_BYTE_OFFSET] & PRIMARY_BIT == 0 {
            TRANSACTION_1_OFFSET
        } else {
            TRANSACTION_0_OFFSET
        };
        let end = start + TRANSACTION_SIZE;

        let mem = &mut self.header[start..end];
        TransactionMutator::new(mem)
    }

    fn swap_primary(&mut self) {
        if self.header[GOD_BYTE_OFFSET] & PRIMARY_BIT == 0 {
            self.header[GOD_BYTE_OFFSET] |= PRIMARY_BIT;
        } else {
            self.header[GOD_BYTE_OFFSET] &= !PRIMARY_BIT;
        }
    }

    fn get_checksum_type(&self) -> ChecksumType {
        ChecksumType::from(self.header[CHECKSUM_TYPE_OFFSET])
    }

    fn set_checksum_type(&mut self, checksum: ChecksumType) {
        self.header[CHECKSUM_TYPE_OFFSET] = checksum.into();
    }

    fn get_max_capacity(&self) -> u64 {
        u64::from_le_bytes(
            self.header[DB_SIZE_OFFSET..DB_SIZE_OFFSET + size_of::<u64>()]
                .try_into()
                .unwrap(),
        )
    }

    fn set_max_capacity(&mut self, max_size: u64) {
        self.header[DB_SIZE_OFFSET..DB_SIZE_OFFSET + size_of::<u64>()]
            .copy_from_slice(&max_size.to_le_bytes());
    }

    fn get_magic_number(&self) -> [u8; MAGICNUMBER.len()] {
        self.header[..MAGICNUMBER.len()].try_into().unwrap()
    }

    fn set_magic_number(&mut self) {
        self.header[..MAGICNUMBER.len()].copy_from_slice(&MAGICNUMBER);
    }

    fn get_superheader_length(&self) -> usize {
        let pages = u32::from_le_bytes(
            self.header[SUPERHEADER_PAGES_OFFSET..(SUPERHEADER_PAGES_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize;
        pages * self.get_page_size()
    }

    fn set_superheader_length(&mut self, length: usize) {
        assert_eq!(length % self.get_page_size(), 0);
        let pages = length / self.get_page_size();
        self.header[SUPERHEADER_PAGES_OFFSET..(SUPERHEADER_PAGES_OFFSET + size_of::<u32>())]
            .copy_from_slice(&(pages as u32).to_le_bytes());
    }

    fn get_region_tracker_state_length(&self) -> usize {
        u32::from_le_bytes(
            self.header
                [REGION_TRACKER_LENGTH_OFFSET..(REGION_TRACKER_LENGTH_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn set_region_tracker_state_length(&mut self, length: usize) {
        self.header
            [REGION_TRACKER_LENGTH_OFFSET..(REGION_TRACKER_LENGTH_OFFSET + size_of::<u32>())]
            .copy_from_slice(&(length as u32).to_le_bytes());
    }

    fn get_region_header_length(&self) -> usize {
        let pages = u32::from_le_bytes(
            self.header
                [REGION_HEADER_PAGES_OFFSET..(REGION_HEADER_PAGES_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize;
        pages * self.get_page_size()
    }

    fn set_region_header_length(&mut self, length: usize) {
        assert_eq!(length % self.get_page_size(), 0);
        let pages = length / self.get_page_size();
        self.header[REGION_HEADER_PAGES_OFFSET..(REGION_HEADER_PAGES_OFFSET + size_of::<u32>())]
            .copy_from_slice(&(pages as u32).to_le_bytes());
    }

    // TODO: remove this method
    fn get_region_max_usable_bytes(&self) -> u64 {
        self.get_region_max_data_pages() as u64 * self.get_page_size() as u64
    }

    fn get_region_max_data_pages(&self) -> usize {
        u32::from_le_bytes(
            self.header
                [REGION_MAX_DATA_PAGES_OFFSET..(REGION_MAX_DATA_PAGES_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn set_region_max_data_pages(&mut self, pages: usize) {
        self.header
            [REGION_MAX_DATA_PAGES_OFFSET..(REGION_MAX_DATA_PAGES_OFFSET + size_of::<u32>())]
            .copy_from_slice(&(pages as u32).to_le_bytes());
    }

    fn get_page_size(&self) -> usize {
        u32::from_le_bytes(
            self.header[PAGE_SIZE_OFFSET..(PAGE_SIZE_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn set_page_size(&mut self, page_size: usize) {
        self.header[PAGE_SIZE_OFFSET..(PAGE_SIZE_OFFSET + size_of::<u32>())]
            .copy_from_slice(&(page_size as u32).to_le_bytes());
    }

    fn get_recovery_required(&self) -> bool {
        self.header[GOD_BYTE_OFFSET] & RECOVERY_REQUIRED != 0
    }

    fn set_recovery(&mut self, required: bool) {
        if required {
            self.header[GOD_BYTE_OFFSET] |= RECOVERY_REQUIRED;
        } else {
            self.header[GOD_BYTE_OFFSET] &= !RECOVERY_REQUIRED;
        }
    }

    fn get_region(&mut self, region: usize, layout: &DatabaseLayout) -> RegionHeaderAccessor {
        let base = layout.region_base_address(region);
        let len = layout.region_layout(region).data_section().start;
        let absolute = base..(base + len);

        // Safety: We own the metadata lock, so there can't be any other references
        // and this function takes &mut self, so the returned lifetime can't overlap with any other
        // calls into MetadataAccessor
        let mem = unsafe { self.mmap.get_memory(absolute) };

        RegionHeaderAccessor::new(mem)
    }

    fn initialize_region_tracker(&mut self, layout: &DatabaseLayout) {
        let max_regions = DatabaseLayout::calculate(
            self.get_max_capacity(),
            self.get_max_capacity(),
            self.get_region_max_usable_bytes(),
            self.get_page_size(),
        )
        .unwrap()
        .num_regions();

        let range = layout.region_tracker_address_range();

        // Safety: We own the metadata lock, so there can't be any other references
        // and this function takes &mut self, so the returned lifetime can't overlap with any other
        // calls into MetadataAccessor
        assert!(range.start >= DB_HEADER_SIZE);
        let mem = unsafe { self.mmap.get_memory_mut(range) };
        RegionTracker::init_new(max_regions, MAX_MAX_PAGE_ORDER + 1, mem);
    }

    // Note: It's very important that the lifetime of the returned allocator accessors is the same
    // as self, since self holds the metadata lock
    fn allocators_mut(
        &mut self,
        layout: &DatabaseLayout,
    ) -> Result<(RegionTracker, RegionsAccessor)> {
        if !self.get_recovery_required() {
            self.set_recovery(true);
            self.mmap.flush()?
        }

        let range = layout.region_tracker_address_range();

        // Safety: We own the metadata lock, so there can't be any other references
        // and this function takes &mut self, so the returned lifetime can't overlap with any other
        // calls into MetadataAccessor
        assert!(range.start >= DB_HEADER_SIZE);
        let mem = unsafe { self.mmap.get_memory_mut(range) };

        // Safety: Same as above, and RegionAccessor promises to only access regional metadata,
        // which does not overlap the above
        let region_accessor = RegionsAccessor {
            mmap: self.mmap,
            layout: layout.clone(),
        };
        Ok((RegionTracker::new(mem), region_accessor))
    }
}

// Tracks the page orders that MAY BE free in each region. This data structure is optimistic, so
// a region may not actually have a page free for a given order
//
// Format:
// num_allocators: u32 number of allocators
// allocator_len: u32 length of each allocator
// data: BtreeBitmap data for each order
pub(crate) struct RegionTracker<'a> {
    data: &'a mut [u8],
}

impl<'a> RegionTracker<'a> {
    pub(crate) fn new(data: &'a mut [u8]) -> Self {
        Self { data }
    }

    pub(crate) fn required_bytes(regions: usize, orders: usize) -> usize {
        2 * size_of::<u32>() + orders * BtreeBitmapMut::required_space(regions)
    }

    pub(crate) fn init_new(regions: usize, orders: usize, data: &'a mut [u8]) -> Self {
        assert!(data.len() >= Self::required_bytes(regions, orders));
        data[..4].copy_from_slice(&(orders as u32).to_le_bytes());
        data[4..8].copy_from_slice(&(BtreeBitmapMut::required_space(regions) as u32).to_le_bytes());

        let mut result = Self { data };
        for i in 0..orders {
            BtreeBitmapMut::init_new(result.get_order_mut(i), regions);
        }

        result
    }

    pub(crate) fn find_free(&self, order: usize) -> Result<u64> {
        let mem = self.get_order(order);
        let accessor = BtreeBitmap::new(mem);
        accessor.find_first_unset()
    }

    pub(crate) fn mark_free(&mut self, order: usize, region: u64) {
        assert!(order < self.suballocators());
        for i in 0..=order {
            let start = 8 + i * self.suballocator_len();
            let end = start + self.suballocator_len();
            let mem = &mut self.data[start..end];
            let mut accessor = BtreeBitmapMut::new(mem);
            accessor.clear(region);
        }
    }

    pub(crate) fn mark_full(&mut self, order: usize, region: u64) {
        assert!(order < self.suballocators());
        for i in order..self.suballocators() {
            let start = 8 + i * self.suballocator_len();
            let end = start + self.suballocator_len();
            let mem = &mut self.data[start..end];
            let mut accessor = BtreeBitmapMut::new(mem);
            accessor.set(region);
        }
    }

    fn suballocator_len(&self) -> usize {
        u32::from_le_bytes(self.data[4..8].try_into().unwrap()) as usize
    }

    fn suballocators(&self) -> usize {
        u32::from_le_bytes(self.data[..4].try_into().unwrap()) as usize
    }

    fn get_order_mut(&mut self, order: usize) -> &mut [u8] {
        assert!(order < self.suballocators());
        let start = 8 + order * self.suballocator_len();
        let end = start + self.suballocator_len();
        &mut self.data[start..end]
    }

    fn get_order(&self, order: usize) -> &[u8] {
        assert!(order < self.suballocators());
        let start = 8 + order * self.suballocator_len();
        let end = start + self.suballocator_len();
        &self.data[start..end]
    }
}

// Safety: RegionAccessor may only access regional metadata, and no other references to it may exist
struct RegionsAccessor<'a> {
    mmap: &'a Mmap,
    layout: DatabaseLayout,
}

impl<'a> RegionsAccessor<'a> {
    fn get_region_mut(&mut self, region: usize) -> RegionHeaderMutator {
        // Safety: We have exclusive access to regional metadata
        let base = self.layout.region_base_address(region);
        let region_header_len = &self.layout.region_layout(region).data_section().start;
        let absolute = base..(base + region_header_len);

        assert!(absolute.start >= self.layout.header_bytes());
        let mem = unsafe { self.mmap.get_memory_mut(absolute) };

        RegionHeaderMutator::new(mem)
    }
}

struct TransactionAccessor<'a> {
    mem: &'a [u8],
    _guard: &'a MutexGuard<'a, MetadataGuard>,
}

impl<'a> TransactionAccessor<'a> {
    fn new(mem: &'a [u8], guard: &'a MutexGuard<'a, MetadataGuard>) -> Self {
        TransactionAccessor { mem, _guard: guard }
    }

    fn verify_checksum(&self, checksum_type: ChecksumType) -> bool {
        let checksum = Checksum::from_le_bytes(
            self.mem[SLOT_CHECKSUM_OFFSET..(SLOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
                .try_into()
                .unwrap(),
        );
        checksum_type.checksum(&self.mem[..SLOT_CHECKSUM_OFFSET]) == checksum
    }

    fn get_root_page(&self) -> Option<(PageNumber, Checksum)> {
        if self.mem[ROOT_NON_NULL_OFFSET] == 0 {
            None
        } else {
            let num = PageNumber::from_le_bytes(
                self.mem[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + PageNumber::serialized_size())]
                    .try_into()
                    .unwrap(),
            );
            let checksum = Checksum::from_le_bytes(
                self.mem[ROOT_CHECKSUM_OFFSET..(ROOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
                    .try_into()
                    .unwrap(),
            );
            Some((num, checksum))
        }
    }

    fn get_freed_root_page(&self) -> Option<(PageNumber, Checksum)> {
        if self.mem[FREED_ROOT_NON_NULL_OFFSET] == 0 {
            None
        } else {
            let num = PageNumber::from_le_bytes(
                self.mem[FREED_ROOT_OFFSET..(FREED_ROOT_OFFSET + PageNumber::serialized_size())]
                    .try_into()
                    .unwrap(),
            );
            let checksum = Checksum::from_le_bytes(
                self.mem[FREED_ROOT_CHECKSUM_OFFSET
                    ..(FREED_ROOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
                    .try_into()
                    .unwrap(),
            );
            Some((num, checksum))
        }
    }

    fn get_last_committed_transaction_id(&self) -> u64 {
        u64::from_le_bytes(
            self.mem[TRANSACTION_ID_OFFSET..(TRANSACTION_ID_OFFSET + size_of::<u64>())]
                .try_into()
                .unwrap(),
        )
    }

    fn get_full_regions(&self) -> usize {
        u32::from_le_bytes(
            self.mem[NUM_FULL_REGIONS_OFFSET..(NUM_FULL_REGIONS_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn get_trailing_region_data_pages(&self) -> Option<usize> {
        let value = u32::from_le_bytes(
            self.mem[TRAILING_REGION_DATA_PAGES_OFFSET
                ..(TRAILING_REGION_DATA_PAGES_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        );
        if value == 0 {
            None
        } else {
            Some(value as usize)
        }
    }

    fn get_version(&self) -> u8 {
        self.mem[VERSION_OFFSET]
    }
}

struct TransactionMutator<'a> {
    mem: &'a mut [u8],
}

impl<'a> TransactionMutator<'a> {
    fn new(mem: &'a mut [u8]) -> Self {
        TransactionMutator { mem }
    }

    fn set_root_page(&mut self, page_number: Option<(PageNumber, Checksum)>) {
        if let Some((num, checksum)) = page_number {
            self.mem[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + PageNumber::serialized_size())]
                .copy_from_slice(&num.to_le_bytes());
            self.mem[ROOT_CHECKSUM_OFFSET..(ROOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
                .copy_from_slice(&checksum.to_le_bytes());
            self.mem[ROOT_NON_NULL_OFFSET] = 1;
        } else {
            self.mem[ROOT_NON_NULL_OFFSET] = 0;
        }
    }

    fn set_freed_root(&mut self, page_number: Option<(PageNumber, Checksum)>) {
        if let Some((num, checksum)) = page_number {
            self.mem[FREED_ROOT_OFFSET..(FREED_ROOT_OFFSET + PageNumber::serialized_size())]
                .copy_from_slice(&num.to_le_bytes());
            self.mem
                [FREED_ROOT_CHECKSUM_OFFSET..(FREED_ROOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
                .copy_from_slice(&checksum.to_le_bytes());
            self.mem[FREED_ROOT_NON_NULL_OFFSET] = 1;
        } else {
            self.mem[FREED_ROOT_NON_NULL_OFFSET] = 0;
        }
    }

    fn set_last_committed_transaction_id(&mut self, transaction_id: u64) {
        self.mem[TRANSACTION_ID_OFFSET..(TRANSACTION_ID_OFFSET + size_of::<u64>())]
            .copy_from_slice(&transaction_id.to_le_bytes());
    }

    fn set_data_section_layout(
        &mut self,
        full_regions: usize,
        trailing_region_data_pages: Option<usize>,
    ) {
        self.mem[NUM_FULL_REGIONS_OFFSET..(NUM_FULL_REGIONS_OFFSET + size_of::<u32>())]
            .copy_from_slice(&(full_regions as u32).to_le_bytes());
        self.mem[TRAILING_REGION_DATA_PAGES_OFFSET
            ..(TRAILING_REGION_DATA_PAGES_OFFSET + size_of::<u32>())]
            .copy_from_slice(&(trailing_region_data_pages.unwrap_or(0) as u32).to_le_bytes());
    }

    fn update_checksum(&mut self, checksum_type: ChecksumType) {
        let checksum = checksum_type.checksum(&self.mem[..SLOT_CHECKSUM_OFFSET]);
        self.mem[SLOT_CHECKSUM_OFFSET..(SLOT_CHECKSUM_OFFSET + size_of::<Checksum>())]
            .copy_from_slice(&checksum.to_le_bytes());
    }

    fn set_version(&mut self, version: u8) {
        self.mem[VERSION_OFFSET] = version;
    }
}

enum AllocationOp {
    Allocate(PageNumber),
    Free(PageNumber),
    FreeUncommitted(PageNumber),
}

pub(crate) struct TransactionalMemory {
    // Pages allocated since the last commit
    allocated_since_commit: Mutex<HashSet<PageNumber>>,
    log_since_commit: Mutex<Vec<AllocationOp>>,
    // True if the allocator state was corrupted when the file was opened
    needs_recovery: bool,
    mmap: Mmap,
    // We use unsafe to access the metadata, and so guard it with this mutex
    // It would be nice if this was a RefCell<&[u8]> on the metadata. However, that would be
    // self-referential, since we also hold the mmap object
    metadata_guard: Mutex<MetadataGuard>,
    layout: Mutex<DatabaseLayout>,
    // The number of PageMut which are outstanding
    #[cfg(debug_assertions)]
    open_dirty_pages: Mutex<HashSet<PageNumber>>,
    // Reference counts of PageImpls that are outstanding
    #[cfg(debug_assertions)]
    read_page_ref_counts: Mutex<HashMap<PageNumber, u64>>,
    // Indicates that a non-durable commit has been made, so reads should be served from the secondary meta page
    read_from_secondary: AtomicBool,
    page_size: usize,
    // We store these separately from the layout because they're static, and accessed on the get_page()
    // code path where there is no locking
    region_size: u64,
    region_header_with_padding_size: usize,
    db_header_size: usize,
    dynamic_growth: bool,
    checksum_type: ChecksumType,
}

impl TransactionalMemory {
    pub(crate) fn new(
        file: File,
        max_capacity: u64,
        requested_page_size: Option<usize>,
        requested_region_size: Option<usize>,
        dynamic_growth: bool,
        write_strategy: Option<WriteStrategy>,
    ) -> Result<Self> {
        #[allow(clippy::assertions_on_constants)]
        {
            assert!(TRANSACTION_LAST_FIELD <= TRANSACTION_SIZE);
        }

        let page_size = requested_page_size.unwrap_or_else(get_page_size);
        assert!(page_size.is_power_of_two());
        if max_capacity < (DB_HEADER_SIZE + page_size * MIN_USABLE_PAGES) as u64 {
            return Err(Error::OutOfSpace);
        }

        let mmap = Mmap::new(file, max_capacity.try_into().unwrap())?;
        if mmap.len() < DB_HEADER_SIZE {
            // Safety: We're growing the mmap
            unsafe {
                mmap.resize(DB_HEADER_SIZE)?;
            }
        }

        let mutex = Mutex::new(MetadataGuard {});
        let mut metadata = unsafe { MetadataAccessor::new(&mmap, mutex.lock().unwrap()) };

        let region_size = requested_region_size
            .map(|x| x as u64)
            .unwrap_or(MAX_USABLE_REGION_SPACE);
        assert!(region_size.is_power_of_two());
        let max_usable_region_bytes = min(region_size, max_capacity.next_power_of_two() as u64);

        if metadata.get_magic_number() != MAGICNUMBER {
            let starting_size = if dynamic_growth {
                MIN_DESIRED_USABLE_BYTES as u64
            } else {
                max_capacity
            };
            let layout = DatabaseLayout::calculate(
                max_capacity,
                starting_size,
                max_usable_region_bytes,
                page_size,
            )?;

            if (mmap.len() as u64) < layout.len() {
                // Safety: We're growing the mmap
                unsafe {
                    mmap.resize(layout.len().try_into().unwrap())?;
                }
            }

            // Explicitly zero the header
            metadata.header.fill(0);

            // Store the page & db size. These are immutable
            metadata.set_page_size(page_size);
            metadata.set_superheader_length(layout.header_bytes());
            metadata.set_region_tracker_state_length(layout.region_tracker_address_range().len());
            metadata.set_region_header_length(layout.full_region_layout().data_section().start);
            metadata.set_region_max_data_pages(layout.full_region_layout().num_pages());
            metadata.set_max_capacity(max_capacity);
            let checksum_type = match write_strategy.unwrap_or_default() {
                WriteStrategy::Checksum => ChecksumType::XXH3_128,
                WriteStrategy::TwoPhase => ChecksumType::Zero,
            };
            metadata.set_checksum_type(checksum_type);

            // Initialize the region tracker. Must be done after writing the page_size and other
            // immutable fields, since it relies on those fields.
            metadata.initialize_region_tracker(&layout);
            let (mut region_tracker, mut regions) = metadata.allocators_mut(&layout)?;

            let num_regions = layout.num_regions();
            // Initialize all the regional allocators
            for i in 0..num_regions {
                let mut region = regions.get_region_mut(i);
                let region_layout = layout.region_layout(i);
                region.initialize(
                    region_layout.num_pages(),
                    layout.full_region_layout().num_pages(),
                );
                let max_order = region.allocator_mut().get_max_order();
                region_tracker.mark_free(max_order, i as u64);
            }
            // Set the allocator to not dirty, because the allocator initialization above will have
            // dirtied it
            metadata.set_recovery(false);

            let mut mutator = metadata.secondary_slot_mut();
            mutator.set_root_page(None);
            mutator.set_freed_root(None);
            mutator.set_last_committed_transaction_id(0);
            mutator.set_data_section_layout(
                layout.num_full_regions(),
                layout.trailing_region_layout().map(|x| x.num_pages()),
            );
            mutator.set_version(FILE_FORMAT_VERSION);
            drop(mutator);
            // Make the state we just wrote the primary
            metadata.swap_primary();

            // Initialize the secondary allocator state
            let mut mutator = metadata.secondary_slot_mut();
            mutator.set_data_section_layout(
                layout.num_full_regions(),
                layout.trailing_region_layout().map(|x| x.num_pages()),
            );
            mutator.set_version(FILE_FORMAT_VERSION);
            drop(mutator);

            mmap.flush()?;
            // Write the magic number only after the data structure is initialized and written to disk
            // to ensure that it's crash safe
            metadata.set_magic_number();
            mmap.flush()?;
        }

        let page_size = metadata.get_page_size();
        if let Some(size) = requested_page_size {
            assert_eq!(page_size, size);
        }
        let version = metadata.primary_slot().get_version();
        if version != FILE_FORMAT_VERSION {
            return Err(Error::Corrupted(format!(
                "Expected file format version {}, found {}",
                FILE_FORMAT_VERSION, version
            )));
        }
        let version = metadata.secondary_slot().get_version();
        if version != FILE_FORMAT_VERSION {
            return Err(Error::Corrupted(format!(
                "Expected file format version {}, found {}",
                FILE_FORMAT_VERSION, version
            )));
        }
        let layout = metadata.get_primary_layout();
        let region_size = layout.full_region_layout().len();
        let region_header_size = layout.full_region_layout().data_section().start;
        let checksum_type = metadata.get_checksum_type();

        let needs_recovery = metadata.get_recovery_required();
        drop(metadata);

        Ok(TransactionalMemory {
            allocated_since_commit: Mutex::new(HashSet::new()),
            log_since_commit: Mutex::new(vec![]),
            needs_recovery,
            mmap,
            metadata_guard: mutex,
            layout: Mutex::new(layout.clone()),
            #[cfg(debug_assertions)]
            open_dirty_pages: Mutex::new(HashSet::new()),
            #[cfg(debug_assertions)]
            read_page_ref_counts: Mutex::new(HashMap::new()),
            read_from_secondary: AtomicBool::new(false),
            page_size,
            region_size,
            region_header_with_padding_size: region_header_size,
            db_header_size: layout.header_bytes(),
            dynamic_growth,
            checksum_type,
        })
    }

    pub(crate) fn needs_repair(&self) -> Result<bool> {
        Ok(self.lock_metadata().get_recovery_required())
    }

    pub(crate) fn needs_checksum_verification(&self) -> Result<bool> {
        Ok(self.lock_metadata().get_checksum_type() == ChecksumType::XXH3_128)
    }

    pub(crate) fn checksum_type(&self) -> ChecksumType {
        self.lock_metadata().get_checksum_type()
    }

    pub(crate) fn repair_primary_corrupted(&self) {
        let mut metadata = self.lock_metadata();
        metadata.swap_primary();
        *self.layout.lock().unwrap() = metadata.get_primary_layout();
    }

    pub(crate) fn begin_repair(&self) -> Result<()> {
        let mut metadata = self.lock_metadata();

        if !metadata
            .primary_slot()
            .verify_checksum(metadata.get_checksum_type())
        {
            metadata.swap_primary();
            *self.layout.lock().unwrap() = metadata.get_primary_layout();
            assert!(metadata
                .primary_slot()
                .verify_checksum(metadata.get_checksum_type()));
        } else {
            // If the secondary is a valid commit, verify that the primary is newer. This handles an edge case where:
            // * the primary bit is flipped to the secondary
            // * a crash occurs during fsync, such that no other data is written out to the secondary. meaning that it contains a valid, but out of date transaction
            let secondary_newer = metadata
                .secondary_slot()
                .get_last_committed_transaction_id()
                > metadata.primary_slot().get_last_committed_transaction_id();
            if secondary_newer
                && metadata
                    .secondary_slot()
                    .verify_checksum(metadata.get_checksum_type())
            {
                metadata.swap_primary();
                *self.layout.lock().unwrap() = metadata.get_primary_layout();
            }
        }

        let layout = self.layout.lock().unwrap();
        metadata.initialize_region_tracker(&layout);
        let (mut region_tracker, mut regions) = metadata.allocators_mut(&layout)?;

        let num_regions = layout.num_regions();
        // Initialize all the regional allocators
        for i in 0..num_regions {
            let mut region = regions.get_region_mut(i);
            let region_layout = layout.region_layout(i);
            region.initialize(
                region_layout.num_pages(),
                layout.full_region_layout().num_pages(),
            );
            let highest_free = region.allocator_mut().highest_free_order().unwrap();
            // Initialize the region tracker
            region_tracker.mark_free(highest_free, i as u64);
        }

        Ok(())
    }

    pub(crate) fn mark_pages_allocated(
        &self,
        allocated_pages: impl Iterator<Item = PageNumber>,
    ) -> Result<()> {
        let mut metadata = self.lock_metadata();
        let layout = self.layout.lock().unwrap();
        let (_, mut regions) = metadata.allocators_mut(&layout)?;

        for page_number in allocated_pages {
            let region_index = page_number.region as usize;
            let mut region = regions.get_region_mut(region_index);
            region.allocator_mut().record_alloc(
                page_number.page_index as u64,
                page_number.page_order as usize,
            );
        }

        Ok(())
    }

    pub(crate) fn end_repair(&mut self) -> Result<()> {
        let mut metadata = self.lock_metadata();
        self.mmap.flush()?;

        metadata.set_recovery(false);
        let result = self.mmap.flush();
        drop(metadata);
        self.needs_recovery = false;

        result
    }

    fn lock_metadata(&self) -> MetadataAccessor {
        // Safety: Access to metadata is only allowed by the owner of the metadata_guard lock
        unsafe { MetadataAccessor::new(&self.mmap, self.metadata_guard.lock().unwrap()) }
    }

    // Commit all outstanding changes and make them visible as the primary
    pub(crate) fn commit(
        &self,
        data_root: Option<(PageNumber, Checksum)>,
        freed_root: Option<(PageNumber, Checksum)>,
        transaction_id: u64,
        eventual: bool,
    ) -> Result {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        #[cfg(debug_assertions)]
        debug_assert!(self.open_dirty_pages.lock().unwrap().is_empty());
        assert!(!self.needs_recovery);

        let mut metadata = self.lock_metadata();
        let checksum_type = metadata.get_checksum_type();
        let mut layout = self.layout.lock().unwrap();

        // Trim surplus file space, before finalizing the commit
        let mut shrunk = false;
        if self.dynamic_growth {
            shrunk = self.try_shrink(&mut metadata, &mut layout)?;
        };

        let mut secondary = metadata.secondary_slot_mut();
        secondary.set_last_committed_transaction_id(transaction_id);
        secondary.set_root_page(data_root);
        secondary.set_freed_root(freed_root);
        secondary.set_data_section_layout(
            layout.num_full_regions(),
            layout.trailing_region_layout().map(|x| x.num_pages()),
        );
        secondary.update_checksum(checksum_type);

        // Use 2-phase commit, if checksums are disabled
        if matches!(self.checksum_type, ChecksumType::Zero) {
            if eventual {
                self.mmap.eventual_flush()?;
            } else {
                self.mmap.flush()?;
            }
        }

        metadata.swap_primary();
        if eventual {
            self.mmap.eventual_flush()?;
        } else {
            self.mmap.flush()?;
        }
        drop(metadata);

        // Safety: try_shrink() only removes unallocated free pages at the end of the database file
        // references to unallocated pages are not allowed to exist, and we've now promoted the
        // shrunked layout to the primary
        if shrunk {
            unsafe {
                self.mmap.resize(layout.len().try_into().unwrap())?;
            }
        }

        self.log_since_commit.lock().unwrap().clear();
        self.allocated_since_commit.lock().unwrap().clear();
        self.read_from_secondary.store(false, Ordering::Release);

        Ok(())
    }

    // Make changes visible, without a durability guarantee
    pub(crate) fn non_durable_commit(
        &self,
        data_root: Option<(PageNumber, Checksum)>,
        freed_root: Option<(PageNumber, Checksum)>,
        transaction_id: u64,
    ) -> Result {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        #[cfg(debug_assertions)]
        debug_assert!(self.open_dirty_pages.lock().unwrap().is_empty());
        assert!(!self.needs_recovery);

        let mut metadata = self.lock_metadata();
        let checksum_type = metadata.get_checksum_type();
        let layout = self.layout.lock().unwrap();
        let mut secondary = metadata.secondary_slot_mut();
        secondary.set_last_committed_transaction_id(transaction_id);
        secondary.set_root_page(data_root);
        secondary.set_freed_root(freed_root);
        secondary.set_data_section_layout(
            layout.num_full_regions(),
            layout.trailing_region_layout().map(|x| x.num_pages()),
        );
        secondary.update_checksum(checksum_type);

        self.log_since_commit.lock().unwrap().clear();
        self.allocated_since_commit.lock().unwrap().clear();
        self.read_from_secondary.store(true, Ordering::Release);

        Ok(())
    }

    pub(crate) fn rollback_uncommitted_writes(&self) -> Result {
        #[cfg(debug_assertions)]
        debug_assert!(self.open_dirty_pages.lock().unwrap().is_empty());
        let mut metadata = self.lock_metadata();
        // The layout to restore
        let restore = if self.read_from_secondary.load(Ordering::Acquire) {
            metadata.get_secondary_layout()
        } else {
            metadata.get_primary_layout()
        };

        let mut layout = self.layout.lock().unwrap();
        let (mut region_tracker, mut regions) = metadata.allocators_mut(&layout)?;
        for op in self.log_since_commit.lock().unwrap().drain(..).rev() {
            match op {
                AllocationOp::Allocate(page_number) => {
                    let region_index = page_number.region as usize;
                    region_tracker.mark_free(page_number.page_order as usize, region_index as u64);
                    let mut region = regions.get_region_mut(region_index);
                    region.allocator_mut().free(
                        page_number.page_index as u64,
                        page_number.page_order as usize,
                    );
                }
                AllocationOp::Free(page_number) | AllocationOp::FreeUncommitted(page_number) => {
                    let region_index = page_number.region as usize;
                    let mut region = regions.get_region_mut(region_index);
                    region.allocator_mut().record_alloc(
                        page_number.page_index as u64,
                        page_number.page_order as usize,
                    );
                }
            }
        }
        self.allocated_since_commit.lock().unwrap().clear();

        // Shrinking only happens during commit
        assert!(restore.len() <= layout.len());
        // Reset the layout, in case it changed during the writes
        if restore.len() < layout.len() {
            // Restore the size of the last region's allocator
            let last_region_index = restore.num_regions() - 1;
            let last_region = restore.region_layout(last_region_index);
            let mut region = regions.get_region_mut(last_region_index);
            region.allocator_mut().resize(last_region.num_pages());

            *layout = restore;
            // Safety: we've rollbacked the transaction, so any data in that was written into
            // space that was grown during this transaction no longer exists
            unsafe {
                self.mmap.resize(layout.len().try_into().unwrap())?;
            }
        }

        Ok(())
    }

    pub(crate) fn get_page(&self, page_number: PageNumber) -> PageImpl {
        // We must not retrieve an immutable reference to a page which already has a mutable ref to it
        #[cfg(debug_assertions)]
        {
            debug_assert!(
                !self.open_dirty_pages.lock().unwrap().contains(&page_number),
                "{:?}",
                page_number
            );
            *(self
                .read_page_ref_counts
                .lock()
                .unwrap()
                .entry(page_number)
                .or_default()) += 1;
        }

        // Safety: we asserted that no mutable references are open
        let mem = unsafe {
            self.mmap.get_memory(page_number.address_range(
                self.db_header_size,
                self.region_size,
                self.region_header_with_padding_size,
                self.page_size,
            ))
        };

        PageImpl {
            mem,
            page_number,
            #[cfg(debug_assertions)]
            open_pages: &self.read_page_ref_counts,
        }
    }

    // Safety: the caller must ensure that no references to the memory in `page` exist
    pub(crate) unsafe fn get_page_mut(&self, page_number: PageNumber) -> PageMut {
        #[cfg(debug_assertions)]
        {
            assert!(!self
                .read_page_ref_counts
                .lock()
                .unwrap()
                .contains_key(&page_number));
            assert!(self.open_dirty_pages.lock().unwrap().insert(page_number));
        }

        let address_range = page_number.address_range(
            self.db_header_size,
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let mem = self.mmap.get_memory_mut(address_range);

        PageMut {
            mem,
            page_number,
            #[cfg(debug_assertions)]
            open_pages: &self.open_dirty_pages,
        }
    }

    pub(crate) fn get_data_root(&self) -> Option<(PageNumber, Checksum)> {
        let metadata = self.lock_metadata();
        if self.read_from_secondary.load(Ordering::Acquire) {
            metadata.secondary_slot().get_root_page()
        } else {
            metadata.primary_slot().get_root_page()
        }
    }

    pub(crate) fn get_freed_root(&self) -> Option<(PageNumber, Checksum)> {
        let metadata = self.lock_metadata();
        if self.read_from_secondary.load(Ordering::Acquire) {
            metadata.secondary_slot().get_freed_root_page()
        } else {
            metadata.primary_slot().get_freed_root_page()
        }
    }

    pub(crate) fn get_last_committed_transaction_id(&self) -> Result<u64> {
        let metadata = self.lock_metadata();
        if self.read_from_secondary.load(Ordering::Acquire) {
            Ok(metadata
                .secondary_slot()
                .get_last_committed_transaction_id())
        } else {
            Ok(metadata.primary_slot().get_last_committed_transaction_id())
        }
    }

    // Safety: the caller must ensure that no references to the memory in `page` exist
    // TODO: add debug_assertion to check for double-free
    pub(crate) unsafe fn free(&self, page: PageNumber) -> Result {
        // Zero fill the page to ensure that deleted data is not stored in the file
        let mut mut_page = self.get_page_mut(page);
        mut_page.memory_mut().fill(0);

        let mut metadata = self.lock_metadata();
        let layout = self.layout.lock().unwrap();
        let (mut region_tracker, mut regions) = metadata.allocators_mut(&layout)?;
        let region_index = page.region as usize;
        // Free in the regional allocator
        let mut region = regions.get_region_mut(region_index);
        region
            .allocator_mut()
            .free(page.page_index as u64, page.page_order as usize);
        // Ensure that the region is marked as having free space
        region_tracker.mark_free(page.page_order as usize, region_index as u64);
        self.log_since_commit
            .lock()
            .unwrap()
            .push(AllocationOp::Free(page));

        Ok(())
    }

    // Frees the page if it was allocated since the last commit. Returns true, if the page was freed
    // Safety: the caller must ensure that no references to the memory in `page` exist
    // TODO: add debug_assertion to check for double-free
    pub(crate) unsafe fn free_if_uncommitted(&self, page: PageNumber) -> Result<bool> {
        if self.allocated_since_commit.lock().unwrap().remove(&page) {
            // Zero fill the page to ensure that deleted data is not stored in the file
            let mut mut_page = self.get_page_mut(page);
            mut_page.memory_mut().fill(0);
            let mut metadata = self.lock_metadata();
            let layout = self.layout.lock().unwrap();
            let (mut region_tracker, mut regions) = metadata.allocators_mut(&layout)?;
            // Free in the regional allocator
            let mut region = regions.get_region_mut(page.region as usize);
            region
                .allocator_mut()
                .free(page.page_index as u64, page.page_order as usize);
            // Ensure that the region is marked as having free space
            region_tracker.mark_free(page.page_order as usize, page.region as u64);

            self.log_since_commit
                .lock()
                .unwrap()
                .push(AllocationOp::FreeUncommitted(page));

            Ok(true)
        } else {
            Ok(false)
        }
    }

    // Page has not been committed
    pub(crate) fn uncommitted(&self, page: PageNumber) -> bool {
        self.allocated_since_commit.lock().unwrap().contains(&page)
    }

    fn allocate_helper(
        &self,
        metadata: &mut MetadataAccessor,
        layout: &DatabaseLayout,
        required_order: usize,
    ) -> Result<PageNumber> {
        let (mut region_tracker, mut regions) = metadata.allocators_mut(layout)?;
        loop {
            let candidate_region = region_tracker.find_free(required_order)? as usize;
            let mut region = regions.get_region_mut(candidate_region);
            match region.allocator_mut().alloc(required_order) {
                Ok(page) => {
                    return Ok(PageNumber::new(
                        candidate_region as u32,
                        page as u32,
                        required_order as u8,
                    ));
                }
                Err(err) => {
                    if matches!(err, Error::OutOfSpace) {
                        // Mark the region, if it's full
                        region_tracker.mark_full(required_order, candidate_region as u64);
                    } else {
                        return Err(err);
                    }
                }
            }
        }
    }

    // Safety: caller must guarantee that no references to free pages at the end of the last region exist
    fn try_shrink(
        &self,
        metadata: &mut MetadataAccessor,
        layout: &mut DatabaseLayout,
    ) -> Result<bool> {
        let last_region_index = layout.num_regions() - 1;
        let last_region = layout.region_layout(last_region_index);
        let region = metadata.get_region(last_region_index, layout);
        let last_allocator = region.allocator();
        let trailing_free = last_allocator.trailing_free_pages();
        let last_allocator_len = last_allocator.len();
        drop(last_allocator);
        // TODO: is this the right shrinkage heuristic?
        if trailing_free < last_allocator_len / 2 {
            return Ok(false);
        }
        let reduce_to_pages = if layout.num_regions() > 1 && trailing_free == last_allocator_len {
            0
        } else {
            max(MIN_USABLE_PAGES, last_allocator_len - trailing_free)
        };

        let (mut region_tracker, mut regions) = metadata.allocators_mut(layout)?;
        let new_usable_bytes = if reduce_to_pages == 0 {
            region_tracker.mark_full(0, last_region_index as u64);
            // drop the whole region
            layout.usable_bytes() - last_region.usable_bytes()
        } else {
            let mut region = regions.get_region_mut(last_region_index);
            region.allocator_mut().resize(reduce_to_pages);
            layout.usable_bytes()
                - ((last_allocator_len - reduce_to_pages) as u64)
                    * (metadata.get_page_size() as u64)
        };

        let new_layout = DatabaseLayout::calculate(
            metadata.get_max_capacity(),
            new_usable_bytes,
            metadata.get_region_max_usable_bytes(),
            self.page_size,
        )?;
        assert!(new_layout.len() <= layout.len());
        assert_eq!(new_layout.header_bytes(), layout.header_bytes());
        assert_eq!(new_layout.header_bytes(), self.db_header_size);

        *layout = new_layout;

        Ok(true)
    }

    fn grow(
        &self,
        metadata: &mut MetadataAccessor,
        layout: &mut DatabaseLayout,
        required_order_allocation: usize,
    ) -> Result<()> {
        let required_growth =
            2u64.pow(required_order_allocation as u32) * metadata.get_page_size() as u64;
        let max_region_size = metadata.get_region_max_usable_bytes();
        let next_desired_size = if layout.num_full_regions() > 0 {
            if let Some(trailing) = layout.trailing_region_layout() {
                if 2 * required_growth < max_region_size - trailing.usable_bytes() {
                    // Fill out the trailing region
                    layout.usable_bytes() + (max_region_size - trailing.usable_bytes())
                } else {
                    // Grow by 1 region
                    layout.usable_bytes() + max_region_size
                }
            } else {
                // Grow by 1 region
                layout.usable_bytes() + max_region_size
            }
        } else {
            // TODO: maybe we should grow by less than 2x each time, or make it configurable?
            max(
                layout.usable_bytes() * 2,
                layout.usable_bytes() + required_growth * 2,
            )
        };
        let new_layout = DatabaseLayout::calculate(
            metadata.get_max_capacity(),
            next_desired_size,
            metadata.get_region_max_usable_bytes(),
            self.page_size,
        )?;
        assert!(new_layout.len() >= layout.len());
        assert_eq!(new_layout.header_bytes(), layout.header_bytes());
        assert_eq!(new_layout.header_bytes(), self.db_header_size);
        if new_layout.len() == layout.len() {
            // Can't grow
            return Err(Error::OutOfSpace);
        }

        // Safety: We're growing the mmap
        unsafe {
            self.mmap.resize(new_layout.len().try_into().unwrap())?;
        }
        for i in 0..new_layout.num_regions() {
            let new_region_base = new_layout.region_base_address(i);
            let new_region = new_layout.region_layout(i);
            if i < layout.num_regions() {
                let old_region_base = layout.region_base_address(i);
                let old_region = layout.region_layout(i);
                assert_eq!(old_region_base, new_region_base);
                if new_region.len() != old_region.len() {
                    let (mut region_tracker, mut regions) = metadata.allocators_mut(&new_layout)?;
                    let mut region = regions.get_region_mut(i);
                    let mut allocator = region.allocator_mut();
                    allocator.resize(new_region.num_pages());
                    let highest_free = allocator.highest_free_order().unwrap();
                    region_tracker.mark_free(highest_free, i as u64);
                }
            } else {
                // brand new region
                let (mut region_tracker, mut regions) = metadata.allocators_mut(&new_layout)?;
                let mut region = regions.get_region_mut(i);
                region.initialize(
                    new_region.num_pages(),
                    new_layout.full_region_layout().num_pages(),
                );
                let highest_free = region.allocator_mut().highest_free_order().unwrap();
                region_tracker.mark_free(highest_free, i as u64);
            }
        }
        *layout = new_layout;
        Ok(())
    }

    pub(crate) fn allocate(&self, allocation_size: usize) -> Result<PageMut> {
        let required_pages = (allocation_size + self.page_size - 1) / self.page_size;
        let required_order = ceil_log2(required_pages);

        let mut metadata = self.lock_metadata();
        let max_capacity = metadata.get_max_capacity();
        let mut layout = self.layout.lock().unwrap();

        let page_number = match self.allocate_helper(&mut metadata, &layout, required_order) {
            Ok(page_number) => page_number,
            Err(err) => {
                if matches!(err, Error::OutOfSpace) && (layout.len() as u64) < max_capacity {
                    self.grow(&mut metadata, &mut layout, required_order)?;
                    self.allocate_helper(&mut metadata, &layout, required_order)?
                } else {
                    return Err(err);
                }
            }
        };

        self.allocated_since_commit
            .lock()
            .unwrap()
            .insert(page_number);
        self.log_since_commit
            .lock()
            .unwrap()
            .push(AllocationOp::Allocate(page_number));
        #[cfg(debug_assertions)]
        {
            assert!(!self
                .read_page_ref_counts
                .lock()
                .unwrap()
                .contains_key(&page_number));
            assert!(self.open_dirty_pages.lock().unwrap().insert(page_number));
        }

        let address_range = page_number.address_range(
            self.db_header_size,
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        // Safety:
        // The address range we're returning was just allocated, so no other references exist
        let mem = unsafe { self.mmap.get_memory_mut(address_range) };
        debug_assert!(mem.len() >= allocation_size);

        Ok(PageMut {
            mem,
            page_number,
            #[cfg(debug_assertions)]
            open_pages: &self.open_dirty_pages,
        })
    }

    pub(crate) fn count_free_pages(&self) -> Result<usize> {
        let mut metadata = self.lock_metadata();
        let layout = self.layout.lock().unwrap();
        let mut count = 0;
        for i in 0..layout.num_regions() {
            let region = metadata.get_region(i, &layout);
            count += region.allocator().count_free_pages();
        }

        // Calculate the number of pages worth of expansion space left, if database grows to max size
        let max_layout = DatabaseLayout::calculate(
            metadata.get_max_capacity(),
            metadata.get_max_capacity(),
            metadata.get_region_max_usable_bytes(),
            self.page_size,
        )
        .unwrap();
        let potential_growth_pages: usize = ((max_layout.usable_bytes() - layout.usable_bytes())
            / (self.page_size as u64))
            .try_into()
            .unwrap();

        Ok(count + potential_growth_pages)
    }

    pub(crate) fn get_page_size(&self) -> usize {
        self.page_size
    }
}

impl Drop for TransactionalMemory {
    fn drop(&mut self) {
        // Commit any non-durable transactions that are outstanding
        if self.read_from_secondary.load(Ordering::Acquire) {
            if let Ok(non_durable_transaction_id) = self.get_last_committed_transaction_id() {
                let root = self.get_data_root();
                let freed_root = self.get_freed_root();
                if self
                    .commit(root, freed_root, non_durable_transaction_id, false)
                    .is_err()
                {
                    eprintln!(
                        "Failure while finalizing non-durable commit. Database may have rolled back"
                    );
                }
            } else {
                eprintln!(
                    "Failure while finalizing non-durable commit. Database may have rolled back"
                );
            }
        }
        if self.mmap.flush().is_ok() && !self.needs_recovery {
            self.lock_metadata().set_recovery(false);
            let _ = self.mmap.flush();
        }
    }
}

#[cfg(test)]
mod test {
    use crate::db::TableDefinition;
    use crate::tree_store::page_store::page_manager::{
        DB_HEADER_SIZE, GOD_BYTE_OFFSET, MAGICNUMBER, MIN_USABLE_PAGES, PRIMARY_BIT,
        RECOVERY_REQUIRED, ROOT_CHECKSUM_OFFSET, TRANSACTION_0_OFFSET, TRANSACTION_1_OFFSET,
    };
    use crate::tree_store::page_store::utils::get_page_size;
    use crate::tree_store::page_store::TransactionalMemory;
    use crate::{Database, Error, ReadableTable, WriteStrategy};
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::mem::size_of;
    use tempfile::NamedTempFile;

    const X: TableDefinition<[u8], [u8]> = TableDefinition::new("x");

    #[test]
    fn repair_allocator_no_checksums() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let max_size = 1024 * 1024;
        let db = unsafe {
            Database::builder()
                .set_write_strategy(WriteStrategy::TwoPhase)
                .create(tmpfile.path(), max_size)
                .unwrap()
        };
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert(b"hello", b"world").unwrap();
        }
        write_txn.commit().unwrap();
        let write_txn = db.begin_write().unwrap();
        let free_pages = write_txn.stats().unwrap().free_pages();
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
            max_size as u64,
            None,
            None,
            true,
            Some(WriteStrategy::TwoPhase)
        )
        .unwrap()
        .needs_repair()
        .unwrap());

        let db2 = unsafe {
            Database::builder()
                .set_write_strategy(WriteStrategy::TwoPhase)
                .create(tmpfile.path(), max_size)
                .unwrap()
        };
        let write_txn = db2.begin_write().unwrap();
        assert_eq!(free_pages, write_txn.stats().unwrap().free_pages());
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert(b"hello2", b"world2").unwrap();
        }
        write_txn.commit().unwrap();
    }

    #[test]
    fn repair_allocator_checksums() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let max_size = 1024 * 1024;
        let db = unsafe {
            Database::builder()
                .set_write_strategy(WriteStrategy::Checksum)
                .create(tmpfile.path(), max_size)
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
            max_size as u64,
            None,
            None,
            true,
            Some(WriteStrategy::Checksum)
        )
        .unwrap()
        .needs_repair()
        .unwrap());

        let db2 = unsafe { Database::create(tmpfile.path(), max_size).unwrap() };
        let write_txn = db2.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            assert_eq!(table.get(b"hello").unwrap().unwrap(), b"world");
            table.insert(b"hello2", b"world2").unwrap();
        }
        write_txn.commit().unwrap();
    }

    #[test]
    fn repair_insert_reserve_regression() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let max_size = 1024 * 1024;
        let db = unsafe {
            Database::builder()
                .set_write_strategy(WriteStrategy::Checksum)
                .create(tmpfile.path(), max_size)
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
            max_size as u64,
            None,
            None,
            true,
            Some(WriteStrategy::Checksum)
        )
        .unwrap()
        .needs_repair()
        .unwrap());

        unsafe { Database::open(tmpfile.path()).unwrap() };
    }

    #[test]
    fn too_small_db() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let result = unsafe { Database::create(tmpfile.path(), 1) };
        assert!(matches!(result, Err(Error::OutOfSpace)));

        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let result = unsafe { Database::create(tmpfile.path(), 1024) };
        assert!(matches!(result, Err(Error::OutOfSpace)));

        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let result =
            unsafe { Database::create(tmpfile.path(), MIN_USABLE_PAGES * get_page_size() - 1) };
        assert!(matches!(result, Err(Error::OutOfSpace)));
    }

    #[test]
    fn smallest_db() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        unsafe {
            Database::create(
                tmpfile.path(),
                DB_HEADER_SIZE + (MIN_USABLE_PAGES + 2) * get_page_size(),
            )
            .unwrap();
        }
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
