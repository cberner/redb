use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::grouped_bitmap::U64GroupedBitMapMut;
use crate::tree_store::page_store::mmap::Mmap;
use crate::tree_store::page_store::utils::get_page_size;
use crate::Error;
use crate::Result;
use std::cmp::{max, min};
use std::collections::HashSet;
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::mem::size_of;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

// Database layout:
// Header (first 128 bytes):
// 9 bytes: magic number
// 1 byte: version number
// 1 byte: page size exponent
// 1 byte: god byte
// 4 bytes: padding to 64-bit aligned
// 8 bytes: database max size
// 8 bytes: upgrade log offset
// 8 bytes: upgrade log length
//
// Commit slot 0 (next 128 bytes):
// 8 bytes: root page
// 8 bytes: last committed transaction id
// (db layout size) bytes: active database layout
//
// Commit slot 1 (next 128 bytes):
// 8 bytes: root page
// 8 bytes: last committed transaction id
// (db layout size) bytes: active database layout

// Regions have a maximum size of 4GiB. A `4GiB - overhead` value is the largest that can be represented,
// because the leaf node format uses 32bit offsets
// TODO: make the region size configurable, for people who want a really small minimum db size
// and to make the tests faster
const MAX_USABLE_REGION_SPACE: usize = 4 * 1024 * 1024 * 1024;
const MAX_PAGE_ORDER: usize = 20;
const MIN_USABLE_PAGES: usize = 10;
const MIN_DESIRED_USABLE_BYTES: usize = 1024 * 1024;

// TODO: set to 1, when version 1.0 is released
const FILE_FORMAT_VERSION: u8 = 100;

// Inspired by PNG's magic number
const MAGICNUMBER: [u8; 9] = [b'r', b'e', b'd', b'b', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A];
const VERSION_OFFSET: usize = MAGICNUMBER.len();
const PAGE_SIZE_OFFSET: usize = VERSION_OFFSET + size_of::<u8>();
const GOD_BYTE_OFFSET: usize = PAGE_SIZE_OFFSET + size_of::<u8>();
const RESERVED: usize = 4;
const REGION_SIZE_OFFSET: usize = GOD_BYTE_OFFSET + size_of::<u8>() + RESERVED;
const DB_SIZE_OFFSET: usize = REGION_SIZE_OFFSET + size_of::<u64>();
const TRANSACTION_SIZE: usize = 128;
const TRANSACTION_0_OFFSET: usize = 128;
const TRANSACTION_1_OFFSET: usize = TRANSACTION_0_OFFSET + TRANSACTION_SIZE;
const DB_HEADER_SIZE: usize = TRANSACTION_1_OFFSET + TRANSACTION_SIZE;

// God byte flags
const PRIMARY_BIT: u8 = 1;
const ALLOCATOR_STATE_DIRTY: u8 = 2;

// Structure of each commit slot
const ROOT_PAGE_OFFSET: usize = 0;
const TRANSACTION_ID_OFFSET: usize = ROOT_PAGE_OFFSET + size_of::<u64>();
const DATA_LAYOUT_OFFSET: usize = TRANSACTION_ID_OFFSET + size_of::<u64>();

fn ceil_log2(x: usize) -> usize {
    if x.is_power_of_two() {
        x.trailing_zeros() as usize
    } else {
        x.next_power_of_two().trailing_zeros() as usize
    }
}

pub(crate) fn get_db_size(path: impl AsRef<Path>) -> Result<usize, io::Error> {
    let mut db_size = [0u8; size_of::<u64>()];
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(DB_SIZE_OFFSET as u64))?;
    file.read_exact(&mut db_size)?;
    Ok(u64::from_be_bytes(db_size) as usize)
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

    fn get_max_capacity(&self) -> usize {
        u64::from_be_bytes(
            self.header[DB_SIZE_OFFSET..DB_SIZE_OFFSET + size_of::<u64>()]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn set_max_capacity(&mut self, max_size: usize) {
        self.header[DB_SIZE_OFFSET..DB_SIZE_OFFSET + size_of::<u64>()]
            .copy_from_slice(&(max_size as u64).to_be_bytes());
    }

    fn get_magic_number(&self) -> [u8; MAGICNUMBER.len()] {
        self.header[..MAGICNUMBER.len()].try_into().unwrap()
    }

    fn set_magic_number(&mut self) {
        self.header[..MAGICNUMBER.len()].copy_from_slice(&MAGICNUMBER);
    }

    fn get_page_size(&self) -> usize {
        1usize << self.header[PAGE_SIZE_OFFSET]
    }

    fn set_page_size(&mut self, page_size: usize) {
        self.header[PAGE_SIZE_OFFSET] = page_size.trailing_zeros() as u8;
    }

    fn get_version(&self) -> u8 {
        self.header[VERSION_OFFSET]
    }

    fn set_version(&mut self, version: u8) {
        self.header[VERSION_OFFSET] = version;
    }

    fn get_allocator_dirty(&self) -> bool {
        self.header[GOD_BYTE_OFFSET] & ALLOCATOR_STATE_DIRTY != 0
    }

    fn set_allocator_dirty(&mut self, dirty: bool) {
        if dirty {
            self.header[GOD_BYTE_OFFSET] |= ALLOCATOR_STATE_DIRTY;
        } else {
            self.header[GOD_BYTE_OFFSET] &= !ALLOCATOR_STATE_DIRTY;
        }
    }

    fn get_regional_allocator(&mut self, region: usize, layout: &DatabaseLayout) -> &[u8] {
        let base = layout.region_base_address(region);
        let relative = layout.region_layout(region).allocator_state;
        let absolute = (base + relative.start)..(base + relative.end);

        // Safety: We own the metadata lock, so there can't be any other references
        // and this function takes &mut self, so the returned lifetime can't overlap with any other
        // calls into MetadataAccessor
        unsafe { self.mmap.get_memory(absolute) }
    }

    // Note: It's very important that the lifetime of the returned allocator accessors is the same
    // as self, since self holds the metadata lock
    fn allocators_mut(
        &mut self,
        layout: &DatabaseLayout,
    ) -> Result<(U64GroupedBitMapMut, RegionsAccessor)> {
        if !self.get_allocator_dirty() {
            self.set_allocator_dirty(true);
            self.mmap.flush()?
        }

        let range = layout.region_allocator_address_range();

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
        Ok((U64GroupedBitMapMut::new(mem), region_accessor))
    }
}

// Safety: RegionAccessor may only access regional metadata, and no other references to it may exist
struct RegionsAccessor<'a> {
    mmap: &'a Mmap,
    layout: DatabaseLayout,
}

impl<'a> RegionsAccessor<'a> {
    fn get_regional_allocator_mut(&mut self, region: usize) -> &mut [u8] {
        // Safety: We have exclusive access to regional metadata
        let base = self.layout.region_base_address(region);
        let relative = &self.layout.region_layout(region).allocator_state;
        let absolute = (base + relative.start)..(base + relative.end);

        assert!(absolute.start >= self.layout.db_header_bytes);
        unsafe { self.mmap.get_memory_mut(absolute) }
    }
}

// On-disk format is:
// lowest 20bits: page index within the region
// second 20bits: region number
// 19bits: reserved
// highest 5bits: page order exponent
//
// Assuming a reasonable page size, like 4kiB, this allows for 4kiB * 2^20 * 2^20 = 4PiB of usable space
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) struct PageNumber {
    region: u32,
    page_index: u32,
    page_order: u8,
}

impl PageNumber {
    // TODO: remove this
    pub(crate) fn null() -> Self {
        Self::new(u16::MAX as u32, 0, 0)
    }

    #[inline(always)]
    pub(crate) const fn serialized_size() -> usize {
        8
    }

    fn new(region: u32, page_index: u32, page_order: u8) -> Self {
        Self {
            region,
            page_index,
            page_order,
        }
    }

    pub(crate) fn to_be_bytes(self) -> [u8; 8] {
        let mut temp = (0x000F_FFFF & self.page_index) as u64;
        temp |= (0x000F_FFFF & self.region as u64) << 20;
        temp |= (0b0001_1111 & self.page_order as u64) << 59;
        temp.to_be_bytes()
    }

    pub(crate) fn from_be_bytes(bytes: [u8; 8]) -> Self {
        let temp = u64::from_be_bytes(bytes);
        let index = (temp & 0x000F_FFFF) as u32;
        let region = ((temp >> 20) & 0x000F_FFFF) as u32;
        let order = (temp >> 59) as u8;

        Self::new(region, index, order)
    }

    fn address_range(
        &self,
        data_section_offset: usize,
        region_size: usize,
        page_size: usize,
    ) -> Range<usize> {
        let regional_start = self.page_index as usize * self.page_size_bytes(page_size);
        debug_assert!(regional_start < region_size);
        let start = regional_start + self.region as usize * region_size;
        let end = start + self.page_size_bytes(page_size);
        (start + data_section_offset)..(end + data_section_offset)
    }

    fn page_size_bytes(&self, page_size: usize) -> usize {
        let pages = 1usize << self.page_order;
        pages * page_size
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

    fn get_root_page(&self) -> Option<PageNumber> {
        let num = PageNumber::from_be_bytes(
            self.mem[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + 8)]
                .try_into()
                .unwrap(),
        );
        if num == PageNumber::null() {
            None
        } else {
            Some(num)
        }
    }

    fn get_last_committed_transaction_id(&self) -> u64 {
        u64::from_be_bytes(
            self.mem[TRANSACTION_ID_OFFSET..(TRANSACTION_ID_OFFSET + size_of::<u64>())]
                .try_into()
                .unwrap(),
        )
    }

    fn get_data_section_layout(&self) -> DatabaseLayout {
        DatabaseLayout::from_be_bytes(
            self.mem[DATA_LAYOUT_OFFSET..(DATA_LAYOUT_OFFSET + DatabaseLayout::serialized_size())]
                .try_into()
                .unwrap(),
        )
    }
}

struct TransactionMutator<'a> {
    mem: &'a mut [u8],
}

impl<'a> TransactionMutator<'a> {
    fn new(mem: &'a mut [u8]) -> Self {
        TransactionMutator { mem }
    }

    fn set_root_page(&mut self, page_number: PageNumber) {
        self.mem[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + 8)]
            .copy_from_slice(&page_number.to_be_bytes());
    }

    fn set_last_committed_transaction_id(&mut self, transaction_id: u64) {
        self.mem[TRANSACTION_ID_OFFSET..(TRANSACTION_ID_OFFSET + size_of::<u64>())]
            .copy_from_slice(&transaction_id.to_be_bytes());
    }

    fn set_data_section_layout(&mut self, layout: &DatabaseLayout) {
        self.mem[DATA_LAYOUT_OFFSET..(DATA_LAYOUT_OFFSET + DatabaseLayout::serialized_size())]
            .copy_from_slice(&layout.to_be_bytes());
    }
}

pub(crate) trait Page {
    fn memory(&self) -> &[u8];

    fn get_page_number(&self) -> PageNumber;
}

pub struct PageImpl<'a> {
    mem: &'a [u8],
    page_number: PageNumber,
}

impl<'a> Debug for PageImpl<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("PageImpl: page_number={:?}", self.page_number))
    }
}

impl<'a> PageImpl<'a> {
    pub(crate) fn into_memory(self) -> &'a [u8] {
        self.mem
    }
}

impl<'a> Page for PageImpl<'a> {
    fn memory(&self) -> &[u8] {
        self.mem
    }

    fn get_page_number(&self) -> PageNumber {
        self.page_number
    }
}

pub(crate) struct PageMut<'a> {
    mem: &'a mut [u8],
    page_number: PageNumber,
    #[cfg(debug_assertions)]
    open_pages: &'a Mutex<HashSet<PageNumber>>,
}

impl<'a> PageMut<'a> {
    pub(crate) fn memory_mut(&mut self) -> &mut [u8] {
        self.mem
    }
}

impl<'a> Page for PageMut<'a> {
    fn memory(&self) -> &[u8] {
        self.mem
    }

    fn get_page_number(&self) -> PageNumber {
        self.page_number
    }
}

#[cfg(debug_assertions)]
impl<'a> Drop for PageMut<'a> {
    fn drop(&mut self) {
        self.open_pages.lock().unwrap().remove(&self.page_number);
    }
}

// Regions are laid out starting with the pages, and ending with the allocator state
#[derive(Clone)]
struct RegionLayout {
    num_pages: usize,
    // Range of the allocator state, relative to the start of the region
    allocator_state: Range<usize>,
}

impl RegionLayout {
    fn calculate_usable_order(space: usize, page_size: usize) -> Option<usize> {
        if space < page_size {
            return None;
        }
        let total_pages = space / page_size;
        let max_order = (64 - total_pages.leading_zeros() - 1) as usize;
        Some(min(MAX_PAGE_ORDER, max_order))
    }

    fn calculate_usable_pages(space: usize, page_size: usize, max_order: usize) -> usize {
        let required_page_capacity = MAX_USABLE_REGION_SPACE / page_size;
        let header_size = BuddyAllocator::required_space(required_page_capacity, max_order);
        assert!(header_size < space);
        (space - header_size) / page_size
    }

    fn calculate(
        available_space: usize,
        desired_usable_bytes: usize,
        page_size: usize,
    ) -> Option<RegionLayout> {
        let required_page_capacity = MAX_USABLE_REGION_SPACE / page_size;
        let max_order = Self::calculate_usable_order(MAX_USABLE_REGION_SPACE, page_size)?;
        let required_header_size =
            BuddyAllocator::required_space(required_page_capacity, max_order);
        if desired_usable_bytes / page_size < MIN_USABLE_PAGES {
            return None;
        }
        if available_space < required_header_size + MIN_USABLE_PAGES * page_size {
            return None;
        }
        let max_region_size = desired_usable_bytes + required_header_size;
        let used_space = min(max_region_size, available_space);

        let num_pages = Self::calculate_usable_pages(used_space, page_size, max_order);
        if num_pages < MIN_USABLE_PAGES {
            return None;
        }
        let allocator_start = num_pages * page_size;
        let allocator_end = allocator_start + required_header_size;

        Some(RegionLayout {
            num_pages,
            allocator_state: allocator_start..allocator_end,
        })
    }

    fn full_region_layout(page_size: usize) -> RegionLayout {
        let max_order = Self::calculate_usable_order(MAX_USABLE_REGION_SPACE, page_size).unwrap();
        let max_region_size = MAX_USABLE_REGION_SPACE
            + BuddyAllocator::required_space(MAX_USABLE_REGION_SPACE / page_size, max_order);

        Self::calculate(max_region_size, MAX_USABLE_REGION_SPACE, page_size).unwrap()
    }

    fn len(&self) -> usize {
        self.allocator_state.end
    }

    fn usable_bytes(&self) -> usize {
        self.allocator_state.start
    }

    fn max_order(&self) -> usize {
        Self::calculate_usable_order(
            MAX_USABLE_REGION_SPACE,
            self.allocator_state.start / self.num_pages,
        )
        .unwrap()
    }

    const fn serialized_size() -> usize {
        size_of::<u32>() + 2 * size_of::<u64>()
    }

    fn to_be_bytes(&self) -> [u8; Self::serialized_size()] {
        let mut result = [0; Self::serialized_size()];
        result[..size_of::<u32>()].copy_from_slice(&(self.num_pages as u32).to_be_bytes());
        result[size_of::<u32>()..size_of::<u32>() + size_of::<u64>()]
            .copy_from_slice(&(self.allocator_state.start as u64).to_be_bytes());
        result[size_of::<u32>() + size_of::<u64>()..]
            .copy_from_slice(&(self.allocator_state.end as u64).to_be_bytes());

        result
    }

    fn from_be_bytes(data: [u8; Self::serialized_size()]) -> Self {
        let num_pages = u32::from_be_bytes(data[..size_of::<u32>()].try_into().unwrap()) as usize;
        let allocator_start = u64::from_be_bytes(
            data[size_of::<u32>()..size_of::<u32>() + size_of::<u64>()]
                .try_into()
                .unwrap(),
        ) as usize;
        let allocator_end = u64::from_be_bytes(
            data[size_of::<u32>() + size_of::<u64>()..]
                .try_into()
                .unwrap(),
        ) as usize;

        Self {
            num_pages,
            allocator_state: allocator_start..allocator_end,
        }
    }
}

#[derive(Clone)]
struct DatabaseLayout {
    db_header_bytes: usize,
    full_region_layout: RegionLayout,
    num_full_regions: usize,
    trailing_partial_region: Option<RegionLayout>,
}

impl DatabaseLayout {
    fn calculate(
        db_capacity: usize,
        mut desired_usable_bytes: usize,
        page_size: usize,
    ) -> Result<Self> {
        desired_usable_bytes = min(desired_usable_bytes, db_capacity);
        let full_region_layout = RegionLayout::full_region_layout(page_size);
        let min_header_size = DB_HEADER_SIZE + U64GroupedBitMapMut::required_bytes(1);
        if db_capacity - min_header_size <= full_region_layout.len() {
            // Single region layout
            let region_layout = RegionLayout::calculate(
                db_capacity - min_header_size,
                desired_usable_bytes,
                page_size,
            )
            .ok_or(Error::OutOfSpace)?;
            Ok(DatabaseLayout {
                db_header_bytes: min_header_size,
                full_region_layout,
                num_full_regions: 0,
                trailing_partial_region: Some(region_layout),
            })
        } else {
            // Multi region layout
            let max_regions = (db_capacity - min_header_size + full_region_layout.len() - 1)
                / full_region_layout.len();
            let db_header_bytes = DB_HEADER_SIZE + U64GroupedBitMapMut::required_bytes(max_regions);
            let max_full_regions = (db_capacity - db_header_bytes) / full_region_layout.len();
            let desired_full_regions = desired_usable_bytes / MAX_USABLE_REGION_SPACE;
            let num_full_regions = min(max_full_regions, desired_full_regions);
            let remaining_space =
                db_capacity - db_header_bytes - num_full_regions * full_region_layout.len();
            let remaining_desired =
                desired_usable_bytes - num_full_regions * MAX_USABLE_REGION_SPACE;
            Ok(DatabaseLayout {
                db_header_bytes,
                full_region_layout,
                num_full_regions,
                trailing_partial_region: RegionLayout::calculate(
                    remaining_space,
                    remaining_desired,
                    page_size,
                ),
            })
        }
    }

    fn num_regions(&self) -> usize {
        if self.trailing_partial_region.is_some() {
            self.num_full_regions + 1
        } else {
            self.num_full_regions
        }
    }

    fn len(&self) -> usize {
        let last = self.num_regions() - 1;
        self.region_base_address(last) + self.region_layout(last).len()
    }

    fn usable_bytes(&self) -> usize {
        let trailing = self
            .trailing_partial_region
            .as_ref()
            .map(RegionLayout::usable_bytes)
            .unwrap_or_default();
        self.num_full_regions * self.full_region_layout.usable_bytes() + trailing
    }

    fn region_allocator_address_range(&self) -> Range<usize> {
        DB_HEADER_SIZE..self.db_header_bytes
    }

    fn region_base_address(&self, region: usize) -> usize {
        assert!(region < self.num_regions());

        self.db_header_bytes + region * self.full_region_layout.len()
    }

    fn region_layout(&self, region: usize) -> RegionLayout {
        assert!(region < self.num_regions());
        if region == self.num_full_regions {
            self.trailing_partial_region.as_ref().unwrap().clone()
        } else {
            self.full_region_layout.clone()
        }
    }

    const fn serialized_size() -> usize {
        2 * size_of::<u64>() + 2 * RegionLayout::serialized_size() + 1
    }

    // TODO: use le instead of be for everything? ARM is le by default, and x86 is always le
    fn to_be_bytes(&self) -> [u8; Self::serialized_size()] {
        let mut result = [0; Self::serialized_size()];
        let mut offset = 0;
        result[offset..offset + size_of::<u64>()]
            .copy_from_slice(&(self.db_header_bytes as u64).to_be_bytes());
        offset += size_of::<u64>();
        result[offset..offset + size_of::<u64>()]
            .copy_from_slice(&(self.num_full_regions as u64).to_be_bytes());
        offset += size_of::<u64>();
        result[offset..offset + RegionLayout::serialized_size()]
            .copy_from_slice(&self.full_region_layout.to_be_bytes());
        offset += RegionLayout::serialized_size();
        if let Some(trailing) = self.trailing_partial_region.as_ref() {
            result[offset..offset + RegionLayout::serialized_size()]
                .copy_from_slice(&trailing.to_be_bytes());
            offset += RegionLayout::serialized_size();
            result[offset] = 1;
        } else {
            result[offset..offset + RegionLayout::serialized_size()].fill(0);
            offset += RegionLayout::serialized_size();
            result[offset] = 0;
        }

        result
    }

    fn from_be_bytes(data: [u8; Self::serialized_size()]) -> Self {
        let mut offset = 0;
        let db_header_bytes =
            u64::from_be_bytes(data[offset..offset + size_of::<u64>()].try_into().unwrap())
                as usize;
        offset += size_of::<u64>();
        let num_full_regions =
            u64::from_be_bytes(data[offset..offset + size_of::<u64>()].try_into().unwrap())
                as usize;
        offset += size_of::<u64>();
        let full_region_layout = RegionLayout::from_be_bytes(
            data[offset..offset + RegionLayout::serialized_size()]
                .try_into()
                .unwrap(),
        );
        offset += RegionLayout::serialized_size();
        let trailing_partial_region = if data[Self::serialized_size() - 1] == 0 {
            None
        } else {
            Some(RegionLayout::from_be_bytes(
                data[offset..offset + RegionLayout::serialized_size()]
                    .try_into()
                    .unwrap(),
            ))
        };

        Self {
            db_header_bytes,
            full_region_layout,
            num_full_regions,
            trailing_partial_region,
        }
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
    // Metadata guard lock should be held when using this to modify the page allocator state
    // May be None, if the allocator state was corrupted when the file was opened
    regional_allocators: Mutex<Option<Vec<BuddyAllocator>>>,
    mmap: Mmap,
    // We use unsafe to access the metadata, and so guard it with this mutex
    // It would be nice if this was a RefCell<&[u8]> on the metadata. However, that would be
    // self-referential, since we also hold the mmap object
    metadata_guard: Mutex<MetadataGuard>,
    layout: Mutex<DatabaseLayout>,
    // The number of PageMut which are outstanding
    #[cfg(debug_assertions)]
    open_dirty_pages: Mutex<HashSet<PageNumber>>,
    // Indicates that a non-durable commit has been made, so reads should be served from the secondary meta page
    read_from_secondary: AtomicBool,
    page_size: usize,
    // We store these separately from the layout because they're static, and accessed on the get_page()
    // code path where there is no locking
    region_size: usize,
    header_size: usize,
}

impl TransactionalMemory {
    pub(crate) fn new(
        file: File,
        max_capacity: usize,
        requested_page_size: Option<usize>,
        dynamic_growth: bool,
    ) -> Result<Self> {
        let mmap = Mmap::new(file, max_capacity)?;
        if max_capacity < DB_HEADER_SIZE {
            return Err(Error::OutOfSpace);
        }

        if mmap.len() < DB_HEADER_SIZE {
            // Safety: We're growing the mmap
            unsafe {
                mmap.resize(DB_HEADER_SIZE)?;
            }
        }

        let mutex = Mutex::new(MetadataGuard {});
        let mut metadata = unsafe { MetadataAccessor::new(&mmap, mutex.lock().unwrap()) };

        if metadata.get_magic_number() != MAGICNUMBER {
            let page_size = requested_page_size.unwrap_or_else(get_page_size);
            assert!(page_size.is_power_of_two());

            let starting_size = if dynamic_growth {
                MIN_DESIRED_USABLE_BYTES
            } else {
                max_capacity
            };
            let layout = DatabaseLayout::calculate(max_capacity, starting_size, page_size)?;

            if mmap.len() < layout.len() {
                // Safety: We're growing the mmap
                unsafe {
                    mmap.resize(layout.len())?;
                }
            }

            // Explicitly zero the header
            metadata.header.fill(0);

            let (mut region_allocator, mut regions) = metadata.allocators_mut(&layout)?;

            // Initialize the region allocator
            let num_regions = layout.num_regions();
            for i in 0..num_regions {
                region_allocator.clear(i);
            }
            for i in num_regions..region_allocator.len() {
                region_allocator.set(i);
            }

            // Initialize all the regional allocators
            for i in 0..num_regions {
                let mem = regions.get_regional_allocator_mut(i);
                let region_layout = layout.region_layout(i);
                BuddyAllocator::init_new(
                    mem,
                    region_layout.num_pages,
                    layout.full_region_layout.num_pages,
                    region_layout.max_order(),
                );
            }
            // Set the allocator to not dirty, because the allocator initialization above will have
            // dirtied it
            metadata.set_allocator_dirty(false);

            // Store the page & db size. These are immutable
            metadata.set_page_size(page_size);
            metadata.set_max_capacity(max_capacity);
            metadata.set_version(FILE_FORMAT_VERSION);

            let mut mutator = metadata.secondary_slot_mut();
            mutator.set_root_page(PageNumber::null());
            mutator.set_last_committed_transaction_id(0);
            mutator.set_data_section_layout(&layout);
            drop(mutator);
            // Make the state we just wrote the primary
            metadata.swap_primary();

            // Initialize the secondary allocator state
            let mut mutator = metadata.secondary_slot_mut();
            mutator.set_data_section_layout(&layout);
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
        assert_eq!(metadata.get_version(), FILE_FORMAT_VERSION);
        let layout = metadata.primary_slot().get_data_section_layout();
        let region_size = layout.full_region_layout.len();

        let regional_allocators = if metadata.get_allocator_dirty() {
            None
        } else {
            let full_regional_allocator = BuddyAllocator::new(
                layout.full_region_layout.num_pages,
                layout.full_region_layout.num_pages,
                layout.full_region_layout.max_order(),
            );
            let mut allocators = vec![full_regional_allocator; layout.num_full_regions];
            if let Some(ref region_layout) = layout.trailing_partial_region {
                let trailing = BuddyAllocator::new(
                    region_layout.num_pages,
                    layout.full_region_layout.num_pages,
                    region_layout.max_order(),
                );
                allocators.push(trailing);
            }

            Some(allocators)
        };
        drop(metadata);

        Ok(TransactionalMemory {
            allocated_since_commit: Mutex::new(HashSet::new()),
            log_since_commit: Mutex::new(vec![]),
            regional_allocators: Mutex::new(regional_allocators),
            mmap,
            metadata_guard: mutex,
            layout: Mutex::new(layout.clone()),
            #[cfg(debug_assertions)]
            open_dirty_pages: Mutex::new(HashSet::new()),
            read_from_secondary: AtomicBool::new(false),
            page_size,
            region_size,
            header_size: layout.db_header_bytes,
        })
    }

    pub(crate) fn needs_repair(&self) -> Result<bool> {
        Ok(self.lock_metadata().get_allocator_dirty())
    }

    pub(crate) fn repair_allocator(
        &self,
        allocated_pages: impl Iterator<Item = PageNumber>,
    ) -> Result<()> {
        let mut metadata = self.lock_metadata();
        let layout = self.layout.lock().unwrap();
        let (mut region_allocator, mut regions) = metadata.allocators_mut(&layout)?;

        // Initialize the region allocator
        let num_regions = layout.num_regions();
        for i in num_regions..region_allocator.len() {
            region_allocator.set(i);
        }
        // Since the region allocator is lazily set, we can leave it all free, and it will be
        // populated lazily when a region is discovered to be full

        // Initialize all the regional allocators
        let mut regional_allocators = vec![];
        for i in 0..num_regions {
            let mem = regions.get_regional_allocator_mut(i);
            let region_layout = layout.region_layout(i);
            regional_allocators.push(BuddyAllocator::init_new(
                mem,
                region_layout.num_pages,
                layout.full_region_layout.num_pages,
                region_layout.max_order(),
            ));
        }

        for page_number in allocated_pages {
            let region = page_number.region as usize;
            let mem = regions.get_regional_allocator_mut(region);
            regional_allocators[region].record_alloc(
                mem,
                page_number.page_index as u64,
                page_number.page_order as usize,
            );
        }
        self.mmap.flush()?;

        metadata.set_allocator_dirty(false);
        self.mmap.flush()?;

        let full_regional_allocator = BuddyAllocator::new(
            layout.full_region_layout.num_pages,
            layout.full_region_layout.num_pages,
            layout.full_region_layout.max_order(),
        );
        let mut allocators = vec![full_regional_allocator; layout.num_full_regions];
        if let Some(ref region_layout) = layout.trailing_partial_region {
            let trailing = BuddyAllocator::new(
                region_layout.num_pages,
                layout.full_region_layout.num_pages,
                region_layout.max_order(),
            );
            allocators.push(trailing);
        }
        drop(metadata);

        let mut guard = self.regional_allocators.lock().unwrap();
        *guard = Some(allocators);

        Ok(())
    }

    fn lock_metadata(&self) -> MetadataAccessor {
        // Safety: Access to metadata is only allowed by the owner of the metadata_guard lock
        unsafe { MetadataAccessor::new(&self.mmap, self.metadata_guard.lock().unwrap()) }
    }

    // Commit all outstanding changes and make them visible as the primary
    pub(crate) fn commit(&self, transaction_id: u64, eventual: bool) -> Result {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        #[cfg(debug_assertions)]
        debug_assert!(self.open_dirty_pages.lock().unwrap().is_empty());
        assert!(self.regional_allocators.lock().unwrap().is_some());

        let mut metadata = self.lock_metadata();
        let layout = self.layout.lock().unwrap();
        let mut secondary = metadata.secondary_slot_mut();
        secondary.set_last_committed_transaction_id(transaction_id);
        secondary.set_data_section_layout(&layout);

        if eventual {
            self.mmap.eventual_flush()?;
        } else {
            self.mmap.flush()?;
        }

        metadata.swap_primary();
        if eventual {
            self.mmap.eventual_flush()?;
        } else {
            self.mmap.flush()?;
        }
        drop(metadata);

        self.log_since_commit.lock().unwrap().clear();
        self.allocated_since_commit.lock().unwrap().clear();
        self.read_from_secondary.store(false, Ordering::Release);

        Ok(())
    }

    // Make changes visible, without a durability guarantee
    pub(crate) fn non_durable_commit(&self, transaction_id: u64) -> Result {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        #[cfg(debug_assertions)]
        debug_assert!(self.open_dirty_pages.lock().unwrap().is_empty());
        assert!(self.regional_allocators.lock().unwrap().is_some());

        let mut metadata = self.lock_metadata();
        let layout = self.layout.lock().unwrap();
        let mut secondary = metadata.secondary_slot_mut();
        secondary.set_last_committed_transaction_id(transaction_id);
        secondary.set_data_section_layout(&layout);

        self.log_since_commit.lock().unwrap().clear();
        self.allocated_since_commit.lock().unwrap().clear();
        self.read_from_secondary.store(true, Ordering::Release);

        Ok(())
    }

    pub(crate) fn rollback_uncommited_writes(&self) -> Result {
        #[cfg(debug_assertions)]
        debug_assert!(self.open_dirty_pages.lock().unwrap().is_empty());
        let mut metadata = self.lock_metadata();
        let regional_guard = self.regional_allocators.lock().unwrap();
        let mut layout = self.layout.lock().unwrap();
        let (mut region_allocator, mut regions) = metadata.allocators_mut(&layout)?;
        for op in self.log_since_commit.lock().unwrap().drain(..).rev() {
            match op {
                AllocationOp::Allocate(page_number) => {
                    let region = page_number.region as usize;
                    region_allocator.clear(region);
                    let mem = regions.get_regional_allocator_mut(region);
                    regional_guard.as_ref().unwrap()[region].free(
                        mem,
                        page_number.page_index as u64,
                        page_number.page_order as usize,
                    );
                }
                AllocationOp::Free(page_number) | AllocationOp::FreeUncommitted(page_number) => {
                    let region = page_number.region as usize;
                    let mem = regions.get_regional_allocator_mut(region);
                    regional_guard.as_ref().unwrap()[region].record_alloc(
                        mem,
                        page_number.page_index as u64,
                        page_number.page_order as usize,
                    );
                }
            }
        }
        self.allocated_since_commit.lock().unwrap().clear();
        // Reset the layout, in case it changed during the writes
        if self.read_from_secondary.load(Ordering::Acquire) {
            *layout = metadata.secondary_slot().get_data_section_layout();
        } else {
            *layout = metadata.primary_slot().get_data_section_layout();
        }

        Ok(())
    }

    pub(crate) fn get_page(&self, page_number: PageNumber) -> PageImpl {
        // We must not retrieve an immutable reference to a page which already has a mutable ref to it
        #[cfg(debug_assertions)]
        debug_assert!(
            !self.open_dirty_pages.lock().unwrap().contains(&page_number),
            "{:?}",
            page_number
        );

        // Safety: we asserted that no mutable references are open
        let mem = unsafe {
            self.mmap.get_memory(page_number.address_range(
                self.header_size,
                self.region_size,
                self.page_size,
            ))
        };

        PageImpl { mem, page_number }
    }

    // Safety: the caller must ensure that no references to the memory in `page` exist
    pub(crate) unsafe fn get_page_mut(&self, page_number: PageNumber) -> PageMut {
        #[cfg(debug_assertions)]
        self.open_dirty_pages.lock().unwrap().insert(page_number);

        let address_range =
            page_number.address_range(self.header_size, self.region_size, self.page_size);
        let mem = self.mmap.get_memory_mut(address_range);

        PageMut {
            mem,
            page_number,
            #[cfg(debug_assertions)]
            open_pages: &self.open_dirty_pages,
        }
    }

    pub(crate) fn get_primary_root_page(&self) -> Option<PageNumber> {
        let metadata = self.lock_metadata();
        if self.read_from_secondary.load(Ordering::Acquire) {
            metadata.secondary_slot().get_root_page()
        } else {
            metadata.primary_slot().get_root_page()
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

    pub(crate) fn set_secondary_root_page(&self, root_page: PageNumber) -> Result {
        self.lock_metadata()
            .secondary_slot_mut()
            .set_root_page(root_page);

        Ok(())
    }

    // Safety: the caller must ensure that no references to the memory in `page` exist
    pub(crate) unsafe fn free(&self, page: PageNumber) -> Result {
        let mut metadata = self.lock_metadata();
        let layout = self.layout.lock().unwrap();
        let (mut region_allocator, mut regions) = metadata.allocators_mut(&layout)?;
        let region = page.region as usize;
        // Free in the regional allocator
        let mem = regions.get_regional_allocator_mut(region);
        self.regional_allocators.lock().unwrap().as_ref().unwrap()[region].free(
            mem,
            page.page_index as u64,
            page.page_order as usize,
        );
        // Ensure that the region is marked as having free space
        region_allocator.clear(region);
        self.log_since_commit
            .lock()
            .unwrap()
            .push(AllocationOp::Free(page));

        Ok(())
    }

    // Frees the page if it was allocated since the last commit. Returns true, if the page was freed
    // Safety: the caller must ensure that no references to the memory in `page` exist
    pub(crate) unsafe fn free_if_uncommitted(&self, page: PageNumber) -> Result<bool> {
        if self.allocated_since_commit.lock().unwrap().remove(&page) {
            let mut metadata = self.lock_metadata();
            let layout = self.layout.lock().unwrap();
            let (mut region_allocator, mut regions) = metadata.allocators_mut(&layout)?;
            // Free in the regional allocator
            let mem = regions.get_regional_allocator_mut(page.region as usize);
            self.regional_allocators.lock().unwrap().as_ref().unwrap()[page.region as usize].free(
                mem,
                page.page_index as u64,
                page.page_order as usize,
            );
            // Ensure that the region is marked as having free space
            region_allocator.clear(page.region as usize);

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
        let regional_guard = self.regional_allocators.lock().unwrap();
        let (mut region_allocator, mut regions) = metadata.allocators_mut(layout)?;
        let mut allocated_page = None;
        let mut allocated_region = 0;
        for region in 0..region_allocator.len() {
            allocated_region = region;
            if !region_allocator.get(region) {
                let mem = regions.get_regional_allocator_mut(region);
                match regional_guard.as_ref().unwrap()[region].alloc(mem, required_order) {
                    Ok(page) => {
                        allocated_page = Some(page);
                        break;
                    }
                    Err(err) => {
                        if matches!(err, Error::OutOfSpace) {
                            // Mark the region, if it's full
                            if required_order == 0 {
                                region_allocator.set(region);
                            }
                        } else {
                            return Err(err);
                        }
                    }
                }
            }
        }

        let page = allocated_page.ok_or(Error::OutOfSpace)?;
        Ok(PageNumber::new(
            allocated_region as u32,
            page as u32,
            required_order as u8,
        ))
    }

    fn grow(
        &self,
        metadata: &mut MetadataAccessor,
        layout: &mut DatabaseLayout,
        required_order_allocation: usize,
    ) -> Result<()> {
        let next_desired_size = if layout.num_full_regions > 0 {
            // Grow by 1 region
            // TODO: prune the trailing partial region, if it exists
            layout.usable_bytes() + MAX_USABLE_REGION_SPACE
        } else {
            let required_growth =
                2usize.pow(required_order_allocation as u32) * metadata.get_page_size();
            max(
                layout.usable_bytes() * 2,
                layout.usable_bytes() + required_growth * 2,
            )
        };
        let new_layout = DatabaseLayout::calculate(
            metadata.get_max_capacity(),
            next_desired_size,
            self.page_size,
        )?;
        assert!(new_layout.len() >= layout.len());
        assert_eq!(new_layout.db_header_bytes, layout.db_header_bytes);
        assert_eq!(new_layout.db_header_bytes, self.header_size);
        if new_layout.len() == layout.len() {
            // Can't grow
            return Err(Error::OutOfSpace);
        }

        // Safety: We're growing the mmap
        unsafe {
            self.mmap.resize(new_layout.len())?;
        }
        let mut allocators = self.regional_allocators.lock().unwrap();
        let mut new_allocators = vec![];
        for i in 0..new_layout.num_regions() {
            let new_region_base = new_layout.region_base_address(i);
            let new_region = new_layout.region_layout(i);
            let new_allocator = if i < layout.num_regions() {
                let old_region_base = layout.region_base_address(i);
                let old_region = layout.region_layout(i);
                assert_eq!(old_region_base, new_region_base);
                if new_region.len() == old_region.len() {
                    // No change
                    allocators.as_ref().unwrap()[i].clone()
                } else {
                    // Make sure the old & new allocator state doesn't overlap,
                    // so that we don't clobber the old state when initializing the
                    // new one
                    assert!(new_region.allocator_state.start > old_region.allocator_state.end);
                    assert_eq!(
                        new_region.allocator_state.len(),
                        old_region.allocator_state.len()
                    );
                    // TODO: move the regional allocator to a header instead of footer
                    // so that this relocation isn't necessary
                    let tmp = { metadata.get_regional_allocator(i, layout).to_vec() };
                    let (mut region_allocator, mut regions) =
                        metadata.allocators_mut(&new_layout)?;
                    region_allocator.clear(i);
                    let mem = regions.get_regional_allocator_mut(i);
                    mem.copy_from_slice(&tmp);

                    let mut allocator = allocators.as_ref().unwrap()[i].clone();
                    allocator.resize(mem, new_region.num_pages);
                    allocator
                }
            } else {
                // brand new region
                let (mut region_allocator, mut regions) = metadata.allocators_mut(&new_layout)?;
                region_allocator.clear(i);
                let mem = regions.get_regional_allocator_mut(i);
                BuddyAllocator::init_new(
                    mem,
                    new_region.num_pages,
                    new_layout.full_region_layout.num_pages,
                    new_region.max_order(),
                )
            };
            new_allocators.push(new_allocator);
        }
        *allocators = Some(new_allocators);
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
                if matches!(err, Error::OutOfSpace) && layout.len() < max_capacity {
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
        self.open_dirty_pages.lock().unwrap().insert(page_number);

        let address_range =
            page_number.address_range(self.header_size, self.region_size, self.page_size);
        // Safety:
        // The address range we're returning was just allocated, so no other references exist
        let mem = unsafe { self.mmap.get_memory_mut(address_range) };
        // Zero the memory
        mem.copy_from_slice(&vec![0u8; page_number.page_size_bytes(self.page_size)]);
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
        let regional_guard = self.regional_allocators.lock().unwrap();
        let layout = self.layout.lock().unwrap();
        let mut count = 0;
        // TODO: with dynamic database growth, this is no longer a meaningful way to count free pages
        for i in 0..layout.num_regions() {
            let mem = metadata.get_regional_allocator(i, &layout);
            count += regional_guard.as_ref().unwrap()[i].count_free_pages(mem);
        }

        Ok(count)
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
                if self.commit(non_durable_transaction_id, false).is_err() {
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
        match self.regional_allocators.lock() {
            Ok(allocators) => {
                if self.mmap.flush().is_ok() && allocators.is_some() {
                    self.lock_metadata().set_allocator_dirty(false);
                    let _ = self.mmap.flush();
                }
            }
            Err(_) => {
                let _ = self.mmap.flush();
                eprintln!("Failure while closing database");
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::db::TableDefinition;
    use crate::tree_store::page_store::page_manager::{
        ALLOCATOR_STATE_DIRTY, GOD_BYTE_OFFSET, MAGICNUMBER, MIN_USABLE_PAGES,
    };
    use crate::tree_store::page_store::utils::get_page_size;
    use crate::tree_store::page_store::TransactionalMemory;
    use crate::{Database, Error};
    use memmap2::MmapMut;
    use std::fs::OpenOptions;
    use tempfile::NamedTempFile;

    const X: TableDefinition<[u8], [u8]> = TableDefinition::new("x");

    #[test]
    fn repair_allocator() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let max_size = 1024 * 1024;
        let db = unsafe { Database::create(tmpfile.path(), max_size).unwrap() };
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert(b"hello", b"world").unwrap();
        }
        write_txn.commit().unwrap();
        let free_pages = db.stats().unwrap().free_pages();
        drop(db);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmpfile.path())
            .unwrap();

        let mut mmap = unsafe { MmapMut::map_mut(&file) }.unwrap();
        mmap[GOD_BYTE_OFFSET] |= ALLOCATOR_STATE_DIRTY;

        mmap.flush().unwrap();
        drop(mmap);

        assert!(TransactionalMemory::new(file, max_size, None, true)
            .unwrap()
            .needs_repair()
            .unwrap());

        let db2 = unsafe { Database::create(tmpfile.path(), max_size).unwrap() };
        assert_eq!(free_pages, db2.stats().unwrap().free_pages());
        let write_txn = db2.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert(b"hello2", b"world2").unwrap();
        }
        write_txn.commit().unwrap();
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
