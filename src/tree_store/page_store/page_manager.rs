use crate::tree_store::btree_base::Checksum;
use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::grouped_bitmap::U64GroupedBitMapMut;
use crate::tree_store::page_store::layout::DatabaseLayout;
use crate::tree_store::page_store::mmap::Mmap;
use crate::tree_store::page_store::utils::get_page_size;
use crate::tree_store::page_store::{hash128_with_seed, PageImpl, PageMut};
use crate::tree_store::PageNumber;
use crate::Error;
use crate::Result;
use std::cmp::{max, min};
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
// Header (first 128 bytes):
// 9 bytes: magic number
// 1 byte: page size exponent
// 1 byte: god byte
// 5 bytes: padding to 64-bit aligned
// 8 bytes: region max usable bytes
// 8 bytes: database max size
//
// Commit slot 0 (next 192 bytes):
// 1 byte: version
// 1 byte: != 0 if root page is non-null
// 1 byte: != 0 if freed table root page is non-null
// 5 bytes: padding
// 8 bytes: root page
// 16 bytes: root checksum
// 8 bytes: freed table root page
// 16 bytes: freed table root checksum
// 8 bytes: last committed transaction id
// (db layout size) bytes: active database layout
// 16 bytes: slot checksum
//
// Commit slot 1 (next 192 bytes):
// Same layout as slot 0

// Regions have a maximum size of 4GiB. A `4GiB - overhead` value is the largest that can be represented,
// because the leaf node format uses 32bit offsets
#[cfg(target_pointer_width = "64")]
const MAX_USABLE_REGION_SPACE: usize = 4 * 1024 * 1024 * 1024;
// Default to only 128MiB on 32bit platforms, since the mmap likely cannot be 4GiB or even close.
#[cfg(target_pointer_width = "32")]
const MAX_USABLE_REGION_SPACE: usize = 128 * 1024 * 1024;
pub(crate) const MAX_PAGE_ORDER: usize = 20;
pub(super) const MIN_USABLE_PAGES: usize = 10;
const MIN_DESIRED_USABLE_BYTES: usize = 1024 * 1024;

// TODO: set to 1, when version 1.0 is released
const FILE_FORMAT_VERSION: u8 = 103;

// Inspired by PNG's magic number
const MAGICNUMBER: [u8; 9] = [b'r', b'e', b'd', b'b', 0x1A, 0x0A, 0xA9, 0x0D, 0x0A];
const PAGE_SIZE_OFFSET: usize = MAGICNUMBER.len();
const GOD_BYTE_OFFSET: usize = PAGE_SIZE_OFFSET + size_of::<u8>();
const CHECKSUM_TYPE_OFFSET: usize = GOD_BYTE_OFFSET + size_of::<u8>();
const RESERVED: usize = 4;
const REGION_MAX_USABLE_OFFSET: usize = CHECKSUM_TYPE_OFFSET + size_of::<u8>() + RESERVED;
const DB_SIZE_OFFSET: usize = REGION_MAX_USABLE_OFFSET + size_of::<u64>();
const TRANSACTION_SIZE: usize = 192;
const TRANSACTION_0_OFFSET: usize = 128;
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
const DATA_LAYOUT_OFFSET: usize = TRANSACTION_ID_OFFSET + size_of::<u64>();
const SLOT_CHECKSUM_OFFSET: usize = DATA_LAYOUT_OFFSET + DatabaseLayout::serialized_size();

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
    Ok(u64::from_le_bytes(db_size) as usize)
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

    fn get_max_capacity(&self) -> usize {
        u64::from_le_bytes(
            self.header[DB_SIZE_OFFSET..DB_SIZE_OFFSET + size_of::<u64>()]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn set_max_capacity(&mut self, max_size: usize) {
        self.header[DB_SIZE_OFFSET..DB_SIZE_OFFSET + size_of::<u64>()]
            .copy_from_slice(&(max_size as u64).to_le_bytes());
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

    fn get_region_max_usable_bytes(&self) -> usize {
        u64::from_le_bytes(
            self.header[REGION_MAX_USABLE_OFFSET..REGION_MAX_USABLE_OFFSET + size_of::<u64>()]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn set_region_max_usable_bytes(&mut self, usable_size: usize) {
        self.header[REGION_MAX_USABLE_OFFSET..REGION_MAX_USABLE_OFFSET + size_of::<u64>()]
            .copy_from_slice(&(usable_size as u64).to_le_bytes());
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

    fn get_regional_allocator(&mut self, region: usize, layout: &DatabaseLayout) -> &[u8] {
        let base = layout.region_base_address(region);
        let len = layout.region_layout(region).header_len();
        let absolute = base..(base + len);

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
        if !self.get_recovery_required() {
            self.set_recovery(true);
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
        let len = &self.layout.region_layout(region).header_len();
        let absolute = base..(base + len);

        assert!(absolute.start >= self.layout.header_bytes());
        unsafe { self.mmap.get_memory_mut(absolute) }
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

    fn get_data_section_layout(&self) -> DatabaseLayout {
        DatabaseLayout::from_le_bytes(
            self.mem[DATA_LAYOUT_OFFSET..(DATA_LAYOUT_OFFSET + DatabaseLayout::serialized_size())]
                .try_into()
                .unwrap(),
        )
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

    fn set_data_section_layout(&mut self, layout: &DatabaseLayout) {
        self.mem[DATA_LAYOUT_OFFSET..(DATA_LAYOUT_OFFSET + DatabaseLayout::serialized_size())]
            .copy_from_slice(&layout.to_le_bytes());
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
    region_header_with_padding_size: usize,
    db_header_size: usize,
    dynamic_growth: bool,
    checksum_type: ChecksumType,
}

impl TransactionalMemory {
    pub(crate) fn new(
        file: File,
        max_capacity: usize,
        requested_page_size: Option<usize>,
        requested_region_size: Option<usize>,
        dynamic_growth: bool,
        use_checksums: Option<bool>,
    ) -> Result<Self> {
        assert!(DATA_LAYOUT_OFFSET + DatabaseLayout::serialized_size() <= TRANSACTION_SIZE);

        let page_size = requested_page_size.unwrap_or_else(get_page_size);
        assert!(page_size.is_power_of_two());
        if max_capacity < DB_HEADER_SIZE + page_size * MIN_USABLE_PAGES {
            return Err(Error::OutOfSpace);
        }

        let mmap = Mmap::new(file, max_capacity)?;
        if mmap.len() < DB_HEADER_SIZE {
            // Safety: We're growing the mmap
            unsafe {
                mmap.resize(DB_HEADER_SIZE)?;
            }
        }

        let mutex = Mutex::new(MetadataGuard {});
        let mut metadata = unsafe { MetadataAccessor::new(&mmap, mutex.lock().unwrap()) };

        if metadata.get_magic_number() != MAGICNUMBER {
            let region_size = requested_region_size.unwrap_or(MAX_USABLE_REGION_SPACE);
            assert!(region_size.is_power_of_two());
            let max_usable_region_bytes = min(region_size, max_capacity.next_power_of_two());

            let starting_size = if dynamic_growth {
                MIN_DESIRED_USABLE_BYTES
            } else {
                max_capacity
            };
            let layout = DatabaseLayout::calculate(
                max_capacity,
                starting_size,
                max_usable_region_bytes,
                page_size,
            )?;

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
                    region_layout.num_pages(),
                    layout.full_region_layout().num_pages(),
                    region_layout.max_order(),
                );
            }
            // Set the allocator to not dirty, because the allocator initialization above will have
            // dirtied it
            metadata.set_recovery(false);

            // Store the page & db size. These are immutable
            metadata.set_page_size(page_size);
            metadata.set_max_capacity(max_capacity);
            metadata.set_region_max_usable_bytes(max_usable_region_bytes);
            let checksum_type = if use_checksums.unwrap_or(true) {
                ChecksumType::XXH3_128
            } else {
                ChecksumType::Zero
            };
            metadata.set_checksum_type(checksum_type);

            let mut mutator = metadata.secondary_slot_mut();
            mutator.set_root_page(None);
            mutator.set_freed_root(None);
            mutator.set_last_committed_transaction_id(0);
            mutator.set_data_section_layout(&layout);
            mutator.set_version(FILE_FORMAT_VERSION);
            drop(mutator);
            // Make the state we just wrote the primary
            metadata.swap_primary();

            // Initialize the secondary allocator state
            let mut mutator = metadata.secondary_slot_mut();
            mutator.set_data_section_layout(&layout);
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
        assert_eq!(metadata.primary_slot().get_version(), FILE_FORMAT_VERSION);
        assert_eq!(metadata.secondary_slot().get_version(), FILE_FORMAT_VERSION);
        let layout = metadata.primary_slot().get_data_section_layout();
        let region_size = layout.full_region_layout().len();
        let region_header_size = layout.full_region_layout().data_section().start;
        let checksum_type = metadata.get_checksum_type();

        let regional_allocators = if metadata.get_recovery_required() {
            None
        } else {
            Some(layout.create_allocators())
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
        *self.layout.lock().unwrap() = metadata.primary_slot().get_data_section_layout();
    }

    pub(crate) fn begin_repair(&self) -> Result<()> {
        let mut metadata = self.lock_metadata();

        if !metadata
            .primary_slot()
            .verify_checksum(metadata.get_checksum_type())
        {
            metadata.swap_primary();
            *self.layout.lock().unwrap() = metadata.primary_slot().get_data_section_layout();
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
                *self.layout.lock().unwrap() = metadata.primary_slot().get_data_section_layout();
            }
        }

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
                region_layout.num_pages(),
                layout.full_region_layout().num_pages(),
                region_layout.max_order(),
            ));
        }

        let mut guard = self.regional_allocators.lock().unwrap();
        *guard = Some(layout.create_allocators());

        Ok(())
    }

    pub(crate) fn mark_pages_allocated(
        &self,
        allocated_pages: impl Iterator<Item = PageNumber>,
    ) -> Result<()> {
        let mut metadata = self.lock_metadata();
        let layout = self.layout.lock().unwrap();
        let (_, mut regions) = metadata.allocators_mut(&layout)?;

        let regional_allocators = self.regional_allocators.lock().unwrap();

        for page_number in allocated_pages {
            let region = page_number.region as usize;
            let mem = regions.get_regional_allocator_mut(region);
            regional_allocators.as_ref().unwrap()[region].record_alloc(
                mem,
                page_number.page_index as u64,
                page_number.page_order as usize,
            );
        }

        Ok(())
    }

    pub(crate) fn end_repair(&self) -> Result<()> {
        let mut metadata = self.lock_metadata();
        self.mmap.flush()?;

        metadata.set_recovery(false);
        self.mmap.flush()
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
        assert!(self.regional_allocators.lock().unwrap().is_some());

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
        secondary.set_data_section_layout(&layout);
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
                self.mmap.resize(layout.len())?;
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
        assert!(self.regional_allocators.lock().unwrap().is_some());

        let mut metadata = self.lock_metadata();
        let checksum_type = metadata.get_checksum_type();
        let layout = self.layout.lock().unwrap();
        let mut secondary = metadata.secondary_slot_mut();
        secondary.set_last_committed_transaction_id(transaction_id);
        secondary.set_root_page(data_root);
        secondary.set_freed_root(freed_root);
        secondary.set_data_section_layout(&layout);
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
            metadata.secondary_slot().get_data_section_layout()
        } else {
            metadata.primary_slot().get_data_section_layout()
        };

        let mut regional_guard = self.regional_allocators.lock().unwrap();
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

        // Shrinking only happens during commit
        assert!(restore.len() <= layout.len());
        // Reset the layout, in case it changed during the writes
        if restore.len() < layout.len() {
            // Drop all the allocators for regions that are being deleted
            regional_guard
                .as_mut()
                .unwrap()
                .drain(restore.num_regions()..);
            // Restore the size of the last region's allocator
            let last_region_index = restore.num_regions() - 1;
            let last_region = restore.region_layout(last_region_index);
            let allocator_data = regions.get_regional_allocator_mut(last_region_index);
            let last_allocator = &mut regional_guard.as_mut().unwrap()[last_region_index];
            last_allocator.resize(allocator_data, last_region.num_pages());

            *layout = restore;
            // Safety: we've rollbacked the transaction, so any data in that was written into
            // space that was grown during this transaction no longer exists
            unsafe {
                self.mmap.resize(layout.len())?;
            }
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
                self.db_header_size,
                self.region_size,
                self.region_header_with_padding_size,
                self.page_size,
            ))
        };

        PageImpl { mem, page_number }
    }

    // Safety: the caller must ensure that no references to the memory in `page` exist
    pub(crate) unsafe fn get_page_mut(&self, page_number: PageNumber) -> PageMut {
        #[cfg(debug_assertions)]
        self.open_dirty_pages.lock().unwrap().insert(page_number);

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
    // TODO: add debug_assertion to check for double-free
    pub(crate) unsafe fn free_if_uncommitted(&self, page: PageNumber) -> Result<bool> {
        if self.allocated_since_commit.lock().unwrap().remove(&page) {
            // Zero fill the page to ensure that deleted data is not stored in the file
            let mut mut_page = self.get_page_mut(page);
            mut_page.memory_mut().fill(0);
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

    // Safety: caller must guarantee that no references to free pages at the end of the last region exist
    fn try_shrink(
        &self,
        metadata: &mut MetadataAccessor,
        layout: &mut DatabaseLayout,
    ) -> Result<bool> {
        let mut allocators = self.regional_allocators.lock().unwrap();
        let last_region_index = layout.num_regions() - 1;
        let last_allocator = allocators.as_ref().unwrap()[last_region_index].clone();
        let last_region = layout.region_layout(last_region_index);
        let allocator_data = metadata.get_regional_allocator(last_region_index, layout);
        let trailing_free = last_allocator.trailing_free_pages(allocator_data);
        // TODO: is this the right shrinkage heuristic?
        if trailing_free < last_allocator.len() / 2 {
            return Ok(false);
        }
        let reduce_to_pages = if layout.num_regions() > 1 && trailing_free == last_allocator.len() {
            0
        } else {
            max(MIN_USABLE_PAGES, last_allocator.len() - trailing_free)
        };

        let (mut region_allocator, mut regions) = metadata.allocators_mut(layout)?;
        let new_usable_bytes = if reduce_to_pages == 0 {
            region_allocator.set(last_region_index);
            allocators
                .as_mut()
                .unwrap()
                .pop()
                .expect("allocators should not be empty");
            // drop the whole region
            layout.usable_bytes() - last_region.usable_bytes()
        } else {
            let mem = regions.get_regional_allocator_mut(last_region_index);
            allocators.as_mut().unwrap()[last_region_index].resize(mem, reduce_to_pages);
            layout.usable_bytes()
                - (last_allocator.len() - reduce_to_pages) * metadata.get_page_size()
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
            2usize.pow(required_order_allocation as u32) * metadata.get_page_size();
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
                let mut allocator = allocators.as_ref().unwrap()[i].clone();
                if new_region.len() != old_region.len() {
                    let (mut region_allocator, mut regions) =
                        metadata.allocators_mut(&new_layout)?;
                    region_allocator.clear(i);
                    let mem = regions.get_regional_allocator_mut(i);
                    allocator.resize(mem, new_region.num_pages());
                }
                allocator
            } else {
                // brand new region
                let (mut region_allocator, mut regions) = metadata.allocators_mut(&new_layout)?;
                region_allocator.clear(i);
                let mem = regions.get_regional_allocator_mut(i);
                BuddyAllocator::init_new(
                    mem,
                    new_region.num_pages(),
                    new_layout.full_region_layout().num_pages(),
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
        let regional_guard = self.regional_allocators.lock().unwrap();
        let layout = self.layout.lock().unwrap();
        let mut count = 0;
        for i in 0..layout.num_regions() {
            let mem = metadata.get_regional_allocator(i, &layout);
            count += regional_guard.as_ref().unwrap()[i].count_free_pages(mem);
        }

        // Calculate the number of pages worth of expansion space left, if database grows to max size
        let max_layout = DatabaseLayout::calculate(
            metadata.get_max_capacity(),
            metadata.get_max_capacity(),
            metadata.get_region_max_usable_bytes(),
            self.page_size,
        )
        .unwrap();
        let potential_growth_pages =
            (max_layout.usable_bytes() - layout.usable_bytes()) / self.page_size;

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
        match self.regional_allocators.lock() {
            Ok(allocators) => {
                if self.mmap.flush().is_ok() && allocators.is_some() {
                    self.lock_metadata().set_recovery(false);
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
                .set_write_strategy(WriteStrategy::Throughput)
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

        assert!(
            TransactionalMemory::new(file, max_size, None, None, true, Some(false))
                .unwrap()
                .needs_repair()
                .unwrap()
        );

        let db2 = unsafe { Database::create(tmpfile.path(), max_size).unwrap() };
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
        let db = unsafe { Database::create(tmpfile.path(), max_size).unwrap() };
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

        assert!(
            TransactionalMemory::new(file, max_size, None, None, true, Some(true))
                .unwrap()
                .needs_repair()
                .unwrap()
        );

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
