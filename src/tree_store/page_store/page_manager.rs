use crate::transaction_tracker::TransactionId;
use crate::transactions::{AllocatorStateKey, AllocatorStateTree, AllocatorStateTreeMut};
use crate::tree_store::btree_base::{BtreeHeader, Checksum};
use crate::tree_store::page_store::base::{MAX_PAGE_INDEX, PageHint};
use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::cached_file::PagedCachedFile;
use crate::tree_store::page_store::fast_hash::{PageNumberHashMap, PageNumberHashSet};
use crate::tree_store::page_store::header::{
    DB_HEADER_SIZE, DatabaseHeader, MAGICNUMBER, TransactionHeader, UnrepairedDatabaseHeader,
};
use crate::tree_store::page_store::layout::DatabaseLayout;
use crate::tree_store::page_store::region::{Allocators, RegionTracker};
use crate::tree_store::page_store::{PageImpl, PageMut, hash128_with_seed};
use crate::tree_store::{Page, PageNumber, PageTrackerPolicy};
use crate::{CacheStats, StorageBackend};
use crate::{DatabaseError, Result, StorageError};
use std::cmp::{max, min};
use std::collections::BTreeMap;
#[cfg(debug_assertions)]
use std::collections::HashMap;
#[cfg(debug_assertions)]
use std::collections::HashSet;
use std::convert::TryInto;
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

// The region header is optional in the v3 file format
// It's an artifact of the v2 file format, so we initialize new databases without headers to save space
const NO_HEADER: u32 = 0;

// Regions have a maximum size of 4GiB. A `4GiB - overhead` value is the largest that can be represented,
// because the leaf node format uses 32bit offsets
const MAX_USABLE_REGION_SPACE: u64 = 4 * 1024 * 1024 * 1024;
// A region holds at most `MAX_PAGE_INDEX + 1` pages (the page index within a region is 20 bits),
// so the largest buddy-allocator order any region can have is log2(MAX_PAGE_INDEX + 1).
// `u32::ilog2` is always <= 31, so the cast to u8 is lossless.
#[allow(clippy::cast_possible_truncation)]
pub(crate) const MAX_MAX_PAGE_ORDER: u8 = (MAX_PAGE_INDEX + 1).ilog2() as u8;
pub(super) const MIN_USABLE_PAGES: u32 = 10;
const MIN_DESIRED_USABLE_BYTES: u64 = 1024 * 1024;

pub(super) const INITIAL_REGIONS: u32 = 1000; // Enough for a 4TiB database

// Original file format. No lengths stored with btrees
pub(crate) const FILE_FORMAT_VERSION1: u8 = 1;
// New file format. All btrees have a separate length stored in their header for constant time access
pub(crate) const FILE_FORMAT_VERSION2: u8 = 2;
// New file format:
// * Allocator state is stored in a system table, instead of in the region headers
// * Freed tree split into two system tables: one for the data tables, and one for the system tables
//   It is no longer stored in a separate tree
// * New "allocated pages table" which tracks the pages allocated, in the data tree, by a transaction.
//   This is a system table. It is only written when a savepoint exists
// * New persistent savepoint format
pub(crate) const FILE_FORMAT_VERSION3: u8 = 3;

#[derive(Copy, Clone)]
pub(crate) enum ShrinkPolicy {
    // Try to shrink the file by the default amount
    Default,
    // Try to shrink the file by the maximum amount
    Maximum,
    // Do not try to shrink the file
    Never,
}

/// Controls how `allocate()` picks a free page.
#[derive(Copy, Clone)]
pub(crate) enum AllocationPolicy {
    /// Find a free block at the requested order, or recursively split from a
    /// higher order. Cheaper than `Lowest`, but after `grow()` has appended
    /// buddy-aligned free blocks at high page indices this can allocate at
    /// high absolute pages.
    Default,
    /// Pick the lowest-page-number allocation across all orders. More
    /// expensive, but keeps trailing pages free so `try_shrink()` can
    /// reclaim recently-grown space.
    Lowest,
}

/// Per-write-transaction handle through which btree mutation code allocates
/// and frees pages. Bundles the shared `TransactionalMemory` with the write
/// transaction's `AllocationPolicy`.
#[derive(Clone)]
pub(crate) struct PageAllocator {
    mem: Arc<TransactionalMemory>,
    policy: AllocationPolicy,
    allocated_since_commit: Arc<Mutex<PageTrackerPolicy>>,
}

impl PageAllocator {
    pub(crate) fn new(mem: Arc<TransactionalMemory>, policy: AllocationPolicy) -> Self {
        Self {
            mem,
            policy,
            allocated_since_commit: Arc::new(Mutex::new(PageTrackerPolicy::new_tracking())),
        }
    }

    // TODO: migrate remaining callers off of this and make it private. Btree
    // mutation code should interact with `TransactionalMemory` exclusively
    // through `PageAllocator`.
    pub(crate) fn mem(&self) -> &Arc<TransactionalMemory> {
        &self.mem
    }

    /// Drains the set of pages allocated since the last commit, returning
    /// them. Used by commit and non-durable commit paths to hand the set over
    /// to `TransactionalMemory`.
    pub(crate) fn take_allocated_since_commit(&self) -> PageNumberHashSet {
        self.allocated_since_commit.lock().unwrap().reset()
    }

    /// Reverses every allocation made since the last commit: drains the
    /// allocated-since-commit set and frees each page.
    pub(crate) fn rollback_all(&self) {
        self.mem.debug_assert_no_dirty_pages();
        let drained = self.take_allocated_since_commit();
        for page in &drained {
            self.mem.free(*page, &mut PageTrackerPolicy::Ignore);
        }
    }

    pub(crate) fn allocate<'a>(
        &self,
        size: usize,
        allocated: &mut PageTrackerPolicy,
    ) -> Result<PageMut<'a>> {
        let page = match self.policy {
            AllocationPolicy::Default => self.mem.allocate(size, allocated)?,
            AllocationPolicy::Lowest => self.mem.allocate_lowest(size, allocated)?,
        };
        self.allocated_since_commit
            .lock()
            .unwrap()
            .insert(page.get_page_number());
        Ok(page)
    }

    // Always allocates at the lowest free page, ignoring `self.policy`. Used
    // by compaction's probe loop where the point is specifically to test
    // whether a page can land below its current position.
    pub(crate) fn allocate_lowest<'a>(
        &self,
        size: usize,
        allocated: &mut PageTrackerPolicy,
    ) -> Result<PageMut<'a>> {
        let page = self.mem.allocate_lowest(size, allocated)?;
        self.allocated_since_commit
            .lock()
            .unwrap()
            .insert(page.get_page_number());
        Ok(page)
    }

    pub(crate) fn free(&self, page: PageNumber, allocated: &mut PageTrackerPolicy) {
        self.allocated_since_commit
            .lock()
            .unwrap()
            .remove_if_present(page);
        self.mem.free(page, allocated);
    }

    pub(crate) fn free_if_uncommitted(
        &self,
        page: PageNumber,
        allocated: &mut PageTrackerPolicy,
    ) -> bool {
        if self
            .allocated_since_commit
            .lock()
            .unwrap()
            .remove_if_present(page)
        {
            self.mem.free(page, allocated);
            true
        } else {
            false
        }
    }

    pub(crate) fn uncommitted(&self, page: PageNumber) -> bool {
        self.allocated_since_commit.lock().unwrap().contains(page)
    }

    pub(crate) fn get_page(&self, page_number: PageNumber, hint: PageHint) -> Result<PageImpl> {
        self.mem.get_page(page_number, hint)
    }

    pub(crate) fn get_page_mut<'a>(&self, page_number: PageNumber) -> Result<PageMut<'a>> {
        self.mem.get_page_mut(page_number)
    }

    pub(crate) fn get_page_size(&self) -> usize {
        self.mem.get_page_size()
    }
}

fn ceil_log2(x: usize) -> u8 {
    if x.is_power_of_two() {
        x.trailing_zeros().try_into().unwrap()
    } else {
        x.next_power_of_two().trailing_zeros().try_into().unwrap()
    }
}

pub(crate) fn xxh3_checksum(data: &[u8]) -> Checksum {
    hash128_with_seed(data, 0)
}

struct InMemoryState {
    header: DatabaseHeader,
    // None until the Database finishes loading allocator state from disk or rebuilding it via
    // repair.
    allocators: Option<Allocators>,
    // True if a non-durable commit has updated the secondary slot and that data should be served
    // to readers until a durable commit promotes it to the primary slot on disk. Protected by the
    // enclosing Mutex so updates happen atomically with the header changes they describe.
    read_from_secondary: bool,
}

impl InMemoryState {
    fn new(header: DatabaseHeader) -> Self {
        Self {
            header,
            allocators: None,
            read_from_secondary: false,
        }
    }

    fn allocators(&self) -> &Allocators {
        self.allocators
            .as_ref()
            .expect("allocators have not been loaded yet")
    }

    fn allocators_mut(&mut self) -> &mut Allocators {
        self.allocators
            .as_mut()
            .expect("allocators have not been loaded yet")
    }

    fn get_region(&self, region: u32) -> &BuddyAllocator {
        &self.allocators().region_allocators[region as usize]
    }

    fn get_region_mut(&mut self, region: u32) -> &mut BuddyAllocator {
        &mut self.allocators_mut().region_allocators[region as usize]
    }

    fn get_region_tracker_mut(&mut self) -> &mut RegionTracker {
        &mut self.allocators_mut().region_tracker
    }

    // Slot that reads should be served from: the secondary when a non-durable commit is pending,
    // otherwise the primary.
    fn latest_slot(&self) -> &TransactionHeader {
        if self.read_from_secondary {
            self.header.secondary_slot()
        } else {
            self.header.primary_slot()
        }
    }
}

pub(crate) struct TransactionalMemory {
    // TODO: Maybe we should move all this unpersisted stuff to another transaction layer.
    // There are durable commits which support persistent and ephemeral savepoints.
    // And then non-durable commits which support only ephemeral savepoints, and basically
    // accumulate in unpersisted memory/file space until a durable commit is made.
    // Maybe the non-durable commit layer should be separate, and build on top of the durable commit layer.
    unpersisted: Mutex<PageNumberHashSet>,
    // Data-tree pages allocated by non-durable commits that have not yet been durably persisted.
    // This is the in-memory replacement for entries
    // in DATA_ALLOCATED_TABLE for non-durable transactions: by keeping the list in-memory we can
    // efficiently remove entries when `free_if_unpersisted` reclaims pages.
    // Flushed to DATA_ALLOCATED_TABLE during durable_commit, and cleared (along with
    // `unpersisted`) once the commit succeeds.
    unpersisted_allocations: Mutex<BTreeMap<TransactionId, PageNumberHashSet>>,
    // Reverse index into `unpersisted_allocations`
    unpersisted_allocation_txn: Mutex<PageNumberHashMap<TransactionId>>,
    storage: PagedCachedFile,
    state: Mutex<InMemoryState>,
    // The number of PageMut which are outstanding
    #[cfg(debug_assertions)]
    open_dirty_pages: Arc<Mutex<HashSet<PageNumber>>>,
    // Reference counts of PageImpls that are outstanding
    #[cfg(debug_assertions)]
    read_page_ref_counts: Arc<Mutex<HashMap<PageNumber, u64>>>,
    // Set of all allocated pages for debugging assertions
    #[cfg(debug_assertions)]
    allocated_pages: Arc<Mutex<PageNumberHashSet>>,
    page_size: u32,
    // We store these separately from the layout because they're static, and accessed on the get_page()
    // code path where there is no locking
    region_size: u64,
    region_header_with_padding_size: u64,
}

impl TransactionalMemory {
    pub(crate) fn new(
        file: Box<dyn StorageBackend>,
        // Allow initializing a new database in an empty file
        allow_initialize: bool,
        page_size: usize,
        requested_region_size: Option<u64>,
        cache_size: usize,
        read_only: bool,
    ) -> Result<Self, DatabaseError> {
        assert!(page_size.is_power_of_two() && page_size >= DB_HEADER_SIZE);

        let region_size = requested_region_size.unwrap_or(MAX_USABLE_REGION_SPACE);
        let region_size = min(
            region_size,
            (u64::from(MAX_PAGE_INDEX) + 1) * page_size as u64,
        );
        assert!(region_size.is_power_of_two());

        let storage = PagedCachedFile::new(file, page_size as u64, cache_size)?;

        let initial_storage_len = storage.raw_file_len()?;

        let magic_number: [u8; MAGICNUMBER.len()] =
            if initial_storage_len >= MAGICNUMBER.len() as u64 {
                storage
                    .read_direct(0, MAGICNUMBER.len())?
                    .try_into()
                    .unwrap()
            } else {
                [0; MAGICNUMBER.len()]
            };

        if initial_storage_len > 0 {
            // File already exists check that the magic number matches
            if magic_number != MAGICNUMBER {
                return Err(StorageError::Io(ErrorKind::InvalidData.into()).into());
            }
        } else {
            // File is empty, check that we're allowed to initialize a new database (i.e. the caller is Database::create() and not open())
            if !allow_initialize {
                return Err(StorageError::Io(ErrorKind::InvalidData.into()).into());
            }
        }

        if magic_number != MAGICNUMBER {
            let region_tracker_required_bytes =
                RegionTracker::new(INITIAL_REGIONS, MAX_MAX_PAGE_ORDER + 1)
                    .to_vec()
                    .len();

            // Make sure that there is enough room to allocate the region tracker into a page
            let size: u64 = max(
                MIN_DESIRED_USABLE_BYTES,
                page_size as u64 * u64::from(MIN_USABLE_PAGES),
            );
            let tracker_space =
                (page_size * region_tracker_required_bytes.div_ceil(page_size)) as u64;
            let starting_size = size + tracker_space;

            let page_capacity = (region_size / u64::try_from(page_size).unwrap())
                .try_into()
                .unwrap();
            let layout = DatabaseLayout::calculate(
                starting_size,
                page_capacity,
                NO_HEADER,
                page_size.try_into().unwrap(),
            );

            {
                let file_len = storage.raw_file_len()?;

                if file_len < layout.len() {
                    storage.resize(layout.len())?;
                }
            }

            let mut header = DatabaseHeader::new(layout, TransactionId::new(0));

            header.recovery_required = false;
            header.two_phase_commit = true;
            storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(false));

            storage.flush()?;
            // Write the magic number only after the data structure is initialized and written to disk
            // to ensure that it's crash safe
            storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(true));
            storage.flush()?;
        }
        let header_bytes = storage.read_direct(0, DB_HEADER_SIZE)?;
        let unrepaired = UnrepairedDatabaseHeader::from_bytes(&header_bytes)?;

        assert_eq!(unrepaired.page_size() as usize, page_size);
        let file_len = storage.raw_file_len()?;
        let needs_recovery = unrepaired.recovery_required(file_len);
        if needs_recovery && read_only {
            return Err(DatabaseError::RepairAborted);
        }
        let (header, _) = unrepaired.finalize(file_len)?;
        if needs_recovery {
            storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(true));
            storage.flush()?;
        }

        let layout = header.layout();
        assert_eq!(layout.len(), storage.raw_file_len()?);
        let region_size = layout.full_region_layout().len();
        let region_header_size = layout.full_region_layout().data_section().start;
        let state = InMemoryState::new(header);

        assert!(page_size >= DB_HEADER_SIZE);

        Ok(Self {
            unpersisted: Mutex::new(PageNumberHashSet::default()),
            unpersisted_allocations: Mutex::new(BTreeMap::new()),
            unpersisted_allocation_txn: Mutex::new(PageNumberHashMap::default()),
            storage,
            state: Mutex::new(state),
            #[cfg(debug_assertions)]
            open_dirty_pages: Arc::new(Mutex::new(HashSet::new())),
            #[cfg(debug_assertions)]
            read_page_ref_counts: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(debug_assertions)]
            allocated_pages: Arc::new(Mutex::new(PageNumberHashSet::default())),
            page_size: page_size.try_into().unwrap(),
            region_size,
            region_header_with_padding_size: region_header_size,
        })
    }

    pub(crate) fn cache_stats(&self) -> CacheStats {
        self.storage.cache_stats()
    }

    pub(crate) fn check_io_errors(&self) -> Result {
        self.storage.check_io_errors()
    }

    // Panics in debug builds if any `PageMut` handed out by `get_page_mut` or
    // `allocate*` has not yet been dropped. Intended as a precondition for
    // commit/abort paths, which assume no mutable page references remain.
    pub(crate) fn debug_assert_no_dirty_pages(&self) {
        #[cfg(debug_assertions)]
        {
            let dirty_pages = self.open_dirty_pages.lock().unwrap();
            debug_assert!(
                dirty_pages.is_empty(),
                "Dirty pages outstanding: {dirty_pages:?}"
            );
        }
    }

    #[cfg(debug_assertions)]
    pub(crate) fn mark_debug_allocated_page(&self, page: PageNumber) {
        assert!(self.allocated_pages.lock().unwrap().insert(page));
    }

    #[cfg(debug_assertions)]
    pub(crate) fn all_allocated_pages(&self) -> Vec<PageNumber> {
        self.allocated_pages
            .lock()
            .unwrap()
            .iter()
            .copied()
            .collect()
    }

    #[cfg(debug_assertions)]
    pub(crate) fn debug_check_allocator_consistency(&self) {
        let state = self.state.lock().unwrap();
        let allocators = state.allocators();
        let mut region_pages = vec![vec![]; allocators.region_allocators.len()];
        for p in self.allocated_pages.lock().unwrap().iter() {
            region_pages[p.region as usize].push(*p);
        }
        for (i, allocator) in allocators.region_allocators.iter().enumerate() {
            allocator.check_allocated_pages(i.try_into().unwrap(), &region_pages[i]);
        }
    }

    pub(crate) fn clear_read_cache(&self) {
        self.storage.invalidate_cache_all();
    }

    pub(crate) fn clear_cache_and_reload(&mut self) -> Result<bool, DatabaseError> {
        self.storage.flush()?;
        self.storage.invalidate_cache_all();

        let header_bytes = self.storage.read_direct(0, DB_HEADER_SIZE)?;
        let unrepaired = UnrepairedDatabaseHeader::from_bytes(&header_bytes)?;
        let (header, was_clean) = unrepaired.finalize(self.storage.raw_file_len()?)?;
        if !was_clean {
            self.storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(true));
            self.storage.flush()?;
        }

        {
            let mut state = self.state.lock().unwrap();
            state.header = header;
            state.read_from_secondary = false;
            // Drop the previous allocator state -- it described the layout that was in memory
            // before the reload. The caller is required to repopulate it (via begin_repair or
            // load_allocator_state) before any allocation/free path runs.
            state.allocators = None;
        }
        // Reloading from disk discards in-memory roots, so drop volatile allocation state
        // that belonged only to those roots.
        self.unpersisted.lock().unwrap().clear();
        self.unpersisted_allocations.lock().unwrap().clear();
        self.unpersisted_allocation_txn.lock().unwrap().clear();

        Ok(was_clean)
    }

    pub(crate) fn begin_writable(&self) -> Result {
        let mut state = self.state.lock().unwrap();
        assert!(!state.header.recovery_required);
        state.header.recovery_required = true;
        self.write_header(&state.header)?;
        self.storage.flush()
    }

    pub(crate) fn used_two_phase_commit(&self) -> bool {
        self.state.lock().unwrap().header.two_phase_commit
    }

    pub(crate) fn allocator_hash(&self) -> u128 {
        self.state.lock().unwrap().allocators().xxh3_hash()
    }

    // Reports whether the backend has seen an I/O failure in this process.
    // Callers use this to skip cleanup that would do further I/O after a
    // previous storage error (e.g. WriteTransaction::drop).
    pub(crate) fn storage_failure(&self) -> bool {
        self.storage.check_io_errors().is_err()
    }

    pub(crate) fn repair_primary_corrupted(&self) {
        let mut state = self.state.lock().unwrap();
        state.header.swap_primary_slot();
    }

    pub(crate) fn begin_repair(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.allocators = Some(Allocators::new(state.header.layout()));
        #[cfg(debug_assertions)]
        self.allocated_pages.lock().unwrap().clear();

        Ok(())
    }

    pub(crate) fn mark_page_allocated(&self, page_number: PageNumber) {
        let mut state = self.state.lock().unwrap();
        let region_index = page_number.region;
        let allocator = state.get_region_mut(region_index);
        allocator.record_alloc(page_number.page_index, page_number.page_order);
        #[cfg(debug_assertions)]
        assert!(self.allocated_pages.lock().unwrap().insert(page_number));
    }

    fn write_header(&self, header: &DatabaseHeader) -> Result {
        self.storage
            .write(0, DB_HEADER_SIZE, true)?
            .mem_mut()
            .copy_from_slice(&header.to_bytes(true));

        Ok(())
    }

    pub(crate) fn end_repair(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.header.recovery_required = false;
        self.write_header(&state.header)?;
        self.storage.flush()?;
        Ok(())
    }

    pub(crate) fn reserve_allocator_state(
        &self,
        tree: &mut AllocatorStateTreeMut,
        transaction_id: TransactionId,
    ) -> Result<u32> {
        let state = self.state.lock().unwrap();
        let layout = state.header.layout();
        let num_regions = layout.num_regions();
        let allocators = state.allocators();
        let region_tracker_len = allocators.region_tracker.to_vec().len();
        let region_lens: Vec<usize> = allocators
            .region_allocators
            .iter()
            .map(|x| x.to_vec().len())
            .collect();
        drop(state);

        for i in 0..num_regions {
            let region_bytes_len = region_lens[i as usize];
            tree.insert(
                &AllocatorStateKey::Region(i),
                &vec![0; region_bytes_len].as_ref(),
            )?;
        }

        tree.insert(
            &AllocatorStateKey::RegionTracker,
            &vec![0; region_tracker_len].as_ref(),
        )?;

        tree.insert(
            &AllocatorStateKey::TransactionId,
            &transaction_id.raw_id().to_le_bytes().as_ref(),
        )?;

        Ok(num_regions)
    }

    // Returns true on success, or false if the number of regions has changed
    pub(crate) fn try_save_allocator_state(
        &self,
        tree: &mut AllocatorStateTreeMut,
        num_regions: u32,
    ) -> Result<bool> {
        // Has the number of regions changed since reserve_allocator_state() was called?
        let state = self.state.lock().unwrap();
        if num_regions != state.header.layout().num_regions() {
            return Ok(false);
        }

        let allocators = state.allocators();
        for i in 0..num_regions {
            let region_bytes = &allocators.region_allocators[i as usize].to_vec();
            if tree
                .get(&AllocatorStateKey::Region(i))?
                .unwrap()
                .value()
                .len()
                < region_bytes.len()
            {
                // The allocator state grew too much since we reserved space
                return Ok(false);
            }
            tree.insert_inplace(&AllocatorStateKey::Region(i), &region_bytes.as_ref())?;
        }

        let region_tracker_bytes = allocators.region_tracker.to_vec();
        if tree
            .get(&AllocatorStateKey::RegionTracker)?
            .unwrap()
            .value()
            .len()
            < region_tracker_bytes.len()
        {
            // The allocator state grew too much since we reserved space
            return Ok(false);
        }
        tree.insert_inplace(
            &AllocatorStateKey::RegionTracker,
            &region_tracker_bytes.as_ref(),
        )?;

        Ok(true)
    }

    // Returns true if the allocator state table is up to date, or false if it's stale
    pub(crate) fn is_valid_allocator_state(&self, tree: &AllocatorStateTree) -> Result<bool> {
        // See if this is stale allocator state left over from a previous transaction. That won't
        // happen during normal operation, since WriteTransaction::commit() always updates the
        // allocator state table before calling TransactionalMemory::commit(), but there are also
        // a few places where TransactionalMemory::commit() is called directly without using a
        // WriteTransaction. When that happens, any existing allocator state table will be left
        // in place but is no longer valid. (And even if there were no such calls today, it would
        // be an easy mistake to make! So it's good that we check.)
        let Some(value) = tree.get(&AllocatorStateKey::TransactionId)? else {
            return Ok(false);
        };
        let transaction_id =
            TransactionId::new(u64::from_le_bytes(value.value().try_into().unwrap()));

        Ok(transaction_id == self.get_last_committed_transaction_id()?)
    }

    pub(crate) fn load_allocator_state(&self, tree: &AllocatorStateTree) -> Result {
        assert!(self.is_valid_allocator_state(tree)?);

        // Load the allocator state
        let mut region_allocators = vec![];
        for region in
            tree.range(&(AllocatorStateKey::Region(0)..=AllocatorStateKey::Region(u32::MAX)))?
        {
            region_allocators.push(BuddyAllocator::from_bytes(region?.value()));
        }

        let region_tracker = RegionTracker::from_bytes(
            tree.get(&AllocatorStateKey::RegionTracker)?
                .unwrap()
                .value(),
        );

        let mut state = self.state.lock().unwrap();
        state.allocators = Some(Allocators {
            region_tracker,
            region_allocators,
        });

        // Resize the allocators to match the current file size
        let layout = state.header.layout();
        state.allocators_mut().resize_to(layout);
        drop(state);

        self.state.lock().unwrap().header.recovery_required = false;

        Ok(())
    }

    #[cfg_attr(not(debug_assertions), expect(unused_variables))]
    pub(crate) fn is_allocated(&self, page: PageNumber) -> bool {
        #[cfg(debug_assertions)]
        {
            let allocated = self.allocated_pages.lock().unwrap();
            allocated.contains(&page)
        }
        #[cfg(not(debug_assertions))]
        {
            unreachable!()
        }
    }

    // Commit all outstanding changes and make them visible as the primary
    pub(crate) fn commit(
        &self,
        data_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
        transaction_id: TransactionId,
        two_phase: bool,
        shrink_policy: ShrinkPolicy,
    ) -> Result {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        self.debug_assert_no_dirty_pages();
        self.storage.check_io_errors()?;

        let mut state = self.state.lock().unwrap();
        // Trim surplus file space, before finalizing the commit
        let shrunk = if !matches!(shrink_policy, ShrinkPolicy::Never) {
            Self::try_shrink(&mut state, matches!(shrink_policy, ShrinkPolicy::Maximum))?
        } else {
            false
        };
        // Copy the header so that we can release the state lock, while we flush the file
        let mut header = state.header.clone();
        drop(state);

        let old_transaction_id = header.secondary_slot().transaction_id;
        let secondary = header.secondary_slot_mut();
        secondary.transaction_id = transaction_id;
        secondary.user_root = data_root;
        secondary.system_root = system_root;

        self.write_header(&header)?;

        // Use 2-phase commit, if checksums are disabled
        if two_phase {
            self.storage.flush()?;
        }

        // Make our new commit the primary, and record whether it was a 2-phase commit.
        // These two bits need to be written atomically
        header.swap_primary_slot();
        header.two_phase_commit = two_phase;

        // Write the new header to disk
        self.write_header(&header)?;
        self.storage.flush()?;

        if shrunk {
            self.storage.resize(header.layout().len())?;
        }
        let mut unpersisted = self.unpersisted.lock().unwrap();
        unpersisted.clear();
        unpersisted.shrink_to_fit();
        // Any remaining unpersisted allocations are now durable. Their records have already been
        // flushed to DATA_ALLOCATED_TABLE (see WriteTransaction::durable_commit).
        self.unpersisted_allocations.lock().unwrap().clear();
        self.unpersisted_allocation_txn.lock().unwrap().clear();

        let mut state = self.state.lock().unwrap();
        assert_eq!(
            state.header.secondary_slot().transaction_id,
            old_transaction_id
        );
        state.header = header;
        state.read_from_secondary = false;
        drop(state);

        Ok(())
    }

    // Make changes visible, without a durability guarantee. `newly_unpersisted` is the set of
    // pages allocated by this transaction; they become part of the unpersisted-page tracking so
    // they can be reclaimed if a subsequent durable commit fails.
    pub(crate) fn non_durable_commit(
        &self,
        data_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
        transaction_id: TransactionId,
        newly_unpersisted: PageNumberHashSet,
    ) -> Result {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        self.debug_assert_no_dirty_pages();
        self.storage.check_io_errors()?;

        self.unpersisted.lock().unwrap().extend(newly_unpersisted);
        self.storage.write_barrier()?;

        let mut state = self.state.lock().unwrap();
        let secondary = state.header.secondary_slot_mut();
        secondary.transaction_id = transaction_id;
        secondary.user_root = data_root;
        secondary.system_root = system_root;
        state.read_from_secondary = true;

        Ok(())
    }

    pub(crate) fn get_page(&self, page_number: PageNumber, hint: PageHint) -> Result<PageImpl> {
        let range = page_number.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (range.end - range.start).try_into().unwrap();
        let mem = self.storage.read(range.start, len, hint)?;

        // We must not retrieve an immutable reference to a page which already has a mutable ref to it
        #[cfg(debug_assertions)]
        {
            let dirty_pages = self.open_dirty_pages.lock().unwrap();
            debug_assert!(!dirty_pages.contains(&page_number), "{page_number:?}");
            *(self
                .read_page_ref_counts
                .lock()
                .unwrap()
                .entry(page_number)
                .or_default()) += 1;
            drop(dirty_pages);
        }

        Ok(PageImpl {
            mem,
            page_number,
            #[cfg(debug_assertions)]
            open_pages: self.read_page_ref_counts.clone(),
        })
    }

    // NOTE: the caller must ensure that the read cache has been invalidated or stale reads my occur
    pub(crate) fn get_page_mut<'txn>(&self, page_number: PageNumber) -> Result<PageMut<'txn>> {
        #[cfg(debug_assertions)]
        {
            assert!(
                !self
                    .read_page_ref_counts
                    .lock()
                    .unwrap()
                    .contains_key(&page_number)
            );
            assert!(!self.open_dirty_pages.lock().unwrap().contains(&page_number));
        }

        let address_range = page_number.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (address_range.end - address_range.start)
            .try_into()
            .unwrap();
        let mem = self.storage.write(address_range.start, len, false)?;

        #[cfg(debug_assertions)]
        {
            assert!(self.open_dirty_pages.lock().unwrap().insert(page_number));
        }

        Ok(PageMut {
            mem,
            page_number,
            _lifetime: PhantomData,
            #[cfg(debug_assertions)]
            open_pages: self.open_dirty_pages.clone(),
        })
    }

    pub(crate) fn get_version(&self) -> u8 {
        let state = self.state.lock().unwrap();
        state.latest_slot().version
    }

    pub(crate) fn get_data_root(&self) -> Option<BtreeHeader> {
        let state = self.state.lock().unwrap();
        state.latest_slot().user_root
    }

    pub(crate) fn get_system_root(&self) -> Option<BtreeHeader> {
        let state = self.state.lock().unwrap();
        state.latest_slot().system_root
    }

    pub(crate) fn get_last_committed_transaction_id(&self) -> Result<TransactionId> {
        let state = self.state.lock()?;
        Ok(state.latest_slot().transaction_id)
    }

    pub(crate) fn get_last_durable_transaction_id(&self) -> Result<TransactionId> {
        let state = self.state.lock()?;
        Ok(state.header.primary_slot().transaction_id)
    }

    pub(crate) fn free(&self, page: PageNumber, allocated: &mut PageTrackerPolicy) {
        self.free_helper(page, allocated);
    }

    fn remove_unpersisted_allocation(&self, page: PageNumber) {
        let Some(txn) = self
            .unpersisted_allocation_txn
            .lock()
            .unwrap()
            .remove(&page)
        else {
            return;
        };
        let mut allocations = self.unpersisted_allocations.lock().unwrap();
        let empty = {
            let pages = allocations
                .get_mut(&txn)
                .expect("unpersisted_allocation_txn points to a missing entry");
            let removed = pages.remove(&page);
            debug_assert!(removed);
            pages.is_empty()
        };
        if empty {
            allocations.remove(&txn);
        }
    }

    fn free_helper(&self, page: PageNumber, allocated: &mut PageTrackerPolicy) {
        #[cfg(debug_assertions)]
        {
            assert!(
                !self
                    .read_page_ref_counts
                    .lock()
                    .unwrap()
                    .contains_key(&page)
            );
            assert!(self.allocated_pages.lock().unwrap().remove(&page));
            assert!(!self.open_dirty_pages.lock().unwrap().contains(&page));
        }
        allocated.remove(page);
        let mut state = self.state.lock().unwrap();
        let region_index = page.region;
        // Free in the regional allocator
        state
            .get_region_mut(region_index)
            .free(page.page_index, page.page_order);
        // Ensure that the region is marked as having free space
        state
            .get_region_tracker_mut()
            .mark_free(page.page_order, region_index);

        let address_range = page.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (address_range.end - address_range.start)
            .try_into()
            .unwrap();
        self.storage.invalidate_cache(address_range.start, len);
        self.storage.cancel_pending_write(address_range.start, len);
    }

    // Frees the page if no durable commit has occurred, since it was allocated. Returns true, if the page was freed
    pub(crate) fn free_if_unpersisted(
        &self,
        page: PageNumber,
        allocated: &mut PageTrackerPolicy,
    ) -> bool {
        if self.unpersisted.lock().unwrap().remove(&page) {
            self.remove_unpersisted_allocation(page);
            self.free_helper(page, allocated);
            true
        } else {
            false
        }
    }

    // Record pages allocated in the data tree by a non-durable transaction. These are tracked in
    // memory instead of being written to DATA_ALLOCATED_TABLE so that `free_if_unpersisted` can
    // efficiently update the allocation list when it reclaims pages.
    pub(crate) fn record_unpersisted_allocations(
        &self,
        transaction_id: TransactionId,
        pages: impl IntoIterator<Item = PageNumber>,
    ) {
        let mut iter = pages.into_iter().peekable();
        if iter.peek().is_none() {
            return;
        }
        let mut allocations = self.unpersisted_allocations.lock().unwrap();
        let mut reverse = self.unpersisted_allocation_txn.lock().unwrap();
        let entry = allocations.entry(transaction_id).or_default();
        for page in iter {
            if entry.insert(page) {
                let prev = reverse.insert(page, transaction_id);
                debug_assert!(prev.is_none(), "page {page:?} already tracked");
            }
        }
    }

    pub(crate) fn take_unpersisted_allocations(
        &self,
    ) -> BTreeMap<TransactionId, PageNumberHashSet> {
        self.unpersisted_allocation_txn.lock().unwrap().clear();
        std::mem::take(&mut *self.unpersisted_allocations.lock().unwrap())
    }

    // Returns all unpersisted data-tree pages allocated strictly after `transaction_id`. Used
    // during savepoint restore to queue pages that need to be freed.
    pub(crate) fn unpersisted_allocations_after(
        &self,
        transaction_id: TransactionId,
    ) -> Vec<PageNumber> {
        let allocations = self.unpersisted_allocations.lock().unwrap();
        let mut result = vec![];
        for pages in allocations.range(transaction_id.next()..).map(|(_, v)| v) {
            result.extend(pages.iter().copied());
        }
        result
    }

    pub(crate) fn unpersisted(&self, page: PageNumber) -> bool {
        self.unpersisted.lock().unwrap().contains(&page)
    }

    pub(crate) fn allocate_helper<'txn>(
        &self,
        allocation_size: usize,
        lowest: bool,
    ) -> Result<PageMut<'txn>> {
        let required_pages = allocation_size.div_ceil(self.get_page_size());
        let required_order = ceil_log2(required_pages);

        let mut state = self.state.lock().unwrap();

        let page_number = if let Some(page_number) =
            Self::allocate_helper_retry(&mut state, required_order, lowest)?
        {
            page_number
        } else {
            self.grow(&mut state, required_order)?;
            Self::allocate_helper_retry(&mut state, required_order, lowest)?.unwrap()
        };

        #[cfg(debug_assertions)]
        {
            assert!(self.allocated_pages.lock().unwrap().insert(page_number));
            assert!(
                !self
                    .read_page_ref_counts
                    .lock()
                    .unwrap()
                    .contains_key(&page_number),
                "Allocated a page that is still referenced! {page_number:?}"
            );
            assert!(!self.open_dirty_pages.lock().unwrap().contains(&page_number));
        }

        let address_range = page_number.address_range(
            self.page_size.into(),
            self.region_size,
            self.region_header_with_padding_size,
            self.page_size,
        );
        let len: usize = (address_range.end - address_range.start)
            .try_into()
            .unwrap();

        #[allow(unused_mut)]
        let mut mem = self.storage.write(address_range.start, len, true)?;
        debug_assert!(mem.mem().len() >= allocation_size);

        #[cfg(debug_assertions)]
        {
            assert!(self.open_dirty_pages.lock().unwrap().insert(page_number));

            // Poison the memory in debug mode to help detect uninitialized reads
            mem.mem_mut().fill(0xFF);
        }

        Ok(PageMut {
            mem,
            page_number,
            _lifetime: PhantomData,
            #[cfg(debug_assertions)]
            open_pages: self.open_dirty_pages.clone(),
        })
    }

    fn allocate_helper_retry(
        state: &mut InMemoryState,
        required_order: u8,
        lowest: bool,
    ) -> Result<Option<PageNumber>> {
        loop {
            let Some(candidate_region) = state.get_region_tracker_mut().find_free(required_order)
            else {
                return Ok(None);
            };
            let region = state.get_region_mut(candidate_region);
            let r = if lowest {
                region.alloc_lowest(required_order)
            } else {
                region.alloc(required_order)
            };
            if let Some(page) = r {
                return Ok(Some(PageNumber::new(
                    candidate_region,
                    page,
                    required_order,
                )));
            }
            // Mark the region, if it's full
            state
                .get_region_tracker_mut()
                .mark_full(required_order, candidate_region);
        }
    }

    fn try_shrink(state: &mut InMemoryState, force: bool) -> Result<bool> {
        let layout = state.header.layout();
        let last_region_index = layout.num_regions() - 1;
        let last_allocator = state.get_region(last_region_index);
        let trailing_free = last_allocator.trailing_free_pages();
        let last_allocator_len = last_allocator.len();
        if trailing_free == 0 {
            return Ok(false);
        }
        if trailing_free < last_allocator_len / 2 && !force {
            return Ok(false);
        }
        let reduce_by = if layout.num_regions() > 1 && trailing_free == last_allocator_len {
            trailing_free
        } else if force {
            // Do not shrink the database to zero size
            min(last_allocator_len - 1, trailing_free)
        } else {
            trailing_free / 2
        };

        let mut new_layout = layout;
        new_layout.reduce_last_region(reduce_by);
        state.allocators_mut().resize_to(new_layout);
        assert!(new_layout.len() <= layout.len());
        state.header.set_layout(new_layout);

        Ok(true)
    }

    fn grow(&self, state: &mut InMemoryState, required_order_allocation: u8) -> Result<()> {
        let layout = state.header.layout();
        let required_growth =
            2u64.pow(required_order_allocation.into()) * u64::from(state.header.page_size());
        let max_region_size = u64::from(state.header.layout().full_region_layout().num_pages())
            * u64::from(state.header.page_size());
        let next_desired_size = if layout.num_full_regions() > 0 {
            if let Some(trailing) = layout.trailing_region_layout() {
                if 2 * required_growth < max_region_size - trailing.usable_bytes() {
                    // Fill out the trailing region
                    layout.usable_bytes() + (max_region_size - trailing.usable_bytes())
                } else {
                    // Fill out trailing & Grow by 1 region
                    layout.usable_bytes() + 2 * max_region_size - trailing.usable_bytes()
                }
            } else {
                // Grow by 1 region
                layout.usable_bytes() + max_region_size
            }
        } else {
            max(
                layout.usable_bytes() * 2,
                layout.usable_bytes() + required_growth * 2,
            )
        };
        let new_layout = DatabaseLayout::calculate(
            next_desired_size,
            state.header.layout().full_region_layout().num_pages(),
            state
                .header
                .layout()
                .full_region_layout()
                .get_header_pages(),
            self.page_size,
        );
        assert!(new_layout.len() >= layout.len());

        self.storage.resize(new_layout.len())?;

        state.allocators_mut().resize_to(new_layout);
        state.header.set_layout(new_layout);
        Ok(())
    }

    fn allocate<'txn>(
        &self,
        allocation_size: usize,
        allocated: &mut PageTrackerPolicy,
    ) -> Result<PageMut<'txn>> {
        let result = self.allocate_helper(allocation_size, false);
        if let Ok(ref page) = result {
            allocated.insert(page.get_page_number());
        }
        result
    }

    fn allocate_lowest<'txn>(
        &self,
        allocation_size: usize,
        allocated: &mut PageTrackerPolicy,
    ) -> Result<PageMut<'txn>> {
        let result = self.allocate_helper(allocation_size, true);
        if let Ok(ref page) = result {
            allocated.insert(page.get_page_number());
        }
        result
    }

    pub(crate) fn count_allocated_pages(&self) -> Result<u64> {
        let state = self.state.lock().unwrap();
        let mut count = 0u64;
        for i in 0..state.header.layout().num_regions() {
            count += u64::from(state.get_region(i).count_allocated_pages());
        }

        Ok(count)
    }

    pub(crate) fn count_free_pages(&self) -> Result<u64> {
        let state = self.state.lock().unwrap();
        let mut count = 0u64;
        for i in 0..state.header.layout().num_regions() {
            count += u64::from(state.get_region(i).count_free_pages());
        }

        Ok(count)
    }

    pub(crate) fn get_page_size(&self) -> usize {
        self.page_size.try_into().unwrap()
    }

    pub(crate) fn close(&self) -> Result {
        if self.storage.check_io_errors().is_ok() && !thread::panicking() {
            let mut state = self.state.lock()?;
            if self.storage.flush().is_ok() {
                state.header.recovery_required = false;
                self.write_header(&state.header)?;
                self.storage.flush()?;
            }
        }

        self.storage.close()?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::page_manager::INITIAL_REGIONS;
    use crate::{Database, TableDefinition};

    // Test that the region tracker expansion code works, by adding more data than fits into the initial max regions
    #[test]
    fn out_of_regions() {
        let tmpfile = crate::create_tempfile();
        let table_definition: TableDefinition<u32, &[u8]> = TableDefinition::new("x");
        let page_size = 1024;
        let big_value = vec![0u8; 5 * page_size];

        let db = Database::builder()
            .set_region_size((8 * page_size).try_into().unwrap())
            .set_page_size(page_size)
            .create(tmpfile.path())
            .unwrap();

        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(table_definition).unwrap();
            for i in 0..=INITIAL_REGIONS {
                table.insert(&i, big_value.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        drop(db);

        let mut db = Database::builder()
            .set_region_size((8 * page_size).try_into().unwrap())
            .set_page_size(page_size)
            .open(tmpfile.path())
            .unwrap();
        assert!(db.check_integrity().unwrap());
    }

    // Make sure the database remains consistent after a panic
    #[test]
    #[cfg(panic = "unwind")]
    fn panic() {
        let tmpfile = crate::create_tempfile();
        let table_definition: TableDefinition<u32, &[u8]> = TableDefinition::new("x");

        let _ = std::panic::catch_unwind(|| {
            let db = Database::create(&tmpfile).unwrap();
            let txn = db.begin_write().unwrap();
            txn.open_table(table_definition).unwrap();
            panic!();
        });

        let mut db = Database::open(tmpfile).unwrap();
        assert!(db.check_integrity().unwrap());
    }
}
