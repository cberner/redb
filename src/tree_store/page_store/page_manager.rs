use crate::transaction_tracker::TransactionId;
use crate::transactions::{AllocatorStateKey, AllocatorStateTree};
use crate::tree_store::btree_base::{BtreeHeader, Checksum};
use crate::tree_store::page_store::base::{PageHint, MAX_PAGE_INDEX};
use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::cached_file::PagedCachedFile;
use crate::tree_store::page_store::header::{DatabaseHeader, DB_HEADER_SIZE, MAGICNUMBER};
use crate::tree_store::page_store::layout::DatabaseLayout;
use crate::tree_store::page_store::region::{Allocators, RegionTracker};
use crate::tree_store::page_store::{hash128_with_seed, PageImpl, PageMut};
use crate::tree_store::{Page, PageNumber};
use crate::StorageBackend;
use crate::{DatabaseError, Result, StorageError};
#[cfg(feature = "logging")]
use log::warn;
use std::cmp::{max, min};
#[cfg(debug_assertions)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(debug_assertions)]
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

// Regions have a maximum size of 4GiB. A `4GiB - overhead` value is the largest that can be represented,
// because the leaf node format uses 32bit offsets
const MAX_USABLE_REGION_SPACE: u64 = 4 * 1024 * 1024 * 1024;
// TODO: remove this constant?
pub(crate) const MAX_MAX_PAGE_ORDER: u8 = 20;
pub(super) const MIN_USABLE_PAGES: u32 = 10;
const MIN_DESIRED_USABLE_BYTES: u64 = 1024 * 1024;

pub(super) const INITIAL_REGIONS: u32 = 1000; // Enough for a 4TiB database

// Original file format. No lengths stored with btrees
pub(crate) const FILE_FORMAT_VERSION1: u8 = 1;
// New file format. All btrees have a separate length stored in their header for constant time access
pub(crate) const FILE_FORMAT_VERSION2: u8 = 2;

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
    allocators: Allocators,
}

impl InMemoryState {
    fn from_bytes(header: DatabaseHeader, file: &PagedCachedFile) -> Result<Self> {
        let allocators = if header.recovery_required {
            Allocators::new(header.layout())
        } else {
            Allocators::from_bytes(&header, file)?
        };
        Ok(Self { header, allocators })
    }

    fn get_region(&self, region: u32) -> &BuddyAllocator {
        &self.allocators.region_allocators[region as usize]
    }

    fn get_region_mut(&mut self, region: u32) -> &mut BuddyAllocator {
        &mut self.allocators.region_allocators[region as usize]
    }

    fn get_region_tracker_mut(&mut self) -> &mut RegionTracker {
        &mut self.allocators.region_tracker
    }
}

pub(crate) struct TransactionalMemory {
    // Pages allocated since the last commit
    // TODO: maybe this should be moved to WriteTransaction?
    allocated_since_commit: Mutex<HashSet<PageNumber>>,
    // True if the allocator state was corrupted when the file was opened
    // TODO: maybe we can remove this flag now that CheckedBackend exists?
    needs_recovery: AtomicBool,
    storage: PagedCachedFile,
    state: Mutex<InMemoryState>,
    // The number of PageMut which are outstanding
    #[cfg(debug_assertions)]
    open_dirty_pages: Arc<Mutex<HashSet<PageNumber>>>,
    // Reference counts of PageImpls that are outstanding
    #[cfg(debug_assertions)]
    read_page_ref_counts: Arc<Mutex<HashMap<PageNumber, u64>>>,
    // Indicates that a non-durable commit has been made, so reads should be served from the secondary meta page
    read_from_secondary: AtomicBool,
    page_size: u32,
    // We store these separately from the layout because they're static, and accessed on the get_page()
    // code path where there is no locking
    region_size: u64,
    region_header_with_padding_size: u64,
}

impl TransactionalMemory {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        file: Box<dyn StorageBackend>,
        page_size: usize,
        requested_region_size: Option<u64>,
        read_cache_size_bytes: usize,
        write_cache_size_bytes: usize,
    ) -> Result<Self, DatabaseError> {
        assert!(page_size.is_power_of_two() && page_size >= DB_HEADER_SIZE);

        let region_size = requested_region_size.unwrap_or(MAX_USABLE_REGION_SPACE);
        let region_size = min(
            region_size,
            (u64::from(MAX_PAGE_INDEX) + 1) * page_size as u64,
        );
        assert!(region_size.is_power_of_two());

        let storage = PagedCachedFile::new(
            file,
            page_size as u64,
            read_cache_size_bytes,
            write_cache_size_bytes,
        )?;

        let magic_number: [u8; MAGICNUMBER.len()] =
            if storage.raw_file_len()? >= MAGICNUMBER.len() as u64 {
                storage
                    .read_direct(0, MAGICNUMBER.len())?
                    .try_into()
                    .unwrap()
            } else {
                [0; MAGICNUMBER.len()]
            };

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
                (page_size * ((region_tracker_required_bytes + page_size - 1) / page_size)) as u64;
            let starting_size = size + tracker_space;

            let layout = DatabaseLayout::calculate(
                starting_size,
                (region_size / u64::try_from(page_size).unwrap())
                    .try_into()
                    .unwrap(),
                page_size.try_into().unwrap(),
            );

            {
                let file_len = storage.raw_file_len()?;

                if file_len < layout.len() {
                    storage.resize(layout.len())?;
                }
            }

            let mut allocators = Allocators::new(layout);

            // Allocate the region tracker in the zeroth region
            let tracker_page = {
                let tracker_required_pages =
                    (allocators.region_tracker.to_vec().len() + page_size - 1) / page_size;
                let required_order = ceil_log2(tracker_required_pages);
                let page_number = allocators.region_allocators[0]
                    .alloc(required_order)
                    .unwrap();
                PageNumber::new(0, page_number, required_order)
            };

            let mut header = DatabaseHeader::new(
                layout,
                TransactionId::new(0),
                FILE_FORMAT_VERSION2,
                tracker_page,
            );

            header.recovery_required = false;
            header.two_phase_commit = true;
            storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(false));
            allocators.flush_to(tracker_page, layout, &storage)?;

            storage.flush(false)?;
            // Write the magic number only after the data structure is initialized and written to disk
            // to ensure that it's crash safe
            storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(true));
            storage.flush(false)?;
        }
        let header_bytes = storage.read_direct(0, DB_HEADER_SIZE)?;
        let (mut header, repair_info) = DatabaseHeader::from_bytes(&header_bytes)?;

        assert_eq!(header.page_size() as usize, page_size);
        assert!(storage.raw_file_len()? >= header.layout().len());
        let needs_recovery =
            header.recovery_required || header.layout().len() != storage.raw_file_len()?;
        if needs_recovery {
            let layout = header.layout();
            let region_max_pages = layout.full_region_layout().num_pages();
            let region_header_pages = layout.full_region_layout().get_header_pages();
            header.set_layout(DatabaseLayout::recalculate(
                storage.raw_file_len()?,
                region_header_pages,
                region_max_pages,
                page_size.try_into().unwrap(),
            ));
            header.pick_primary_for_repair(repair_info)?;
            assert!(!repair_info.invalid_magic_number);
            storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(true));
            storage.flush(false)?;
        }

        let layout = header.layout();
        assert_eq!(layout.len(), storage.raw_file_len()?);
        let region_size = layout.full_region_layout().len();
        let region_header_size = layout.full_region_layout().data_section().start;

        let state = InMemoryState::from_bytes(header, &storage)?;

        assert!(page_size >= DB_HEADER_SIZE);

        Ok(Self {
            allocated_since_commit: Mutex::new(HashSet::new()),
            needs_recovery: AtomicBool::new(needs_recovery),
            storage,
            state: Mutex::new(state),
            #[cfg(debug_assertions)]
            open_dirty_pages: Arc::new(Mutex::new(HashSet::new())),
            #[cfg(debug_assertions)]
            read_page_ref_counts: Arc::new(Mutex::new(HashMap::new())),
            read_from_secondary: AtomicBool::new(false),
            page_size: page_size.try_into().unwrap(),
            region_size,
            region_header_with_padding_size: region_header_size,
        })
    }

    pub(crate) fn check_io_errors(&self) -> Result {
        self.storage.check_io_errors()
    }

    #[cfg(any(test, fuzzing))]
    pub(crate) fn all_allocated_pages(&self) -> Vec<PageNumber> {
        self.state.lock().unwrap().allocators.all_allocated()
    }

    #[cfg(any(test, fuzzing))]
    pub(crate) fn tracker_page(&self) -> PageNumber {
        self.state.lock().unwrap().header.region_tracker()
    }

    pub(crate) fn clear_read_cache(&self) {
        self.storage.invalidate_cache_all();
    }

    pub(crate) fn clear_cache_and_reload(&mut self) -> Result<bool, DatabaseError> {
        assert!(self.allocated_since_commit.lock().unwrap().is_empty());

        self.storage.flush(false)?;
        self.storage.invalidate_cache_all();

        let header_bytes = self.storage.read_direct(0, DB_HEADER_SIZE)?;
        let (mut header, repair_info) = DatabaseHeader::from_bytes(&header_bytes)?;
        // TODO: This ends up always being true because this is called from check_integrity() once the db is already open
        // TODO: Also we should recheck the layout
        let mut was_clean = true;
        if header.recovery_required {
            if !header.pick_primary_for_repair(repair_info)? {
                was_clean = false;
            }
            if repair_info.invalid_magic_number {
                return Err(StorageError::Corrupted("Invalid magic number".to_string()).into());
            }
            self.storage
                .write(0, DB_HEADER_SIZE, true)?
                .mem_mut()
                .copy_from_slice(&header.to_bytes(true));
            self.storage.flush(false)?;
        }

        self.needs_recovery
            .store(header.recovery_required, Ordering::Release);
        self.state.lock().unwrap().header = header;

        Ok(was_clean)
    }

    pub(crate) fn begin_writable(&self) -> Result {
        let mut state = self.state.lock().unwrap();
        assert!(!state.header.recovery_required);
        state.header.recovery_required = true;
        self.write_header(&state.header)?;
        self.storage.flush(false)
    }

    pub(crate) fn needs_repair(&self) -> Result<bool> {
        Ok(self.state.lock().unwrap().header.recovery_required)
    }

    pub(crate) fn used_two_phase_commit(&self) -> bool {
        self.state.lock().unwrap().header.two_phase_commit
    }

    pub(crate) fn allocator_hash(&self) -> u128 {
        self.state.lock().unwrap().allocators.xxh3_hash()
    }

    // TODO: need a clearer distinction between this and needs_repair()
    pub(crate) fn storage_failure(&self) -> bool {
        self.needs_recovery.load(Ordering::Acquire)
    }

    pub(crate) fn repair_primary_corrupted(&self) {
        let mut state = self.state.lock().unwrap();
        state.header.swap_primary_slot();
    }

    pub(crate) fn begin_repair(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.allocators = Allocators::new(state.header.layout());

        Ok(())
    }

    pub(crate) fn mark_page_allocated(&self, page_number: PageNumber) {
        let mut state = self.state.lock().unwrap();
        let region_index = page_number.region;
        let allocator = state.get_region_mut(region_index);
        allocator.record_alloc(page_number.page_index, page_number.page_order);
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
        let tracker_len = state.allocators.region_tracker.to_vec().len();
        let tracker_page = state.header.region_tracker();

        let allocator = state.get_region_mut(tracker_page.region);
        // Allocate a new tracker page, if the old one was overwritten or is too small
        if allocator.is_allocated(tracker_page.page_index, tracker_page.page_order)
            || tracker_page.page_size_bytes(self.page_size) < tracker_len as u64
        {
            drop(state);
            let new_tracker_page = self.allocate(tracker_len)?.get_page_number();

            let mut state = self.state.lock().unwrap();
            state.header.set_region_tracker(new_tracker_page);
            self.write_header(&state.header)?;
            self.storage.flush(false)?;
        } else {
            allocator.record_alloc(tracker_page.page_index, tracker_page.page_order);
            drop(state);
        }

        let mut state = self.state.lock().unwrap();
        let tracker_page = state.header.region_tracker();
        state
            .allocators
            .flush_to(tracker_page, state.header.layout(), &self.storage)?;

        state.header.recovery_required = false;
        self.write_header(&state.header)?;
        let result = self.storage.flush(false);
        self.needs_recovery.store(false, Ordering::Release);

        result
    }

    pub(crate) fn reserve_allocator_state(
        &self,
        tree: &mut AllocatorStateTree,
        transaction_id: TransactionId,
    ) -> Result<u32> {
        let state = self.state.lock().unwrap();
        let layout = state.header.layout();
        let num_regions = layout.num_regions();
        let region_header_len = layout.full_region_layout().get_header_pages()
            * layout.full_region_layout().page_size();
        let region_tracker_len = state.allocators.region_tracker.to_vec().len();
        drop(state);

        for i in 0..num_regions {
            tree.insert(
                &AllocatorStateKey::Region(i),
                &vec![0; region_header_len as usize].as_ref(),
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
        tree: &mut AllocatorStateTree,
        num_regions: u32,
    ) -> Result<bool> {
        // Has the number of regions changed since reserve_allocator_state() was called?
        if num_regions != self.state.lock().unwrap().header.layout().num_regions() {
            return Ok(false);
        }

        for i in 0..num_regions {
            let region_bytes =
                &self.state.lock().unwrap().allocators.region_allocators[i as usize].to_vec();
            tree.insert_inplace(&AllocatorStateKey::Region(i), &region_bytes.as_ref())?;
        }

        let region_tracker_bytes = self
            .state
            .lock()
            .unwrap()
            .allocators
            .region_tracker
            .to_vec();
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
        let transaction_id = TransactionId::new(u64::from_le_bytes(
            tree.get(&AllocatorStateKey::TransactionId)?
                .unwrap()
                .value()
                .try_into()
                .unwrap(),
        ));

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

        let region_tracker = RegionTracker::from_page(
            tree.get(&AllocatorStateKey::RegionTracker)?
                .unwrap()
                .value(),
        );

        let mut state = self.state.lock().unwrap();
        state.allocators = Allocators {
            region_tracker,
            region_allocators,
        };

        // Resize the allocators to match the current file size
        let layout = state.header.layout();
        state.allocators.resize_to(layout);
        drop(state);

        // Allocate a larger region tracker page if necessary. This also happens automatically on
        // shutdown, but we do it here because we want our allocator state to exactly match the
        // result of running a full repair
        self.ensure_region_tracker_page()?;

        self.state.lock().unwrap().header.recovery_required = false;
        self.needs_recovery.store(false, Ordering::Release);

        Ok(())
    }

    pub(crate) fn is_allocated(&self, page: PageNumber) -> bool {
        let state = self.state.lock().unwrap();
        let allocator = state.get_region(page.region);

        allocator.is_allocated(page.page_index, page.page_order)
    }

    // Make sure we have a large enough region-tracker page. This uses allocate_non_transactional(),
    // so it should only be called from outside a transaction
    fn ensure_region_tracker_page(&self) -> Result {
        let state = self.state.lock().unwrap();
        let tracker_len = state.allocators.region_tracker.to_vec().len();
        let mut tracker_page = state.header.region_tracker();
        drop(state);

        if tracker_page.page_size_bytes(self.page_size) < (tracker_len as u64) {
            // Allocate a larger tracker page
            self.free(tracker_page);
            tracker_page = self
                .allocate_non_transactional(tracker_len)?
                .get_page_number();
            let mut state = self.state.lock().unwrap();
            state.header.set_region_tracker(tracker_page);
        }

        Ok(())
    }

    // Relocates the region tracker to a lower page, if possible
    // Returns true if the page was moved
    pub(crate) fn relocate_region_tracker(&self) -> Result<bool> {
        let state = self.state.lock().unwrap();
        let region_tracker_size = state
            .header
            .region_tracker()
            .page_size_bytes(self.page_size);
        let old_tracker_page = state.header.region_tracker();
        // allocate acquires this lock, so we need to drop it
        drop(state);
        // Allocate the new page. Unlike other region-tracker allocations, this happens inside
        // a transaction, so we use an ordinary allocation (which gets committed or rolled back
        // along with the rest of the transaction) rather than allocate_non_transactional()
        let new_page = self.allocate_lowest(region_tracker_size.try_into().unwrap())?;
        if new_page.get_page_number().is_before(old_tracker_page) {
            let mut state = self.state.lock().unwrap();
            state.header.set_region_tracker(new_page.get_page_number());
            drop(state);
            self.free(old_tracker_page);
            Ok(true)
        } else {
            let new_page_number = new_page.get_page_number();
            drop(new_page);
            self.free(new_page_number);
            Ok(false)
        }
    }

    pub(crate) fn get_raw_allocator_states(&self) -> Vec<Vec<u8>> {
        let state = self.state.lock().unwrap();

        let mut regional_allocators = vec![];
        for i in 0..state.header.layout().num_regions() {
            regional_allocators.push(state.get_region(i).make_state_for_savepoint());
        }

        regional_allocators
    }

    // Commit all outstanding changes and make them visible as the primary
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit(
        &self,
        data_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
        freed_root: Option<BtreeHeader>,
        transaction_id: TransactionId,
        eventual: bool,
        two_phase: bool,
    ) -> Result {
        let result = self.commit_inner(
            data_root,
            system_root,
            freed_root,
            transaction_id,
            eventual,
            two_phase,
        );
        if result.is_err() {
            self.needs_recovery.store(true, Ordering::Release);
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_inner(
        &self,
        data_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
        freed_root: Option<BtreeHeader>,
        transaction_id: TransactionId,
        eventual: bool,
        two_phase: bool,
    ) -> Result {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        #[cfg(debug_assertions)]
        debug_assert!(self.open_dirty_pages.lock().unwrap().is_empty());
        assert!(!self.needs_recovery.load(Ordering::Acquire));

        let mut state = self.state.lock().unwrap();
        // Trim surplus file space, before finalizing the commit
        let shrunk = Self::try_shrink(&mut state)?;
        // Copy the header so that we can release the state lock, while we flush the file
        let mut header = state.header.clone();
        drop(state);

        let old_transaction_id = header.secondary_slot().transaction_id;
        let secondary = header.secondary_slot_mut();
        secondary.transaction_id = transaction_id;
        secondary.user_root = data_root;
        secondary.system_root = system_root;
        secondary.freed_root = freed_root;

        self.write_header(&header)?;

        // Use 2-phase commit, if checksums are disabled
        if two_phase {
            self.storage.flush(eventual)?;
        }

        // Make our new commit the primary, and record whether it was a 2-phase commit.
        // These two bits need to be written atomically
        header.swap_primary_slot();
        header.two_phase_commit = two_phase;

        // Write the new header to disk
        self.write_header(&header)?;
        self.storage.flush(eventual)?;

        if shrunk {
            let result = self.storage.resize(header.layout().len());
            if result.is_err() {
                // TODO: it would be nice to have a more cohesive approach to setting this.
                // we do it in commit() & rollback() on failure, but there are probably other places that need it
                self.needs_recovery.store(true, Ordering::Release);
                return result;
            }
        }
        self.allocated_since_commit.lock().unwrap().clear();

        let mut state = self.state.lock().unwrap();
        assert_eq!(
            state.header.secondary_slot().transaction_id,
            old_transaction_id
        );
        state.header = header;
        self.read_from_secondary.store(false, Ordering::Release);
        // Hold lock until read_from_secondary is set to false, so that the new primary state is read.
        // TODO: maybe we can remove the whole read_from_secondary flag?
        drop(state);

        Ok(())
    }

    // Make changes visible, without a durability guarantee
    pub(crate) fn non_durable_commit(
        &self,
        data_root: Option<BtreeHeader>,
        system_root: Option<BtreeHeader>,
        freed_root: Option<BtreeHeader>,
        transaction_id: TransactionId,
    ) -> Result {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        #[cfg(debug_assertions)]
        debug_assert!(self.open_dirty_pages.lock().unwrap().is_empty());
        assert!(!self.needs_recovery.load(Ordering::Acquire));

        self.allocated_since_commit.lock().unwrap().clear();
        self.storage.write_barrier()?;

        let mut state = self.state.lock().unwrap();
        let secondary = state.header.secondary_slot_mut();
        secondary.transaction_id = transaction_id;
        secondary.user_root = data_root;
        secondary.system_root = system_root;
        secondary.freed_root = freed_root;

        // TODO: maybe we can remove this flag and just update the in-memory DatabaseHeader state?
        self.read_from_secondary.store(true, Ordering::Release);

        Ok(())
    }

    pub(crate) fn rollback_uncommitted_writes(&self) -> Result {
        let result = self.rollback_uncommitted_writes_inner();
        if result.is_err() {
            self.needs_recovery.store(true, Ordering::Release);
        }
        result
    }

    fn rollback_uncommitted_writes_inner(&self) -> Result {
        #[cfg(debug_assertions)]
        {
            let dirty_pages = self.open_dirty_pages.lock().unwrap();
            debug_assert!(
                dirty_pages.is_empty(),
                "Dirty pages outstanding: {dirty_pages:?}"
            );
        }
        assert!(!self.needs_recovery.load(Ordering::Acquire));
        let mut state = self.state.lock().unwrap();
        let mut guard = self.allocated_since_commit.lock().unwrap();
        for page_number in guard.iter() {
            let region_index = page_number.region;
            state
                .get_region_tracker_mut()
                .mark_free(page_number.page_order, region_index);
            state
                .get_region_mut(region_index)
                .free(page_number.page_index, page_number.page_order);

            let address = page_number.address_range(
                self.page_size.into(),
                self.region_size,
                self.region_header_with_padding_size,
                self.page_size,
            );
            let len: usize = (address.end - address.start).try_into().unwrap();
            self.storage.invalidate_cache(address.start, len);
            self.storage.cancel_pending_write(address.start, len);
        }
        guard.clear();

        Ok(())
    }

    // TODO: make all callers explicitly provide a hint
    pub(crate) fn get_page(&self, page_number: PageNumber) -> Result<PageImpl> {
        self.get_page_extended(page_number, PageHint::None)
    }

    pub(crate) fn get_page_extended(
        &self,
        page_number: PageNumber,
        hint: PageHint,
    ) -> Result<PageImpl> {
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
    pub(crate) fn get_page_mut(&self, page_number: PageNumber) -> Result<PageMut> {
        #[cfg(debug_assertions)]
        {
            assert!(!self
                .read_page_ref_counts
                .lock()
                .unwrap()
                .contains_key(&page_number));
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
            #[cfg(debug_assertions)]
            open_pages: self.open_dirty_pages.clone(),
        })
    }

    pub(crate) fn get_version(&self) -> u8 {
        let state = self.state.lock().unwrap();
        if self.read_from_secondary.load(Ordering::Acquire) {
            state.header.secondary_slot().version
        } else {
            state.header.primary_slot().version
        }
    }

    pub(crate) fn get_data_root(&self) -> Option<BtreeHeader> {
        let state = self.state.lock().unwrap();
        if self.read_from_secondary.load(Ordering::Acquire) {
            state.header.secondary_slot().user_root
        } else {
            state.header.primary_slot().user_root
        }
    }

    pub(crate) fn get_system_root(&self) -> Option<BtreeHeader> {
        let state = self.state.lock().unwrap();
        if self.read_from_secondary.load(Ordering::Acquire) {
            state.header.secondary_slot().system_root
        } else {
            state.header.primary_slot().system_root
        }
    }

    pub(crate) fn get_freed_root(&self) -> Option<BtreeHeader> {
        let state = self.state.lock().unwrap();
        if self.read_from_secondary.load(Ordering::Acquire) {
            state.header.secondary_slot().freed_root
        } else {
            state.header.primary_slot().freed_root
        }
    }

    pub(crate) fn get_layout(&self) -> DatabaseLayout {
        self.state.lock().unwrap().header.layout()
    }

    pub(crate) fn get_last_committed_transaction_id(&self) -> Result<TransactionId> {
        let state = self.state.lock().unwrap();
        if self.read_from_secondary.load(Ordering::Acquire) {
            Ok(state.header.secondary_slot().transaction_id)
        } else {
            Ok(state.header.primary_slot().transaction_id)
        }
    }

    pub(crate) fn free(&self, page: PageNumber) {
        self.allocated_since_commit.lock().unwrap().remove(&page);
        self.free_helper(page);
    }

    fn free_helper(&self, page: PageNumber) {
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

    // Frees the page if it was allocated since the last commit. Returns true, if the page was freed
    pub(crate) fn free_if_uncommitted(&self, page: PageNumber) -> bool {
        if self.allocated_since_commit.lock().unwrap().remove(&page) {
            self.free_helper(page);
            true
        } else {
            false
        }
    }

    // Page has not been committed
    pub(crate) fn uncommitted(&self, page: PageNumber) -> bool {
        self.allocated_since_commit.lock().unwrap().contains(&page)
    }

    pub(crate) fn allocate_helper(
        &self,
        allocation_size: usize,
        lowest: bool,
        transactional: bool,
    ) -> Result<PageMut> {
        let required_pages = (allocation_size + self.get_page_size() - 1) / self.get_page_size();
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

        if transactional {
            self.allocated_since_commit
                .lock()
                .unwrap()
                .insert(page_number);
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

    fn try_shrink(state: &mut InMemoryState) -> Result<bool> {
        let layout = state.header.layout();
        let last_region_index = layout.num_regions() - 1;
        let last_allocator = state.get_region(last_region_index);
        let trailing_free = last_allocator.trailing_free_pages();
        let last_allocator_len = last_allocator.len();
        if trailing_free < last_allocator_len / 2 {
            return Ok(false);
        }
        let reduce_by = if layout.num_regions() > 1 && trailing_free == last_allocator_len {
            trailing_free
        } else {
            trailing_free / 2
        };

        let mut new_layout = layout;
        new_layout.reduce_last_region(reduce_by);
        state.allocators.resize_to(new_layout);
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
            self.page_size,
        );
        assert!(new_layout.len() >= layout.len());

        let result = self.storage.resize(new_layout.len());
        if result.is_err() {
            // TODO: it would be nice to have a more cohesive approach to setting this.
            // we do it in commit() & rollback() on failure, but there are probably other places that need it
            self.needs_recovery.store(true, Ordering::Release);
            return result;
        }

        state.allocators.resize_to(new_layout);
        state.header.set_layout(new_layout);
        Ok(())
    }

    pub(crate) fn allocate(&self, allocation_size: usize) -> Result<PageMut> {
        self.allocate_helper(allocation_size, false, true)
    }

    pub(crate) fn allocate_lowest(&self, allocation_size: usize) -> Result<PageMut> {
        self.allocate_helper(allocation_size, true, true)
    }

    // Allocate a page not associated with any transaction. The page is immediately considered committed,
    // and won't be rolled back if an abort happens. This is only used for the region tracker
    fn allocate_non_transactional(&self, allocation_size: usize) -> Result<PageMut> {
        self.allocate_helper(allocation_size, false, false)
    }

    pub(crate) fn count_allocated_pages(&self) -> Result<u64> {
        let state = self.state.lock().unwrap();
        let mut count = 0u64;
        for i in 0..state.header.layout().num_regions() {
            count += u64::from(state.get_region(i).count_allocated_pages());
        }

        Ok(count)
    }

    pub(crate) fn get_page_size(&self) -> usize {
        self.page_size.try_into().unwrap()
    }
}

impl Drop for TransactionalMemory {
    fn drop(&mut self) {
        if thread::panicking() {
            return;
        }

        // Allocate a larger region-tracker page if necessary
        if self.ensure_region_tracker_page().is_err() {
            #[cfg(feature = "logging")]
            warn!("Failure while flushing allocator state. Repair required at restart.");
            return;
        }

        let mut state = self.state.lock().unwrap();
        if state
            .allocators
            .flush_to(
                state.header.region_tracker(),
                state.header.layout(),
                &self.storage,
            )
            .is_err()
        {
            #[cfg(feature = "logging")]
            warn!("Failure while flushing allocator state. Repair required at restart.");
            return;
        }

        if self.storage.flush(false).is_ok() && !self.needs_recovery.load(Ordering::Acquire) {
            state.header.recovery_required = false;
            let _ = self.write_header(&state.header);
            let _ = self.storage.flush(false);
        }
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
