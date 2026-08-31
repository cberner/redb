use crate::CacheStats;
use crate::db::{
    ConcurrencyMode, FULL_RANGE, InternalStorageBackend, SHARED_WRITER_BYTE, byte_range,
};
#[cfg(feature = "experimental-multiprocess")]
use crate::db::{IMMUTABLE_READER_BYTE, SHARED_READER_BYTE, TXN_BASE, WRITER_BYTE};
use crate::io;
use crate::sync::Mutex;
use crate::transaction_tracker::TransactionId;
use crate::transactions::{AllocatorStateKey, AllocatorStateTree, AllocatorStateTreeMut};
use crate::tree_store::btree_base::{BtreeHeader, Checksum};
use crate::tree_store::page_store::base::{MAX_PAGE_INDEX, PageHint};
use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::cached_file::PagedCachedFile;
use crate::tree_store::page_store::fast_hash::{PageNumberHashMap, PageNumberHashSet, Shrink};
use crate::tree_store::page_store::header::{
    DB_HEADER_SIZE, DatabaseHeader, MAGICNUMBER, TransactionHeader, UnrepairedDatabaseHeader,
};
use crate::tree_store::page_store::layout::DatabaseLayout;
use crate::tree_store::page_store::region::{Allocators, RegionTracker};
use crate::tree_store::page_store::{PageImpl, PageMut, hash128_with_seed};
use crate::tree_store::{Page, PageNumber, PageTracker};
use crate::{DatabaseError, Result, StorageError};
use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::format;
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use core::cmp::{max, min};
use core::convert::TryInto;
use core::marker::PhantomData;
use core::mem;
#[cfg(feature = "experimental-multiprocess")]
use core::ops::Range;
use core::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "logging")]
use log::warn;

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

/// Read-only view over `TransactionalMemory` exposing only the methods that
/// btree read paths and stats helpers need. Cheap to clone -- a single
/// `Arc<TransactionalMemory>` bump.
///
/// Construct one from `PageAllocator::resolver()` (write-transaction context)
/// or `PageResolver::new(mem)` (read-transaction context). Read-only btree
/// types accept `PageResolver` rather than `Arc<TransactionalMemory>` so that
/// they cannot be used to bypass `PageAllocator`'s allocation tracking.
#[derive(Clone)]
pub(crate) struct PageResolver {
    mem: Arc<TransactionalMemory>,
}

impl PageResolver {
    pub(crate) fn new(mem: Arc<TransactionalMemory>) -> Self {
        Self { mem }
    }

    pub(crate) fn get_page(&self, page_number: PageNumber, hint: PageHint) -> Result<PageImpl> {
        self.mem.get_page(page_number, hint)
    }

    pub(crate) fn count_allocated_pages(&self) -> Result<u64> {
        self.mem.count_allocated_pages()
    }
}

// Shards for `UncommittedPages`, padded to a cache line: the contention being removed is cores
// handing one lock's line back and forth, worst between cores in different L3 domains, which
// shards sharing a line would reintroduce.
const UNCOMMITTED_SHARDS: usize = 64;

#[repr(align(64))]
struct UncommittedShard(Mutex<PageNumberHashSet>);

/// The pages a write transaction has allocated since its last commit, which `uncommitted()`
/// answers from and rollback frees. Every allocation, free and `uncommitted()` check consults it,
/// and it can never be switched off, so it is sharded by page to keep concurrent writers -- tables
/// of one transaction may be written from different threads -- off a single lock.
struct UncommittedPages {
    shards: Vec<UncommittedShard>,
}

impl UncommittedPages {
    fn new() -> Self {
        Self {
            shards: (0..UNCOMMITTED_SHARDS)
                .map(|_| UncommittedShard(Mutex::new(PageNumberHashSet::default())))
                .collect(),
        }
    }

    fn shard(&self, page: PageNumber) -> &Mutex<PageNumberHashSet> {
        &self.shards[page.page_index as usize % UNCOMMITTED_SHARDS].0
    }

    fn insert(&self, page: PageNumber) {
        assert!(self.shard(page).lock().unwrap().insert(page));
    }

    /// Removes `page` if present. Returns whether it was in the set.
    fn remove(&self, page: PageNumber) -> bool {
        self.shard(page).lock().unwrap().remove(&page)
    }

    fn contains(&self, page: PageNumber) -> bool {
        self.shard(page).lock().unwrap().contains(&page)
    }

    /// Drains every shard, returning all the pages recorded.
    fn take_all(&self) -> PageNumberHashSet {
        let mut result = PageNumberHashSet::default();
        for shard in &self.shards {
            result.extend(mem::take(&mut *shard.0.lock().unwrap()));
        }
        result
    }
}

/// Per-write-transaction handle through which btree mutation code allocates
/// and frees pages. Bundles the shared `TransactionalMemory` with the write
/// transaction's `AllocationPolicy`.
#[derive(Clone)]
pub(crate) struct PageAllocator {
    mem: Arc<TransactionalMemory>,
    policy: AllocationPolicy,
    allocated_since_commit: Arc<UncommittedPages>,
}

impl PageAllocator {
    pub(crate) fn new(mem: Arc<TransactionalMemory>, policy: AllocationPolicy) -> Self {
        Self {
            mem,
            policy,
            allocated_since_commit: Arc::new(UncommittedPages::new()),
        }
    }

    /// Returns a `PageResolver` for constructing read-only views of this transaction's pages.
    pub(crate) fn resolver(&self) -> PageResolver {
        PageResolver::new(self.mem.clone())
    }

    /// Drains the set of pages allocated since the last commit, returning
    /// them. Used by commit and non-durable commit paths to hand the set over
    /// to `TransactionalMemory`.
    pub(crate) fn take_allocated_since_commit(&self) -> PageNumberHashSet {
        self.allocated_since_commit.take_all()
    }

    // Takes ownership of pages an earlier transaction allocated, so this one can update them in
    // place. Only sound once this transaction can no longer abort, since rollback_all() frees
    // whatever is adopted. The page leaves the unpersisted set: it is an ordinary uncommitted page
    // from here, and the copy-on-write that may free it does not maintain that set.
    pub(crate) fn adopt_unpersisted(&self, pages: impl IntoIterator<Item = PageNumber>) {
        for page in pages {
            assert!(self.mem.claim_unpersisted(page));
            self.allocated_since_commit.insert(page);
        }
    }

    /// Reverses every allocation made since the last commit: drains the
    /// allocated-since-commit set and frees each page.
    pub(crate) fn rollback_all(&self) {
        self.mem.debug_assert_no_dirty_pages();
        let drained = self.take_allocated_since_commit();
        for page in &drained {
            self.mem.free(*page, &PageTracker::ignore());
        }
    }

    pub(crate) fn allocate<'a>(&self, size: usize, allocated: &PageTracker) -> Result<PageMut<'a>> {
        let page = match self.policy {
            AllocationPolicy::Default => self.mem.allocate(size, allocated)?,
            AllocationPolicy::Lowest => self.mem.allocate_lowest(size, allocated)?,
        };
        self.allocated_since_commit.insert(page.get_page_number());
        Ok(page)
    }

    // Always allocates at the lowest free page, ignoring `self.policy`. Used
    // by compaction's probe loop where the point is specifically to test
    // whether a page can land below its current position.
    pub(crate) fn allocate_lowest<'a>(
        &self,
        size: usize,
        allocated: &PageTracker,
    ) -> Result<PageMut<'a>> {
        let page = self.mem.allocate_lowest(size, allocated)?;
        self.allocated_since_commit.insert(page.get_page_number());
        Ok(page)
    }

    pub(crate) fn free(&self, page: PageNumber, allocated: &PageTracker) {
        self.allocated_since_commit.remove(page);
        self.mem.free(page, allocated);
    }

    pub(crate) fn free_if_uncommitted(&self, page: PageNumber, allocated: &PageTracker) -> bool {
        if self.allocated_since_commit.remove(page) {
            self.mem.free(page, allocated);
            true
        } else {
            false
        }
    }

    // Frees the page immediately if it was allocated in this transaction;
    // otherwise defers it to `freed` for release at commit.
    pub(crate) fn conditional_free(
        &self,
        page: PageNumber,
        allocated: &PageTracker,
        freed: &mut Vec<PageNumber>,
    ) {
        if !self.free_if_uncommitted(page, allocated) {
            freed.push(page);
        }
    }

    pub(crate) fn uncommitted(&self, page: PageNumber) -> bool {
        self.allocated_since_commit.contains(page)
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

/// What non-durable commits record in memory instead of in the file. All of it is volatile by
/// construction -- a crash rolls those commits back -- so it shares one lifecycle, and `clear()`
/// is the only way it is emptied: by the durable `commit()` that persists what it stands in for,
/// or the `clear_cache_and_reload()` that abandons it. It lives here because of that coupling,
/// and because reclaiming a page consults it and the allocator together.
#[derive(Default)]
struct UnpersistedState {
    // Pages allocated by non-durable commits. Still reclaimable while they are here, since no
    // durable commit references them.
    pages: PageNumberHashSet,
    // Data-tree pages allocated per transaction: the in-memory stand-in for DATA_ALLOCATED_TABLE,
    // kept in memory so that reclaiming a page can cheaply drop its record. Flushed to the table
    // by durable_commit().
    allocations: BTreeMap<TransactionId, PageNumberHashSet>,
    // Reverse index into `allocations`
    allocation_txn: PageNumberHashMap<TransactionId>,
    // Data-tree pages freed per transaction: the in-memory stand-in for DATA_FREED_TABLE. Held
    // here for the same reason as `allocations` -- the commits that freed them are volatile, so a
    // crash that loses the records also rolls back the commits they describe.
    data_freed: BTreeMap<TransactionId, Vec<PageNumber>>,
    // System-tree pages allocated by the post-commit epilogue, which the next durable commit may
    // take over once it is irreversible. Always a subset of `pages`, so a reused page number is
    // never mistaken for the allocation that held it before.
    post_commit_allocations: PageNumberHashSet,
}

impl UnpersistedState {
    fn clear(&mut self) {
        self.pages.clear();
        self.pages.shrink();
        self.allocations.clear();
        self.allocation_txn.clear();
        self.data_freed.clear();
        self.post_commit_allocations.clear();
    }

    fn contains(&self, page: PageNumber) -> bool {
        self.pages.contains(&page)
    }

    fn extend(&mut self, pages: PageNumberHashSet) {
        self.pages.extend(pages);
    }

    /// Claims `page` for reclamation, returning whether this call got it. The page and its
    /// allocation record are dropped together, so a claimed page leaves nothing to write out.
    fn claim(&mut self, page: PageNumber) -> bool {
        if !self.pages.remove(&page) {
            return false;
        }
        // Keeps the subset invariant; see the field.
        self.post_commit_allocations.remove(&page);
        if let Some(txn) = self.allocation_txn.remove(&page) {
            let pages = self
                .allocations
                .get_mut(&txn)
                .expect("allocation_txn points to a missing entry");
            let removed = pages.remove(&page);
            debug_assert!(removed);
            if pages.is_empty() {
                self.allocations.remove(&txn);
            }
        }
        true
    }

    fn record_allocations(
        &mut self,
        transaction_id: TransactionId,
        pages: impl IntoIterator<Item = PageNumber>,
    ) {
        let entry = self.allocations.entry(transaction_id).or_default();
        for page in pages {
            if entry.insert(page) {
                let prev = self.allocation_txn.insert(page, transaction_id);
                debug_assert!(prev.is_none(), "page {page:?} already tracked");
            }
        }
        if entry.is_empty() {
            self.allocations.remove(&transaction_id);
        }
    }

    fn take_allocations(&mut self) -> BTreeMap<TransactionId, PageNumberHashSet> {
        self.allocation_txn.clear();
        mem::take(&mut self.allocations)
    }

    /// The pages allocated by transactions after `transaction_id`, which a savepoint restore to
    /// that point has to queue for freeing.
    fn allocations_after(&self, transaction_id: TransactionId) -> Vec<PageNumber> {
        self.allocations
            .range(transaction_id.next()..)
            .flat_map(|(_, pages)| pages.iter().copied())
            .collect()
    }

    fn record_data_freed(&mut self, transaction_id: TransactionId, pages: Vec<PageNumber>) {
        if !pages.is_empty() {
            self.data_freed
                .entry(transaction_id)
                .or_default()
                .extend(pages);
        }
    }

    /// The records of transactions in `start..end`, copied out so that the caller can reclaim
    /// pages without holding this state locked.
    fn data_freed_in_range(
        &self,
        start: TransactionId,
        end: TransactionId,
    ) -> Vec<(TransactionId, Vec<PageNumber>)> {
        // The bounds are derived independently -- `start` from the oldest unprocessed non-durable
        // commit, `end` from the oldest live read -- so a live read older than that commit inverts
        // the range, which BTreeMap::range panics on.
        if start >= end {
            return vec![];
        }
        self.data_freed
            .range(start..end)
            .map(|(id, pages)| (*id, pages.clone()))
            .collect()
    }

    /// Replaces a transaction's record with the pages that were not reclaimed, dropping the
    /// record entirely when none are left.
    fn replace_data_freed(&mut self, transaction_id: TransactionId, pages: Vec<PageNumber>) {
        if pages.is_empty() {
            self.data_freed.remove(&transaction_id);
        } else {
            self.data_freed.insert(transaction_id, pages);
        }
    }

    fn take_data_freed(&mut self) -> BTreeMap<TransactionId, Vec<PageNumber>> {
        mem::take(&mut self.data_freed)
    }

    /// Drops the records of transactions after `transaction_id`, whose commits a savepoint restore
    /// has discarded.
    fn drop_data_freed_after(&mut self, transaction_id: TransactionId) {
        self.data_freed.split_off(&transaction_id.next());
    }

    /// The pages this state keeps allocated but unreachable from the roots: those freed by a
    /// non-durable commit and not yet released. An allocator rebuild has to mark them allocated,
    /// exactly as it does for the pages named by the on-disk freed tables.
    fn pages_pending_free(&self) -> Vec<PageNumber> {
        self.data_freed.values().flatten().copied().collect()
    }
}

/// The header bytes themselves rather than a byte standing for them, so that Windows' mandatory
/// locks hold back another process's read of them.
#[cfg(feature = "experimental-multiprocess")]
pub(crate) const HEADER_LOCK: Range<u64> = 0..DB_HEADER_SIZE as u64;

/// A hold on the header lock, released when it drops.
#[cfg(feature = "experimental-multiprocess")]
struct HeaderGuard<'a> {
    storage: Option<&'a PagedCachedFile>,
    _in_process: crate::sync::MutexGuard<'a, ()>,
}

#[cfg(feature = "experimental-multiprocess")]
impl Drop for HeaderGuard<'_> {
    fn drop(&mut self) {
        if let Some(storage) = self.storage {
            let _ = storage.unlock_range(HEADER_LOCK);
        }
    }
}

/// A hold on the writer byte, released when it drops. Owns its handle, unlike `HeaderGuard`,
/// since a write transaction outlives the call that takes it.
#[cfg(feature = "experimental-multiprocess")]
pub(crate) struct MultiProcessWriterGuard {
    mem: Arc<TransactionalMemory>,
}

#[cfg(feature = "experimental-multiprocess")]
impl Drop for MultiProcessWriterGuard {
    fn drop(&mut self) {
        let _ = self.mem.storage.unlock_range(byte_range(WRITER_BYTE));
    }
}

pub(crate) struct TransactionalMemory {
    unpersisted: Mutex<UnpersistedState>,
    storage: PagedCachedFile,
    state: Mutex<InMemoryState>,
    // The number of PageMut which are outstanding
    #[cfg(debug_assertions)]
    open_dirty_pages: Arc<Mutex<PageNumberHashSet>>,
    // Reference counts of PageImpls that are outstanding
    #[cfg(debug_assertions)]
    read_page_ref_counts: Arc<Mutex<PageNumberHashMap<u64>>>,
    // Set of all allocated pages for debugging assertions
    #[cfg(debug_assertions)]
    allocated_pages: Arc<Mutex<PageNumberHashSet>>,
    // While set, no allocator state is persisted and shutdowns are not recorded as clean,
    // forcing the next open to rebuild the state from the committed trees
    needs_repair: AtomicBool,
    #[cfg(feature = "experimental-multiprocess")]
    concurrency_mode: ConcurrencyMode,
    // Held for the duration of every header file lock. Ensures in-process threads do not overlap
    // acquiring the lock. Because it's an OFD lock on a single FD, only one may take it at a time.
    // TODO: This could probably be optimized, so that multiple threads can read at once, by making
    // this a counter or RwLock
    #[cfg(feature = "experimental-multiprocess")]
    in_process_header_lock: Mutex<()>,
    page_size: u32,
    // We store these separately from the layout because they're static, and accessed on the get_page()
    // code path where there is no locking
    region_size: u64,
    region_header_with_padding_size: u64,
}

impl TransactionalMemory {
    /// The locks a handle holds for as long as the database is open, taken before anything is
    /// read from the storage.
    fn lock_for_open(
        storage: &PagedCachedFile,
        read_only: bool,
        concurrency_mode: ConcurrencyMode,
    ) -> Result<(), DatabaseError> {
        match concurrency_mode {
            ConcurrencyMode::SingleProcess => Self::lock_whole_storage(storage, read_only),
            #[cfg(feature = "experimental-multiprocess")]
            mode => Self::lock_mode_bytes(storage, read_only, mode),
            // The shared modes are only reachable through the feature-gated setter
            #[cfg(not(feature = "experimental-multiprocess"))]
            _ => unreachable!(),
        }
    }

    fn lock_whole_storage(storage: &PagedCachedFile, read_only: bool) -> Result<(), DatabaseError> {
        // A caller-supplied backend has no locks, which is not a platform limitation
        if !storage.locks_expected() {
            return Ok(());
        }

        let result = if read_only {
            storage.try_lock_shared_range(FULL_RANGE)
        } else {
            storage.try_lock_range(FULL_RANGE)
        };

        match result {
            Ok(true) => {}
            Ok(false) => return Err(DatabaseError::DatabaseAlreadyOpen),
            Err(err) if io::is_unsupported(&err) => {
                #[cfg(feature = "logging")]
                warn!(
                    "File locks not supported on this platform. You must ensure that only a single process opens the database file, at a time"
                );

                return Ok(());
            }
            Err(err) => return Err(err.into()),
        }

        // A multi-writer cohort holds nothing a read-only open's shared locks conflict with, so
        // the open probes for one, which its own ranges leave uncovered for the purpose
        if read_only {
            match Self::locked_for_multi_process_writing(storage) {
                Ok(true) => return Err(DatabaseError::DatabaseAlreadyOpen),
                Ok(false) => {}
                // Without byte-range locks there is no multi-process handle to find
                Err(ref err) if io::is_unsupported(err) => {}
                Err(err) => return Err(err.into()),
            }
        }

        Ok(())
    }

    /// Whether a multi-writer cohort has the database open. Not gated on the feature that forms
    /// one: the cohort is another process, which may have been built with it when this was not.
    fn locked_for_multi_process_writing(storage: &PagedCachedFile) -> Result<bool, io::Error> {
        storage.query_lock_range(byte_range(SHARED_WRITER_BYTE))
    }

    #[cfg(feature = "experimental-multiprocess")]
    fn lock_mode_bytes(
        storage: &PagedCachedFile,
        read_only: bool,
        concurrency_mode: ConcurrencyMode,
    ) -> Result<(), DatabaseError> {
        let acquired = if read_only {
            storage.try_lock_shared_range(byte_range(SHARED_READER_BYTE))?
        } else {
            match concurrency_mode {
                ConcurrencyMode::SingleProcess => unreachable!(),
                ConcurrencyMode::SingleWriterProcess => {
                    storage.try_lock_range(byte_range(SHARED_WRITER_BYTE))?
                        && storage.try_lock_range(byte_range(WRITER_BYTE))?
                }
                ConcurrencyMode::MultiWriterProcess => {
                    storage.try_lock_shared_range(byte_range(SHARED_WRITER_BYTE))?
                        // Ensure we didn't race with an Immutable open of the database
                        && !storage.query_lock_range(byte_range(IMMUTABLE_READER_BYTE))?
                }
            }
        };

        if acquired {
            Ok(())
        } else {
            Err(DatabaseError::DatabaseAlreadyOpen)
        }
    }

    /// Takes the storage and the mutex rather than `&self`, so that the open path can hold the
    /// header before there is a `TransactionalMemory` to hold it from.
    #[cfg(feature = "experimental-multiprocess")]
    fn lock_header<'a>(
        storage: &'a PagedCachedFile,
        in_process: &'a Mutex<()>,
        concurrency_mode: ConcurrencyMode,
        exclusive: bool,
    ) -> Result<HeaderGuard<'a>> {
        let guard = in_process.lock().unwrap();
        if !concurrency_mode.is_multi_process_writable() {
            return Ok(HeaderGuard {
                storage: None,
                _in_process: guard,
            });
        }
        if exclusive {
            storage.lock_range(HEADER_LOCK)?;
        } else {
            storage.lock_shared_range(HEADER_LOCK)?;
        }

        Ok(HeaderGuard {
            storage: Some(storage),
            _in_process: guard,
        })
    }

    /// Acquire the multi-process writer lock
    #[cfg(feature = "experimental-multiprocess")]
    pub(crate) fn lock_multi_process_writer(
        mem: &Arc<Self>,
    ) -> Result<Option<MultiProcessWriterGuard>> {
        // The other modes settle who writes when the database is opened: a single-writer holds this byte
        // for its lifetime, and taking it again here would convert that hold and then drop it.
        if !matches!(mem.concurrency_mode, ConcurrencyMode::MultiWriterProcess) {
            return Ok(None);
        }
        mem.storage.lock_range(byte_range(WRITER_BYTE))?;

        Ok(Some(MultiProcessWriterGuard { mem: mem.clone() }))
    }

    #[cfg(feature = "experimental-multiprocess")]
    fn active_transaction_byte(id: TransactionId) -> Result<u64> {
        match TXN_BASE.checked_add(id.raw_id()) {
            Some(offset) if offset < 1 << 63 => Ok(offset),
            _ => Err(StorageError::Corrupted(format!(
                "transaction id {} is outside the multi-process lock range",
                id.raw_id()
            ))),
        }
    }

    /// Marks `id` active, keeping the pages its snapshot references from being reclaimed by any
    /// process until [`Self::unlock_mp_transaction`]. The shared header hold orders this against
    /// a writer's reclamation scan, which holds it exclusively: the scan either sees this lock
    /// or ran entirely before it.
    ///
    /// Caller must not attempt to re-lock an already locked transaction.
    ///
    /// The caller must guarantee that `id` cannot already have been collected.
    #[cfg(feature = "experimental-multiprocess")]
    pub(crate) fn lock_mp_transaction(&self, id: TransactionId) -> Result {
        // Nothing to publish to: a single-process writers lock the whole file
        if !self.concurrency_mode.is_multi_process_writable() {
            return Ok(());
        }
        let byte = Self::active_transaction_byte(id)?;
        let _guard = Self::lock_header(
            &self.storage,
            &self.in_process_header_lock,
            self.concurrency_mode,
            false,
        )?;
        // Refused only by an exclusive holder, which the shared header hold excludes
        if !self.storage.try_lock_shared_range(byte_range(byte))? {
            return Err(StorageError::Corrupted(
                "another process holds an active transaction byte exclusively".to_string(),
            ));
        }

        Ok(())
    }

    /// Releases the byte. Taken without the header lock: releasing only widens what a scanning
    /// writer may reclaim, and a scan that misses it reclaims less
    #[cfg(feature = "experimental-multiprocess")]
    pub(crate) fn unlock_mp_transaction(&self, id: TransactionId) -> Result {
        if !self.concurrency_mode.is_multi_process_writable() {
            return Ok(());
        }
        self.storage
            .unlock_range(byte_range(Self::active_transaction_byte(id)?))?;

        Ok(())
    }

    pub(crate) fn new(
        file: Box<dyn InternalStorageBackend>,
        // Allow initializing a new database in an empty file
        allow_initialize: bool,
        page_size: usize,
        requested_region_size: Option<u64>,
        cache_size: usize,
        read_only: bool,
        concurrency_mode: ConcurrencyMode,
    ) -> Result<Self, DatabaseError> {
        assert!(page_size.is_power_of_two() && page_size >= DB_HEADER_SIZE);

        let region_size = requested_region_size.unwrap_or(MAX_USABLE_REGION_SPACE);
        let region_size = min(
            region_size,
            (u64::from(MAX_PAGE_INDEX) + 1) * page_size as u64,
        );
        assert!(region_size.is_power_of_two());

        let storage = PagedCachedFile::new(file, page_size as u64, cache_size);
        // Dropping the storage releases whatever this took, so an open that fails below does too
        Self::lock_for_open(&storage, read_only, concurrency_mode)?;
        #[cfg(feature = "experimental-multiprocess")]
        let in_process_header_lock = Mutex::new(());

        let initial_storage_len = storage.raw_file_len()?;

        let magic_number: [u8; MAGICNUMBER.len()] =
            if initial_storage_len >= MAGICNUMBER.len() as u64 {
                #[cfg(feature = "experimental-multiprocess")]
                let _guard =
                    Self::lock_header(&storage, &in_process_header_lock, concurrency_mode, false)?;
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
                return Err(StorageError::Io(io::invalid_data(
                    "Not a redb database: magic number mismatch",
                ))
                .into());
            }
        } else {
            // File is empty, check that we're allowed to initialize a new database (i.e. the caller is Database::create() and not open())
            if !allow_initialize {
                return Err(StorageError::Io(io::invalid_data(
                    "Database file is empty and creating a new database was not requested",
                ))
                .into());
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
            {
                #[cfg(feature = "experimental-multiprocess")]
                let _guard =
                    Self::lock_header(&storage, &in_process_header_lock, concurrency_mode, true)?;
                storage
                    .write(0, DB_HEADER_SIZE, true)?
                    .mem_mut()
                    .copy_from_slice(&header.to_bytes(false));
                storage.flush()?;
            }
            // Write the magic number only after the data structure is initialized and written to disk
            // to ensure that it's crash safe
            {
                #[cfg(feature = "experimental-multiprocess")]
                let _guard =
                    Self::lock_header(&storage, &in_process_header_lock, concurrency_mode, true)?;
                storage
                    .write(0, DB_HEADER_SIZE, true)?
                    .mem_mut()
                    .copy_from_slice(&header.to_bytes(true));
                storage.flush()?;
            }
        }
        let header_bytes = {
            #[cfg(feature = "experimental-multiprocess")]
            let _guard =
                Self::lock_header(&storage, &in_process_header_lock, concurrency_mode, false)?;
            storage.read_direct(0, DB_HEADER_SIZE)?
        };
        let unrepaired =
            UnrepairedDatabaseHeader::from_bytes(&header_bytes, page_size.try_into().unwrap())?;
        let file_len = storage.raw_file_len()?;
        let needs_recovery = unrepaired.recovery_required(file_len);
        if needs_recovery && read_only {
            return Err(DatabaseError::RepairAborted);
        }
        let (header, _) = unrepaired.finalize(file_len)?;
        if needs_recovery {
            #[cfg(feature = "experimental-multiprocess")]
            let _guard =
                Self::lock_header(&storage, &in_process_header_lock, concurrency_mode, true)?;
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
            unpersisted: Mutex::new(UnpersistedState::default()),
            storage,
            state: Mutex::new(state),
            #[cfg(debug_assertions)]
            open_dirty_pages: Arc::new(Mutex::new(PageNumberHashSet::default())),
            #[cfg(debug_assertions)]
            read_page_ref_counts: Arc::new(Mutex::new(PageNumberHashMap::default())),
            #[cfg(debug_assertions)]
            allocated_pages: Arc::new(Mutex::new(PageNumberHashSet::default())),
            needs_repair: AtomicBool::new(false),
            #[cfg(feature = "experimental-multiprocess")]
            concurrency_mode,
            #[cfg(feature = "experimental-multiprocess")]
            in_process_header_lock,
            page_size: page_size.try_into().unwrap(),
            region_size,
            region_header_with_padding_size: region_header_size,
        })
    }

    // An order read from a corrupted file would otherwise size a multi-terabyte read buffer, whose
    // failed allocation aborts the process instead of returning an error.
    fn check_page_order(page: PageNumber) -> Result<()> {
        if page.page_order > MAX_MAX_PAGE_ORDER {
            return Err(StorageError::Corrupted(format!(
                "Page {page:?} has order greater than the maximum of {MAX_MAX_PAGE_ORDER}"
            )));
        }
        Ok(())
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
    #[cfg_attr(redb_no_std, expect(dead_code))]
    pub(crate) fn all_allocated_pages(&self) -> Vec<PageNumber> {
        self.allocated_pages
            .lock()
            .unwrap()
            .iter()
            .copied()
            .collect()
    }

    #[cfg(debug_assertions)]
    #[cfg_attr(redb_no_std, expect(dead_code))]
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
        // The in-memory state is being discarded for the on-disk state, so buffered writes --
        // which can only belong to the discarded state -- are dropped rather than written out;
        // after an external truncation, writing them could even fail beyond the end of the file.
        // Both caches are cleared before the fallible sync, so an early error return cannot
        // leave cached pages that disagree with the file.
        self.storage.discard_write_buffer();
        self.storage.invalidate_cache_all();
        self.storage.sync_file()?;

        let header_bytes = {
            #[cfg(feature = "experimental-multiprocess")]
            let _guard = Self::lock_header(
                &self.storage,
                &self.in_process_header_lock,
                self.concurrency_mode,
                false,
            )?;
            self.storage.read_direct(0, DB_HEADER_SIZE)?
        };
        let unrepaired = UnrepairedDatabaseHeader::from_bytes(&header_bytes, self.page_size)?;
        let (header, was_clean) = unrepaired.finalize(self.storage.raw_file_len()?)?;
        if !was_clean {
            self.write_header(&header)?;
            self.storage.flush()?;
        }

        {
            let mut state = self.state.lock().unwrap();
            state.header = header;
            state.read_from_secondary = false;
            // Drop the previous allocator state -- it described the layout that was in memory
            // before the reload. The caller is required to repopulate it (via reset_allocator_state or
            // load_allocator_state) before any allocation/free path runs.
            state.allocators = None;
        }
        // Reloading from disk discards in-memory roots, so drop volatile allocation state
        // that belonged only to those roots.
        self.unpersisted.lock().unwrap().clear();

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

    // Replaces the in-memory allocator state with a fresh, empty one sized to the current
    // layout. The caller is responsible for repopulating it by marking reachable pages allocated.
    pub(crate) fn reset_allocator_state(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        state.allocators = Some(Allocators::new(state.header.layout()));
        #[cfg(debug_assertions)]
        self.allocated_pages.lock().unwrap().clear();

        Ok(())
    }

    // Discards an allocator state that no longer describes the file. Callers that allocate or free
    // must check for one first, since those paths have no way to work without it.
    //
    // Runs during panic unwinding, so it must tolerate poisoned locks rather than double-panic.
    // The poison is deliberately left set: subsequent lock users fail rather than trusting state
    // touched by a panicking thread.
    pub(crate) fn invalidate_allocator_state(&self) {
        self.state
            .lock()
            .unwrap_or_else(crate::sync::PoisonError::into_inner)
            .allocators = None;
        #[cfg(debug_assertions)]
        self.allocated_pages
            .lock()
            .unwrap_or_else(crate::sync::PoisonError::into_inner)
            .clear();
    }

    pub(crate) fn allocator_state_loaded(&self) -> bool {
        self.state.lock().unwrap().allocators.is_some()
    }

    pub(crate) fn mark_needs_repair(&self) {
        self.needs_repair.store(true, Ordering::Release);
    }

    pub(crate) fn needs_repair(&self) -> bool {
        self.needs_repair.load(Ordering::Acquire)
    }

    // The in-memory allocator state has been rebuilt from the committed trees, so it can be
    // trusted again
    pub(crate) fn clear_needs_repair(&self) {
        self.needs_repair.store(false, Ordering::Release);
    }

    // The freed tables name pages that no page walk ever reads, so this is the only place those
    // page numbers are validated.
    pub(crate) fn mark_page_allocated(&self, page_number: PageNumber) -> Result<()> {
        Self::check_page_order(page_number)?;
        let mut state = self.state.lock()?;
        // Unlike the read path, this is only reached while rebuilding the allocator state, and the
        // state lock is already held, so validating against the layout costs nothing here
        let layout = state.header.layout();
        if page_number.region >= layout.num_regions() {
            return Err(StorageError::Corrupted(format!(
                "Page {page_number:?} is in region {}, but the database has {} region(s)",
                page_number.region,
                layout.num_regions()
            )));
        }
        let region_pages = u64::from(layout.region_layout(page_number.region).num_pages());
        // Cannot overflow: page_index is at most 2^32, and the order was bounded above
        let end_page = (u64::from(page_number.page_index) + 1) << page_number.page_order;
        if end_page > region_pages {
            return Err(StorageError::Corrupted(format!(
                "Page {page_number:?} extends past the end of its region, which has {region_pages} pages"
            )));
        }

        let allocator = state.get_region_mut(page_number.region);
        if !allocator.record_alloc(page_number.page_index, page_number.page_order) {
            return Err(StorageError::Corrupted(format!(
                "Page {page_number:?} overlaps a page that is already allocated"
            )));
        }
        #[cfg(debug_assertions)]
        assert!(self.allocated_pages.lock().unwrap().insert(page_number));

        Ok(())
    }

    fn write_header(&self, header: &DatabaseHeader) -> Result {
        #[cfg(feature = "experimental-multiprocess")]
        if self.concurrency_mode.is_multi_process_writable() {
            // Write directly, while holding the header lock, so that other processes cannot observe
            // a torn write.
            let bytes = header.to_bytes(true);
            let _guard = Self::lock_header(
                &self.storage,
                &self.in_process_header_lock,
                self.concurrency_mode,
                true,
            )?;
            return self.storage.write_direct(0, &bytes);
        }
        self.storage
            .write(0, DB_HEADER_SIZE, true)?
            .mem_mut()
            .copy_from_slice(&header.to_bytes(true));

        Ok(())
    }

    // Durably clears the recovery flag, marking the repair as complete.
    pub(crate) fn clear_recovery_required(&self) -> Result<()> {
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

        // Crash recovery orders the slots by transaction id, so every commit must be newer than
        // the primary it supersedes; the secondary is overwritten below
        assert!(
            transaction_id > header.primary_slot().transaction_id,
            "commit transaction id not newer than the primary slot's"
        );

        let old_transaction_id = header.secondary_slot().transaction_id;
        header.write_secondary_slot(transaction_id, data_root, system_root);

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
        // Everything this stood in for is now durable: durable_commit() flushed the allocation
        // records to DATA_ALLOCATED_TABLE before reaching here.
        self.unpersisted.lock().unwrap().clear();

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
        self.storage.write_barrier();

        let mut state = self.state.lock().unwrap();
        state
            .header
            .write_secondary_slot(transaction_id, data_root, system_root);
        state.read_from_secondary = true;

        Ok(())
    }

    pub(crate) fn get_page(&self, page_number: PageNumber, hint: PageHint) -> Result<PageImpl> {
        Self::check_page_order(page_number)?;
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
        Self::check_page_order(page_number)?;
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

    // True when a non-durable commit has been made visible to readers but not yet flushed to the
    // durable primary slot.
    pub(crate) fn pending_non_durable_commit(&self) -> bool {
        self.state.lock().unwrap().read_from_secondary
    }

    // True if the backing file is exactly the size the in-memory layout expects. redb only ever
    // sizes the file to a layout length, so an external truncation or extension makes them differ;
    // a pending non-durable commit must not be promoted then, since committing the layout would
    // leave it inconsistent with the file.
    pub(crate) fn file_len_matches_layout(&self) -> Result<bool> {
        let file_len = self.storage.raw_file_len()?;
        let state = self.state.lock().unwrap();
        Ok(file_len == state.header.layout().len())
    }

    // True if the on-disk durable primary slot's checksum is corrupt. Read from disk, since the
    // in-memory copy of an originally-clean slot wouldn't show external/failed-commit corruption.
    pub(crate) fn durable_primary_slot_corrupt(&self) -> Result<bool, DatabaseError> {
        let header_bytes = {
            #[cfg(feature = "experimental-multiprocess")]
            let _guard = Self::lock_header(
                &self.storage,
                &self.in_process_header_lock,
                self.concurrency_mode,
                false,
            )?;
            self.storage.read_direct(0, DB_HEADER_SIZE)?
        };
        let disk_header = UnrepairedDatabaseHeader::from_bytes(&header_bytes, self.page_size)?;
        Ok(disk_header.primary_corrupted())
    }

    // The durable (primary slot) roots, regardless of any pending non-durable commit served from
    // the secondary slot.
    pub(crate) fn get_durable_data_root(&self) -> Option<BtreeHeader> {
        self.state.lock().unwrap().header.primary_slot().user_root
    }

    pub(crate) fn get_durable_system_root(&self) -> Option<BtreeHeader> {
        self.state.lock().unwrap().header.primary_slot().system_root
    }

    pub(crate) fn free(&self, page: PageNumber, allocated: &PageTracker) {
        self.free_helper(page, allocated);
    }

    fn free_helper(&self, page: PageNumber, allocated: &PageTracker) {
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
        // Free in the regional allocator. free() returns the order of the resulting block, which is
        // larger than page_order when buddies merged.
        let freed_order = state
            .get_region_mut(region_index)
            .free(page.page_index, page.page_order);
        // Mark the region free at the merged order, not just page_order: leaving the tracker's
        // higher-order bits stale after a merge would hide the reclaimed space from find_free.
        state
            .get_region_tracker_mut()
            .mark_free(freed_order, region_index);

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

    // Drops the page from the unpersisted set without freeing it. Returns whether it was there.
    pub(crate) fn claim_unpersisted(&self, page: PageNumber) -> bool {
        self.unpersisted.lock().unwrap().claim(page)
    }

    // Frees the page if no durable commit has occurred, since it was allocated. Returns true, if the page was freed
    pub(crate) fn free_if_unpersisted(&self, page: PageNumber, allocated: &PageTracker) -> bool {
        if self.unpersisted.lock().unwrap().claim(page) {
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
        self.unpersisted
            .lock()
            .unwrap()
            .record_allocations(transaction_id, pages);
    }

    pub(crate) fn take_unpersisted_allocations(
        &self,
    ) -> BTreeMap<TransactionId, PageNumberHashSet> {
        self.unpersisted.lock().unwrap().take_allocations()
    }

    pub(crate) fn record_post_commit_allocations(
        &self,
        pages: impl IntoIterator<Item = PageNumber>,
    ) {
        self.unpersisted
            .lock()
            .unwrap()
            .post_commit_allocations
            .extend(pages);
    }

    pub(crate) fn take_post_commit_allocations(&self) -> PageNumberHashSet {
        mem::take(&mut self.unpersisted.lock().unwrap().post_commit_allocations)
    }

    // Returns all unpersisted data-tree pages allocated strictly after `transaction_id`. Used
    // during savepoint restore to queue pages that need to be freed.
    pub(crate) fn unpersisted_allocations_after(
        &self,
        transaction_id: TransactionId,
    ) -> Vec<PageNumber> {
        self.unpersisted
            .lock()
            .unwrap()
            .allocations_after(transaction_id)
    }

    pub(crate) fn unpersisted(&self, page: PageNumber) -> bool {
        self.unpersisted.lock().unwrap().contains(page)
    }

    // Record the data-tree pages a non-durable commit freed, for the next durable commit to write
    // to DATA_FREED_TABLE.
    pub(crate) fn record_unpersisted_data_freed(
        &self,
        transaction_id: TransactionId,
        pages: Vec<PageNumber>,
    ) {
        self.unpersisted
            .lock()
            .unwrap()
            .record_data_freed(transaction_id, pages);
    }

    // Offers every page freed by a transaction in `start..end` to `free_page`, dropping the ones
    // it reports having reclaimed. Returns the transactions considered.
    pub(crate) fn process_unpersisted_data_freed(
        &self,
        start: TransactionId,
        end: TransactionId,
        mut free_page: impl FnMut(PageNumber) -> bool,
    ) -> Vec<TransactionId> {
        // The records are copied out before `free_page` runs, because reclaiming a page locks this
        // same state. Only the committing write transaction mutates these records, so nothing can
        // add to a transaction's record in between.
        let snapshot = self
            .unpersisted
            .lock()
            .unwrap()
            .data_freed_in_range(start, end);
        let mut transaction_ids = Vec::with_capacity(snapshot.len());
        for (transaction_id, pages) in snapshot {
            let kept: Vec<PageNumber> = pages.into_iter().filter(|p| !free_page(*p)).collect();
            self.unpersisted
                .lock()
                .unwrap()
                .replace_data_freed(transaction_id, kept);
            transaction_ids.push(transaction_id);
        }
        transaction_ids
    }

    pub(crate) fn take_unpersisted_data_freed(&self) -> BTreeMap<TransactionId, Vec<PageNumber>> {
        self.unpersisted.lock().unwrap().take_data_freed()
    }

    pub(crate) fn drop_unpersisted_data_freed_after(&self, transaction_id: TransactionId) {
        self.unpersisted
            .lock()
            .unwrap()
            .drop_data_freed_after(transaction_id);
    }

    pub(crate) fn unpersisted_data_freed_pages(&self) -> Vec<PageNumber> {
        self.unpersisted.lock().unwrap().pages_pending_free()
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
        // Make the larger file durable before its layout can reach the on-disk header. A
        // subsequent commit writes this layout into the header, whose layout fields are shared by
        // both commit slots; if a crash persisted that header but not the file extension, every
        // open would fail with "File truncated below stored layout" even though the previous
        // durable state was intact. This mirrors the shrink path, which reduces the file only
        // after the smaller layout is durable.
        self.storage.sync_file()?;

        state.allocators_mut().resize_to(new_layout);
        state.header.set_layout(new_layout);
        Ok(())
    }

    fn allocate<'txn>(
        &self,
        allocation_size: usize,
        allocated: &PageTracker,
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
        allocated: &PageTracker,
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
        let shutdown_result = self.flush_shutdown_header();
        // The backend's close() contract guarantees it is called exactly once, so it must be
        // called even if the shutdown writes above failed
        let close_result = self.storage.close();
        shutdown_result.and(close_result)
    }

    fn flush_shutdown_header(&self) -> Result {
        if self.storage.check_io_errors().is_ok() && !crate::panicking() {
            let mut state = self.state.lock()?;
            // Clearing the flag asserts that this process left the file consistent, which requires
            // an allocator state describing what it wrote, and one not marked for repair.
            if state.allocators.is_some() && !self.needs_repair() && self.storage.flush().is_ok() {
                state.header.recovery_required = false;
                self.write_header(&state.header)?;
                self.storage.flush()?;
            }
        }

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

    // A panic raised while the state mutex is held (e.g. an allocator assertion) poisons it.
    // invalidate_allocator_state() runs while unwinding from such a panic, so it must recover
    // the lock rather than double-panic; the poison itself is left set.
    #[test]
    #[cfg(panic = "unwind")]
    fn invalidate_allocator_state_tolerates_poison() {
        use super::TransactionalMemory;
        use crate::tree_store::{InMemoryBackend, LocklessBackend};

        let mem = TransactionalMemory::new(
            LocklessBackend::boxed(InMemoryBackend::new()),
            true,
            4096,
            None,
            0,
            false,
            crate::db::ConcurrencyMode::SingleProcess,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();

        std::thread::scope(|s| {
            let result = s
                .spawn(|| {
                    let _guard = mem.state.lock().unwrap();
                    panic!("poison the state mutex");
                })
                .join();
            assert!(result.is_err());
        });
        assert!(mem.state.is_poisoned());

        // Must not panic, despite the poisoned lock
        mem.invalidate_allocator_state();

        // The poison stays set, so later accesses fail rather than trust the state
        assert!(mem.state.is_poisoned());
        assert!(mem.get_last_committed_transaction_id().is_err());
    }

    // Rebuilding the allocator state feeds mark_page_allocated() page numbers straight out of the
    // freed tables, which no page walk ever reads. A corrupted entry has to be reported rather than
    // indexing the region allocators, tripping the bitmap's bounds assertion, or walking the buddy
    // allocator past its maximum order. See https://github.com/cberner/redb/issues/1333
    #[test]
    fn mark_page_allocated_rejects_corrupt_page_numbers() {
        use super::{MAX_PAGE_INDEX, TransactionalMemory};
        use crate::StorageError;
        use crate::tree_store::page_store::base::MAX_REGIONS;
        use crate::tree_store::{InMemoryBackend, LocklessBackend, PageNumber};

        let page_size = 4096;
        let mem = TransactionalMemory::new(
            LocklessBackend::boxed(InMemoryBackend::new()),
            true,
            page_size,
            Some(64 * page_size as u64),
            0,
            false,
            crate::db::ConcurrencyMode::SingleProcess,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();

        let corrupt = [
            // Past the end of the layout, which would index the region allocators out of bounds
            PageNumber::new(MAX_REGIONS - 1, 0, 0),
            // Past the end of its region, which the bitmap asserts on
            PageNumber::new(0, MAX_PAGE_INDEX, 0),
            // An order no region can have
            PageNumber::from_le_bytes((31u64 << 59).to_le_bytes()),
        ];
        for page in corrupt {
            assert!(
                matches!(
                    mem.mark_page_allocated(page),
                    Err(StorageError::Corrupted(_))
                ),
                "{page:?} was not rejected"
            );
        }

        // Naming the same page twice walks the allocator up past its maximum order looking for a
        // parent to split
        mem.mark_page_allocated(PageNumber::new(0, 0, 0)).unwrap();
        assert!(matches!(
            mem.mark_page_allocated(PageNumber::new(0, 0, 0)),
            Err(StorageError::Corrupted(_))
        ));
    }

    // A page order read from a corrupted file can be far larger than any real page, which the read
    // path would use to size its buffer.
    #[test]
    fn oversized_page_order_is_rejected() {
        use super::TransactionalMemory;
        use crate::StorageError;
        use crate::tree_store::page_store::base::PageHint;
        use crate::tree_store::{InMemoryBackend, LocklessBackend, Page, PageNumber, PageTracker};

        let page_size = 4096;
        let mem = TransactionalMemory::new(
            LocklessBackend::boxed(InMemoryBackend::new()),
            true,
            page_size,
            Some(64 * page_size as u64),
            0,
            false,
            crate::db::ConcurrencyMode::SingleProcess,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();

        let valid = mem.allocate_helper(1, false).unwrap();
        let valid_page = valid.get_page_number();
        drop(valid);

        // order = 31, which would be read as a 2^31 page (8TiB) allocation
        let bad_order = PageNumber::from_le_bytes((31u64 << 59).to_le_bytes());

        assert!(matches!(
            mem.get_page(bad_order, PageHint::None),
            Err(StorageError::Corrupted(_))
        ));
        assert!(matches!(
            mem.mark_page_allocated(bad_order),
            Err(StorageError::Corrupted(_))
        ));
        mem.get_page(valid_page, PageHint::None).unwrap();

        mem.free(valid_page, &PageTracker::ignore());
    }

    // Freeing pages that buddy-merge into a higher order must re-mark the region tracker at the
    // merged order. Otherwise the tracker stays marked full at that order, find_free skips the
    // region even though a free block exists, and the file grows (and compact() stalls) instead of
    // reusing the space.
    #[test]
    fn free_merge_remarks_region_tracker() {
        use super::TransactionalMemory;
        use crate::tree_store::{InMemoryBackend, LocklessBackend, Page, PageTracker};

        // Small pages and regions keep the reproduction cheap to set up.
        let page_size = 128 * 1024;
        let region_size = 16 * page_size as u64;
        let mem = TransactionalMemory::new(
            LocklessBackend::boxed(InMemoryBackend::new()),
            true,
            page_size,
            Some(region_size),
            0,
            false,
            crate::db::ConcurrencyMode::SingleProcess,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();

        let ignore = PageTracker::ignore();

        // Fill region 0 with order-0 pages. The allocation that spills past region 0 fails on it
        // first, which marks region 0 full at every order.
        let mut region0_pages = vec![];
        loop {
            let page = mem.allocate_helper(1, false).unwrap();
            let number = page.get_page_number();
            drop(page);
            if number.region == 0 {
                region0_pages.push(number);
            } else {
                // First page past region 0: it has done its job of forcing region 0 full. Give it
                // back so the spilled-into region is left entirely free.
                mem.free(number, &ignore);
                break;
            }
        }
        assert!(
            region0_pages.len() >= 2,
            "test needs at least two pages in region 0, got {}",
            region0_pages.len()
        );

        // Free everything in region 0. The order-0 pages buddy-merge back into larger blocks, so
        // region 0 regains free space above order 0.
        for page in region0_pages {
            mem.free(page, &ignore);
        }

        // An order-1 allocation must reuse region 0's merged free block. Before the fix the tracker
        // still marked region 0 full above order 0, so find_free skipped it and this landed in a
        // higher region.
        let reused = mem.allocate_helper(2 * page_size, false).unwrap();
        assert_eq!(
            reused.get_page_number().region,
            0,
            "order-1 allocation should reuse the merged free block in region 0"
        );
    }
}

/// The header lock, against real files.
#[cfg(all(
    test,
    feature = "experimental-multiprocess",
    any(target_os = "linux", target_vendor = "apple", windows)
))]
mod header_lock_test {
    use super::{HeaderGuard, TransactionalMemory};
    use crate::db::ConcurrencyMode;
    use crate::tree_store::PAGE_SIZE;
    use crate::tree_store::page_store::file_backend::FileBackend;
    use crate::{DatabaseError, Result};
    use alloc::sync::Arc;
    use std::path::Path;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    fn hold(mem: &TransactionalMemory, exclusive: bool) -> Result<HeaderGuard<'_>> {
        TransactionalMemory::lock_header(
            &mem.storage,
            &mem.in_process_header_lock,
            mem.concurrency_mode,
            exclusive,
        )
    }

    fn open(path: &Path, mode: ConcurrencyMode) -> Result<Arc<TransactionalMemory>, DatabaseError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        Ok(Arc::new(TransactionalMemory::new(
            Box::new(FileBackend::new(file)?),
            true,
            PAGE_SIZE,
            None,
            0,
            false,
            mode,
        )?))
    }

    fn open_read_only(
        path: &Path,
        mode: ConcurrencyMode,
    ) -> Result<Arc<TransactionalMemory>, DatabaseError> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        Ok(Arc::new(TransactionalMemory::new(
            Box::new(crate::tree_store::ReadOnlyBackend::new(Box::new(
                FileBackend::new(file)?,
            ))),
            false,
            PAGE_SIZE,
            None,
            0,
            true,
            mode,
        )?))
    }

    /// Through the read-only backend the open wraps its storage in
    #[test]
    fn a_read_only_handle_takes_shared_holds() {
        let tmpfile = crate::create_tempfile();
        open(tmpfile.path(), ConcurrencyMode::SingleProcess).unwrap();

        let reader = open_read_only(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();
        let held = hold(&reader, false).unwrap();

        let peer = open_read_only(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();
        drop(hold(&peer, false).unwrap());
        drop(held);
    }

    /// The other half of the hold covering the write: the bytes are on the file when
    /// `write_header()` returns, rather than waiting in the buffer for a flush outside the hold.
    #[test]
    fn the_header_reaches_the_file_before_write_header_returns() {
        use super::DB_HEADER_SIZE;

        let tmpfile = crate::create_tempfile();
        let writer = open(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();

        let mut header = writer.state.lock().unwrap().header.clone();
        header.recovery_required = !header.recovery_required;
        let expected = header.to_bytes(true);
        writer.write_header(&header).unwrap();

        // Read around redb entirely, and with no flush in between
        let on_disk = std::fs::read(tmpfile.path()).unwrap();
        assert_eq!(&on_disk[..DB_HEADER_SIZE], &expected[..]);
    }

    /// The hold has to cover the write reaching the file, which is what `write_header()` does
    /// directly rather than leaving to whenever the buffer is next flushed.
    #[test]
    fn the_hold_covers_the_write_reaching_the_file() {
        let tmpfile = crate::create_tempfile();
        let writer = open(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();
        let reader = open(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();

        // Taken before the hold: another one in this process would wait on it
        let header = writer.state.lock().unwrap().header.clone();
        let held = hold(&reader, false).unwrap();

        let (tx, rx) = mpsc::channel();
        let writing = thread::spawn(move || {
            writer.write_header(&header).unwrap();
            tx.send(()).unwrap();
        });

        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "the header reached the file while a reader held the lock"
        );
        drop(held);
        rx.recv_timeout(Duration::from_secs(10))
            .expect("the write never completed");
        writing.join().unwrap();
    }

    /// Which is what keeps a header mid-write from being read in another process
    #[test]
    fn an_exclusive_hold_excludes_a_shared_one() {
        let tmpfile = crate::create_tempfile();
        let writer = open(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();
        let reader = open(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();

        let guard = hold(&writer, true).unwrap();
        let (tx, rx) = mpsc::channel();
        let waiting = thread::spawn(move || {
            let _held = hold(&reader, false).unwrap();
            tx.send(()).unwrap();
        });

        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "the reader entered its hold while the writer held one"
        );
        drop(guard);
        rx.recv_timeout(Duration::from_secs(10))
            .expect("the reader never entered its hold");
        waiting.join().unwrap();
    }

    /// Only a write needs the header to itself
    #[test]
    fn shared_holds_admit_each_other() {
        let tmpfile = crate::create_tempfile();
        let first = open(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();
        let second = open(tmpfile.path(), ConcurrencyMode::MultiWriterProcess).unwrap();

        let held = hold(&first, false).unwrap();
        let (tx, rx) = mpsc::channel();
        let waiting = thread::spawn(move || {
            let _also = hold(&second, false).unwrap();
            tx.send(()).unwrap();
        });

        rx.recv_timeout(Duration::from_secs(10))
            .expect("a second reader was held off");
        waiting.join().unwrap();
        drop(held);
    }

    /// It holds the whole storage, which covers the header bytes: a hold that locked and
    /// released them would punch a hole in that lock.
    #[test]
    fn a_hold_does_not_puncture_the_whole_storage_lock() {
        let tmpfile = crate::create_tempfile();
        let held = open(tmpfile.path(), ConcurrencyMode::SingleProcess).unwrap();

        for exclusive in [false, true] {
            let guard = hold(&held, exclusive).unwrap();
            drop(guard);
        }

        assert!(matches!(
            open(tmpfile.path(), ConcurrencyMode::SingleProcess),
            Err(DatabaseError::DatabaseAlreadyOpen)
        ));
    }
}

/// The locking protocol an open runs before it reads anything, against real files. Only the
/// platforms with byte-range locks can run it.
#[cfg(all(test, any(target_os = "linux", target_vendor = "apple", windows)))]
mod lock_protocol_test {
    use super::TransactionalMemory;
    use crate::DatabaseError;
    use crate::db::ConcurrencyMode;
    use crate::tree_store::page_store::cached_file::PagedCachedFile;
    use crate::tree_store::page_store::file_backend::FileBackend;
    use crate::tree_store::page_store::file_backend::range_lock::RangeLock;
    use std::fs::{File, OpenOptions};
    use std::path::Path;

    fn reopen(path: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap()
    }

    /// The locks an open takes. A failure drops the storage, which is what releases whatever it
    /// had taken.
    fn open_file(
        file: File,
        read_only: bool,
        concurrency_mode: ConcurrencyMode,
    ) -> Result<PagedCachedFile, DatabaseError> {
        let storage = PagedCachedFile::new(Box::new(FileBackend::new(file)?), 4096, 0);
        TransactionalMemory::lock_for_open(&storage, read_only, concurrency_mode)?;

        Ok(storage)
    }

    fn open(
        path: &Path,
        read_only: bool,
        concurrency_mode: ConcurrencyMode,
    ) -> Result<PagedCachedFile, DatabaseError> {
        open_file(reopen(path), read_only, concurrency_mode)
    }

    fn single_process(path: &Path, read_only: bool) -> Result<PagedCachedFile, DatabaseError> {
        open(path, read_only, ConcurrencyMode::SingleProcess)
    }

    fn refused(result: Result<PagedCachedFile, DatabaseError>) -> bool {
        matches!(result, Err(DatabaseError::DatabaseAlreadyOpen))
    }

    /// The cohort is another process, which may have been built with the multi-process feature
    /// when this one was not, so the probe is not gated on being able to form one
    #[test]
    fn a_read_only_open_refuses_a_cohort_this_build_cannot_form() {
        use crate::db::{SHARED_WRITER_BYTE, byte_range};

        let tmpfile = crate::create_tempfile();
        let cohort = reopen(tmpfile.path());
        assert!(
            cohort
                .try_lock_shared_range(byte_range(SHARED_WRITER_BYTE))
                .unwrap()
        );

        assert!(refused(single_process(tmpfile.path(), true)));
    }

    /// Only a multi-writer cohort admits a second writer, and every mode byte is free again once
    /// its holder closes
    #[cfg(feature = "experimental-multiprocess")]
    #[test]
    fn the_modes_exclude_each_other() {
        use ConcurrencyMode::{MultiWriterProcess, SingleWriterProcess};

        let tmpfile = crate::create_tempfile();
        for (held, joining, admitted) in [
            (SingleWriterProcess, SingleWriterProcess, false),
            (SingleWriterProcess, MultiWriterProcess, false),
            (MultiWriterProcess, SingleWriterProcess, false),
            (MultiWriterProcess, MultiWriterProcess, true),
        ] {
            let first = open(tmpfile.path(), false, held).unwrap();
            let second = open(tmpfile.path(), false, joining);
            assert_eq!(second.is_ok(), admitted, "{held:?} then {joining:?}");

            let reader = open(tmpfile.path(), true, MultiWriterProcess).unwrap();
            reader.close().unwrap();
            if let Ok(second) = second {
                second.close().unwrap();
            }
            first.close().unwrap();
        }
    }

    /// A single-process handle joins nothing, so it and a multi-process one refuse each other --
    /// unless both are read-only, since neither holds a byte the other could find
    #[cfg(feature = "experimental-multiprocess")]
    #[test]
    fn a_single_process_handle_and_a_multi_process_one_refuse_each_other() {
        let tmpfile = crate::create_tempfile();
        for mode in [
            ConcurrencyMode::SingleWriterProcess,
            ConcurrencyMode::MultiWriterProcess,
        ] {
            for read_only in [false, true] {
                let plain = single_process(tmpfile.path(), read_only).unwrap();
                assert!(refused(open(tmpfile.path(), false, mode)));
                assert_eq!(refused(open(tmpfile.path(), true, mode)), !read_only);
                plain.close().unwrap();

                let held = open(tmpfile.path(), read_only, mode).unwrap();
                assert!(refused(single_process(tmpfile.path(), false)));
                assert_eq!(refused(single_process(tmpfile.path(), true)), !read_only);
                held.close().unwrap();
            }
        }
    }

    /// Dropping the file does not suffice: a caller holding a `try_clone()` of the file it
    /// handed over keeps the open file description, and so the locks, alive
    #[cfg(feature = "experimental-multiprocess")]
    #[test]
    fn a_refused_open_releases_the_bytes_it_took() {
        use crate::db::{SHARED_WRITER_BYTE, WRITER_BYTE, byte_range};

        let tmpfile = crate::create_tempfile();
        let holder = reopen(tmpfile.path());
        assert!(holder.try_lock_range(byte_range(WRITER_BYTE)).unwrap());

        // The shared writer byte is taken before the conflict on the writer byte
        let file = reopen(tmpfile.path());
        let kept_by_the_caller = file.try_clone().unwrap();
        assert!(refused(open_file(
            file,
            false,
            ConcurrencyMode::SingleWriterProcess
        )));

        let observer = reopen(tmpfile.path());
        let byte = byte_range(SHARED_WRITER_BYTE);
        assert!(observer.try_lock_range(byte.clone()).unwrap());
        observer.unlock_range(byte).unwrap();
        drop(kept_by_the_caller);

        holder.unlock_range(byte_range(WRITER_BYTE)).unwrap();
        open(tmpfile.path(), false, ConcurrencyMode::SingleWriterProcess)
            .unwrap()
            .close()
            .unwrap();
    }
}

#[cfg(test)]
mod lock_failure_test {
    use super::TransactionalMemory;
    use crate::db::{ConcurrencyMode, FULL_RANGE, InternalStorageBackend};
    use crate::io;
    use crate::sync::Mutex;
    use crate::tree_store::InMemoryBackend;
    use crate::tree_store::page_store::cached_file::PagedCachedFile;
    use crate::{DatabaseError, StorageBackend, StorageError};
    use alloc::boxed::Box;
    use alloc::sync::Arc;
    use alloc::vec::Vec;
    use core::ops::Range;

    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    enum Answer {
        Acquired,
        Refused,
        Unsupported,
        Failed,
    }

    impl Answer {
        fn result(self) -> Result<bool, io::Error> {
            match self {
                Answer::Acquired => Ok(true),
                Answer::Refused => Ok(false),
                Answer::Unsupported => Err(io::unsupported("no byte-range locks here")),
                Answer::Failed => Err(io::invalid_input("the lock could not be taken")),
            }
        }
    }

    #[derive(Debug)]
    struct AnsweringBackend {
        inner: InMemoryBackend,
        taking: Answer,
        querying: Answer,
        held: Arc<Mutex<Vec<Range<u64>>>>,
        released: Arc<Mutex<Vec<Range<u64>>>>,
    }

    impl AnsweringBackend {
        fn take(&self, range: Range<u64>) -> Result<bool, io::Error> {
            let acquired = self.taking.result();
            if matches!(acquired, Ok(true)) {
                self.held.lock().unwrap().push(range);
            }

            acquired
        }
    }

    impl InternalStorageBackend for AnsweringBackend {
        fn try_lock_range(&self, range: Range<u64>) -> Result<bool, io::Error> {
            self.take(range)
        }

        fn try_lock_shared_range(&self, range: Range<u64>) -> Result<bool, io::Error> {
            self.take(range)
        }

        #[cfg(feature = "experimental-multiprocess")]
        fn lock_range(&self, range: Range<u64>) -> Result<(), io::Error> {
            self.take(range).map(|_| ())
        }

        #[cfg(feature = "experimental-multiprocess")]
        fn lock_shared_range(&self, range: Range<u64>) -> Result<(), io::Error> {
            self.take(range).map(|_| ())
        }

        fn unlock_range(&self, range: Range<u64>) -> Result<(), io::Error> {
            self.held.lock().unwrap().retain(|held| *held != range);
            self.released.lock().unwrap().push(range);
            Ok(())
        }

        fn query_lock_range(&self, _range: Range<u64>) -> Result<bool, io::Error> {
            self.querying.result()
        }
    }

    impl StorageBackend for AnsweringBackend {
        fn close(&self) -> Result<(), io::Error> {
            for range in self.held.lock().unwrap().drain(..) {
                self.released.lock().unwrap().push(range);
            }

            Ok(())
        }

        fn len(&self) -> Result<u64, io::Error> {
            self.inner.len()
        }

        fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
            self.inner.read(offset, out)
        }

        fn set_len(&self, len: u64) -> Result<(), io::Error> {
            self.inner.set_len(len)
        }

        fn sync_data(&self) -> Result<(), io::Error> {
            self.inner.sync_data()
        }

        fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
            self.inner.write(offset, data)
        }
    }

    type Released = Arc<Mutex<Vec<Range<u64>>>>;

    fn storage(taking: Answer, querying: Answer) -> (PagedCachedFile, Released) {
        let released: Released = Arc::new(Mutex::new(Vec::new()));
        let storage = PagedCachedFile::new(
            Box::new(AnsweringBackend {
                inner: InMemoryBackend::new(),
                taking,
                querying,
                held: Arc::new(Mutex::new(Vec::new())),
                released: released.clone(),
            }),
            4096,
            0,
        );

        (storage, released)
    }

    fn open_as(
        concurrency_mode: ConcurrencyMode,
        read_only: bool,
        taking: Answer,
        querying: Answer,
    ) -> Result<(), DatabaseError> {
        TransactionalMemory::lock_for_open(
            &storage(taking, querying).0,
            read_only,
            concurrency_mode,
        )
    }

    fn open(read_only: bool, taking: Answer, querying: Answer) -> Result<(), DatabaseError> {
        open_as(ConcurrencyMode::SingleProcess, read_only, taking, querying)
    }

    fn failed(result: Result<(), DatabaseError>) -> bool {
        matches!(result, Err(DatabaseError::Storage(StorageError::Io(_))))
    }

    #[test]
    fn a_platform_without_locks_opens_anyway() {
        open(false, Answer::Unsupported, Answer::Unsupported).unwrap();
        open(true, Answer::Unsupported, Answer::Unsupported).unwrap();

        // ... including one whose locks work but which cannot be asked about them
        open(true, Answer::Acquired, Answer::Unsupported).unwrap();
    }

    #[test]
    fn a_conflicting_lock_reports_the_database_already_open() {
        for read_only in [false, true] {
            assert!(matches!(
                open(read_only, Answer::Refused, Answer::Acquired),
                Err(DatabaseError::DatabaseAlreadyOpen)
            ));
        }
    }

    #[test]
    fn a_lock_that_fails_for_any_other_reason_fails_the_open() {
        assert!(failed(open(false, Answer::Failed, Answer::Acquired)));
        assert!(failed(open(true, Answer::Failed, Answer::Acquired)));

        // ... and so does a probe that fails, which only a read-only open makes
        assert!(failed(open(true, Answer::Acquired, Answer::Failed)));
        open(false, Answer::Acquired, Answer::Failed).unwrap();
    }

    // A read-only open included: its mode still declares writers that need the locks
    #[cfg(feature = "experimental-multiprocess")]
    #[test]
    fn a_shared_mode_without_locks_fails_the_open() {
        for mode in [
            ConcurrencyMode::SingleWriterProcess,
            ConcurrencyMode::MultiWriterProcess,
        ] {
            for read_only in [false, true] {
                assert!(failed(open_as(
                    mode,
                    read_only,
                    Answer::Unsupported,
                    Answer::Unsupported
                )));
            }
        }
    }

    #[test]
    fn the_lock_is_released_at_close() {
        for taking in [Answer::Acquired, Answer::Unsupported] {
            let (storage, released) = storage(taking, Answer::Unsupported);
            TransactionalMemory::lock_for_open(&storage, false, ConcurrencyMode::SingleProcess)
                .unwrap();
            storage.close().unwrap();

            let expected: Vec<Range<u64>> = if taking == Answer::Acquired {
                vec![FULL_RANGE]
            } else {
                vec![]
            };
            assert_eq!(*released.lock().unwrap(), expected);
        }
    }
}
