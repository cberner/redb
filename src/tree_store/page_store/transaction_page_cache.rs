use crate::tree_store::page_store::base::{PageImpl, PageNumber};
use std::sync::OnceLock;

// Number of slots. Sized to hold roughly the top two levels below the root of a large
// btree. Must be a power of two.
const SLOTS: usize = 128;
// Number of slots probed per page. Probing a second slot greatly reduces the number of
// hot pages that go uncached due to slot collisions.
const PROBE_LEN: usize = 2;

/// A small, bounded page cache owned by a single read transaction.
///
/// Concurrent readers of the same hot pages (the top of a btree) contend in the shared
/// page cache: every lookup acquires a lock stripe, updates the LRU second-chance flag,
/// and increments the page's `Arc` refcount, all of which are writes to memory shared
/// between threads. This cache serves repeated reads of a hot page within one
/// transaction as plain `&PageImpl` borrows: hits perform no writes to shared memory at
/// all, so they cannot cause cache-line contention between threads.
///
/// Safety of caching relies on the snapshot invariant that read transactions already
/// depend on (see `Btree::cached_root` and `AccessGuard`): while a read transaction is
/// live, every page reachable from its snapshot remains allocated and its contents are
/// immutable, because writes are copy-on-write and freed pages are only reused once no
/// live read transaction can reference them. Therefore a cached page can never go
/// stale. Write transactions can free and reallocate pages within the transaction, so
/// they must not use this cache; it is only constructed for read transactions.
///
/// Slots are filled on first use and never evicted or replaced, which both keeps hits
/// write-free and naturally retains the top of the tree: those pages are touched on
/// every lookup, so they claim slots first. A page whose slots are already taken is
/// simply served from the shared page cache as before. Since the cache holds pages for
/// the lifetime of the transaction, it is restricted to `SLOTS` single (order-zero)
/// pages to bound the pinned memory.
pub(crate) struct ReadTransactionPageCache {
    slots: Box<[OnceLock<(PageNumber, PageImpl)>]>,
}

impl ReadTransactionPageCache {
    pub(crate) fn new() -> Self {
        Self {
            slots: (0..SLOTS).map(|_| OnceLock::new()).collect(),
        }
    }

    fn probe_slots(
        &self,
        page_number: PageNumber,
    ) -> impl Iterator<Item = &OnceLock<(PageNumber, PageImpl)>> {
        let packed = u64::from_le_bytes(page_number.to_le_bytes());
        // Fibonacci hashing: the high bits of the product are well mixed
        let index = (packed.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize % SLOTS;
        (0..PROBE_LEN).map(move |i| &self.slots[(index + i) % SLOTS])
    }

    /// Returns the cached page, if present.
    pub(crate) fn get(&self, page_number: PageNumber) -> Option<&PageImpl> {
        for slot in self.probe_slots(page_number) {
            if let Some((cached_number, page)) = slot.get()
                && *cached_number == page_number
            {
                return Some(page);
            }
        }
        None
    }

    /// Caches `page` if a slot is free, returning a reference to the cached copy. If
    /// all of its slots are taken by other pages, returns `page` back to the caller.
    pub(crate) fn insert(
        &self,
        page_number: PageNumber,
        mut page: PageImpl,
    ) -> Result<&PageImpl, PageImpl> {
        debug_assert!(page_number.page_order == 0);
        for slot in self.probe_slots(page_number) {
            match slot.set((page_number, page)) {
                Ok(()) => {
                    let (_, cached) = slot.get().unwrap();
                    return Ok(cached);
                }
                Err((_, rejected)) => {
                    // The slot was concurrently filled. If it holds this same page (a
                    // racing insert by another thread), serve the stored copy.
                    let (cached_number, cached) = slot.get().unwrap();
                    if *cached_number == page_number {
                        return Ok(cached);
                    }
                    page = rejected;
                }
            }
        }
        Err(page)
    }

    /// Drops all cached pages. Called before the transaction releases its snapshot, so
    /// that no page reference outlives the transaction that pinned it.
    pub(crate) fn clear(&mut self) {
        for slot in &mut self.slots {
            slot.take();
        }
    }
}

#[cfg(test)]
mod test {
    use super::{ReadTransactionPageCache, SLOTS};
    use crate::tree_store::page_store::backends::InMemoryBackend;
    use crate::tree_store::page_store::base::{Page, PageHint, PageNumber, PageTrackerPolicy};
    use crate::tree_store::page_store::header::PAGE_SIZE;
    use crate::tree_store::page_store::page_manager::{
        AllocationPolicy, PageAllocator, TransactionalMemory,
    };
    use std::sync::{Arc, Mutex};

    fn create_memory() -> Arc<TransactionalMemory> {
        let mem = TransactionalMemory::new(
            Box::new(InMemoryBackend::new()),
            true,
            PAGE_SIZE,
            None,
            0,
            false,
        )
        .unwrap();
        mem.reset_allocator_state().unwrap();
        Arc::new(mem)
    }

    fn allocate_pages(mem: &Arc<TransactionalMemory>, count: usize) -> Vec<PageNumber> {
        let allocator = PageAllocator::new(mem.clone(), AllocationPolicy::Default);
        let allocated = Mutex::new(PageTrackerPolicy::new_tracking());
        let mut guard = allocated.lock().unwrap();
        (0..count)
            .map(|_| {
                allocator
                    .allocate(PAGE_SIZE, &mut guard)
                    .unwrap()
                    .get_page_number()
            })
            .collect()
    }

    #[test]
    fn cached_pages_are_served_by_reference() {
        let mem = create_memory();
        let pages = allocate_pages(&mem, 2);
        let cache = ReadTransactionPageCache::new();

        assert!(cache.get(pages[0]).is_none());
        let page = mem.get_page(pages[0], PageHint::None).unwrap();
        let cached = cache.insert(pages[0], page).unwrap();
        assert_eq!(cached.get_page_number(), pages[0]);

        let hit = cache.get(pages[0]).unwrap();
        assert_eq!(hit.get_page_number(), pages[0]);
        assert!(cache.get(pages[1]).is_none());

        // A second insert of the same page (e.g. two racing threads) serves the stored copy
        let duplicate = mem.get_page(pages[0], PageHint::None).unwrap();
        let cached = cache.insert(pages[0], duplicate).unwrap();
        assert_eq!(cached.get_page_number(), pages[0]);
    }

    #[test]
    fn bounded_and_first_wins() {
        let mem = create_memory();
        let pages = allocate_pages(&mem, 3 * SLOTS);
        let cache = ReadTransactionPageCache::new();

        let mut inserted = 0;
        for page_number in &pages {
            let page = mem.get_page(*page_number, PageHint::None).unwrap();
            if cache.insert(*page_number, page).is_ok() {
                inserted += 1;
            }
        }
        // The cache never holds more pages than it has slots, and rejects the overflow
        assert!(inserted <= SLOTS);
        assert!(inserted > 0);

        // Every lookup returns the exact requested page or nothing, and pages that were
        // rejected on insert stay absent (first-wins, no eviction)
        let mut hits = 0;
        for page_number in &pages {
            if let Some(page) = cache.get(*page_number) {
                assert_eq!(page.get_page_number(), *page_number);
                hits += 1;
            }
        }
        assert_eq!(hits, inserted);
    }
}
