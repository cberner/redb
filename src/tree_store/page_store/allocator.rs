use crate::tree_store::PageNumber;

/// Trait for page allocation strategies within a redb region.
///
/// The default implementation is [`BuddyAllocator`](super::buddy_allocator::BuddyAllocator),
/// which uses a buddy system with power-of-two page orders. Custom implementations can provide
/// different allocation strategies (e.g., slab allocators with non-power-of-two size classes)
/// to trade off between allocation speed and space efficiency.
///
/// # Allocator Contract
///
/// - Pages are identified by a `(page_number, order)` pair, where order determines the page size
///   as `base_page_size * 2^order`.
/// - An allocator manages pages within a single region.
/// - `alloc` and `alloc_lowest` return a page index at the requested order, or `None` if no
///   pages are available at that order.
/// - `free` releases a previously allocated page. The caller guarantees that the page was
///   previously allocated.
/// - `record_alloc` marks a page as allocated without going through the normal allocation path.
///   This is used during recovery and repair.
/// - Serialization (`to_vec`) and deserialization (via [`PageAllocatorFactory::from_bytes`])
///   must be lossless — a round-trip must preserve the exact allocator state.
pub trait PageAllocator: Send + Sync {
    /// Returns a hash of the allocator state, used for consistency checking.
    fn xxh3_hash(&self) -> u128;

    /// Serializes the allocator state to a byte vector.
    ///
    /// The format must be understood by the corresponding [`PageAllocatorFactory::from_bytes`].
    fn to_vec(&self) -> Vec<u8>;

    /// Returns the highest order that has any free pages, or `None` if fully allocated.
    fn highest_free_order(&self) -> Option<u8>;

    /// Returns the number of pages currently allocated.
    fn count_allocated_pages(&self) -> u32;

    /// Returns the number of pages currently free.
    fn count_free_pages(&self) -> u32;

    /// Returns the maximum page order supported by this allocator.
    fn get_max_order(&self) -> u8;

    /// Returns the number of free pages at the end of the region.
    ///
    /// This is used to determine whether the region can be shrunk.
    fn trailing_free_pages(&self) -> u32;

    /// Checks that the set of allocated pages matches expectations.
    ///
    /// This is a debug/test-only consistency check. In release builds, this may be a no-op.
    /// The default implementation does nothing. Override this to add custom consistency checks
    /// during development and testing.
    #[allow(private_interfaces)]
    fn check_allocated_pages(&self, _region: u32, _allocated_pages: &[PageNumber]) {}

    /// Returns the total number of pages (allocated + free) managed by this allocator.
    fn len(&self) -> u32;

    /// Returns true if the allocator manages zero pages.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Resizes the allocator to manage `new_size` total pages.
    ///
    /// If growing, new pages are marked as free. If shrinking, the caller must ensure
    /// that no allocated pages exist beyond `new_size`.
    fn resize(&mut self, new_size: u32);

    /// Allocates the lowest-indexed page of the given order.
    ///
    /// Returns the page index at the given order, or `None` if no page is available.
    /// This is used for system allocations that benefit from stable, low page indices.
    fn alloc_lowest(&mut self, order: u8) -> Option<u32>;

    /// Allocates any available page of the given order.
    ///
    /// Returns the page index at the given order, or `None` if no page is available.
    fn alloc(&mut self, order: u8) -> Option<u32>;

    /// Records that a page has been allocated, without going through the normal allocation path.
    ///
    /// The page may currently be free at any order — the allocator must split parent pages
    /// as necessary. This is used during recovery to rebuild allocator state from the page tree.
    fn record_alloc(&mut self, page_number: u32, order: u8);

    /// Frees a previously allocated page.
    ///
    /// The caller guarantees that the page at `(page_number, order)` was previously allocated.
    /// The allocator should merge buddy pages as appropriate.
    fn free(&mut self, page_number: u32, order: u8);
}

/// Factory for creating [`PageAllocator`] instances.
///
/// This trait is used to construct allocators for new regions and to deserialize
/// allocator state from disk. Implement this trait alongside [`PageAllocator`] to
/// provide a custom allocation strategy.
///
/// The factory must be `Send + Sync` so it can be shared across threads within the
/// database.
pub trait PageAllocatorFactory: Send + Sync {
    /// Creates a new allocator for a region with `num_pages` pages and a maximum
    /// capacity of `max_page_capacity` pages.
    ///
    /// All pages should initially be marked as free.
    fn create(&self, num_pages: u32, max_page_capacity: u32) -> Box<dyn PageAllocator>;

    /// Deserializes an allocator from bytes previously produced by [`PageAllocator::to_vec`].
    fn deserialize(&self, data: &[u8]) -> Box<dyn PageAllocator>;
}
