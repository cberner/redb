use crate::Error;
use std::convert::TryInto;
use std::mem::size_of;

struct U64GroupedBitMap<'a> {
    data: &'a mut [u8],
}

impl<'a> U64GroupedBitMap<'a> {
    fn data_index_of(&self, bit: usize) -> (usize, usize) {
        ((bit / 64) as usize * size_of::<u64>(), (bit % 64) as usize)
    }

    fn select_mask(bit: usize) -> u64 {
        1u64 << (bit as u64)
    }

    fn len(&self) -> usize {
        self.data.len() * 8
    }

    // Returns true iff the bit's group is all set
    fn set(&mut self, bit: usize) -> bool {
        let (index, bit_index) = self.data_index_of(bit);
        let mut group = u64::from_be_bytes(self.data[index..(index + 8)].try_into().unwrap());
        group |= Self::select_mask(bit_index);
        self.data[index..(index + 8)].copy_from_slice(&group.to_be_bytes());

        group == u64::MAX
    }

    fn get(&mut self, bit: usize) -> bool {
        let (index, bit_index) = self.data_index_of(bit);
        let group = u64::from_be_bytes(self.data[index..(index + 8)].try_into().unwrap());
        group & Self::select_mask(bit_index) != 0
    }

    fn clear(&mut self, bit: usize) {
        let (index, bit_index) = self.data_index_of(bit);
        let mut group = u64::from_be_bytes(self.data[index..(index + 8)].try_into().unwrap());
        group &= !Self::select_mask(bit_index);
        self.data[index..(index + 8)].copy_from_slice(&group.to_be_bytes());
    }

    fn first_unset(&self, start_bit: usize, end_bit: usize) -> Option<usize> {
        assert_eq!(start_bit % 64, 0);
        assert_eq!(end_bit, start_bit + 64);

        let (index, _) = self.data_index_of(start_bit);
        let group = u64::from_be_bytes(self.data[index..(index + 8)].try_into().unwrap());
        match group.trailing_ones() {
            64 => None,
            x => Some(start_bit + x as usize),
        }
    }

    fn count_unset(&self) -> usize {
        self.data.iter().map(|x| x.count_zeros() as usize).sum()
    }
}

struct PageAllocator {
    num_pages: usize,
    tree_level_offsets: Vec<(usize, usize)>,
}

// Stores a 64-way bit-tree of allocated pages.
// Does not hold a reference to the data, so that this structure can be initialized once, without
// borrowing the data array
//
// Data structure format:
// num_pages: big endian u64
// root: u64
// subtree layer: 2-64 u64s
// ...consecutive layers. Except for the last level, all sub-trees of the root must be complete
impl PageAllocator {
    fn new(num_pages: usize) -> Self {
        let mut tree_level_offsets = vec![];

        let mut offset = 0;
        // Skip the num_pages header
        offset += size_of::<u64>();
        // root level
        tree_level_offsets.push((offset, offset + size_of::<u64>()));
        offset += size_of::<u64>();

        // Intermediate levels
        if Self::required_tree_height(num_pages) > 2 {
            for i in 1..(Self::required_tree_height(num_pages) - 1) {
                let len = Self::required_subtrees(num_pages) * 64usize.pow(i as u32) / 8;
                tree_level_offsets.push((offset, offset + len));
                offset += len;
            }
        }

        // Leaf level
        if Self::required_tree_height(num_pages) > 1 {
            let len = (num_pages + 63) / 64 * size_of::<u64>();
            tree_level_offsets.push((offset, offset + len));
            offset += len;
        }

        assert_eq!(
            tree_level_offsets.len(),
            Self::required_tree_height(num_pages)
        );
        assert_eq!(offset, Self::required_space(num_pages));

        Self {
            num_pages,
            tree_level_offsets,
        }
    }

    fn init_new(data: &mut [u8], num_pages: usize) -> Self {
        assert!(data.len() >= Self::required_space(num_pages));
        // Zero the memory, so that all pages are free
        for value in data.iter_mut() {
            *value = 0;
        }

        data[..8].copy_from_slice(&(num_pages as u64).to_be_bytes());

        let result = Self::new(num_pages);

        // Mark all the subtrees that don't exist
        for i in Self::required_subtrees(num_pages)..64 {
            result.get_level(data, 0).set(i);
        }

        if result.get_height() > 1 {
            // Mark excess space in the leaves
            let mut leaf_level = result.get_level(data, result.get_height() - 1);
            for i in num_pages..leaf_level.len() {
                leaf_level.set(i);
            }
        }

        if result.get_height() > 2 {
            // Mark excess index space in the last subtree
            let total_indexable_pages = result.get_level(data, result.get_height() - 2).len() * 64;
            for i in (num_pages + 63)..total_indexable_pages {
                result.update_to_root(data, i, true);
            }
        }

        result
    }

    /// Returns the number of bytes required for the data argument of new()
    fn required_space(num_pages: usize) -> usize {
        if Self::required_tree_height(num_pages) == 1 {
            assert!(num_pages <= 64);
            // Space for num_pages header, and root
            2 * size_of::<u64>()
        } else if Self::required_tree_height(num_pages) == 2 {
            // Space for num_pages header, and root
            2 * size_of::<u64>() +
                // Space for the leaves
                (num_pages + 63) / 64 * size_of::<u64>()
        } else {
            // Space for num_pages header, and root
            2 * size_of::<u64>() +
                // Space for the subtrees
                Self::required_subtrees(num_pages) * Self::required_interior_bytes_per_subtree(num_pages) +
                // Space for the leaves
                (num_pages + 63) / 64 * size_of::<u64>()
        }
    }

    fn required_interior_bytes_per_subtree(num_pages: usize) -> usize {
        let subtree_height = Self::required_tree_height(num_pages) - 1;
        (1..subtree_height)
            .map(|i| 64usize.pow(i as u32))
            .sum::<usize>()
            / 8
    }

    fn required_subtrees(num_pages: usize) -> usize {
        let height = Self::required_tree_height(num_pages);
        let pages_per_subtree = 64usize.pow((height - 1) as u32);

        (num_pages + pages_per_subtree - 1) / pages_per_subtree
    }

    fn required_tree_height(num_pages: usize) -> usize {
        let mut height = 1;
        let mut storable = 64;
        while num_pages > storable {
            storable *= 64;
            height += 1;
        }

        height
    }

    fn count_free_pages(&self, data: &mut [u8]) -> usize {
        self.get_level(data, self.get_height() - 1).count_unset()
    }

    fn get_level<'a>(&self, data: &'a mut [u8], i: usize) -> U64GroupedBitMap<'a> {
        let (start, end) = self.tree_level_offsets[i];
        U64GroupedBitMap {
            data: &mut data[start..end],
        }
    }

    fn get_num_pages(&self) -> u64 {
        self.num_pages as u64
    }

    fn get_height(&self) -> usize {
        self.tree_level_offsets.len()
    }

    // Recursively update to the root, starting at the given entry in the given height
    // full parameter must be set if all bits in the entry's group of u64 are full
    fn update_to_root(&self, data: &mut [u8], page_number: usize, mut full: bool) {
        if self.get_height() == 1 {
            return;
        }

        let mut parent_height = self.get_height() - 2;
        let mut parent_entry = page_number / 64;
        loop {
            full = if full {
                self.get_level(data, parent_height).set(parent_entry)
            } else {
                self.get_level(data, parent_height).clear(parent_entry);
                false
            };

            if parent_height == 0 {
                break;
            }
            parent_height -= 1;
            parent_entry /= 64;
        }
    }

    fn is_allocated(&self, data: &mut [u8], page_number: u64) -> bool {
        self.get_level(data, self.get_height() - 1)
            .get(page_number as usize)
    }

    /// data must have been initialized by Self::init_new()
    fn alloc(&self, data: &mut [u8]) -> Result<u64, Error> {
        if let Some(mut entry) = self.get_level(data, 0).first_unset(0, 64) {
            let mut height = 0;

            while height < self.get_height() - 1 {
                height += 1;
                entry *= 64;
                entry = self
                    .get_level(data, height)
                    .first_unset(entry, entry + 64)
                    .unwrap();
            }

            assert!(entry < self.get_num_pages() as usize);
            self.record_alloc(data, entry as u64);
            Ok(entry as u64)
        } else {
            Err(Error::OutOfSpace)
        }
    }

    /// data must have been initialized by Self::init_new()
    fn record_alloc(&self, data: &mut [u8], page_number: u64) {
        assert!(page_number < self.get_num_pages());
        let full = self
            .get_level(data, self.get_height() - 1)
            .set(page_number as usize);
        self.update_to_root(data, page_number as usize, full);
    }

    /// data must have been initialized by Self::init_new()
    fn free(&self, data: &mut [u8], page_number: u64) {
        assert!(page_number < self.get_num_pages());
        self.get_level(data, self.get_height() - 1)
            .clear(page_number as usize);
        self.update_to_root(data, page_number as usize, false);
    }
}

pub(crate) struct BuddyAllocator {
    orders: Vec<PageAllocator>,
}

// Handles allocation of dynamically sized pages, supports pages of up to page_size * 2^max_order bytes
//
// Data structure format:
// max_order: big endian u64
// order_offsets: array of (u64, u64), with offset & length for PageAllocator structure for the given order
// ... PageAllocator structures
impl BuddyAllocator {
    pub(crate) fn new(mut num_pages: usize, max_order: usize) -> Self {
        let mut orders = vec![];
        for _ in 0..=max_order {
            orders.push(PageAllocator::new(num_pages));
            num_pages = Self::higher_order(num_pages);
        }

        Self { orders }
    }

    pub(crate) fn init_new(data: &mut [u8], mut num_pages: usize, max_order: usize) -> Self {
        assert!(data.len() >= Self::required_space(num_pages, max_order));
        data[..size_of::<u64>()].copy_from_slice(&(max_order as u64).to_be_bytes());

        let mut metadata_offset = size_of::<u64>();
        let mut data_offset = size_of::<u64>() + 2 * (max_order + 1) * size_of::<u64>();

        let mut orders = vec![];
        for order in 0..=max_order {
            let required = PageAllocator::required_space(num_pages);
            data[metadata_offset..metadata_offset + size_of::<u64>()]
                .copy_from_slice(&(data_offset as u64).to_be_bytes());
            data[metadata_offset + size_of::<u64>()..metadata_offset + 2 * size_of::<u64>()]
                .copy_from_slice(&(required as u64).to_be_bytes());
            orders.push(PageAllocator::init_new(
                Self::get_order_bytes(data, order),
                num_pages,
            ));
            num_pages = Self::higher_order(num_pages);
            metadata_offset += 2 * size_of::<u64>();
            data_offset += required;
        }

        // Mark all the lower pages which exist in upper orders
        for (order, allocator) in orders.iter().enumerate() {
            if let Some(next_allocator) = orders.get(order + 1) {
                let next_bytes = Self::get_order_bytes(data, order + 1);
                let next_pages = next_allocator.count_free_pages(next_bytes) as u64;
                let bytes = Self::get_order_bytes(data, order);
                for i in 0..(2 * next_pages) {
                    allocator.record_alloc(bytes, i);
                }
            }
        }

        Self { orders }
    }

    /// Returns the number of bytes required for the data argument of new()
    pub(crate) fn required_space(mut num_pages: usize, max_order: usize) -> usize {
        let mut required = size_of::<u64>() + 2 * (max_order + 1) * size_of::<u64>();
        for _ in 0..=max_order {
            required += PageAllocator::required_space(num_pages);
            num_pages = Self::higher_order(num_pages);
        }

        required
    }

    pub(crate) fn count_free_pages(&self, data: &mut [u8]) -> usize {
        let mut pages = 0;
        for (order, allocator) in self.orders.iter().enumerate() {
            pages += allocator.count_free_pages(Self::get_order_bytes(data, order))
                * 2usize.pow(order as u32);
        }

        pages
    }

    fn get_order_offset_and_length(data: &[u8], order: usize) -> (usize, usize) {
        let max_order = u64::from_be_bytes(data[..size_of::<u64>()].try_into().unwrap()) as usize;
        assert!(order <= max_order);
        let base = size_of::<u64>() + order * 2 * size_of::<u64>();
        let offset =
            u64::from_be_bytes(data[base..base + size_of::<u64>()].try_into().unwrap()) as usize;
        let length = u64::from_be_bytes(
            data[base + size_of::<u64>()..base + 2 * size_of::<u64>()]
                .try_into()
                .unwrap(),
        ) as usize;

        (offset, length)
    }

    fn get_order_bytes(data: &mut [u8], order: usize) -> &mut [u8] {
        let (offset, length) = Self::get_order_offset_and_length(data, order);
        &mut data[offset..(offset + length)]
    }

    fn higher_order(page_number: usize) -> usize {
        page_number / 2
    }

    fn buddy_page(page_number: usize) -> usize {
        page_number ^ 1
    }

    /// data must have been initialized by Self::init_new()
    pub(crate) fn alloc(&self, data: &mut [u8], order: usize) -> Result<u64, Error> {
        if order >= self.orders.len() {
            return Err(Error::OutOfSpace);
        }
        match self.orders[order].alloc(Self::get_order_bytes(data, order)) {
            Ok(x) => Ok(x),
            Err(e) => {
                match e {
                    Error::OutOfSpace => {
                        // Try to allocate a higher order page and split it
                        let upper_page = self.alloc(data, order + 1)?;
                        let (free1, free2) = (upper_page * 2, upper_page * 2 + 1);
                        debug_assert!(self.orders[order]
                            .is_allocated(Self::get_order_bytes(data, order), free1));
                        debug_assert!(self.orders[order]
                            .is_allocated(Self::get_order_bytes(data, order), free2));
                        self.orders[order].free(Self::get_order_bytes(data, order), free2);

                        Ok(free1)
                    }
                    other => Err(other),
                }
            }
        }
    }

    /// data must have been initialized by Self::init_new(), and page_number must be free
    pub(crate) fn record_alloc(&self, data: &mut [u8], page_number: u64, order: usize) {
        assert!(order < self.orders.len());
        if self.orders[order].is_allocated(Self::get_order_bytes(data, order), page_number) {
            // Need to split parent page
            // TODO: make the use of usize vs u64 consistent
            let upper_page = Self::higher_order(page_number as usize) as u64;
            self.record_alloc(data, upper_page, order + 1);

            let (free1, free2) = (upper_page * 2, upper_page * 2 + 1);
            debug_assert!(free1 == page_number || free2 == page_number);
            if free1 == page_number {
                self.orders[order].free(Self::get_order_bytes(data, order), free2);
            } else {
                self.orders[order].free(Self::get_order_bytes(data, order), free1);
            }
        } else {
            self.orders[order].record_alloc(Self::get_order_bytes(data, order), page_number);
        }
    }

    /// data must have been initialized by Self::init_new()
    pub(crate) fn free(&self, data: &mut [u8], page_number: u64, order: usize) {
        if order == self.orders.len() - 1 {
            self.orders[order].free(Self::get_order_bytes(data, order), page_number);
            return;
        }

        let buddy = Self::buddy_page(page_number as usize) as u64;
        if self.orders[order].is_allocated(Self::get_order_bytes(data, order), buddy) {
            self.orders[order].free(Self::get_order_bytes(data, order), page_number);
        } else {
            // Merge into higher order page
            self.orders[order].record_alloc(Self::get_order_bytes(data, order), buddy);
            self.free(
                data,
                Self::higher_order(page_number as usize) as u64,
                order + 1,
            );
        }
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::page_allocator::{BuddyAllocator, PageAllocator};
    use crate::Error;
    use rand::prelude::IteratorRandom;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::HashSet;
    use std::convert::TryInto;

    #[test]
    fn alloc() {
        let num_pages = 2;
        let mut data = vec![0; PageAllocator::required_space(num_pages)];
        let allocator = PageAllocator::init_new(&mut data, num_pages);
        for i in 0..num_pages {
            assert_eq!(i as u64, allocator.alloc(&mut data).unwrap());
        }
        assert!(matches!(
            allocator.alloc(&mut data).unwrap_err(),
            Error::OutOfSpace
        ));
    }

    #[test]
    fn record_alloc_buddy() {
        let num_pages = 256;
        let max_order = 7;
        let mut data = vec![0; BuddyAllocator::required_space(num_pages, max_order)];
        let allocator = BuddyAllocator::init_new(&mut data, num_pages, max_order);
        assert_eq!(allocator.count_free_pages(&mut data), num_pages);

        for page in 0..num_pages {
            allocator.record_alloc(&mut data, page as u64, 0);
        }
        assert_eq!(allocator.count_free_pages(&mut data), 0);

        assert!(matches!(
            allocator.alloc(&mut data, 0).unwrap_err(),
            Error::OutOfSpace
        ));

        for page in 0..num_pages {
            allocator.free(&mut data, page as u64, 0);
        }
        assert_eq!(allocator.count_free_pages(&mut data), num_pages);
    }

    #[test]
    fn buddy_merge() {
        let num_pages = 256;
        let max_order = 7;
        let mut data = vec![0; BuddyAllocator::required_space(num_pages, max_order)];
        let allocator = BuddyAllocator::init_new(&mut data, num_pages, max_order);
        assert_eq!(allocator.count_free_pages(&mut data), num_pages);

        for _ in 0..num_pages {
            allocator.alloc(&mut data, 0).unwrap();
        }
        for page in 0..num_pages {
            allocator.free(&mut data, page as u64, 0);
        }
        assert_eq!(allocator.count_free_pages(&mut data), num_pages);

        // Test that everything got merged back together, so that we fill order 7 allocations
        for _ in 0..(num_pages / 2usize.pow(7)) {
            allocator.alloc(&mut data, 7).unwrap();
        }
    }

    #[test]
    fn alloc_large() {
        let num_pages = 256;
        let max_order = 7;
        let mut data = vec![0; BuddyAllocator::required_space(num_pages, max_order)];
        let allocator = BuddyAllocator::init_new(&mut data, num_pages, max_order);
        assert_eq!(allocator.count_free_pages(&mut data), num_pages);

        let mut allocated = vec![];
        for order in 0..=max_order {
            allocated.push((allocator.alloc(&mut data, order).unwrap(), order));
        }
        assert_eq!(allocator.count_free_pages(&mut data), 1);

        for order in 1..=max_order {
            assert!(matches!(
                allocator.alloc(&mut data, order).unwrap_err(),
                Error::OutOfSpace
            ));
        }

        for (page, order) in allocated {
            allocator.free(&mut data, page, order);
        }
        assert_eq!(allocator.count_free_pages(&mut data), num_pages);
    }

    #[test]
    fn record_alloc() {
        let mut data = vec![0; PageAllocator::required_space(2)];
        let allocator = PageAllocator::init_new(&mut data, 2);
        allocator.record_alloc(&mut data, 0);
        assert_eq!(1, allocator.alloc(&mut data).unwrap());
        assert!(matches!(
            allocator.alloc(&mut data).unwrap_err(),
            Error::OutOfSpace
        ));
    }

    #[test]
    fn free() {
        let mut data = vec![0; PageAllocator::required_space(1)];
        let allocator = PageAllocator::init_new(&mut data, 1);
        assert_eq!(0, allocator.alloc(&mut data).unwrap());
        assert!(matches!(
            allocator.alloc(&mut data).unwrap_err(),
            Error::OutOfSpace
        ));
        allocator.free(&mut data, 0);
        assert_eq!(0, allocator.alloc(&mut data).unwrap());
    }

    #[test]
    fn reuse_lowest() {
        let num_pages = 65;
        let mut data = vec![0; PageAllocator::required_space(num_pages)];
        let allocator = PageAllocator::init_new(&mut data, num_pages);
        for i in 0..num_pages {
            assert_eq!(i as u64, allocator.alloc(&mut data).unwrap());
        }
        allocator.free(&mut data, 5);
        allocator.free(&mut data, 15);
        assert_eq!(5, allocator.alloc(&mut data).unwrap());
        assert_eq!(15, allocator.alloc(&mut data).unwrap());
        assert!(matches!(
            allocator.alloc(&mut data).unwrap_err(),
            Error::OutOfSpace
        ));
    }

    #[test]
    fn all_space_used() {
        let num_pages = 65;
        let mut data = vec![0; PageAllocator::required_space(num_pages)];
        let allocator = PageAllocator::init_new(&mut data, num_pages);
        // Allocate everything
        while allocator.alloc(&mut data).is_ok() {}
        // The last u64 must be used, since the leaf layer is compact
        let l = data.len();
        assert_ne!(0, u64::from_be_bytes(data[(l - 8)..].try_into().unwrap()));
    }

    #[test]
    fn random_pattern() {
        let seed = rand::thread_rng().gen();
        // Print the seed to debug for reproducibility, in case this test fails
        dbg!(seed);
        let mut rng = StdRng::seed_from_u64(seed);

        let num_pages = rng.gen_range(2..10000);
        let mut data = vec![0; PageAllocator::required_space(num_pages)];
        let allocator = PageAllocator::init_new(&mut data, num_pages);
        let mut allocated = HashSet::new();

        for _ in 0..(num_pages * 2) {
            if rng.gen_bool(0.75) {
                if let Ok(page) = allocator.alloc(&mut data) {
                    allocated.insert(page);
                } else {
                    assert_eq!(allocated.len(), num_pages);
                }
            } else {
                if let Some(to_free) = allocated.iter().choose(&mut rng).cloned() {
                    allocator.free(&mut data, to_free);
                    allocated.remove(&to_free);
                }
            }
        }

        for _ in allocated.len()..num_pages {
            allocator.alloc(&mut data).unwrap();
        }
        assert!(matches!(
            allocator.alloc(&mut data).unwrap_err(),
            Error::OutOfSpace
        ));

        for i in 0..num_pages {
            allocator.free(&mut data, i as u64);
        }

        for _ in 0..num_pages {
            allocator.alloc(&mut data).unwrap();
        }
        assert!(matches!(
            allocator.alloc(&mut data).unwrap_err(),
            Error::OutOfSpace
        ));
    }
}
