use crate::tree_store::page_store::bitmap::{
    BtreeBitmap, BtreeBitmapMut, U64GroupedBitmap, U64GroupedBitmapMut,
};
use crate::tree_store::page_store::page_manager::MAX_MAX_PAGE_ORDER;
use crate::tree_store::PageNumber;
use std::cmp::min;
use std::collections::HashSet;
use std::mem::size_of;

const MAX_ORDER_OFFSET: usize = 0;
const PADDING: usize = 3;
const NUM_PAGES_OFFSET: usize = MAX_ORDER_OFFSET + size_of::<u8>() + PADDING;
const FREE_END_OFFSETS: usize = NUM_PAGES_OFFSET + size_of::<u32>();

fn calculate_usable_order(pages: u32) -> u8 {
    let max_order = (32 - pages.leading_zeros() - 1).try_into().unwrap();
    min(MAX_MAX_PAGE_ORDER, max_order)
}

fn get_order_allocated_start(data: &[u8], i: u8) -> usize {
    if i == 0 {
        get_allocated_data_start(data)
    } else {
        get_order_allocated_end(data, i - 1)
    }
}

fn get_order_allocated_end(data: &[u8], i: u8) -> usize {
    let index =
        FREE_END_OFFSETS + (get_max_order(data) as usize + 1 + (i as usize)) * size_of::<u32>();
    u32::from_le_bytes(data[index..(index + size_of::<u32>())].try_into().unwrap())
        .try_into()
        .unwrap()
}

fn get_allocated_data_start(data: &[u8]) -> usize {
    // Allocated structs starts after the freed index
    get_order_free_end(data, get_max_order(data))
}

fn get_order_allocated_bytes(data: &[u8], order: u8) -> &[u8] {
    let start = get_order_allocated_start(data, order);
    let end = get_order_allocated_end(data, order);
    &data[start..end]
}

fn get_order_allocated_bytes_mut(data: &mut [u8], order: u8) -> &mut [u8] {
    let start = get_order_allocated_start(data, order);
    let end = get_order_allocated_end(data, order);
    &mut data[start..end]
}

fn get_order_free_start(data: &[u8], i: u8) -> usize {
    if i == 0 {
        get_free_data_start(data)
    } else {
        get_order_free_end(data, i - 1)
    }
}

fn get_order_free_end(data: &[u8], i: u8) -> usize {
    let index = FREE_END_OFFSETS + (i as usize) * size_of::<u32>();
    u32::from_le_bytes(data[index..(index + size_of::<u32>())].try_into().unwrap())
        .try_into()
        .unwrap()
}

fn get_free_data_start(data: &[u8]) -> usize {
    FREE_END_OFFSETS + 2 * ((get_max_order(data) + 1) as usize) * size_of::<u32>()
}

fn get_order_free_bytes(data: &[u8], order: u8) -> &[u8] {
    let start = get_order_free_start(data, order);
    let end = get_order_free_end(data, order);
    &data[start..end]
}

fn get_order_free_bytes_mut(data: &mut [u8], order: u8) -> &mut [u8] {
    let start = get_order_free_start(data, order);
    let end = get_order_free_end(data, order);
    &mut data[start..end]
}

fn get_max_order(data: &[u8]) -> u8 {
    data[MAX_ORDER_OFFSET]
}

fn get_num_pages(data: &[u8]) -> u32 {
    u32::from_le_bytes(
        data[NUM_PAGES_OFFSET..(NUM_PAGES_OFFSET + size_of::<u32>())]
            .try_into()
            .unwrap(),
    )
}

fn next_higher_order(page_number: u32) -> u32 {
    page_number / 2
}

fn buddy_page(page_number: u32) -> u32 {
    page_number ^ 1
}

// Handles allocation of dynamically sized pages, supports pages of up to page_size * 2^max_order bytes
//
// Pages are marked free at only a single order, and it must always be the largest order
//
// Data structure format:
// max_order: u8
// padding: 3 bytes
// num_pages: u32
// free_ends: array of u32, with ending offset for BtreeBitmap structure for the given order
// allocated_ends: array of u32, with ending offset for U64GroupedBitmap structure for the given order
// ... BtreeBitmap structures
pub(crate) struct BuddyAllocator<'a> {
    data: &'a [u8],
}

impl<'a> BuddyAllocator<'a> {
    pub(super) fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    #[inline]
    pub(crate) fn highest_free_order(&self) -> Option<u8> {
        (0..=self.get_max_order())
            .rev()
            .find(|order| self.get_order_free(*order).has_unset())
    }

    pub(crate) fn count_allocated_pages(&self) -> u32 {
        self.len() - self.count_free_pages()
    }

    pub(crate) fn count_free_pages(&self) -> u32 {
        let mut pages = 0;
        for order in 0..=self.get_max_order() {
            pages += self.get_order_free(order).count_unset() * 2u32.pow(order.try_into().unwrap());
        }
        pages
    }

    pub(crate) fn capacity(&self) -> u32 {
        self.get_order_free(0).len()
    }

    fn find_free_order(&self, mut page: u32) -> Option<u8> {
        for order in 0..=self.get_max_order() {
            if !self.get_order_free(order).get(page) {
                return Some(order);
            }
            page = next_higher_order(page);
        }
        None
    }

    pub(crate) fn trailing_free_pages(&self) -> u32 {
        let mut free_pages = 0;
        let mut next_page = self.len() - 1;
        while let Some(order) = self.find_free_order(next_page) {
            let order_size = 2u32.pow(order.into());
            free_pages += order_size;
            if order_size > next_page {
                break;
            }
            next_page -= order_size;
        }

        free_pages
    }

    // Reduced state for savepoint, which includes only the list of allocated pages
    // Format:
    // 1 byte: max order
    // 4 bytes: num pages
    // 4 * (max order + 1) bytes: end offsets
    pub(crate) fn make_state_for_savepoint(&self) -> Vec<u8> {
        let mut result = vec![self.get_max_order()];
        result.extend(self.len().to_le_bytes());

        let mut data_offset = result.len() + size_of::<u32>() * (self.get_max_order() as usize + 1);
        for order in 0..=self.get_max_order() {
            let bytes = get_order_allocated_bytes(self.data, order);
            data_offset += bytes.len();
            result.extend(u32::try_from(data_offset).unwrap().to_le_bytes());
        }

        for order in 0..=self.get_max_order() {
            result.extend(get_order_allocated_bytes(self.data, order));
        }

        result
    }

    pub(crate) fn get_allocated_pages_since_savepoint(
        &self,
        region: u32,
        state: &[u8],
        output: &mut Vec<PageNumber>,
    ) {
        let max_order = state[0];
        let num_pages = u32::from_le_bytes(state[1..5].try_into().unwrap());

        let mut data_start = 5 + size_of::<u32>() * (max_order as usize + 1);

        for order in 0..=max_order {
            let offset_index = 5 + size_of::<u32>() * (order as usize);
            let data_end = u32::from_le_bytes(
                state[offset_index..(offset_index + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let bytes = &state[data_start..data_end];
            let savepoint_allocated = U64GroupedBitmap::new(bytes);
            let self_allocated = self.get_order_allocated(order);
            for i in self_allocated.difference(savepoint_allocated) {
                if i >= num_pages {
                    break;
                }
                output.push(PageNumber::new(region, i, order));
            }
            data_start = data_end;
        }
    }

    pub(crate) fn get_allocated_pages(&self, region: u32) -> HashSet<PageNumber> {
        let mut result = HashSet::new();

        for order in 0..=self.get_max_order() {
            let allocated = self.get_order_allocated(order);
            for i in allocated.iter() {
                if i >= self.len() {
                    break;
                }
                result.insert(PageNumber::new(region, i, order));
            }
        }

        #[cfg(test)]
        // Check the result against the free index to be sure it matches
        {
            let mut free_check = HashSet::new();
            for i in 0..self.len() {
                if self.find_free_order(i).is_none() {
                    free_check.insert(PageNumber::new(region, i, 0));
                }
            }

            let mut check_result = HashSet::new();
            for page in result.iter() {
                check_result.extend(page.to_order0());
            }
            assert_eq!(free_check, check_result);
        }

        result
    }

    fn get_max_order(&self) -> u8 {
        self.data[0]
    }

    pub(crate) fn len(&self) -> u32 {
        get_num_pages(self.data)
    }

    fn get_order_free(&self, order: u8) -> BtreeBitmap {
        assert!(order <= self.get_max_order());
        BtreeBitmap::new(get_order_free_bytes(self.data, order))
    }

    fn get_order_allocated(&self, order: u8) -> U64GroupedBitmap {
        assert!(order <= self.get_max_order());
        U64GroupedBitmap::new(get_order_allocated_bytes(self.data, order))
    }
}

pub(crate) struct BuddyAllocatorMut<'a> {
    data: &'a mut [u8],
}

impl<'a> BuddyAllocatorMut<'a> {
    pub(super) fn new(data: &'a mut [u8]) -> Self {
        Self { data }
    }

    pub(crate) fn init_new(data: &'a mut [u8], num_pages: u32, max_page_capacity: u32) -> Self {
        let max_order = calculate_usable_order(max_page_capacity);
        assert!(data.len() >= Self::required_space(max_page_capacity));
        data[MAX_ORDER_OFFSET] = max_order;
        data[NUM_PAGES_OFFSET..(NUM_PAGES_OFFSET + size_of::<u32>())]
            .copy_from_slice(&num_pages.to_le_bytes());

        let mut metadata_offset = FREE_END_OFFSETS;
        let mut data_offset = get_free_data_start(data);

        let mut pages_for_order = max_page_capacity;
        for order in 0..=max_order {
            let required = BtreeBitmapMut::required_space(pages_for_order);
            data_offset += required;
            data[metadata_offset..metadata_offset + size_of::<u32>()]
                .copy_from_slice(&u32::try_from(data_offset).unwrap().to_le_bytes());
            BtreeBitmapMut::init_new(get_order_free_bytes_mut(data, order), pages_for_order);
            pages_for_order = next_higher_order(pages_for_order);
            metadata_offset += size_of::<u32>();
        }

        // Mark the available pages, starting with the highest order
        let mut accounted_pages = 0;
        for order in (0..=max_order).rev() {
            let order_data = get_order_free_bytes_mut(data, order);
            let mut allocator = BtreeBitmapMut::new(order_data);
            let order_size = 2u32.pow(order.into());
            while accounted_pages + order_size <= num_pages {
                let page = accounted_pages / order_size;
                allocator.clear(page);
                accounted_pages += order_size;
            }
        }
        assert_eq!(accounted_pages, num_pages);

        // Initialize the allocated page index
        assert_eq!(data_offset, get_allocated_data_start(data));
        let mut pages_for_order = max_page_capacity;
        for order in 0..=max_order {
            let required = U64GroupedBitmapMut::required_bytes(pages_for_order);
            data_offset += required;
            data[metadata_offset..metadata_offset + size_of::<u32>()]
                .copy_from_slice(&u32::try_from(data_offset).unwrap().to_le_bytes());
            let order_bytes = get_order_allocated_bytes_mut(data, order);
            U64GroupedBitmapMut::init_empty(order_bytes);
            pages_for_order = next_higher_order(pages_for_order);
            metadata_offset += size_of::<u32>();
        }

        Self { data }
    }

    pub(crate) fn resize(&mut self, new_size: u32) {
        self.debug_check_consistency();
        assert!(new_size <= self.capacity());
        if new_size > self.len() {
            let mut processed_pages = self.len();
            // Align to the highest order possible
            while processed_pages < new_size {
                let order: u8 = processed_pages.trailing_zeros().try_into().unwrap();
                let order_size = 2u32.pow(order.into());
                let page = processed_pages / order_size;
                debug_assert_eq!(processed_pages % order_size, 0);
                if order >= self.get_max_order() || processed_pages + order_size > new_size {
                    break;
                }
                self.free_inner(page, order);
                processed_pages += order_size;
            }
            // Allocate the remaining space, at the highest order
            for order in (0..=self.get_max_order()).rev() {
                let order_size = 2u32.pow(order.into());
                while processed_pages + order_size <= new_size {
                    let page = processed_pages / order_size;
                    self.free_inner(page, order);
                    processed_pages += order_size;
                }
            }
            assert_eq!(processed_pages, new_size);
            self.debug_check_consistency();
        } else {
            let mut processed_pages = new_size;
            // Align to the highest order possible
            while processed_pages < self.len() {
                let order: u8 = processed_pages.trailing_zeros().try_into().unwrap();
                let order_size = 2u32.pow(order.into());
                let page = processed_pages / order_size;
                debug_assert_eq!(processed_pages % order_size, 0);
                if order >= self.get_max_order() || processed_pages + order_size > self.len() {
                    break;
                }
                self.record_alloc_inner(page, order);
                processed_pages += order_size;
            }
            // Allocate the remaining space, at the highest order
            for order in (0..=self.get_max_order()).rev() {
                let order_size = 2u32.pow(order.into());
                while processed_pages + order_size <= self.len() {
                    let page = processed_pages / order_size;
                    self.record_alloc_inner(page, order);
                    processed_pages += order_size;
                }
            }
            assert_eq!(processed_pages, self.len());
        }
        self.data[NUM_PAGES_OFFSET..(NUM_PAGES_OFFSET + size_of::<u32>())]
            .copy_from_slice(&new_size.to_le_bytes());
    }

    #[allow(unused_variables)]
    fn debug_check_consistency(&self) {
        // Don't enable when fuzzing, because this is kind of expensive
        #[cfg(all(debug_assertions, not(fuzzing)))]
        {
            let mut processed = 0;
            // Ensure that no page is free at multiple orders
            while processed < self.len() {
                let mut found = false;
                let mut page = processed;
                for order in 0..=self.get_max_order() {
                    let order_data = get_order_free_bytes(self.data, order);
                    let allocator = BtreeBitmap::new(order_data);
                    if !allocator.get(page) {
                        assert!(!found);
                        found = true;
                    }
                    page = next_higher_order(page);
                }
                processed += 1;
            }

            // Ensure that all buddy pages are merged, except at the highest order
            for order in (0..self.get_max_order()).rev() {
                let order_len = self.len() / (2u32.pow(order.into()));
                let order_bytes = get_order_free_bytes(self.data, order);
                let allocator = BtreeBitmap::new(order_bytes);
                for page in 0..order_len {
                    if !allocator.get(page) {
                        let buddy = buddy_page(page);
                        let buddy_allocated = allocator.get(buddy);
                        assert!(buddy_allocated, "order={order} page={page} buddy={buddy}",);
                    }
                }
            }
        }
    }

    /// Returns the number of bytes required for the data argument of new()
    pub(crate) fn required_space(mut capacity: u32) -> usize {
        let max_order = calculate_usable_order(capacity) as usize;
        let mut required = FREE_END_OFFSETS + 2 * (max_order + 1) * size_of::<u32>();
        for _ in 0..=max_order {
            // Index of free pages
            required += BtreeBitmapMut::required_space(capacity);
            // Index of allocated pages
            required += U64GroupedBitmapMut::required_bytes(capacity);
            capacity = next_higher_order(capacity);
        }

        required
    }

    pub(crate) fn capacity(&self) -> u32 {
        BuddyAllocator::new(self.data).capacity()
    }

    pub(crate) fn len(&self) -> u32 {
        get_num_pages(self.data)
    }

    #[cfg(test)]
    fn count_allocated_pages(&self) -> u32 {
        BuddyAllocator::new(self.data).count_allocated_pages()
    }

    pub(crate) fn highest_free_order(&self) -> Option<u8> {
        BuddyAllocator::new(self.data).highest_free_order()
    }

    // Allocates a page of the given order at the lowest index possible. Will split a higher order page,
    // to get one of lower index
    pub(crate) fn alloc_lowest(&mut self, order: u8) -> Option<u32> {
        let page = self.alloc_lowest_inner(order);
        if let Some(page_number) = page {
            debug_assert!(!self.get_order_allocated(order).get(page_number));
            self.get_order_allocated_mut(order).set(page_number);
        }
        page
    }

    pub(crate) fn alloc_lowest_inner(&mut self, order: u8) -> Option<u32> {
        // Lowest index at the requested order
        let mut best_index_at_order = self.alloc_inner(order)?;
        // Best (index, order) found
        let mut best = (best_index_at_order, order);

        // Find the lowest index at which we can fill this allocation, across all orders
        let mut multiplier = 2;
        for i in (order + 1)..=self.get_max_order() {
            if let Some(index) = self.alloc_inner(i) {
                let index_at_order = index * multiplier;
                if index_at_order < best_index_at_order {
                    self.free_inner(best.0, best.1);
                    best_index_at_order = index_at_order;
                    best = (index, i);
                } else {
                    self.free_inner(index, i);
                }
            }
            multiplier *= 2;
        }

        // Split the page, until we get to the requested order
        while best.1 > order {
            let (best_index, best_order) = best;
            let (free1, free2) = (best_index * 2, best_index * 2 + 1);
            let mut allocator = self.get_order_free_mut(best_order - 1);
            debug_assert!(allocator.get(free1));
            debug_assert!(allocator.get(free2));
            allocator.clear(free2);
            best = (free1, best_order - 1);
        }
        assert_eq!(best.0, best_index_at_order);

        Some(best.0)
    }

    pub(crate) fn alloc(&mut self, order: u8) -> Option<u32> {
        let page = self.alloc_inner(order);
        if let Some(page_number) = page {
            debug_assert!(!self.get_order_allocated(order).get(page_number));
            self.get_order_allocated_mut(order).set(page_number);
        }
        page
    }

    pub(crate) fn alloc_inner(&mut self, order: u8) -> Option<u32> {
        if order > self.get_max_order() {
            return None;
        }
        let mut allocator = self.get_order_free_mut(order);
        if let Some(x) = allocator.alloc() {
            Some(x)
        } else {
            // Try to allocate a higher order page and split it
            drop(allocator);
            let upper_page = self.alloc_inner(order + 1)?;
            let (free1, free2) = (upper_page * 2, upper_page * 2 + 1);
            let mut allocator = self.get_order_free_mut(order);
            debug_assert!(allocator.get(free1));
            debug_assert!(allocator.get(free2));
            allocator.clear(free2);

            Some(free1)
        }
    }

    /// data must have been initialized by Self::init_new(), and page_number must be free
    pub(crate) fn record_alloc(&mut self, page_number: u32, order: u8) {
        assert!(order <= self.get_max_order());
        // Only record the allocation for the actual page
        self.get_order_allocated_mut(order).set(page_number);
        // Split parent pages as necessary, and update the free index
        self.record_alloc_inner(page_number, order);
    }

    pub(crate) fn record_alloc_inner(&mut self, page_number: u32, order: u8) {
        let mut allocator = self.get_order_free_mut(order);
        if allocator.get(page_number) {
            // Need to split parent page
            let upper_page = next_higher_order(page_number);
            drop(allocator);
            self.record_alloc_inner(upper_page, order + 1);
            let mut allocator = self.get_order_free_mut(order);

            let (free1, free2) = (upper_page * 2, upper_page * 2 + 1);
            debug_assert!(free1 == page_number || free2 == page_number);
            if free1 == page_number {
                allocator.clear(free2);
            } else {
                allocator.clear(free1);
            }
        } else {
            allocator.set(page_number);
        }
    }

    /// data must have been initialized by Self::init_new()
    pub(crate) fn free(&mut self, page_number: u32, order: u8) {
        debug_assert!(self.get_order_free_mut(order).get(page_number));
        debug_assert!(self.get_order_allocated(order).get(page_number));

        self.get_order_allocated_mut(order).clear(page_number);

        // Update the free index and merge free pages
        self.free_inner(page_number, order);
    }

    pub(crate) fn free_inner(&mut self, page_number: u32, order: u8) {
        if order == self.get_max_order() {
            let mut allocator = self.get_order_free_mut(order);
            allocator.clear(page_number);
            return;
        }

        let mut allocator = self.get_order_free_mut(order);
        let buddy = buddy_page(page_number);
        if allocator.get(buddy) {
            allocator.clear(page_number);
        } else {
            // Merge into higher order page
            allocator.set(buddy);
            self.free_inner(next_higher_order(page_number), order + 1);
        }
    }

    pub(crate) fn is_allocated(&self, page_number: u32, order: u8) -> bool {
        self.get_order_allocated(order).get(page_number)
    }

    pub(super) fn get_max_order(&self) -> u8 {
        self.data[0]
    }

    fn get_order_free_mut(&mut self, order: u8) -> BtreeBitmapMut {
        assert!(order <= self.get_max_order());
        BtreeBitmapMut::new(get_order_free_bytes_mut(self.data, order))
    }

    fn get_order_allocated_mut(&mut self, order: u8) -> U64GroupedBitmapMut {
        assert!(order <= self.get_max_order());
        U64GroupedBitmapMut::new(get_order_allocated_bytes_mut(self.data, order))
    }

    fn get_order_allocated(&self, order: u8) -> U64GroupedBitmap {
        assert!(order <= self.get_max_order());
        U64GroupedBitmap::new(get_order_allocated_bytes(self.data, order))
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::buddy_allocator::BuddyAllocatorMut;

    #[test]
    fn record_alloc_buddy() {
        let num_pages = 256;
        let mut data = vec![0; BuddyAllocatorMut::required_space(num_pages)];
        let mut allocator = BuddyAllocatorMut::init_new(&mut data, num_pages, num_pages);
        assert_eq!(allocator.count_allocated_pages(), 0);

        for page in 0..num_pages {
            allocator.record_alloc(page, 0);
        }
        assert_eq!(allocator.count_allocated_pages(), num_pages);

        assert!(allocator.alloc(0).is_none());

        for page in 0..num_pages {
            allocator.free(page, 0);
        }
        assert_eq!(allocator.count_allocated_pages(), 0);
    }

    #[test]
    fn buddy_merge() {
        let num_pages = 256;
        let mut data = vec![0; BuddyAllocatorMut::required_space(num_pages)];
        let mut allocator = BuddyAllocatorMut::init_new(&mut data, num_pages, num_pages);
        assert_eq!(allocator.count_allocated_pages(), 0);

        for _ in 0..num_pages {
            allocator.alloc(0).unwrap();
        }
        for page in 0..num_pages {
            allocator.free(page, 0);
        }
        assert_eq!(allocator.count_allocated_pages(), 0);

        // Test that everything got merged back together, so that we fill order 7 allocations
        for _ in 0..(num_pages / 2u32.pow(7)) {
            allocator.alloc(7).unwrap();
        }
    }

    #[test]
    fn alloc_large() {
        let num_pages = 256;
        let max_order = 7;
        let mut data = vec![0; BuddyAllocatorMut::required_space(num_pages)];
        let mut allocator = BuddyAllocatorMut::init_new(&mut data, num_pages, num_pages);
        assert_eq!(allocator.count_allocated_pages(), 0);

        let mut allocated = vec![];
        for order in 0..=max_order {
            allocated.push((allocator.alloc(order).unwrap(), order));
        }
        assert_eq!(allocator.count_allocated_pages(), num_pages - 1);

        for order in 1..=max_order {
            assert!(allocator.alloc(order).is_none());
        }

        for (page, order) in allocated {
            allocator.free(page, order);
        }
        assert_eq!(allocator.count_allocated_pages(), 0);
    }
}
