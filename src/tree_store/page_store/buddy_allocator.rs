use crate::tree_store::page_store::bitmap::{BtreeBitmap, U64GroupedBitmap};
use crate::tree_store::page_store::page_manager::MAX_MAX_PAGE_ORDER;
use crate::tree_store::PageNumber;
use std::cmp::min;
#[cfg(test)]
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

fn next_higher_order(page_number: u32) -> u32 {
    page_number / 2
}

fn buddy_page(page_number: u32) -> u32 {
    page_number ^ 1
}

// Handles allocation of dynamically sized pages, supports pages of up to page_size * 2^max_order bytes
//
// Pages are marked free at only a single order, and it must always be the largest order
pub(crate) struct BuddyAllocator {
    allocated: Vec<U64GroupedBitmap>,
    free: Vec<BtreeBitmap>,
    len: u32,
    max_order: u8,
}

impl BuddyAllocator {
    pub(crate) fn new(num_pages: u32, max_page_capacity: u32) -> Self {
        let max_order = calculate_usable_order(max_page_capacity);

        let mut pages_for_order = max_page_capacity;
        let mut free = vec![];
        let mut allocated = vec![];
        for _ in 0..=max_order {
            free.push(BtreeBitmap::new(pages_for_order));

            allocated.push(U64GroupedBitmap::new_empty(
                pages_for_order,
                pages_for_order,
            ));
            pages_for_order = next_higher_order(pages_for_order);
        }

        // Mark the available pages, starting with the highest order
        let mut accounted_pages = 0;
        for order in (0..=max_order).rev() {
            let order_size = 2u32.pow(order.into());
            while accounted_pages + order_size <= num_pages {
                let page = accounted_pages / order_size;
                free[order as usize].clear(page);
                accounted_pages += order_size;
            }
        }
        assert_eq!(accounted_pages, num_pages);

        Self {
            allocated,
            free,
            len: num_pages,
            max_order,
        }
    }

    // Data structure format:
    // max_order: u8
    // padding: 3 bytes
    // num_pages: u32
    // free_ends: array of u32, with ending offset for BtreeBitmap structure for the given order
    // allocated_ends: array of u32, with ending offset for U64GroupedBitmap structure for the given order
    // ... BtreeBitmap structures
    // ... U64GroupedBitmap structures
    pub(crate) fn to_vec(&self) -> Vec<u8> {
        let mut result = vec![];
        result.push(self.max_order);
        result.extend([0u8; 3]);
        result.extend(self.len.to_le_bytes());

        let mut data_offset = result.len() + (self.max_order as usize + 1) * 2 * size_of::<u32>();
        let end_metadata = data_offset;
        for order in self.free.iter() {
            data_offset += order.to_vec().len();
            let offset_u32: u32 = data_offset.try_into().unwrap();
            result.extend(offset_u32.to_le_bytes());
        }
        for order in self.allocated.iter() {
            data_offset += order.to_vec().len();
            let offset_u32: u32 = data_offset.try_into().unwrap();
            result.extend(offset_u32.to_le_bytes());
        }
        assert_eq!(end_metadata, result.len());
        for order in self.free.iter() {
            result.extend(&order.to_vec());
        }
        for order in self.allocated.iter() {
            result.extend(&order.to_vec());
        }

        result
    }

    pub(crate) fn from_bytes(data: &[u8]) -> Self {
        let max_order = data[MAX_ORDER_OFFSET];
        let num_pages = u32::from_le_bytes(
            data[NUM_PAGES_OFFSET..(NUM_PAGES_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        );

        let mut metadata = FREE_END_OFFSETS;
        let mut data_start = FREE_END_OFFSETS + (max_order as usize + 1) * 2 * size_of::<u32>();

        let mut free = vec![];
        for _ in 0..=max_order {
            let data_end = u32::from_le_bytes(
                data[metadata..(metadata + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            free.push(BtreeBitmap::from_bytes(&data[data_start..data_end]));
            data_start = data_end;
            metadata += size_of::<u32>();
        }
        let mut allocated = vec![];
        for _ in 0..=max_order {
            let data_end = u32::from_le_bytes(
                data[metadata..(metadata + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            allocated.push(U64GroupedBitmap::from_bytes(&data[data_start..data_end]));
            data_start = data_end;
            metadata += size_of::<u32>();
        }

        Self {
            allocated,
            free,
            len: num_pages,
            max_order,
        }
    }

    #[inline]
    pub(crate) fn highest_free_order(&self) -> Option<u8> {
        (0..=self.max_order)
            .rev()
            .find(|order| self.get_order_free(*order).has_unset())
    }

    pub(crate) fn count_allocated_pages(&self) -> u32 {
        self.len() - self.count_free_pages()
    }

    pub(crate) fn count_free_pages(&self) -> u32 {
        let mut pages = 0;
        for order in 0..=self.max_order {
            pages += self.get_order_free(order).count_unset() * 2u32.pow(order.try_into().unwrap());
        }
        pages
    }

    pub(crate) fn capacity(&self) -> u32 {
        self.get_order_free(0).capacity()
    }

    pub(crate) fn get_max_order(&self) -> u8 {
        self.max_order
    }

    fn find_free_order(&self, mut page: u32) -> Option<u8> {
        for order in 0..=self.max_order {
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
    // TODO: maybe this should return a Vec<U64GroupedBitmap>?
    pub(crate) fn make_state_for_savepoint(&self) -> Vec<u8> {
        let mut result = vec![self.max_order];
        result.extend(self.len().to_le_bytes());

        let mut data_offset = result.len() + size_of::<u32>() * (self.max_order as usize + 1);
        for order in 0..=self.max_order {
            data_offset += self.allocated[order as usize].to_vec().len();
            result.extend(u32::try_from(data_offset).unwrap().to_le_bytes());
        }

        for order in 0..=self.max_order {
            result.extend(&self.allocated[order as usize].to_vec());
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
            let savepoint_allocated = U64GroupedBitmap::from_bytes(bytes);
            let self_allocated = self.get_order_allocated(order);
            for i in self_allocated.difference(&savepoint_allocated) {
                if i >= num_pages {
                    break;
                }
                output.push(PageNumber::new(region, i, order));
            }
            data_start = data_end;
        }
    }

    pub(crate) fn get_allocated_pages(&self, region: u32, output: &mut Vec<PageNumber>) {
        for order in 0..=self.max_order {
            let allocated = self.get_order_allocated(order);
            for i in allocated.iter() {
                if i >= self.len() {
                    break;
                }
                output.push(PageNumber::new(region, i, order));
            }
        }

        #[cfg(test)]
        // Check the result against the free index to be sure it matches
        {
            let mut allocated_check = HashSet::new();

            for order in 0..=self.max_order {
                let allocated = self.get_order_allocated(order);
                for i in allocated.iter() {
                    if i >= self.len() {
                        break;
                    }
                    allocated_check.insert(PageNumber::new(region, i, order));
                }
            }

            let mut free_check = HashSet::new();
            for i in 0..self.len() {
                if self.find_free_order(i).is_none() {
                    free_check.insert(PageNumber::new(region, i, 0));
                }
            }

            let mut check_result = HashSet::new();
            for page in allocated_check.iter() {
                check_result.extend(page.to_order0());
            }
            assert_eq!(free_check, check_result);
        }
    }

    pub(crate) fn len(&self) -> u32 {
        self.len
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
                if order >= self.max_order || processed_pages + order_size > new_size {
                    break;
                }
                self.free_inner(page, order);
                processed_pages += order_size;
            }
            // Allocate the remaining space, at the highest order
            for order in (0..=self.max_order).rev() {
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
                if order >= self.max_order {
                    break;
                }
                let order_size = 2u32.pow(order.into());
                let page = processed_pages / order_size;
                debug_assert_eq!(processed_pages % order_size, 0);
                if processed_pages + order_size > self.len() {
                    break;
                }
                self.record_alloc_inner(page, order);
                processed_pages += order_size;
            }
            // Allocate the remaining space, at the highest order
            for order in (0..=self.max_order).rev() {
                let order_size = 2u32.pow(order.into());
                while processed_pages + order_size <= self.len() {
                    let page = processed_pages / order_size;
                    self.record_alloc_inner(page, order);
                    processed_pages += order_size;
                }
            }
            assert_eq!(processed_pages, self.len());
        }
        self.len = new_size;
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
                for order in 0..=self.max_order {
                    let allocator = &self.free[order as usize];
                    if !allocator.get(page) {
                        assert!(!found);
                        found = true;
                    }
                    page = next_higher_order(page);
                }
                processed += 1;
            }

            // Ensure that all buddy pages are merged, except at the highest order
            for order in (0..self.max_order).rev() {
                let order_len = self.len() / (2u32.pow(order.into()));
                let allocator = &self.free[order as usize];
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
        for i in (order + 1)..=self.max_order {
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
            let allocator = self.get_order_free_mut(best_order - 1);
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
        if order > self.max_order {
            return None;
        }
        let allocator = self.get_order_free_mut(order);
        if let Some(x) = allocator.alloc() {
            Some(x)
        } else {
            // Try to allocate a higher order page and split it
            let upper_page = self.alloc_inner(order + 1)?;
            let (free1, free2) = (upper_page * 2, upper_page * 2 + 1);
            let allocator = self.get_order_free_mut(order);
            debug_assert!(allocator.get(free1));
            debug_assert!(allocator.get(free2));
            allocator.clear(free2);

            Some(free1)
        }
    }

    /// data must have been initialized by Self::init_new(), and page_number must be free
    pub(crate) fn record_alloc(&mut self, page_number: u32, order: u8) {
        assert!(order <= self.max_order);
        // Only record the allocation for the actual page
        self.get_order_allocated_mut(order).set(page_number);
        // Split parent pages as necessary, and update the free index
        self.record_alloc_inner(page_number, order);
    }

    pub(crate) fn record_alloc_inner(&mut self, page_number: u32, order: u8) {
        let allocator = self.get_order_free_mut(order);
        if allocator.get(page_number) {
            // Need to split parent page
            let upper_page = next_higher_order(page_number);
            self.record_alloc_inner(upper_page, order + 1);
            let allocator = self.get_order_free_mut(order);

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
        if order == self.max_order {
            let allocator = self.get_order_free_mut(order);
            allocator.clear(page_number);
            return;
        }

        let allocator = self.get_order_free_mut(order);
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

    fn get_order_free_mut(&mut self, order: u8) -> &mut BtreeBitmap {
        &mut self.free[order as usize]
    }

    fn get_order_allocated_mut(&mut self, order: u8) -> &mut U64GroupedBitmap {
        &mut self.allocated[order as usize]
    }

    fn get_order_free(&self, order: u8) -> &BtreeBitmap {
        &self.free[order as usize]
    }

    fn get_order_allocated(&self, order: u8) -> &U64GroupedBitmap {
        &self.allocated[order as usize]
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;

    #[test]
    fn record_alloc_buddy() {
        let num_pages = 256;
        let mut allocator = BuddyAllocator::new(num_pages, num_pages);
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
        let mut allocator = BuddyAllocator::new(num_pages, num_pages);
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
        let mut allocator = BuddyAllocator::new(num_pages, num_pages);
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

    #[test]
    fn serialized_size() {
        // Check that serialized size is as expected for a full region
        let max_region_pages = 1024 * 1024;
        let allocator = BuddyAllocator::new(max_region_pages, max_region_pages);
        let max_region_pages = max_region_pages as u64;
        // 2x because that's the integral of 1/2^x to account for all the 21 orders
        let allocated_state_bits = 2 * max_region_pages;
        // + u32 * 21 because of the length field
        let allocated_state_bytes = allocated_state_bits / 8 + 4 * 21;

        // Add 2/64 to account for the intermediate index of the bitmap tree
        let free_state_bits = allocated_state_bits + allocated_state_bits * 2 / 64;
        // Add u32s to account for the stored offsets. Might be a bit of an over estimate
        let free_state_bytes = free_state_bits / 8 + 4 * 21 * 3 + 21 * 4;
        // BuddyAllocator overhead
        let buddy_state_bytes = free_state_bytes + allocated_state_bytes + 21 * 2 * 4 + 8;

        assert!((allocator.to_vec().len() as u64) <= buddy_state_bytes);
    }
}
