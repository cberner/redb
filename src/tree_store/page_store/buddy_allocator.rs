use crate::tree_store::page_store::page_allocator::PageAllocator;
use crate::Error;
use crate::Result;
use std::mem::size_of;

#[derive(Clone)]
pub(crate) struct BuddyAllocator {
    orders: Vec<PageAllocator>,
    num_pages: usize,
    capacity: usize,
}

// Handles allocation of dynamically sized pages, supports pages of up to page_size * 2^max_order bytes
//
// Pages are marked free at only a single order, and it must always be the largest order
//
// Data structure format:
// max_order: u8
// padding: 3 bytes
// order_ends: array of u32, with ending offset for PageAllocator structure for the given order
// ... PageAllocator structures
impl BuddyAllocator {
    pub(crate) fn new(num_pages: usize, max_capacity: usize, max_order: usize) -> Self {
        let mut orders = vec![];
        let mut pages_for_order = max_capacity;
        for _ in 0..=max_order {
            orders.push(PageAllocator::new(pages_for_order));
            pages_for_order = Self::next_higher_order(pages_for_order as u64) as usize;
        }

        Self {
            orders,
            num_pages,
            capacity: max_capacity,
        }
    }

    pub(crate) fn init_new(
        data: &mut [u8],
        num_pages: usize,
        max_page_capacity: usize,
        max_order: usize,
    ) -> Self {
        assert!(data.len() >= Self::required_space(max_page_capacity, max_order));
        data[0] = max_order.try_into().unwrap();

        let mut metadata_offset = 4;
        let mut data_offset = 4 + (max_order + 1) * size_of::<u32>();

        let mut orders = vec![];
        let mut pages_for_order = max_page_capacity;
        for order in 0..=max_order {
            let required = PageAllocator::required_space(pages_for_order);
            data_offset += required;
            data[metadata_offset..metadata_offset + size_of::<u32>()]
                .copy_from_slice(&(data_offset as u32).to_le_bytes());
            orders.push(PageAllocator::init_new(
                Self::get_order_bytes_mut(data, order),
                pages_for_order,
            ));
            pages_for_order = Self::next_higher_order(pages_for_order as u64) as usize;
            metadata_offset += size_of::<u32>();
        }

        // Mark the available pages, starting with the highest order
        let mut accounted_pages = 0;
        for (order, allocator) in orders.iter().enumerate().rev() {
            let data = Self::get_order_bytes_mut(data, order);
            let order_size = 2usize.pow(order as u32);
            while accounted_pages + order_size <= num_pages {
                let page = accounted_pages / order_size;
                allocator.free(data, page as u64);
                accounted_pages += order_size;
            }
        }
        assert_eq!(accounted_pages, num_pages);

        Self {
            orders,
            num_pages,
            capacity: max_page_capacity,
        }
    }

    pub(crate) fn resize(&mut self, data: &mut [u8], new_size: usize) {
        self.debug_check_consistency(data);
        assert!(new_size <= self.capacity);
        if new_size > self.num_pages {
            let mut processed_pages = self.num_pages;
            // Align to the highest order possible
            while processed_pages < new_size {
                let order = processed_pages.trailing_zeros() as usize;
                let order_size = 2usize.pow(order as u32);
                let page = processed_pages / order_size;
                debug_assert_eq!(processed_pages % order_size, 0);
                if order >= self.orders.len() || processed_pages + order_size > new_size {
                    break;
                }
                self.free(data, page as u64, order);
                processed_pages += order_size;
            }
            // Allocate the remaining space, at the highest order
            for order in (0..self.orders.len()).rev() {
                let order_size = 2usize.pow(order as u32);
                while processed_pages + order_size <= new_size {
                    let page = processed_pages / order_size;
                    self.free(data, page as u64, order);
                    processed_pages += order_size;
                }
            }
            assert_eq!(processed_pages, new_size);
            self.debug_check_consistency(data);
        } else {
            let mut processed_pages = new_size;
            // Align to the highest order possible
            while processed_pages < self.num_pages {
                let order = processed_pages.trailing_zeros() as usize;
                let order_size = 2usize.pow(order as u32);
                let page = processed_pages / order_size;
                debug_assert_eq!(processed_pages % order_size, 0);
                if order >= self.orders.len() || processed_pages + order_size > self.num_pages {
                    break;
                }
                self.record_alloc(data, page as u64, order);
                processed_pages += order_size;
            }
            // Allocate the remaining space, at the highest order
            for order in (0..self.orders.len()).rev() {
                let order_size = 2usize.pow(order as u32);
                while processed_pages + order_size <= self.num_pages {
                    let page = processed_pages / order_size;
                    self.record_alloc(data, page as u64, order);
                    processed_pages += order_size;
                }
            }
            assert_eq!(processed_pages, self.num_pages);
        }
        self.num_pages = new_size;
    }

    #[allow(unused_variables)]
    fn debug_check_consistency(&self, data: &[u8]) {
        // Don't enable when fuzzing, because this is kind of expensive
        #[cfg(all(debug_assertions, not(fuzzing)))]
        {
            let mut processed = 0;
            // Ensure that no page is free at multiple orders
            while processed < self.num_pages {
                let mut found = false;
                let mut page = processed as u64;
                for (order, allocator) in self.orders.iter().enumerate() {
                    if !allocator.is_allocated(Self::get_order_bytes(data, order), page) {
                        assert!(!found);
                        found = true;
                    }
                    page = Self::next_higher_order(page);
                }
                processed += 1;
            }

            // Ensure that all buddy pages are merged, except at the highest order
            for (order, allocator) in self.orders.iter().enumerate().rev().skip(1) {
                for page in 0..allocator.get_num_pages() {
                    let order_bytes = Self::get_order_bytes(data, order);
                    if !allocator.is_allocated(order_bytes, page) {
                        let buddy = Self::buddy_page(page);
                        let buddy_allocated = allocator.is_allocated(order_bytes, buddy);
                        assert!(
                            buddy_allocated,
                            "order={} page={} buddy={}",
                            order, page, buddy
                        );
                    }
                }
            }
        }
    }

    /// Returns the number of bytes required for the data argument of new()
    pub(crate) fn required_space(mut capacity: usize, max_order: usize) -> usize {
        let mut required = 4 + (max_order + 1) * size_of::<u32>();
        for _ in 0..=max_order {
            required += PageAllocator::required_space(capacity);
            capacity = Self::next_higher_order(capacity as u64) as usize;
        }

        required
    }

    pub(crate) fn highest_free_order(&self, data: &[u8]) -> Option<usize> {
        for (order, allocator) in self.orders.iter().enumerate().rev() {
            // TODO: optimize this. We don't need to count all the free pages, just determine whether there is at least one
            if allocator.count_free_pages(Self::get_order_bytes(data, order)) > 0 {
                return Some(order);
            }
        }

        None
    }

    pub(crate) fn count_free_pages(&self, data: &[u8]) -> usize {
        let mut pages = 0;
        for (order, allocator) in self.orders.iter().enumerate() {
            pages += allocator.count_free_pages(Self::get_order_bytes(data, order))
                * 2usize.pow(order as u32);
        }

        pages
    }

    pub(crate) fn len(&self) -> usize {
        self.num_pages
    }

    fn find_free_order(&self, data: &[u8], mut page: u64) -> Option<usize> {
        for (order, allocator) in self.orders.iter().enumerate() {
            if !allocator.is_allocated(Self::get_order_bytes(data, order), page) {
                return Some(order);
            }
            page = Self::next_higher_order(page);
        }

        None
    }

    pub(crate) fn trailing_free_pages(&self, data: &[u8]) -> usize {
        let mut free_pages = 0;
        let mut next_page = self.num_pages - 1;
        while let Some(order) = self.find_free_order(data, next_page as u64) {
            let order_size = 2usize.pow(order as u32);
            free_pages += order_size;
            if order_size > next_page {
                break;
            }
            next_page -= order_size;
        }

        free_pages
    }

    fn get_order_start_offset(data: &[u8], order: usize) -> usize {
        if order == 0 {
            let max_order = data[0] as usize;
            4 + (max_order + 1) * size_of::<u32>()
        } else {
            Self::get_order_end_offset(data, order - 1)
        }
    }

    fn get_order_end_offset(data: &[u8], order: usize) -> usize {
        let base = 4 + order * size_of::<u32>();
        u32::from_le_bytes(data[base..base + size_of::<u32>()].try_into().unwrap()) as usize
    }

    fn get_order_offset_and_length(data: &[u8], order: usize) -> (usize, usize) {
        let max_order = data[0] as usize;
        assert!(order <= max_order);
        let offset = Self::get_order_start_offset(data, order);
        let end = Self::get_order_end_offset(data, order);
        let length = end - offset;
        // Data starts with max_order
        assert!(offset > size_of::<u32>());

        (offset, length)
    }

    fn get_order_bytes(data: &[u8], order: usize) -> &[u8] {
        let (offset, length) = Self::get_order_offset_and_length(data, order);
        &data[offset..(offset + length)]
    }

    fn get_order_bytes_mut(data: &mut [u8], order: usize) -> &mut [u8] {
        let (offset, length) = Self::get_order_offset_and_length(data, order);
        &mut data[offset..(offset + length)]
    }

    fn next_higher_order(page_number: u64) -> u64 {
        page_number / 2
    }

    fn buddy_page(page_number: u64) -> u64 {
        page_number ^ 1
    }

    /// data must have been initialized by Self::init_new()
    pub(crate) fn alloc(&self, data: &mut [u8], order: usize) -> Result<u64> {
        if order >= self.orders.len() {
            return Err(Error::OutOfSpace);
        }
        match self.orders[order].alloc(Self::get_order_bytes_mut(data, order)) {
            Ok(x) => Ok(x),
            Err(e) => {
                match e {
                    Error::OutOfSpace => {
                        // Try to allocate a higher order page and split it
                        let upper_page = self.alloc(data, order + 1)?;
                        let (free1, free2) = (upper_page * 2, upper_page * 2 + 1);
                        debug_assert!(self.orders[order]
                            .is_allocated(Self::get_order_bytes_mut(data, order), free1));
                        debug_assert!(self.orders[order]
                            .is_allocated(Self::get_order_bytes_mut(data, order), free2));
                        self.orders[order].free(Self::get_order_bytes_mut(data, order), free2);

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
        if self.orders[order].is_allocated(Self::get_order_bytes_mut(data, order), page_number) {
            // Need to split parent page
            let upper_page = Self::next_higher_order(page_number);
            self.record_alloc(data, upper_page, order + 1);

            let (free1, free2) = (upper_page * 2, upper_page * 2 + 1);
            debug_assert!(free1 == page_number || free2 == page_number);
            if free1 == page_number {
                self.orders[order].free(Self::get_order_bytes_mut(data, order), free2);
            } else {
                self.orders[order].free(Self::get_order_bytes_mut(data, order), free1);
            }
        } else {
            self.orders[order].record_alloc(Self::get_order_bytes_mut(data, order), page_number);
        }
    }

    /// data must have been initialized by Self::init_new()
    pub(crate) fn free(&self, data: &mut [u8], page_number: u64, order: usize) {
        if order == self.orders.len() - 1 {
            self.orders[order].free(Self::get_order_bytes_mut(data, order), page_number);
            return;
        }

        let buddy = Self::buddy_page(page_number);
        if self.orders[order].is_allocated(Self::get_order_bytes_mut(data, order), buddy) {
            self.orders[order].free(Self::get_order_bytes_mut(data, order), page_number);
        } else {
            // Merge into higher order page
            self.orders[order].record_alloc(Self::get_order_bytes_mut(data, order), buddy);
            self.free(data, Self::next_higher_order(page_number), order + 1);
        }
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
    use crate::Error;

    #[test]
    fn record_alloc_buddy() {
        let num_pages = 256;
        let max_order = 7;
        let mut data = vec![0; BuddyAllocator::required_space(num_pages, max_order)];
        let allocator = BuddyAllocator::init_new(&mut data, num_pages, num_pages, max_order);
        assert_eq!(allocator.count_free_pages(&data), num_pages);

        for page in 0..num_pages {
            allocator.record_alloc(&mut data, page as u64, 0);
        }
        assert_eq!(allocator.count_free_pages(&data), 0);

        assert!(matches!(
            allocator.alloc(&mut data, 0).unwrap_err(),
            Error::OutOfSpace
        ));

        for page in 0..num_pages {
            allocator.free(&mut data, page as u64, 0);
        }
        assert_eq!(allocator.count_free_pages(&data), num_pages);
    }

    #[test]
    fn buddy_merge() {
        let num_pages = 256;
        let max_order = 7;
        let mut data = vec![0; BuddyAllocator::required_space(num_pages, max_order)];
        let allocator = BuddyAllocator::init_new(&mut data, num_pages, num_pages, max_order);
        assert_eq!(allocator.count_free_pages(&data), num_pages);

        for _ in 0..num_pages {
            allocator.alloc(&mut data, 0).unwrap();
        }
        for page in 0..num_pages {
            allocator.free(&mut data, page as u64, 0);
        }
        assert_eq!(allocator.count_free_pages(&data), num_pages);

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
        let allocator = BuddyAllocator::init_new(&mut data, num_pages, num_pages, max_order);
        assert_eq!(allocator.count_free_pages(&data), num_pages);

        let mut allocated = vec![];
        for order in 0..=max_order {
            allocated.push((allocator.alloc(&mut data, order).unwrap(), order));
        }
        assert_eq!(allocator.count_free_pages(&data), 1);

        for order in 1..=max_order {
            assert!(matches!(
                allocator.alloc(&mut data, order).unwrap_err(),
                Error::OutOfSpace
            ));
        }

        for (page, order) in allocated {
            allocator.free(&mut data, page, order);
        }
        assert_eq!(allocator.count_free_pages(&data), num_pages);
    }
}
