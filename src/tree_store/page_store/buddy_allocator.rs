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
// max_order: big endian u64
// order_offsets: array of (u64, u64), with offset & length for PageAllocator structure for the given order
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
        data[..size_of::<u64>()].copy_from_slice(&(max_order as u64).to_be_bytes());

        let mut metadata_offset = size_of::<u64>();
        let mut data_offset = size_of::<u64>() + 2 * (max_order + 1) * size_of::<u64>();

        let mut orders = vec![];
        let mut pages_for_order = max_page_capacity;
        for order in 0..=max_order {
            let required = PageAllocator::required_space(pages_for_order);
            data[metadata_offset..metadata_offset + size_of::<u64>()]
                .copy_from_slice(&(data_offset as u64).to_be_bytes());
            data[metadata_offset + size_of::<u64>()..metadata_offset + 2 * size_of::<u64>()]
                .copy_from_slice(&(required as u64).to_be_bytes());
            orders.push(PageAllocator::init_new(
                Self::get_order_bytes_mut(data, order),
                pages_for_order,
            ));
            pages_for_order = Self::next_higher_order(pages_for_order as u64) as usize;
            metadata_offset += 2 * size_of::<u64>();
            data_offset += required;
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
        assert!(new_size <= self.capacity);
        // TODO: optimize to avoid all these writes
        for i in self.num_pages..new_size {
            self.free(data, i as u64, 0);
        }
        self.num_pages = new_size;
    }

    /// Returns the number of bytes required for the data argument of new()
    pub(crate) fn required_space(mut capacity: usize, max_order: usize) -> usize {
        let mut required = size_of::<u64>() + 2 * (max_order + 1) * size_of::<u64>();
        for _ in 0..=max_order {
            required += PageAllocator::required_space(capacity);
            capacity = Self::next_higher_order(capacity as u64) as usize;
        }

        required
    }

    pub(crate) fn count_free_pages(&self, data: &[u8]) -> usize {
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
        // Data starts with max_order
        assert!(offset > size_of::<u64>());

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
