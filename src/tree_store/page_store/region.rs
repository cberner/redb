#[cfg(any(test, fuzzing))]
use crate::tree_store::PageNumber;
use crate::tree_store::page_store::bitmap::BtreeBitmap;
use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::layout::DatabaseLayout;
use crate::tree_store::page_store::page_manager::{INITIAL_REGIONS, MAX_MAX_PAGE_ORDER};
use crate::tree_store::page_store::xxh3_checksum;
use std::cmp::{self, max};
use std::mem::size_of;

// Tracks the page orders that MAY BE free in each region. This data structure is optimistic, so
// a region may not actually have a page free for a given order
pub(crate) struct RegionTracker {
    order_trackers: Vec<BtreeBitmap>,
}

impl RegionTracker {
    pub(crate) fn new(regions: u32, orders: u8) -> Self {
        let mut data = vec![];
        for _ in 0..orders {
            data.push(BtreeBitmap::new(regions));
        }
        Self {
            order_trackers: data,
        }
    }

    // Format:
    // num_orders: u32 number of order allocators
    // allocator_len: u32 length of each allocator
    // data: BtreeBitmap data for each order
    pub(super) fn to_vec(&self) -> Vec<u8> {
        let mut result = vec![];
        let orders: u32 = self.order_trackers.len().try_into().unwrap();
        let allocator_len: u32 = self.order_trackers[0].to_vec().len().try_into().unwrap();
        result.extend(orders.to_le_bytes());
        result.extend(allocator_len.to_le_bytes());
        for order in &self.order_trackers {
            result.extend(&order.to_vec());
        }
        result
    }

    // May contain trailing data
    pub(super) fn from_page(page: &[u8]) -> Self {
        let orders = u32::from_le_bytes(page[..size_of::<u32>()].try_into().unwrap());
        let allocator_len = u32::from_le_bytes(
            page[size_of::<u32>()..2 * size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        let mut data = vec![];
        let mut start = 2 * size_of::<u32>();
        for _ in 0..orders {
            data.push(BtreeBitmap::from_bytes(
                &page[start..(start + allocator_len)],
            ));
            start += allocator_len;
        }

        Self {
            order_trackers: data,
        }
    }

    pub(crate) fn find_free(&self, order: u8) -> Option<u32> {
        self.order_trackers[order as usize].find_first_unset()
    }

    pub(crate) fn mark_free(&mut self, order: u8, region: u32) {
        let order: usize = order.into();
        for i in 0..=order {
            self.order_trackers[i].clear(region);
        }
    }

    pub(crate) fn mark_full(&mut self, order: u8, region: u32) {
        let order: usize = order.into();
        assert!(order < self.order_trackers.len());
        for i in order..self.order_trackers.len() {
            self.order_trackers[i].set(region);
        }
    }

    fn expand(&mut self, new_capacity: u32) {
        let mut new_trackers = vec![];
        for order in 0..self.order_trackers.len() {
            let mut new_bitmap = BtreeBitmap::new(new_capacity);
            for region in 0..self.order_trackers[order].len() {
                if !self.order_trackers[order].get(region) {
                    new_bitmap.clear(region);
                }
            }
            new_trackers.push(new_bitmap);
        }

        self.order_trackers = new_trackers;
    }

    fn capacity(&self) -> u32 {
        self.order_trackers[0].capacity()
    }

    fn len(&self) -> u32 {
        self.order_trackers[0].len()
    }
}

pub(super) struct Allocators {
    pub(super) region_tracker: RegionTracker,
    pub(super) region_allocators: Vec<BuddyAllocator>,
}

impl Allocators {
    pub(super) fn new(layout: DatabaseLayout) -> Self {
        let mut region_allocators = vec![];
        let initial_regions = max(INITIAL_REGIONS, layout.num_regions());
        let mut region_tracker = RegionTracker::new(initial_regions, MAX_MAX_PAGE_ORDER + 1);
        for i in 0..layout.num_regions() {
            let region_layout = layout.region_layout(i);
            let allocator = BuddyAllocator::new(
                region_layout.num_pages(),
                layout.full_region_layout().num_pages(),
            );
            let max_order = allocator.get_max_order();
            region_tracker.mark_free(max_order, i);
            region_allocators.push(allocator);
        }

        Self {
            region_tracker,
            region_allocators,
        }
    }

    #[cfg(any(test, fuzzing))]
    pub(super) fn all_allocated(&self) -> Vec<PageNumber> {
        let mut pages = vec![];
        for (i, allocator) in self.region_allocators.iter().enumerate() {
            allocator.get_allocated_pages(i.try_into().unwrap(), &mut pages);
        }
        pages
    }

    pub(crate) fn xxh3_hash(&self) -> u128 {
        // Ignore the region tracker because it is an optimistic cache, and so may not match
        // between repairs of the allocators
        let mut result = 0;
        for allocator in &self.region_allocators {
            result ^= xxh3_checksum(&allocator.to_vec());
        }
        result
    }

    pub(super) fn resize_to(&mut self, new_layout: DatabaseLayout) {
        let shrink = match (new_layout.num_regions() as usize).cmp(&self.region_allocators.len()) {
            cmp::Ordering::Less => true,
            cmp::Ordering::Equal => {
                let allocator = self.region_allocators.last().unwrap();
                let last_region = new_layout
                    .trailing_region_layout()
                    .unwrap_or_else(|| new_layout.full_region_layout());
                match last_region.num_pages().cmp(&allocator.len()) {
                    cmp::Ordering::Less => true,
                    cmp::Ordering::Equal => {
                        // No-op
                        return;
                    }
                    cmp::Ordering::Greater => false,
                }
            }
            cmp::Ordering::Greater => false,
        };

        if shrink {
            // Drop all regions that were removed
            for i in new_layout.num_regions()..(self.region_allocators.len().try_into().unwrap()) {
                self.region_tracker.mark_full(0, i);
            }
            self.region_allocators
                .drain((new_layout.num_regions() as usize)..);

            // Resize the last region
            let last_region = new_layout
                .trailing_region_layout()
                .unwrap_or_else(|| new_layout.full_region_layout());
            let allocator = self.region_allocators.last_mut().unwrap();
            if allocator.len() > last_region.num_pages() {
                allocator.resize(last_region.num_pages());
            }
        } else {
            let old_num_regions = self.region_allocators.len();
            for i in 0..new_layout.num_regions() {
                let new_region = new_layout.region_layout(i);
                if (i as usize) < old_num_regions {
                    let allocator = &mut self.region_allocators[i as usize];
                    assert!(new_region.num_pages() >= allocator.len());
                    if new_region.num_pages() != allocator.len() {
                        allocator.resize(new_region.num_pages());
                        let highest_free = allocator.highest_free_order().unwrap();
                        self.region_tracker.mark_free(highest_free, i);
                    }
                } else {
                    // brand new region
                    let allocator = BuddyAllocator::new(
                        new_region.num_pages(),
                        new_layout.full_region_layout().num_pages(),
                    );
                    let highest_free = allocator.highest_free_order().unwrap();
                    // TODO: we should be calling .capacity(), and resizing if possible
                    if i >= self.region_tracker.len() {
                        self.region_tracker
                            .expand(self.region_tracker.capacity() * 2);
                    }
                    self.region_tracker.mark_free(highest_free, i);
                    self.region_allocators.push(allocator);
                }
            }
        }
    }
}

// Region header
// Note: unused as of v3 file format
pub(crate) struct RegionHeader {}

impl RegionHeader {
    pub(crate) fn header_pages_expensive(page_size: u32, pages_per_region: u32) -> u32 {
        let page_size = u64::from(page_size);
        // TODO: this is kind of expensive. Maybe it should be cached
        let allocator = BuddyAllocator::new(pages_per_region, pages_per_region);
        let result = 8u64 + allocator.to_vec().len() as u64;
        result.div_ceil(page_size).try_into().unwrap()
    }
}
