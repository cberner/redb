use crate::tree_store::page_store::base::MAX_REGIONS;
use crate::tree_store::page_store::bitmap::BtreeBitmap;
use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::layout::DatabaseLayout;
use crate::tree_store::page_store::page_manager::{INITIAL_REGIONS, MAX_MAX_PAGE_ORDER};
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
            data.push(BtreeBitmap::new(regions, MAX_REGIONS));
        }
        Self {
            order_trackers: data,
        }
    }

    // Format:
    // num_orders: u32 number of order allocators
    // allocator_lens: u32 length of each allocator
    // data: BtreeBitmap data for each order
    pub(super) fn to_vec(&self) -> Vec<u8> {
        let mut result = vec![];
        let orders: u32 = self.order_trackers.len().try_into().unwrap();
        let allocator_lens: Vec<u32> = self
            .order_trackers
            .iter()
            .map(|x| x.to_vec().len().try_into().unwrap())
            .collect();
        result.extend(orders.to_le_bytes());
        for allocator_len in allocator_lens {
            result.extend(allocator_len.to_le_bytes());
        }
        for order in &self.order_trackers {
            result.extend(&order.to_vec());
        }
        result
    }

    pub(super) fn from_bytes(page: &[u8]) -> Self {
        let orders = u32::from_le_bytes(page[..size_of::<u32>()].try_into().unwrap());
        let mut start = size_of::<u32>();
        let mut allocator_lens = vec![];
        for _ in 0..orders {
            let allocator_len =
                u32::from_le_bytes(page[start..start + size_of::<u32>()].try_into().unwrap())
                    as usize;
            allocator_lens.push(allocator_len);
            start += size_of::<u32>();
        }
        let mut data = vec![];
        for allocator_len in allocator_lens {
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

    fn resize(&mut self, new_capacity: u32) {
        for order in &mut self.order_trackers {
            order.resize(new_capacity, true);
        }
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

    pub(crate) fn xxh3_hash(&self) -> u128 {
        // Ignore the region tracker because it is an optimistic cache, and so may not match
        // between repairs of the allocators
        let mut result = 0;
        for allocator in &self.region_allocators {
            result ^= allocator.xxh3_hash();
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
                    if i >= self.region_tracker.len() {
                        self.region_tracker.resize(i + 1);
                    }
                    self.region_tracker.mark_free(highest_free, i);
                    self.region_allocators.push(allocator);
                }
            }
        }
    }
}
