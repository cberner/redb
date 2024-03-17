use crate::tree_store::page_store::bitmap::BtreeBitmap;
use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::cached_file::{CachePriority, PagedCachedFile};
use crate::tree_store::page_store::header::DatabaseHeader;
use crate::tree_store::page_store::layout::DatabaseLayout;
use crate::tree_store::page_store::page_manager::{INITIAL_REGIONS, MAX_MAX_PAGE_ORDER};
use crate::tree_store::page_store::xxh3_checksum;
use crate::tree_store::PageNumber;
use crate::Result;
use std::cmp;
use std::mem::size_of;

const REGION_FORMAT_VERSION: u8 = 1;
const ALLOCATOR_LENGTH_OFFSET: usize = 4;
const ALLOCATOR_OFFSET: usize = ALLOCATOR_LENGTH_OFFSET + size_of::<u32>();

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
        for order in self.order_trackers.iter() {
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
        let mut region_tracker = RegionTracker::new(INITIAL_REGIONS, MAX_MAX_PAGE_ORDER + 1);
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
        let mut result = xxh3_checksum(&self.region_tracker.to_vec());
        for allocator in self.region_allocators.iter() {
            result ^= xxh3_checksum(&allocator.to_vec());
        }
        result
    }

    pub(super) fn from_bytes(header: &DatabaseHeader, storage: &PagedCachedFile) -> Result<Self> {
        let page_size = header.page_size();
        let region_header_size =
            header.layout().full_region_layout().get_header_pages() * page_size;
        let region_size = header.layout().full_region_layout().num_pages() as u64
            * page_size as u64
            + region_header_size as u64;
        let range = header.region_tracker().address_range(
            page_size as u64,
            region_size,
            region_header_size as u64,
            page_size,
        );
        let len: usize = (range.end - range.start).try_into().unwrap();
        let region_tracker = storage.read_direct(range.start, len)?;
        let mut region_allocators = vec![];
        let layout = header.layout();
        for i in 0..layout.num_regions() {
            let base = layout.region_base_address(i);
            let header_len: usize = layout
                .region_layout(i)
                .data_section()
                .start
                .try_into()
                .unwrap();

            let mem = storage.read_direct(base, header_len)?;
            region_allocators.push(RegionHeader::deserialize(&mem));
        }

        Ok(Self {
            region_tracker: RegionTracker::from_page(&region_tracker),
            region_allocators,
        })
    }

    pub(super) fn flush_to(
        &self,
        region_tracker_page: PageNumber,
        layout: DatabaseLayout,
        storage: &PagedCachedFile,
    ) -> Result {
        let page_size = layout.full_region_layout().page_size();
        let region_header_size =
            (layout.full_region_layout().get_header_pages() * page_size) as u64;
        let region_size =
            layout.full_region_layout().num_pages() as u64 * page_size as u64 + region_header_size;
        let mut region_tracker_mem = {
            let range = region_tracker_page.address_range(
                page_size as u64,
                region_size,
                region_header_size,
                page_size,
            );
            let len: usize = (range.end - range.start).try_into().unwrap();
            storage.write(range.start, len, false, |_| CachePriority::High)?
        };
        let tracker_bytes = self.region_tracker.to_vec();
        region_tracker_mem.mem_mut()[..tracker_bytes.len()].copy_from_slice(&tracker_bytes);

        assert_eq!(self.region_allocators.len(), layout.num_regions() as usize);
        for i in 0..layout.num_regions() {
            let base = layout.region_base_address(i);
            let len: usize = layout
                .region_layout(i)
                .data_section()
                .start
                .try_into()
                .unwrap();

            let mut mem = storage.write(base, len, false, |_| CachePriority::High)?;
            RegionHeader::serialize(&self.region_allocators[i as usize], mem.mem_mut());
        }

        Ok(())
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
// 1 byte: region format version
// 3 bytes: padding
// 4 bytes: length of the allocator state in bytes
// n bytes: the allocator state
pub(crate) struct RegionHeader {}

impl RegionHeader {
    pub(crate) fn header_pages_expensive(page_size: u32, pages_per_region: u32) -> u32 {
        let page_size = page_size as u64;
        // TODO: this is kind of expensive. Maybe it should be cached
        let allocator = BuddyAllocator::new(pages_per_region, pages_per_region);
        let result = 8u64 + allocator.to_vec().len() as u64;
        ((result + page_size - 1) / page_size).try_into().unwrap()
    }

    fn serialize(allocator: &BuddyAllocator, output: &mut [u8]) {
        let serialized = allocator.to_vec();
        let len: u32 = serialized.len().try_into().unwrap();
        output[0] = REGION_FORMAT_VERSION;
        output[ALLOCATOR_LENGTH_OFFSET..(ALLOCATOR_LENGTH_OFFSET + size_of::<u32>())]
            .copy_from_slice(&len.to_le_bytes());
        output[ALLOCATOR_OFFSET..(ALLOCATOR_OFFSET + serialized.len())]
            .copy_from_slice(&serialized);
    }

    fn deserialize(data: &[u8]) -> BuddyAllocator {
        assert_eq!(REGION_FORMAT_VERSION, data[0]);
        let allocator_len = u32::from_le_bytes(
            data[ALLOCATOR_LENGTH_OFFSET..(ALLOCATOR_LENGTH_OFFSET + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize;
        BuddyAllocator::from_bytes(&data[ALLOCATOR_OFFSET..(ALLOCATOR_OFFSET + allocator_len)])
    }
}
