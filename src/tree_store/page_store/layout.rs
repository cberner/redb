use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::page_manager::{
    RegionTracker, DB_HEADER_SIZE, MAX_MAX_PAGE_ORDER, MIN_USABLE_PAGES,
};
use crate::{Error, Result};
use std::cmp::min;
use std::ops::Range;

fn round_up_to_multiple_of(value: usize, multiple: usize) -> usize {
    if value % multiple == 0 {
        value
    } else {
        value + multiple - value % multiple
    }
}

// Regions are laid out starting with the allocator state header, followed by the pages aligned
// to the next page
#[derive(Clone, Debug)]
pub(super) struct RegionLayout {
    num_pages: usize,
    // Offset where pages start
    pages_start: usize,
    max_order: usize,
    page_size: usize,
}

impl RegionLayout {
    pub(super) fn new(
        num_pages: usize,
        page_capacity: usize,
        header_size: usize,
        page_size: usize,
    ) -> Self {
        // TODO: remove this, since its already stored in the regional allocator
        let max_order =
            Self::calculate_usable_order((page_capacity as u64) * (page_size as u64), page_size)
                .unwrap();
        Self {
            num_pages,
            pages_start: header_size,
            max_order,
            page_size,
        }
    }

    fn calculate_usable_order(space: u64, page_size: usize) -> Option<usize> {
        if space < page_size as u64 {
            return None;
        }
        let total_pages = space / (page_size as u64);
        let max_order = (64 - total_pages.leading_zeros() - 1) as usize;
        Some(min(MAX_MAX_PAGE_ORDER, max_order))
    }

    fn calculate_usable_pages(
        space: u64,
        max_usable_region_bytes: u64,
        page_size: usize,
    ) -> Option<usize> {
        let header_size = Self::header_with_padding(max_usable_region_bytes, page_size)? as u64;
        assert!(header_size < space);
        Some(
            ((space - header_size) / (page_size as u64))
                .try_into()
                .unwrap(),
        )
    }

    fn header_size(max_usable_region_bytes: u64, page_size: usize) -> Option<usize> {
        let max_order = Self::calculate_usable_order(max_usable_region_bytes, page_size)?;
        let page_capacity = max_usable_region_bytes / (page_size as u64);
        Some(BuddyAllocator::required_space(
            page_capacity.try_into().unwrap(),
            max_order,
        ))
    }

    fn header_with_padding(max_usable_region_bytes: u64, page_size: usize) -> Option<usize> {
        let header_size = Self::header_size(max_usable_region_bytes, page_size)?;
        Some(if header_size % page_size == 0 {
            header_size
        } else {
            header_size + page_size - header_size % page_size
        })
    }

    pub(super) fn calculate(
        available_space: u64,
        desired_usable_bytes: u64,
        max_usable_region_bytes: u64,
        page_size: usize,
    ) -> Option<RegionLayout> {
        let max_order = Self::calculate_usable_order(max_usable_region_bytes, page_size)?;
        let required_header_size = Self::header_with_padding(max_usable_region_bytes, page_size)?;
        if desired_usable_bytes / (page_size as u64) < MIN_USABLE_PAGES as u64 {
            return None;
        }
        if available_space < (required_header_size + MIN_USABLE_PAGES * page_size) as u64 {
            return None;
        }
        let max_region_size = desired_usable_bytes + required_header_size as u64;
        let used_space = min(max_region_size, available_space);

        let num_pages =
            Self::calculate_usable_pages(used_space, max_usable_region_bytes, page_size)?;
        if num_pages < MIN_USABLE_PAGES {
            return None;
        }

        Some(RegionLayout {
            num_pages,
            pages_start: required_header_size,
            max_order,
            page_size,
        })
    }

    fn full_region_layout(max_usable_region_bytes: u64, page_size: usize) -> RegionLayout {
        let max_region_size = max_usable_region_bytes
            + Self::header_with_padding(max_usable_region_bytes, page_size).unwrap() as u64;

        Self::calculate(
            max_region_size,
            max_usable_region_bytes,
            max_usable_region_bytes,
            page_size,
        )
        .unwrap()
    }

    pub(super) fn data_section(&self) -> Range<usize> {
        let usable: usize = self.usable_bytes().try_into().unwrap();
        self.pages_start..(self.pages_start + usable)
    }

    pub(super) fn num_pages(&self) -> usize {
        self.num_pages
    }

    pub(super) fn len(&self) -> u64 {
        self.pages_start as u64 + self.usable_bytes()
    }

    pub(super) fn usable_bytes(&self) -> u64 {
        self.page_size as u64 * self.num_pages as u64
    }

    pub(super) fn max_order(&self) -> usize {
        self.max_order
    }
}

#[derive(Clone)]
pub(super) struct DatabaseLayout {
    db_header_bytes: usize,
    region_tracker_range: Range<usize>,
    full_region_layout: RegionLayout,
    num_full_regions: usize,
    trailing_partial_region: Option<RegionLayout>,
}

impl DatabaseLayout {
    pub(super) fn new(
        superheader_bytes: usize,
        region_tracker_len: usize,
        full_regions: usize,
        full_region: RegionLayout,
        trailing_region: Option<RegionLayout>,
    ) -> Self {
        Self {
            db_header_bytes: superheader_bytes,
            region_tracker_range: DB_HEADER_SIZE..(DB_HEADER_SIZE + region_tracker_len),
            full_region_layout: full_region,
            num_full_regions: full_regions,
            trailing_partial_region: trailing_region,
        }
    }

    pub(super) fn calculate(
        db_capacity: u64,
        mut desired_usable_bytes: u64,
        max_usable_region_bytes: u64,
        page_size: usize,
    ) -> Result<Self> {
        desired_usable_bytes = min(desired_usable_bytes, db_capacity);
        let full_region_layout =
            RegionLayout::full_region_layout(max_usable_region_bytes, page_size);
        let min_header_size =
            DB_HEADER_SIZE + RegionTracker::required_bytes(1, MAX_MAX_PAGE_ORDER + 1);
        let max_regions: usize =
            ((db_capacity - (min_header_size as u64) + full_region_layout.len() - 1)
                / full_region_layout.len())
            .try_into()
            .unwrap();
        let db_header_bytes =
            DB_HEADER_SIZE + RegionTracker::required_bytes(max_regions, MAX_MAX_PAGE_ORDER + 1);
        let region_tracker_range = DB_HEADER_SIZE..db_header_bytes;
        // Pad to be page aligned
        let super_header_size = round_up_to_multiple_of(db_header_bytes, page_size);
        if db_capacity < (super_header_size + MIN_USABLE_PAGES * page_size) as u64 {
            return Err(Error::OutOfSpace);
        }
        let result = if desired_usable_bytes <= full_region_layout.usable_bytes()
            || db_capacity - (super_header_size as u64) <= full_region_layout.len()
        {
            // Single region layout
            let region_layout = RegionLayout::calculate(
                db_capacity - super_header_size as u64,
                desired_usable_bytes,
                max_usable_region_bytes,
                page_size,
            )
            .ok_or(Error::OutOfSpace)?;
            DatabaseLayout {
                db_header_bytes: super_header_size,
                region_tracker_range,
                full_region_layout,
                num_full_regions: 0,
                trailing_partial_region: Some(region_layout),
            }
        } else {
            // Multi region layout
            let max_full_regions =
                (db_capacity - super_header_size as u64) / full_region_layout.len();
            let desired_full_regions = desired_usable_bytes / max_usable_region_bytes;
            let num_full_regions = min(max_full_regions, desired_full_regions);
            let remaining_space = db_capacity
                - (super_header_size as u64)
                - num_full_regions * full_region_layout.len();
            let remaining_desired =
                desired_usable_bytes - num_full_regions * max_usable_region_bytes;
            assert!(num_full_regions > 0);
            let trailing_region = RegionLayout::calculate(
                remaining_space,
                remaining_desired,
                max_usable_region_bytes,
                page_size,
            );
            // TODO: change the calculation to use a fixed header size for all regions, including the trailing one
            let trailing_region = if let Some(region) = trailing_region {
                if region.pages_start != full_region_layout.pages_start {
                    None
                } else {
                    Some(region)
                }
            } else {
                None
            };
            DatabaseLayout {
                db_header_bytes: super_header_size,
                region_tracker_range,
                full_region_layout,
                num_full_regions: num_full_regions.try_into().unwrap(),
                trailing_partial_region: trailing_region,
            }
        };

        assert_eq!(result.db_header_bytes % page_size, 0);
        Ok(result)
    }

    pub(super) fn create_allocators(&self) -> Vec<BuddyAllocator> {
        let full_regional_allocator = BuddyAllocator::new(
            self.full_region_layout().num_pages(),
            self.full_region_layout().num_pages(),
            self.full_region_layout().max_order(),
        );
        let mut allocators = vec![full_regional_allocator; self.num_full_regions()];
        if let Some(region_layout) = self.trailing_region_layout() {
            let trailing = BuddyAllocator::new(
                region_layout.num_pages(),
                self.full_region_layout().num_pages(),
                region_layout.max_order(),
            );
            allocators.push(trailing);
        }

        allocators
    }

    pub(super) fn full_region_layout(&self) -> &RegionLayout {
        &self.full_region_layout
    }

    pub(super) fn trailing_region_layout(&self) -> Option<&RegionLayout> {
        self.trailing_partial_region.as_ref()
    }

    pub(super) fn num_full_regions(&self) -> usize {
        self.num_full_regions
    }

    pub(super) fn num_regions(&self) -> usize {
        if self.trailing_partial_region.is_some() {
            self.num_full_regions + 1
        } else {
            self.num_full_regions
        }
    }

    pub(super) fn len(&self) -> u64 {
        let last = self.num_regions() - 1;
        (self.region_base_address(last) as u64) + self.region_layout(last).len()
    }

    pub(super) fn usable_bytes(&self) -> u64 {
        let trailing = self
            .trailing_partial_region
            .as_ref()
            .map(RegionLayout::usable_bytes)
            .unwrap_or_default();
        (self.num_full_regions as u64) * self.full_region_layout.usable_bytes() + trailing
    }

    pub(super) fn header_bytes(&self) -> usize {
        self.db_header_bytes
    }

    pub(super) fn region_tracker_address_range(&self) -> Range<usize> {
        self.region_tracker_range.clone()
    }

    pub(super) fn region_base_address(&self, region: usize) -> usize {
        assert!(region < self.num_regions());

        ((self.db_header_bytes as u64) + (region as u64) * self.full_region_layout.len())
            .try_into()
            .unwrap()
    }

    pub(super) fn region_layout(&self, region: usize) -> RegionLayout {
        assert!(region < self.num_regions());
        if region == self.num_full_regions {
            self.trailing_partial_region.as_ref().unwrap().clone()
        } else {
            self.full_region_layout.clone()
        }
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::layout::RegionLayout;

    #[test]
    fn full_layout() {
        let layout = RegionLayout::full_region_layout(512 * 4096, 4096);
        assert_eq!(layout.num_pages, 512);
        assert_eq!(layout.page_size, 4096);
        assert_eq!(layout.max_order, 9);
    }
}
