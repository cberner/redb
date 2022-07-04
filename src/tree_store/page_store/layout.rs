use crate::tree_store::page_store::buddy_allocator::BuddyAllocator;
use crate::tree_store::page_store::grouped_bitmap::U64GroupedBitMapMut;
use crate::tree_store::page_store::page_manager::{
    DB_HEADER_SIZE, MAX_PAGE_ORDER, MIN_USABLE_PAGES,
};
use crate::{Error, Result};
use std::cmp::min;
use std::mem::size_of;
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
    allocator_state_len: usize,
    max_order: usize,
    page_size: usize,
}

impl RegionLayout {
    fn calculate_usable_order(space: usize, page_size: usize) -> Option<usize> {
        if space < page_size {
            return None;
        }
        let total_pages = space / page_size;
        let max_order = (64 - total_pages.leading_zeros() - 1) as usize;
        Some(min(MAX_PAGE_ORDER, max_order))
    }

    fn calculate_usable_pages(
        space: usize,
        max_usable_region_bytes: usize,
        page_size: usize,
    ) -> Option<usize> {
        let header_size = Self::header_with_padding(max_usable_region_bytes, page_size)?;
        assert!(header_size < space);
        Some((space - header_size) / page_size)
    }

    fn header_size(max_usable_region_bytes: usize, page_size: usize) -> Option<usize> {
        let max_order = Self::calculate_usable_order(max_usable_region_bytes, page_size)?;
        let page_capacity = max_usable_region_bytes / page_size;
        Some(BuddyAllocator::required_space(page_capacity, max_order))
    }

    fn header_with_padding(max_usable_region_bytes: usize, page_size: usize) -> Option<usize> {
        let header_size = Self::header_size(max_usable_region_bytes, page_size)?;
        Some(if header_size % page_size == 0 {
            header_size
        } else {
            header_size + page_size - header_size % page_size
        })
    }

    pub(super) fn calculate(
        available_space: usize,
        desired_usable_bytes: usize,
        max_usable_region_bytes: usize,
        page_size: usize,
    ) -> Option<RegionLayout> {
        let max_order = Self::calculate_usable_order(max_usable_region_bytes, page_size)?;
        let required_header_size = Self::header_with_padding(max_usable_region_bytes, page_size)?;
        if desired_usable_bytes / page_size < MIN_USABLE_PAGES {
            return None;
        }
        if available_space < required_header_size + MIN_USABLE_PAGES * page_size {
            return None;
        }
        let max_region_size = desired_usable_bytes + required_header_size;
        let used_space = min(max_region_size, available_space);

        let num_pages =
            Self::calculate_usable_pages(used_space, max_usable_region_bytes, page_size)?;
        if num_pages < MIN_USABLE_PAGES {
            return None;
        }

        Some(RegionLayout {
            num_pages,
            pages_start: required_header_size,
            allocator_state_len: Self::header_size(max_usable_region_bytes, page_size)?,
            max_order,
            page_size,
        })
    }

    fn full_region_layout(max_usable_region_bytes: usize, page_size: usize) -> RegionLayout {
        let max_region_size = max_usable_region_bytes
            + Self::header_with_padding(max_usable_region_bytes, page_size).unwrap();

        Self::calculate(
            max_region_size,
            max_usable_region_bytes,
            max_usable_region_bytes,
            page_size,
        )
        .unwrap()
    }

    pub(super) fn header_len(&self) -> usize {
        self.allocator_state_len
    }

    pub(super) fn data_section(&self) -> Range<usize> {
        self.pages_start..(self.pages_start + self.usable_bytes())
    }

    pub(super) fn num_pages(&self) -> usize {
        self.num_pages
    }

    pub(super) fn len(&self) -> usize {
        self.pages_start + self.usable_bytes()
    }

    pub(super) fn usable_bytes(&self) -> usize {
        self.page_size * self.num_pages
    }

    pub(super) fn max_order(&self) -> usize {
        self.max_order
    }

    const fn serialized_size() -> usize {
        5 * size_of::<u32>()
    }

    pub(super) fn to_le_bytes(&self) -> [u8; Self::serialized_size()] {
        let mut result = [0; Self::serialized_size()];
        let mut offset = 0;
        result[offset..(offset + size_of::<u32>())]
            .copy_from_slice(&(self.page_size as u32).to_le_bytes());
        offset += size_of::<u32>();
        result[offset..(offset + size_of::<u32>())]
            .copy_from_slice(&(self.pages_start as u32).to_le_bytes());
        offset += size_of::<u32>();
        result[offset..(offset + size_of::<u32>())]
            .copy_from_slice(&(self.num_pages as u32).to_le_bytes());
        offset += size_of::<u32>();
        result[offset..(offset + size_of::<u32>())]
            .copy_from_slice(&(self.allocator_state_len as u32).to_le_bytes());
        offset += size_of::<u32>();
        result[offset..(offset + size_of::<u32>())]
            .copy_from_slice(&(self.max_order as u32).to_le_bytes());

        result
    }

    pub(super) fn from_le_bytes(data: [u8; Self::serialized_size()]) -> Self {
        let mut offset = 0;
        let page_size = u32::from_le_bytes(
            data[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += size_of::<u32>();
        let pages_start = u32::from_le_bytes(
            data[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += size_of::<u32>();
        let num_pages = u32::from_le_bytes(
            data[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += size_of::<u32>();
        let allocator_state_len = u32::from_le_bytes(
            data[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += size_of::<u32>();
        let max_order = u32::from_le_bytes(
            data[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize;

        Self {
            num_pages,
            pages_start,
            allocator_state_len,
            max_order,
            page_size,
        }
    }
}

#[derive(Clone)]
pub(super) struct DatabaseLayout {
    db_header_bytes: usize,
    region_allocator_range: Range<usize>,
    full_region_layout: RegionLayout,
    num_full_regions: usize,
    trailing_partial_region: Option<RegionLayout>,
}

impl DatabaseLayout {
    pub(super) fn calculate(
        db_capacity: usize,
        mut desired_usable_bytes: usize,
        max_usable_region_bytes: usize,
        page_size: usize,
    ) -> Result<Self> {
        desired_usable_bytes = min(desired_usable_bytes, db_capacity);
        let full_region_layout =
            RegionLayout::full_region_layout(max_usable_region_bytes, page_size);
        let min_header_size = DB_HEADER_SIZE + U64GroupedBitMapMut::required_bytes(1);
        let region_allocator_range = DB_HEADER_SIZE..min_header_size;
        // Pad to be page aligned
        let min_header_size = round_up_to_multiple_of(min_header_size, page_size);
        if db_capacity < min_header_size + MIN_USABLE_PAGES * page_size {
            return Err(Error::OutOfSpace);
        }
        let result = if desired_usable_bytes <= full_region_layout.usable_bytes() {
            // Single region layout
            let region_layout = RegionLayout::calculate(
                db_capacity - min_header_size,
                desired_usable_bytes,
                max_usable_region_bytes,
                page_size,
            )
            .ok_or(Error::OutOfSpace)?;
            DatabaseLayout {
                db_header_bytes: min_header_size,
                region_allocator_range,
                full_region_layout,
                num_full_regions: 0,
                trailing_partial_region: Some(region_layout),
            }
        } else {
            // Multi region layout
            let max_regions = (db_capacity - min_header_size + full_region_layout.len() - 1)
                / full_region_layout.len();
            let db_header_bytes = DB_HEADER_SIZE + U64GroupedBitMapMut::required_bytes(max_regions);
            let region_allocator_range = DB_HEADER_SIZE..db_header_bytes;
            // Pad to be page aligned
            let db_header_bytes = round_up_to_multiple_of(db_header_bytes, page_size);
            let max_full_regions = (db_capacity - db_header_bytes) / full_region_layout.len();
            let desired_full_regions = desired_usable_bytes / max_usable_region_bytes;
            let num_full_regions = min(max_full_regions, desired_full_regions);
            let remaining_space =
                db_capacity - db_header_bytes - num_full_regions * full_region_layout.len();
            let remaining_desired =
                desired_usable_bytes - num_full_regions * max_usable_region_bytes;
            assert!(num_full_regions > 0);
            DatabaseLayout {
                db_header_bytes,
                region_allocator_range,
                full_region_layout,
                num_full_regions,
                trailing_partial_region: RegionLayout::calculate(
                    remaining_space,
                    remaining_desired,
                    max_usable_region_bytes,
                    page_size,
                ),
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

    pub(super) fn len(&self) -> usize {
        let last = self.num_regions() - 1;
        self.region_base_address(last) + self.region_layout(last).len()
    }

    pub(super) fn usable_bytes(&self) -> usize {
        let trailing = self
            .trailing_partial_region
            .as_ref()
            .map(RegionLayout::usable_bytes)
            .unwrap_or_default();
        self.num_full_regions * self.full_region_layout.usable_bytes() + trailing
    }

    pub(super) fn header_bytes(&self) -> usize {
        self.db_header_bytes
    }

    pub(super) fn region_allocator_address_range(&self) -> Range<usize> {
        self.region_allocator_range.clone()
    }

    pub(super) fn region_base_address(&self, region: usize) -> usize {
        assert!(region < self.num_regions());

        self.db_header_bytes + region * self.full_region_layout.len()
    }

    pub(super) fn region_layout(&self, region: usize) -> RegionLayout {
        assert!(region < self.num_regions());
        if region == self.num_full_regions {
            self.trailing_partial_region.as_ref().unwrap().clone()
        } else {
            self.full_region_layout.clone()
        }
    }

    pub(super) const fn serialized_size() -> usize {
        4 * size_of::<u64>() + 2 * RegionLayout::serialized_size() + 1
    }

    pub(super) fn to_le_bytes(&self) -> [u8; Self::serialized_size()] {
        let mut result = [0; Self::serialized_size()];
        let mut offset = 0;
        result[offset..offset + size_of::<u64>()]
            .copy_from_slice(&(self.db_header_bytes as u64).to_le_bytes());
        offset += size_of::<u64>();
        result[offset..offset + size_of::<u64>()]
            .copy_from_slice(&(self.num_full_regions as u64).to_le_bytes());
        offset += size_of::<u64>();

        result[offset..offset + size_of::<u64>()]
            .copy_from_slice(&(self.region_allocator_range.start as u64).to_le_bytes());
        offset += size_of::<u64>();
        result[offset..offset + size_of::<u64>()]
            .copy_from_slice(&(self.region_allocator_range.end as u64).to_le_bytes());
        offset += size_of::<u64>();

        result[offset..offset + RegionLayout::serialized_size()]
            .copy_from_slice(&self.full_region_layout.to_le_bytes());
        offset += RegionLayout::serialized_size();
        if let Some(trailing) = self.trailing_partial_region.as_ref() {
            result[offset..offset + RegionLayout::serialized_size()]
                .copy_from_slice(&trailing.to_le_bytes());
            offset += RegionLayout::serialized_size();
            result[offset] = 1;
        } else {
            result[offset..offset + RegionLayout::serialized_size()].fill(0);
            offset += RegionLayout::serialized_size();
            result[offset] = 0;
        }

        result
    }

    pub(super) fn from_le_bytes(data: [u8; Self::serialized_size()]) -> Self {
        let mut offset = 0;
        let db_header_bytes =
            u64::from_le_bytes(data[offset..offset + size_of::<u64>()].try_into().unwrap())
                as usize;
        offset += size_of::<u64>();
        let num_full_regions =
            u64::from_le_bytes(data[offset..offset + size_of::<u64>()].try_into().unwrap())
                as usize;
        offset += size_of::<u64>();

        let region_allocator_start =
            u64::from_le_bytes(data[offset..offset + size_of::<u64>()].try_into().unwrap())
                as usize;
        offset += size_of::<u64>();
        let region_allocator_end =
            u64::from_le_bytes(data[offset..offset + size_of::<u64>()].try_into().unwrap())
                as usize;
        offset += size_of::<u64>();

        let full_region_layout = RegionLayout::from_le_bytes(
            data[offset..offset + RegionLayout::serialized_size()]
                .try_into()
                .unwrap(),
        );
        offset += RegionLayout::serialized_size();
        let trailing_partial_region = if data[Self::serialized_size() - 1] == 0 {
            None
        } else {
            Some(RegionLayout::from_le_bytes(
                data[offset..offset + RegionLayout::serialized_size()]
                    .try_into()
                    .unwrap(),
            ))
        };

        Self {
            db_header_bytes,
            region_allocator_range: region_allocator_start..region_allocator_end,
            full_region_layout,
            num_full_regions,
            trailing_partial_region,
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
