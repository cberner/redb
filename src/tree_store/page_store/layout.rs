use crate::tree_store::page_store::buddy_allocator::BuddyAllocatorMut;
use crate::tree_store::page_store::page_manager::DB_HEADER_SIZE;
use crate::Result;
use std::ops::Range;

fn round_up_to_multiple_of(value: u64, multiple: u64) -> u64 {
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
    num_pages: u32,
    // Offset where data pages start
    header_pages: u32,
    page_size: u32,
}

impl RegionLayout {
    pub(super) fn new(num_pages: u32, header_pages: u32, page_size: u32) -> Self {
        Self {
            num_pages,
            header_pages,
            page_size,
        }
    }

    fn header_pages(page_capacity: u32, page_size: u32) -> u32 {
        let mut header_size: u32 =
            BuddyAllocatorMut::required_space(page_capacity.try_into().unwrap())
                .try_into()
                .unwrap();

        if header_size % page_size != 0 {
            header_size += page_size - header_size % page_size;
        }

        header_size / page_size
    }

    pub(super) fn calculate(
        desired_usable_bytes: u64,
        page_capacity: u32,
        page_size: u32,
    ) -> RegionLayout {
        assert!(desired_usable_bytes <= page_capacity as u64 * page_size as u64);
        let header_pages = Self::header_pages(page_capacity, page_size);
        let num_pages =
            round_up_to_multiple_of(desired_usable_bytes, page_size.into()) / page_size as u64;

        Self {
            num_pages: num_pages.try_into().unwrap(),
            header_pages,
            page_size,
        }
    }

    fn full_region_layout(page_capacity: u32, page_size: u32) -> RegionLayout {
        let header_pages = Self::header_pages(page_capacity, page_size);

        Self {
            num_pages: page_capacity,
            header_pages,
            page_size,
        }
    }

    pub(super) fn data_section(&self) -> Range<usize> {
        let usable: usize = self.usable_bytes().try_into().unwrap();
        let header_bytes: usize = (self.header_pages * self.page_size).try_into().unwrap();
        header_bytes..(header_bytes + usable)
    }

    pub(super) fn get_header_pages(&self) -> u32 {
        self.header_pages
    }

    pub(super) fn num_pages(&self) -> u32 {
        self.num_pages
    }

    pub(super) fn len(&self) -> u64 {
        (self.header_pages as u64) * (self.page_size as u64) + self.usable_bytes()
    }

    pub(super) fn usable_bytes(&self) -> u64 {
        self.page_size as u64 * self.num_pages as u64
    }
}

#[derive(Clone)]
pub(super) struct DatabaseLayout {
    full_region_layout: RegionLayout,
    num_full_regions: u32,
    trailing_partial_region: Option<RegionLayout>,
}

impl DatabaseLayout {
    pub(super) fn new(
        full_regions: u32,
        full_region: RegionLayout,
        trailing_region: Option<RegionLayout>,
    ) -> Self {
        Self {
            full_region_layout: full_region,
            num_full_regions: full_regions,
            trailing_partial_region: trailing_region,
        }
    }

    pub(super) fn calculate(
        desired_usable_bytes: u64,
        page_capacity: u32,
        page_size: u32,
    ) -> Result<Self> {
        let full_region_layout = RegionLayout::full_region_layout(page_capacity, page_size);
        let result = if desired_usable_bytes <= full_region_layout.usable_bytes() {
            // Single region layout
            let region_layout =
                RegionLayout::calculate(desired_usable_bytes, page_capacity, page_size);
            DatabaseLayout {
                full_region_layout,
                num_full_regions: 0,
                trailing_partial_region: Some(region_layout),
            }
        } else {
            // Multi region layout
            let full_regions = desired_usable_bytes / full_region_layout.usable_bytes();
            let remaining_desired =
                desired_usable_bytes - full_regions * full_region_layout.usable_bytes();
            assert!(full_regions > 0);
            let trailing_region = if remaining_desired > 0 {
                Some(RegionLayout::calculate(
                    remaining_desired,
                    page_capacity,
                    page_size,
                ))
            } else {
                None
            };
            if let Some(ref region) = trailing_region {
                // All regions must have the same header size
                assert_eq!(region.header_pages, full_region_layout.header_pages);
            }
            DatabaseLayout {
                full_region_layout,
                num_full_regions: full_regions.try_into().unwrap(),
                trailing_partial_region: trailing_region,
            }
        };

        Ok(result)
    }

    pub(super) fn full_region_layout(&self) -> &RegionLayout {
        &self.full_region_layout
    }

    pub(super) fn trailing_region_layout(&self) -> Option<&RegionLayout> {
        self.trailing_partial_region.as_ref()
    }

    pub(super) fn num_full_regions(&self) -> u32 {
        self.num_full_regions
    }

    pub(super) fn num_regions(&self) -> u32 {
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

    // TODO: remove this method
    pub(super) fn superheader_pages(&self) -> u32 {
        assert!(self.full_region_layout.page_size as usize >= DB_HEADER_SIZE);
        1
    }

    // TODO: remove this method
    pub(super) fn superheader_bytes(&self) -> usize {
        (self.superheader_pages() * self.full_region_layout.page_size) as usize
    }

    pub(super) fn region_base_address(&self, region: u32) -> usize {
        assert!(region < self.num_regions());

        ((self.superheader_bytes() as u64) + (region as u64) * self.full_region_layout.len())
            .try_into()
            .unwrap()
    }

    pub(super) fn region_layout(&self, region: u32) -> RegionLayout {
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
        let layout = RegionLayout::full_region_layout(512, 4096);
        assert_eq!(layout.num_pages, 512);
        assert_eq!(layout.page_size, 4096);
    }
}
