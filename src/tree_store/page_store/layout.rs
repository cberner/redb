use crate::tree_store::page_store::region::RegionHeader;
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RegionLayout {
    num_pages: u32,
    // Offset where data pages start
    header_pages: u32,
    page_size: u32,
}

impl RegionLayout {
    pub(super) fn new(num_pages: u32, header_pages: u32, page_size: u32) -> Self {
        assert!(num_pages > 0);
        Self {
            num_pages,
            header_pages,
            page_size,
        }
    }

    pub(super) fn calculate(
        desired_usable_bytes: u64,
        page_capacity: u32,
        page_size: u32,
    ) -> RegionLayout {
        assert!(desired_usable_bytes <= page_capacity as u64 * page_size as u64);
        let header_pages = RegionHeader::header_pages_expensive(page_size, page_capacity);
        let num_pages =
            round_up_to_multiple_of(desired_usable_bytes, page_size.into()) / page_size as u64;

        Self {
            num_pages: num_pages.try_into().unwrap(),
            header_pages,
            page_size,
        }
    }

    fn full_region_layout(page_capacity: u32, page_size: u32) -> RegionLayout {
        let header_pages = RegionHeader::header_pages_expensive(page_size, page_capacity);

        Self {
            num_pages: page_capacity,
            header_pages,
            page_size,
        }
    }

    pub(super) fn data_section(&self) -> Range<u64> {
        let header_bytes = self.header_pages as u64 * self.page_size as u64;
        header_bytes..(header_bytes + self.usable_bytes())
    }

    pub(super) fn get_header_pages(&self) -> u32 {
        self.header_pages
    }

    pub(super) fn num_pages(&self) -> u32 {
        self.num_pages
    }

    pub(super) fn page_size(&self) -> u32 {
        self.page_size
    }

    pub(super) fn len(&self) -> u64 {
        (self.header_pages as u64) * (self.page_size as u64) + self.usable_bytes()
    }

    pub(super) fn usable_bytes(&self) -> u64 {
        self.page_size as u64 * self.num_pages as u64
    }
}

#[derive(Clone, Copy, Debug)]
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

    pub(super) fn reduce_last_region(&mut self, pages: u32) {
        if let Some(ref mut trailing) = self.trailing_partial_region {
            assert!(pages <= trailing.num_pages);
            trailing.num_pages -= pages;
            if trailing.num_pages == 0 {
                self.trailing_partial_region = None;
            }
        } else {
            self.num_full_regions -= 1;
            let full_layout = self.full_region_layout;
            if full_layout.num_pages > pages {
                self.trailing_partial_region = Some(RegionLayout::new(
                    full_layout.num_pages - pages,
                    full_layout.header_pages,
                    full_layout.page_size,
                ));
            }
        }
    }

    pub(super) fn recalculate(
        file_len: u64,
        region_header_pages_u32: u32,
        region_max_data_pages_u32: u32,
        page_size_u32: u32,
    ) -> Self {
        let page_size = page_size_u32 as u64;
        let region_header_pages = region_header_pages_u32 as u64;
        let region_max_data_pages = region_max_data_pages_u32 as u64;
        // Super-header
        let mut remaining = file_len - page_size;
        let full_region_size = (region_header_pages + region_max_data_pages) * page_size;
        let full_regions = remaining / full_region_size;
        remaining -= full_regions * full_region_size;
        let trailing = if remaining >= (region_header_pages + 1) * page_size {
            remaining -= region_header_pages * page_size;
            let remaining: u32 = remaining.try_into().unwrap();
            let data_pages = remaining / page_size_u32;
            assert!(data_pages < region_max_data_pages_u32);
            Some(RegionLayout::new(
                data_pages,
                region_header_pages_u32,
                page_size_u32,
            ))
        } else {
            None
        };
        let full_layout = RegionLayout::new(
            region_max_data_pages_u32,
            region_header_pages_u32,
            page_size_u32,
        );

        Self {
            full_region_layout: full_layout,
            num_full_regions: full_regions.try_into().unwrap(),
            trailing_partial_region: trailing,
        }
    }

    pub(super) fn calculate(desired_usable_bytes: u64, page_capacity: u32, page_size: u32) -> Self {
        let full_region_layout = RegionLayout::full_region_layout(page_capacity, page_size);
        if desired_usable_bytes <= full_region_layout.usable_bytes() {
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
        }
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
        self.region_base_address(last) + self.region_layout(last).len()
    }

    pub(super) fn usable_bytes(&self) -> u64 {
        let trailing = self
            .trailing_partial_region
            .as_ref()
            .map(RegionLayout::usable_bytes)
            .unwrap_or_default();
        (self.num_full_regions as u64) * self.full_region_layout.usable_bytes() + trailing
    }

    pub(super) fn region_base_address(&self, region: u32) -> u64 {
        assert!(region < self.num_regions());
        (self.full_region_layout.page_size() as u64)
            + (region as u64) * self.full_region_layout.len()
    }

    pub(super) fn region_layout(&self, region: u32) -> RegionLayout {
        assert!(region < self.num_regions());
        if region == self.num_full_regions {
            self.trailing_partial_region.unwrap()
        } else {
            self.full_region_layout
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
