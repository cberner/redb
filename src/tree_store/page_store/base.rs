use crate::tree_store::page_store::cached_file::WritablePage;
use crate::tree_store::page_store::page_manager::MAX_MAX_PAGE_ORDER;
#[cfg(debug_assertions)]
use std::collections::HashMap;
#[cfg(debug_assertions)]
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::Arc;
#[cfg(debug_assertions)]
use std::sync::Mutex;

pub(crate) const MAX_VALUE_LENGTH: usize = 3 * 1024 * 1024 * 1024;
pub(crate) const MAX_PAGE_INDEX: u32 = 0x000F_FFFF;

// On-disk format is:
// lowest 20bits: page index within the region
// second 20bits: region number
// 19bits: reserved
// highest 5bits: page order exponent
//
// Assuming a reasonable page size, like 4kiB, this allows for 4kiB * 2^20 * 2^20 = 4PiB of usable space
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) struct PageNumber {
    pub(crate) region: u32,
    pub(crate) page_index: u32,
    pub(crate) page_order: u8,
}

impl PageNumber {
    #[inline(always)]
    pub(crate) const fn serialized_size() -> usize {
        8
    }

    pub(crate) fn new(region: u32, page_index: u32, page_order: u8) -> Self {
        debug_assert!(region <= 0x000F_FFFF);
        debug_assert!(page_index <= MAX_PAGE_INDEX);
        debug_assert!(page_order <= MAX_MAX_PAGE_ORDER);
        Self {
            region,
            page_index,
            page_order,
        }
    }

    pub(crate) fn to_le_bytes(self) -> [u8; 8] {
        let mut temp = (0x000F_FFFF & self.page_index) as u64;
        temp |= (0x000F_FFFF & self.region as u64) << 20;
        temp |= (0b0001_1111 & self.page_order as u64) << 59;
        temp.to_le_bytes()
    }

    pub(crate) fn from_le_bytes(bytes: [u8; 8]) -> Self {
        let temp = u64::from_le_bytes(bytes);
        let index = (temp & 0x000F_FFFF) as u32;
        let region = ((temp >> 20) & 0x000F_FFFF) as u32;
        let order = (temp >> 59) as u8;

        Self {
            region,
            page_index: index,
            page_order: order,
        }
    }

    // Returns true if this PageNumber is before the other PageNumber in the file layout
    pub(crate) fn is_before(&self, other: PageNumber) -> bool {
        if self.region < other.region {
            return true;
        }
        if self.region > other.region {
            return false;
        }
        let self_order0 = self.page_index * 2u32.pow(self.page_order as u32);
        let other_order0 = other.page_index * 2u32.pow(other.page_order as u32);
        assert_ne!(self_order0, other_order0, "{self:?} overlaps {other:?}");
        self_order0 < other_order0
    }

    #[cfg(test)]
    pub(crate) fn to_order0(self) -> Vec<PageNumber> {
        let mut pages = vec![self];
        loop {
            let mut progress = false;
            let mut new_pages = vec![];
            for page in pages {
                if page.page_order == 0 {
                    new_pages.push(page);
                } else {
                    progress = true;
                    new_pages.push(PageNumber::new(
                        page.region,
                        page.page_index * 2,
                        page.page_order - 1,
                    ));
                    new_pages.push(PageNumber::new(
                        page.region,
                        page.page_index * 2 + 1,
                        page.page_order - 1,
                    ));
                }
            }
            pages = new_pages;
            if !progress {
                break;
            }
        }

        pages
    }

    pub(crate) fn address_range(
        &self,
        data_section_offset: u64,
        region_size: u64,
        region_pages_start: u64,
        page_size: u32,
    ) -> Range<u64> {
        let regional_start =
            region_pages_start + (self.page_index as u64) * self.page_size_bytes(page_size);
        debug_assert!(regional_start < region_size);
        let region_base = (self.region as u64) * region_size;
        let start = data_section_offset + region_base + regional_start;
        let end = start + self.page_size_bytes(page_size);
        start..end
    }

    pub(crate) fn page_size_bytes(&self, page_size: u32) -> u64 {
        let pages = 1u64 << self.page_order;
        pages * (page_size as u64)
    }
}

impl Debug for PageNumber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "r{}.{}/{}",
            self.region, self.page_index, self.page_order
        )
    }
}

pub(crate) trait Page {
    fn memory(&self) -> &[u8];

    fn get_page_number(&self) -> PageNumber;
}

pub struct PageImpl {
    pub(super) mem: Arc<Vec<u8>>,
    pub(super) page_number: PageNumber,
    #[cfg(debug_assertions)]
    pub(super) open_pages: Arc<Mutex<HashMap<PageNumber, u64>>>,
}

impl PageImpl {
    pub(crate) fn to_arc_vec(&self) -> Arc<Vec<u8>> {
        self.mem.clone()
    }
}

impl Debug for PageImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("PageImpl: page_number={:?}", self.page_number))
    }
}

#[cfg(debug_assertions)]
impl Drop for PageImpl {
    fn drop(&mut self) {
        let mut open_pages = self.open_pages.lock().unwrap();
        let value = open_pages.get_mut(&self.page_number).unwrap();
        assert!(*value > 0);
        *value -= 1;
        if *value == 0 {
            open_pages.remove(&self.page_number);
        }
    }
}

impl Page for PageImpl {
    fn memory(&self) -> &[u8] {
        self.mem.as_ref()
    }

    fn get_page_number(&self) -> PageNumber {
        self.page_number
    }
}

impl Clone for PageImpl {
    fn clone(&self) -> Self {
        #[cfg(debug_assertions)]
        {
            *self
                .open_pages
                .lock()
                .unwrap()
                .get_mut(&self.page_number)
                .unwrap() += 1;
        }
        Self {
            mem: self.mem.clone(),
            page_number: self.page_number,
            #[cfg(debug_assertions)]
            open_pages: self.open_pages.clone(),
        }
    }
}

pub(crate) struct PageMut {
    pub(super) mem: WritablePage,
    pub(super) page_number: PageNumber,
    #[cfg(debug_assertions)]
    pub(super) open_pages: Arc<Mutex<HashSet<PageNumber>>>,
}

impl PageMut {
    pub(crate) fn memory_mut(&mut self) -> &mut [u8] {
        self.mem.mem_mut()
    }
}

impl Page for PageMut {
    fn memory(&self) -> &[u8] {
        self.mem.mem()
    }

    fn get_page_number(&self) -> PageNumber {
        self.page_number
    }
}

#[cfg(debug_assertions)]
impl Drop for PageMut {
    fn drop(&mut self) {
        assert!(self.open_pages.lock().unwrap().remove(&self.page_number));
    }
}

#[derive(Copy, Clone)]
pub(crate) enum PageHint {
    None,
    Clean,
}

#[cfg(test)]
mod test {
    use crate::tree_store::PageNumber;

    #[test]
    fn last_page() {
        let region_data_size = 2u64.pow(32);
        let page_size = 4096;
        let pages_per_region = region_data_size / page_size;
        let region_header_size = 2u64.pow(16);
        let last_page_index = pages_per_region - 1;
        let page_number = PageNumber::new(1, last_page_index.try_into().unwrap(), 0);
        page_number.address_range(
            4096,
            region_data_size + region_header_size,
            region_header_size,
            page_size.try_into().unwrap(),
        );
    }
}
