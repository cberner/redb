use crate::tree_store::page_store::page_manager::MAX_MAX_PAGE_ORDER;
#[cfg(debug_assertions)]
use std::collections::HashMap;
#[cfg(debug_assertions)]
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
#[cfg(debug_assertions)]
use std::sync::Mutex;

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
        debug_assert!(page_index <= 0x000F_FFFF);
        debug_assert!(page_order <= MAX_MAX_PAGE_ORDER.try_into().unwrap());
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

    pub(crate) fn address_range(
        &self,
        data_section_offset: usize,
        region_size: u64,
        region_pages_start: usize,
        page_size: usize,
    ) -> Range<usize> {
        let regional_start =
            region_pages_start + self.page_index as usize * self.page_size_bytes(page_size);
        debug_assert!((regional_start as u64) < region_size);
        let region_base: usize = ((self.region as u64) * region_size).try_into().unwrap();
        let start = data_section_offset + region_base + regional_start;
        let end = start + self.page_size_bytes(page_size);
        start..end
    }

    pub(crate) fn page_size_bytes(&self, page_size: usize) -> usize {
        let pages = 1usize << self.page_order;
        pages * page_size
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

pub struct PageImpl<'a> {
    pub(super) mem: &'a [u8],
    pub(super) page_number: PageNumber,
    #[cfg(debug_assertions)]
    pub(super) open_pages: &'a Mutex<HashMap<PageNumber, u64>>,
}

impl<'a> Debug for PageImpl<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("PageImpl: page_number={:?}", self.page_number))
    }
}

#[cfg(debug_assertions)]
impl<'a> Drop for PageImpl<'a> {
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

impl<'a> PageImpl<'a> {
    pub(crate) fn into_memory(self) -> &'a [u8] {
        self.mem
    }
}

impl<'a> Page for PageImpl<'a> {
    fn memory(&self) -> &[u8] {
        self.mem
    }

    fn get_page_number(&self) -> PageNumber {
        self.page_number
    }
}

impl<'a> Clone for PageImpl<'a> {
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
            mem: self.mem,
            page_number: self.page_number,
            #[cfg(debug_assertions)]
            open_pages: self.open_pages,
        }
    }
}

pub(crate) struct PageMut<'a> {
    pub(super) mem: &'a mut [u8],
    pub(super) page_number: PageNumber,
    #[cfg(debug_assertions)]
    pub(super) open_pages: &'a Mutex<HashSet<PageNumber>>,
}

impl<'a> PageMut<'a> {
    pub(crate) fn memory_mut(&mut self) -> &mut [u8] {
        self.mem
    }
}

impl<'a> Page for PageMut<'a> {
    fn memory(&self) -> &[u8] {
        self.mem
    }

    fn get_page_number(&self) -> PageNumber {
        self.page_number
    }
}

#[cfg(debug_assertions)]
impl<'a> Drop for PageMut<'a> {
    fn drop(&mut self) {
        assert!(self.open_pages.lock().unwrap().remove(&self.page_number));
    }
}
