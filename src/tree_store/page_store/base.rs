use crate::tree_store::page_store::cached_file::WritablePage;
use crate::tree_store::page_store::page_manager::MAX_MAX_PAGE_ORDER;
use std::cmp::Ordering;
#[cfg(debug_assertions)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::mem;
use std::ops::Range;
use std::sync::Arc;
#[cfg(debug_assertions)]
use std::sync::Mutex;

pub(crate) const MAX_VALUE_LENGTH: usize = 3 * 1024 * 1024 * 1024;
pub(crate) const MAX_PAIR_LENGTH: usize = 3 * 1024 * 1024 * 1024 + 768 * 1024 * 1024;
pub(crate) const MAX_PAGE_INDEX: u32 = 0x000F_FFFF;
pub(crate) const MAX_REGIONS: u32 = 0x0010_0000;

// On-disk format is:
// lowest 20bits: page index within the region
// second 20bits: region number
// 19bits: reserved
// highest 5bits: page order exponent
//
// Assuming a reasonable page size, like 4kiB, this allows for 4kiB * 2^20 * 2^20 = 4PiB of usable space
#[derive(Copy, Clone, Eq, PartialEq)]
pub(crate) struct PageNumber {
    pub(crate) region: u32,
    pub(crate) page_index: u32,
    pub(crate) page_order: u8,
}

impl Hash for PageNumber {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // TODO: maybe we should store these fields as a single u64 in PageNumber. The field access
        // will be a little more expensive, but I think it's less frequent than these hashes
        let mut temp = 0x000F_FFFF & u64::from(self.page_index);
        temp |= (0x000F_FFFF & u64::from(self.region)) << 20;
        temp |= (0b0001_1111 & u64::from(self.page_order)) << 59;
        state.write_u64(temp);
    }
}

// PageNumbers are ordered as determined by their starting address in the database file
impl Ord for PageNumber {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.region.cmp(&other.region) {
            Ordering::Less => Ordering::Less,
            Ordering::Equal => {
                let self_order0 = self.page_index * 2u32.pow(self.page_order.into());
                let other_order0 = other.page_index * 2u32.pow(other.page_order.into());
                assert!(
                    self_order0 != other_order0 || self.page_order == other.page_order,
                    "{self:?} overlaps {other:?}, but is not equal"
                );
                self_order0.cmp(&other_order0)
            }
            Ordering::Greater => Ordering::Greater,
        }
    }
}

impl PartialOrd for PageNumber {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PageNumber {
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
        let mut temp = 0x000F_FFFF & u64::from(self.page_index);
        temp |= (0x000F_FFFF & u64::from(self.region)) << 20;
        temp |= (0b0001_1111 & u64::from(self.page_order)) << 59;
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
            region_pages_start + u64::from(self.page_index) * self.page_size_bytes(page_size);
        debug_assert!(regional_start < region_size);
        let region_base = u64::from(self.region) * region_size;
        let start = data_section_offset + region_base + regional_start;
        let end = start + self.page_size_bytes(page_size);
        start..end
    }

    pub(crate) fn page_size_bytes(&self, page_size: u32) -> u64 {
        let pages = 1u64 << self.page_order;
        pages * u64::from(page_size)
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
    pub(super) mem: Arc<[u8]>,
    pub(super) page_number: PageNumber,
    #[cfg(debug_assertions)]
    pub(super) open_pages: Arc<Mutex<HashMap<PageNumber, u64>>>,
}

impl PageImpl {
    pub(crate) fn to_arc(&self) -> Arc<[u8]> {
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

pub(crate) enum PageTrackerPolicy {
    Ignore,
    Track(HashSet<PageNumber>),
    Closed,
}

impl PageTrackerPolicy {
    pub(crate) fn new_tracking() -> Self {
        PageTrackerPolicy::Track(HashSet::new())
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            PageTrackerPolicy::Ignore | PageTrackerPolicy::Closed => true,
            PageTrackerPolicy::Track(x) => x.is_empty(),
        }
    }

    pub(super) fn remove(&mut self, page: PageNumber) {
        match self {
            PageTrackerPolicy::Ignore => {}
            PageTrackerPolicy::Track(x) => {
                assert!(x.remove(&page));
            }
            PageTrackerPolicy::Closed => {
                panic!("Page tracker is closed");
            }
        }
    }

    pub(super) fn insert(&mut self, page: PageNumber) {
        match self {
            PageTrackerPolicy::Ignore => {}
            PageTrackerPolicy::Track(x) => {
                assert!(x.insert(page));
            }
            PageTrackerPolicy::Closed => {
                panic!("Page tracker is closed");
            }
        }
    }

    pub(crate) fn close(&mut self) -> HashSet<PageNumber> {
        let old = mem::replace(self, PageTrackerPolicy::Closed);
        match old {
            PageTrackerPolicy::Ignore => HashSet::new(),
            PageTrackerPolicy::Track(x) => x,
            PageTrackerPolicy::Closed => {
                panic!("Page tracker is closed");
            }
        }
    }
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
