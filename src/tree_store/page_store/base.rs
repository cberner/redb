use crate::transaction_tracker::TransactionId;
use crate::tree_store::page_store::cached_file::WritablePage;
use crate::tree_store::page_store::page_manager::MAX_MAX_PAGE_ORDER;
use crate::Result;
#[cfg(debug_assertions)]
use std::collections::HashMap;
#[cfg(debug_assertions)]
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
#[cfg(not(debug_assertions))]
use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;
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

    // TODO: should return a u64 so that this works with large files on 32bit platforms
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

// TODO: remove this in favor of multiple Page implementations
#[derive(Clone)]
pub(super) enum PageHack<'a> {
    Ref(&'a [u8]),
    ArcMem(Arc<Vec<u8>>),
}

impl<'a> AsRef<[u8]> for PageHack<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            PageHack::Ref(x) => x,
            PageHack::ArcMem(x) => x,
        }
    }
}

// TODO: remove this in favor of multiple Page implementations
pub(super) enum PageHackMut<'a> {
    Ref(&'a mut [u8]),
    Writable(WritablePage<'a>),
}

impl<'a> AsRef<[u8]> for PageHackMut<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            PageHackMut::Ref(x) => x,
            PageHackMut::Writable(x) => x.mem(),
        }
    }
}

impl<'a> AsMut<[u8]> for PageHackMut<'a> {
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            PageHackMut::Ref(x) => x,
            PageHackMut::Writable(x) => x.mem_mut(),
        }
    }
}

pub struct PageImpl<'a> {
    pub(super) mem: PageHack<'a>,
    pub(super) page_number: PageNumber,
    #[cfg(debug_assertions)]
    pub(super) open_pages: &'a Mutex<HashMap<PageNumber, u64>>,
    #[cfg(not(debug_assertions))]
    pub(super) _debug_lifetime: PhantomData<&'a ()>,
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

impl<'a> Page for PageImpl<'a> {
    fn memory(&self) -> &[u8] {
        self.mem.as_ref()
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
            mem: self.mem.clone(),
            page_number: self.page_number,
            #[cfg(debug_assertions)]
            open_pages: self.open_pages,
            #[cfg(not(debug_assertions))]
            _debug_lifetime: Default::default(),
        }
    }
}

pub(crate) struct PageMut<'a> {
    pub(super) mem: PageHackMut<'a>,
    pub(super) page_number: PageNumber,
    #[cfg(debug_assertions)]
    pub(super) open_pages: &'a Mutex<HashSet<PageNumber>>,
    #[cfg(not(debug_assertions))]
    pub(super) _debug_lifetime: PhantomData<&'a ()>,
}

impl<'a> PageMut<'a> {
    pub(crate) fn memory_mut(&mut self) -> &mut [u8] {
        self.mem.as_mut()
    }
}

impl<'a> Page for PageMut<'a> {
    fn memory(&self) -> &[u8] {
        self.mem.as_ref()
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

#[derive(Copy, Clone)]
pub(crate) enum PageHint {
    None,
    Clean,
}

// TODO simplify this trait. It leaks a lot of details of the two implementations
pub(super) trait PhysicalStorage: Send + Sync {
    /// SAFETY: Caller must ensure that the values passed to this method are monotonically increasing
    // TODO: Remove this method and replace it with a call that returns an accessor that uses Arc to reference count the mmaps
    unsafe fn mark_transaction(&self, id: TransactionId);

    /// SAFETY: Caller must ensure that all references, from get_memory() or get_memory_mut(), created
    /// before the matching (same value) call to mark_transaction() have been dropped
    unsafe fn gc(&self, oldest_live_id: TransactionId) -> Result;

    /// SAFETY: if `new_len < len()`, caller must ensure that no references to
    /// memory in `new_len..len()` exist
    unsafe fn resize(&self, new_len: u64) -> Result;

    fn flush(&self) -> Result;

    fn eventual_flush(&self) -> Result;

    // Make writes visible to readers, but does not guarantee any durability
    fn write_barrier(&self) -> Result;

    // Read with caching. Caller must not read overlapping ranges without first calling invalidate_cache().
    // Doing so will not cause UB, but is a logic error.
    //
    // Safety: caller must ensure that [start, end) does not alias any existing references returned
    // from .write()
    unsafe fn read(&self, offset: u64, len: usize, hint: PageHint) -> Result<PageHack>;

    // Safety: caller must ensure that [start, end) does not alias any existing references returned
    // from .read() or .write()
    unsafe fn write(&self, offset: u64, len: usize) -> Result<PageHackMut>;

    // Read directly from the file, ignoring any cached data
    fn read_direct(&self, offset: u64, len: usize) -> Result<Vec<u8>>;

    // Discard pending writes to the given range
    fn cancel_pending_write(&self, offset: u64, len: usize);

    // Invalidate any caching of the given range. After this call overlapping reads of the range are allowed
    fn invalidate_cache(&self, offset: u64, len: usize);
}
