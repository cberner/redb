use crate::Error;
use memmap2::MmapMut;
use std::cell::{Ref, RefCell, RefMut};
use std::convert::TryInto;

pub(in crate) const ALL_MEMORY_HACK: u64 = u64::MAX;
pub(in crate) const DB_METADATA_PAGE: u64 = 0;

pub struct Page<'a> {
    mem: Ref<'a, [u8]>,
    page_number: u64,
}

impl<'a> Page<'a> {
    pub(in crate) fn memory(&self) -> &[u8] {
        &self.mem
    }

    #[allow(dead_code)]
    pub(in crate) fn get_page_number(&self) -> u64 {
        self.page_number
    }
}

pub(in crate) struct PageMut<'a> {
    mem: RefMut<'a, [u8]>,
    page_number: u64,
}

impl<'a> PageMut<'a> {
    pub(in crate) fn memory(&self) -> &[u8] {
        &self.mem
    }

    pub(in crate) fn memory_mut(&mut self) -> &mut [u8] {
        &mut self.mem
    }

    pub(in crate) fn get_page_number(&self) -> u64 {
        self.page_number
    }
}

pub(in crate) struct PageManager {
    next_free_page: RefCell<u64>,
    mmap: RefCell<MmapMut>,
}

impl PageManager {
    pub(in crate) const fn state_size() -> usize {
        8
    }

    pub(in crate) fn initialize(output: &mut [u8]) {
        output[0..8].copy_from_slice(&1u64.to_be_bytes());
    }

    pub(in crate) fn restore(mmap: MmapMut, state_offset: usize) -> Self {
        let next_free_page = u64::from_be_bytes(
            mmap[state_offset..(state_offset + Self::state_size())]
                .try_into()
                .unwrap(),
        );
        PageManager {
            next_free_page: RefCell::new(next_free_page),
            mmap: RefCell::new(mmap),
        }
    }

    pub(in crate) fn fsync(&self) -> Result<(), Error> {
        self.mmap.borrow().flush()?;

        Ok(())
    }

    pub(in crate) fn get_page(&self, page_number: u64) -> Page {
        // TODO: remove this
        if page_number == ALL_MEMORY_HACK {
            Page {
                mem: Ref::map(self.mmap.borrow(), |m| &m[..]),
                page_number,
            }
        } else {
            assert!(page_number < *self.next_free_page.borrow());
            let start = page_number as usize * page_size::get();
            let end = start + page_size::get();

            Page {
                mem: Ref::map(self.mmap.borrow(), |m| &m[start..end]),
                page_number,
            }
        }
    }

    pub(in crate) fn get_page_mut(&self, page_number: u64) -> PageMut {
        // TODO: remove this
        if page_number == ALL_MEMORY_HACK {
            PageMut {
                mem: RefMut::map(self.mmap.borrow_mut(), |m| &mut m[..]),
                page_number,
            }
        } else {
            assert!(page_number < *self.next_free_page.borrow());
            let start = page_number as usize * page_size::get();
            let end = start + page_size::get();

            PageMut {
                mem: RefMut::map(self.mmap.borrow_mut(), |m| &mut m[start..end]),
                page_number,
            }
        }
    }

    pub(in crate) fn hack_set_free_page_to_next_after(&self, offset: u64) {
        *self.next_free_page.borrow_mut() =
            (offset + page_size::get() as u64 - 1) / page_size::get() as u64;
    }

    pub(in crate) fn allocate(&self) -> PageMut {
        let page_number = *self.next_free_page.borrow();
        *self.next_free_page.borrow_mut() += 1;

        self.get_page_mut(page_number)
    }

    pub(in crate) fn store_state(&self, output: &mut [u8]) {
        output.copy_from_slice(&self.next_free_page.borrow().to_be_bytes());
    }
}
