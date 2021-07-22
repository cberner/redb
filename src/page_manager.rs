use crate::Error;
use memmap2::MmapMut;
use std::cell::{Cell, Ref, RefCell, RefMut};
use std::collections::HashSet;
use std::convert::TryInto;

const DB_METADATA_PAGE: u64 = 0;

const MAGICNUMBER: [u8; 4] = [b'r', b'e', b'd', b'b'];
const PRIMARY_BIT_OFFSET: usize = MAGICNUMBER.len();
const TRANSACTION_SIZE: usize = 128;
const TRANSACTION_0_OFFSET: usize = 128;
const TRANSACTION_1_OFFSET: usize = TRANSACTION_0_OFFSET + TRANSACTION_SIZE;
const DB_METAPAGE_SIZE: usize = TRANSACTION_1_OFFSET + 128;

// Structure of each metapage
const ROOT_PAGE_OFFSET: usize = 0;
const ALLOCATOR_STATE_OFFSET: usize = 8;

fn get_primary(mmap: &RefCell<MmapMut>) -> Ref<[u8]> {
    let start = if mmap.borrow()[PRIMARY_BIT_OFFSET] == 0 {
        TRANSACTION_0_OFFSET
    } else {
        TRANSACTION_1_OFFSET
    };
    let end = start + TRANSACTION_SIZE;

    Ref::map(mmap.borrow(), |m| &m[start..end])
}

fn get_secondary(mmap: &RefCell<MmapMut>) -> RefMut<[u8]> {
    let start = if mmap.borrow()[PRIMARY_BIT_OFFSET] == 0 {
        TRANSACTION_1_OFFSET
    } else {
        TRANSACTION_0_OFFSET
    };
    let end = start + TRANSACTION_SIZE;

    RefMut::map(mmap.borrow_mut(), |m| &mut m[start..end])
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(in crate) struct PageNumber(pub u64);

impl PageNumber {
    pub(in crate) fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

struct TransactionAccessor<'a> {
    mem: Ref<'a, [u8]>,
}

impl<'a> TransactionAccessor<'a> {
    fn new(mem: Ref<'a, [u8]>) -> Self {
        TransactionAccessor { mem }
    }

    fn get_root_page(&self) -> Option<PageNumber> {
        let num = u64::from_be_bytes(
            self.mem[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + 8)]
                .try_into()
                .unwrap(),
        );
        if num == 0 {
            None
        } else {
            Some(PageNumber(num))
        }
    }

    fn get_allocator_state(&self) -> u64 {
        u64::from_be_bytes(
            self.mem[ALLOCATOR_STATE_OFFSET..(ALLOCATOR_STATE_OFFSET + 8)]
                .try_into()
                .unwrap(),
        )
    }
}

struct TransactionMutator<'a> {
    mem: RefMut<'a, [u8]>,
}

impl<'a> TransactionMutator<'a> {
    fn new(mem: RefMut<'a, [u8]>) -> Self {
        TransactionMutator { mem }
    }

    fn set_root_page(&mut self, page_number: PageNumber) {
        self.mem[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + 8)]
            .copy_from_slice(&page_number.to_be_bytes());
    }

    fn set_allocator_state(&mut self, allocator_state: u64) {
        self.mem[ALLOCATOR_STATE_OFFSET..(ALLOCATOR_STATE_OFFSET + 8)]
            .copy_from_slice(&allocator_state.to_be_bytes());
    }
}

pub struct Page<'a> {
    mem: Ref<'a, [u8]>,
    page_number: PageNumber,
}

impl<'a> Page<'a> {
    pub(in crate) fn memory(&self) -> &[u8] {
        &self.mem
    }

    pub(in crate) fn get_page_number(&self) -> PageNumber {
        self.page_number
    }
}

pub(in crate) struct PageMut<'a> {
    mem: RefMut<'a, [u8]>,
    page_number: PageNumber,
    open_pages: &'a RefCell<HashSet<PageNumber>>,
}

impl<'a> PageMut<'a> {
    pub(in crate) fn memory(&self) -> &[u8] {
        &self.mem
    }

    pub(in crate) fn memory_mut(&mut self) -> &mut [u8] {
        &mut self.mem
    }

    pub(in crate) fn get_page_number(&self) -> PageNumber {
        self.page_number
    }
}

impl<'a> Drop for PageMut<'a> {
    fn drop(&mut self) {
        self.open_pages.borrow_mut().remove(&self.page_number);
    }
}

pub(in crate) struct TransactionalMemory {
    next_free_page: Cell<PageNumber>,
    mmap: RefCell<MmapMut>,
    // The number of PageMut which are outstanding
    open_dirty_pages: RefCell<HashSet<PageNumber>>,
}

impl TransactionalMemory {
    pub(in crate) fn new(mmap: MmapMut) -> Result<Self, Error> {
        // Ensure that the database metadata fits into the first page
        assert!(page_size::get() >= DB_METAPAGE_SIZE);

        let mmap = RefCell::new(mmap);
        if mmap.borrow()[0..MAGICNUMBER.len()] != MAGICNUMBER {
            // Explicitly zero the memory
            mmap.borrow_mut()[0..DB_METAPAGE_SIZE].copy_from_slice(&[0; DB_METAPAGE_SIZE]);

            // Set to 1, so that we can mutate the first transaction state
            mmap.borrow_mut()[PRIMARY_BIT_OFFSET] = 1;
            let mut mutator = TransactionMutator::new(get_secondary(&mmap));
            mutator.set_root_page(PageNumber(0));
            mutator.set_allocator_state(DB_METADATA_PAGE + 1);
            drop(mutator);
            // Make the state we just wrote the primary
            mmap.borrow_mut()[PRIMARY_BIT_OFFSET] = 0;

            mmap.borrow().flush()?;
            // Write the magic number only after the data structure is initialized and written to disk
            // to ensure that it's crash safe
            mmap.borrow_mut()[0..MAGICNUMBER.len()].copy_from_slice(&MAGICNUMBER);
            mmap.borrow().flush()?;
        }
        let accessor = TransactionAccessor::new(get_primary(&mmap));
        let next_free_page = Cell::new(PageNumber(accessor.get_allocator_state()));
        drop(accessor);

        Ok(TransactionalMemory {
            next_free_page,
            mmap,
            open_dirty_pages: RefCell::new(HashSet::new()),
        })
    }

    // Commit all outstanding changes and make them visible as the primary
    pub(in crate) fn commit(&self) -> Result<(), Error> {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        assert!(self.open_dirty_pages.borrow().is_empty());
        let mut mutator = TransactionMutator::new(get_secondary(&self.mmap));
        mutator.set_allocator_state(self.next_free_page.get().0);
        drop(mutator);
        self.mmap.borrow().flush()?;

        let next = match self.mmap.borrow()[PRIMARY_BIT_OFFSET] {
            0 => 1,
            1 => 0,
            _ => unreachable!(),
        };
        self.mmap.borrow_mut()[PRIMARY_BIT_OFFSET] = next;
        self.mmap.borrow().flush()?;

        Ok(())
    }

    pub(in crate) fn get_page(&self, page_number: PageNumber) -> Page {
        assert!(page_number < self.next_free_page.get());
        let start = page_number.0 as usize * page_size::get();
        let end = start + page_size::get();

        Page {
            mem: Ref::map(self.mmap.borrow(), |m| &m[start..end]),
            page_number,
        }
    }

    pub(in crate) fn get_primary_root_page(&self) -> Option<PageNumber> {
        TransactionAccessor::new(get_primary(&self.mmap)).get_root_page()
    }

    pub(in crate) fn set_secondary_root_page(&self, root_page: PageNumber) {
        let mut mutator = TransactionMutator::new(get_secondary(&self.mmap));
        mutator.set_root_page(root_page);
    }

    pub(in crate) fn allocate(&self) -> PageMut {
        let page_number = self.next_free_page.get();
        self.next_free_page.set(PageNumber(page_number.0 + 1));

        let start = page_number.0 as usize * page_size::get();
        let end = start + page_size::get();

        self.open_dirty_pages.borrow_mut().insert(page_number);

        PageMut {
            mem: RefMut::map(self.mmap.borrow_mut(), |m| &mut m[start..end]),
            page_number,
            open_pages: &self.open_dirty_pages,
        }
    }
}
