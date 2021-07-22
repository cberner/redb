use crate::Error;
use memmap2::MmapMut;
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::{Mutex, MutexGuard};

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

// Marker struct for the mutex guarding the meta page
struct MetapageGuard;

fn get_primary(metapage: &[u8]) -> &[u8] {
    let start = if metapage[PRIMARY_BIT_OFFSET] == 0 {
        TRANSACTION_0_OFFSET
    } else {
        TRANSACTION_1_OFFSET
    };
    let end = start + TRANSACTION_SIZE;

    &metapage[start..end]
}

fn get_secondary(metapage: &mut [u8]) -> &mut [u8] {
    let start = if metapage[PRIMARY_BIT_OFFSET] == 0 {
        TRANSACTION_1_OFFSET
    } else {
        TRANSACTION_0_OFFSET
    };
    let end = start + TRANSACTION_SIZE;

    &mut metapage[start..end]
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(in crate) struct PageNumber(pub u64);

impl PageNumber {
    pub(in crate) fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

struct TransactionAccessor<'a> {
    mem: &'a [u8],
    _guard: MutexGuard<'a, MetapageGuard>,
}

impl<'a> TransactionAccessor<'a> {
    fn new(mem: &'a [u8], guard: MutexGuard<'a, MetapageGuard>) -> Self {
        TransactionAccessor { mem, _guard: guard }
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
    mem: &'a mut [u8],
    _guard: MutexGuard<'a, MetapageGuard>,
}

impl<'a> TransactionMutator<'a> {
    fn new(mem: &'a mut [u8], guard: MutexGuard<'a, MetapageGuard>) -> Self {
        TransactionMutator { mem, _guard: guard }
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
    mem: &'a [u8],
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
    mem: &'a mut [u8],
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
    mmap: MmapMut,
    // We use unsafe to access the metapage (page 0), and so guard it with this mutex
    // It would be nice if this was a RefCell<&[u8]> on the metapage. However, that would be
    // self-referential, since we also hold the mmap object
    metapage_guard: Mutex<MetapageGuard>,
    // The number of PageMut which are outstanding
    open_dirty_pages: RefCell<HashSet<PageNumber>>,
}

impl TransactionalMemory {
    pub(in crate) fn new(mut mmap: MmapMut) -> Result<Self, Error> {
        // Ensure that the database metadata fits into the first page
        assert!(page_size::get() >= DB_METAPAGE_SIZE);

        let mutex = Mutex::new(MetapageGuard {});
        if mmap[0..MAGICNUMBER.len()] != MAGICNUMBER {
            // Explicitly zero the memory
            mmap[0..DB_METAPAGE_SIZE].copy_from_slice(&[0; DB_METAPAGE_SIZE]);

            // Set to 1, so that we can mutate the first transaction state
            mmap[PRIMARY_BIT_OFFSET] = 1;
            let mut mutator =
                TransactionMutator::new(get_secondary(&mut mmap), mutex.lock().unwrap());
            mutator.set_root_page(PageNumber(0));
            mutator.set_allocator_state(DB_METADATA_PAGE + 1);
            drop(mutator);
            // Make the state we just wrote the primary
            mmap[PRIMARY_BIT_OFFSET] = 0;

            mmap.flush()?;
            // Write the magic number only after the data structure is initialized and written to disk
            // to ensure that it's crash safe
            mmap[0..MAGICNUMBER.len()].copy_from_slice(&MAGICNUMBER);
            mmap.flush()?;
        }
        let accessor = TransactionAccessor::new(get_primary(&mmap), mutex.lock().unwrap());
        let next_free_page = Cell::new(PageNumber(accessor.get_allocator_state()));
        drop(accessor);

        Ok(TransactionalMemory {
            next_free_page,
            mmap,
            metapage_guard: mutex,
            open_dirty_pages: RefCell::new(HashSet::new()),
        })
    }

    fn acquire_mutable_metapage(&self) -> (&mut [u8], MutexGuard<MetapageGuard>) {
        let guard = self.metapage_guard.lock().unwrap();
        let ptr = &self.mmap as *const MmapMut as *mut MmapMut;
        // Safety: we acquire the metapage lock and only access the metapage
        let mem = unsafe { &mut (*ptr)[0..DB_METAPAGE_SIZE] };

        (mem, guard)
    }

    // Commit all outstanding changes and make them visible as the primary
    pub(in crate) fn commit(&self) -> Result<(), Error> {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        assert!(self.open_dirty_pages.borrow().is_empty());

        let (mmap, guard) = self.acquire_mutable_metapage();

        let mut mutator = TransactionMutator::new(get_secondary(mmap), guard);
        mutator.set_allocator_state(self.next_free_page.get().0);
        drop(mutator);
        self.mmap.flush()?;

        let next = match self.mmap[PRIMARY_BIT_OFFSET] {
            0 => 1,
            1 => 0,
            _ => unreachable!(),
        };
        let (mmap, guard) = self.acquire_mutable_metapage();
        mmap[PRIMARY_BIT_OFFSET] = next;
        drop(guard); // Ensure the guard lives past the write on the previous line
        self.mmap.flush()?;

        Ok(())
    }

    pub(in crate) fn get_page(&self, page_number: PageNumber) -> Page {
        assert!(page_number < self.next_free_page.get());
        // We must not retrieve an immutable reference to a page which already has a mutable ref to it
        assert!(!self.open_dirty_pages.borrow().contains(&page_number));
        let start = page_number.0 as usize * page_size::get();
        let end = start + page_size::get();

        Page {
            mem: &self.mmap[start..end],
            page_number,
        }
    }

    pub(in crate) fn get_primary_root_page(&self) -> Option<PageNumber> {
        TransactionAccessor::new(get_primary(&self.mmap), self.metapage_guard.lock().unwrap())
            .get_root_page()
    }

    pub(in crate) fn set_secondary_root_page(&self, root_page: PageNumber) {
        let (mmap, guard) = self.acquire_mutable_metapage();
        let mut mutator = TransactionMutator::new(get_secondary(mmap), guard);
        mutator.set_root_page(root_page);
    }

    pub(in crate) fn allocate(&self) -> PageMut {
        let page_number = self.next_free_page.get();

        self.next_free_page.set(PageNumber(page_number.0 + 1));

        self.open_dirty_pages.borrow_mut().insert(page_number);

        let start = page_number.0 as usize * page_size::get();
        let end = start + page_size::get();

        let address = &self.mmap as *const MmapMut as *mut MmapMut;
        // Safety:
        // All PageMut are registered in open_dirty_pages, and no immutable references are allowed
        // to those pages
        let mem = unsafe { &mut (*address)[start..end] };

        PageMut {
            mem,
            page_number,
            open_pages: &self.open_dirty_pages,
        }
    }
}
