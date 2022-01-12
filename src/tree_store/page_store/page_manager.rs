use crate::tree_store::page_store::mmap::Mmap;
use crate::tree_store::page_store::page_allocator::BuddyAllocator;
use crate::tree_store::page_store::utils::get_page_size;
use crate::Error;
use memmap2::MmapRaw;
use std::cell::RefCell;
use std::cmp::min;
use std::collections::HashSet;
use std::convert::TryInto;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

const MAX_PAGE_ORDER: usize = 20;

const DB_METADATA_PAGE: u64 = 0;

const MAGICNUMBER: [u8; 4] = [b'r', b'e', b'd', b'b'];
const VERSION_OFFSET: usize = MAGICNUMBER.len();
const PAGE_SIZE_OFFSET: usize = VERSION_OFFSET + 1;
const DB_SIZE_OFFSET: usize = PAGE_SIZE_OFFSET + 1;
const PRIMARY_BIT_OFFSET: usize = DB_SIZE_OFFSET + size_of::<u64>();
const TRANSACTION_SIZE: usize = 128;
const TRANSACTION_0_OFFSET: usize = 128;
const TRANSACTION_1_OFFSET: usize = TRANSACTION_0_OFFSET + TRANSACTION_SIZE;
const DB_METAPAGE_SIZE: usize = TRANSACTION_1_OFFSET + TRANSACTION_SIZE;

// Structure of each metapage
const ROOT_PAGE_OFFSET: usize = 0;
const ROOT_PAGE_MESSAGES_OFFSET: usize = ROOT_PAGE_OFFSET + size_of::<u64>();
const TRANSACTION_ID_OFFSET: usize = ROOT_PAGE_MESSAGES_OFFSET + size_of::<u8>();
// Memory pointed to by this ptr is logically part of the metapage
const ALLOCATOR_STATE_PTR_OFFSET: usize = TRANSACTION_ID_OFFSET + size_of::<u128>();
const ALLOCATOR_STATE_LEN_OFFSET: usize = ALLOCATOR_STATE_PTR_OFFSET + size_of::<u64>();
// TODO: these dirty flags should be part of the PRIMARY_BIT byte, so that they can be written atomically
const ALLOCATOR_STATE_DIRTY_OFFSET: usize = ALLOCATOR_STATE_LEN_OFFSET + size_of::<u64>();

pub(crate) fn get_db_size(path: impl AsRef<Path>) -> Result<usize, io::Error> {
    let mut db_size = [0u8; size_of::<u64>()];
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(DB_SIZE_OFFSET as u64))?;
    file.read_exact(&mut db_size)?;

    Ok(u64::from_be_bytes(db_size) as usize)
}

// TODO: make this function transactional and crash safe
pub(crate) fn expand_db_size(path: impl AsRef<Path>, new_size: usize) -> Result<(), Error> {
    let old_size = get_db_size(path.as_ref())?;

    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    file.seek(SeekFrom::Start(PAGE_SIZE_OFFSET as u64))?;
    let mut buffer = [0; 1];
    file.read_exact(&mut buffer)?;

    let page_size = 1usize << buffer[0];
    let max_order = TransactionalMemory::calculate_usable_order(old_size, page_size as usize);
    let old_usable_pages =
        TransactionalMemory::calculate_usable_pages(old_size, page_size as usize, max_order);
    let max_order = TransactionalMemory::calculate_usable_order(new_size, page_size as usize);
    let usable_pages =
        TransactionalMemory::calculate_usable_pages(new_size, page_size as usize, max_order);
    assert!(usable_pages >= old_usable_pages);

    let allocator_state_size = BuddyAllocator::required_space(usable_pages, max_order);

    // Dirty the allocator state, so that it will be rebuilt
    file.seek(SeekFrom::Start(
        (TRANSACTION_0_OFFSET + ALLOCATOR_STATE_DIRTY_OFFSET) as u64,
    ))?;
    file.write_all(&[1])?;
    file.seek(SeekFrom::Start(
        (TRANSACTION_1_OFFSET + ALLOCATOR_STATE_DIRTY_OFFSET) as u64,
    ))?;
    file.write_all(&[1])?;

    // Write the new allocator state pointers
    let start = new_size - 2 * allocator_state_size;
    file.seek(SeekFrom::Start(
        (TRANSACTION_0_OFFSET + ALLOCATOR_STATE_PTR_OFFSET) as u64,
    ))?;
    file.write_all(&(start as u64).to_be_bytes())?;
    file.seek(SeekFrom::Start(
        (TRANSACTION_0_OFFSET + ALLOCATOR_STATE_LEN_OFFSET) as u64,
    ))?;
    file.write_all(&(allocator_state_size as u64).to_be_bytes())?;
    let start = new_size - allocator_state_size;
    file.seek(SeekFrom::Start(
        (TRANSACTION_1_OFFSET + ALLOCATOR_STATE_PTR_OFFSET) as u64,
    ))?;
    file.write_all(&(start as u64).to_be_bytes())?;
    file.seek(SeekFrom::Start(
        (TRANSACTION_1_OFFSET + ALLOCATOR_STATE_LEN_OFFSET) as u64,
    ))?;
    file.write_all(&(allocator_state_size as u64).to_be_bytes())?;

    file.sync_all()?;
    file.seek(SeekFrom::Start(DB_SIZE_OFFSET as u64))?;
    file.write_all(&(new_size as u64).to_be_bytes())?;
    file.sync_all()?;

    file.set_len(new_size as u64)?;
    file.sync_all()?;

    Ok(())
}

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

// Warning! This method is only safe to use when modifying the allocator state and when the dirty bit
// is already set and fsync'ed to the backing file
fn get_primary_mut(metapage: &mut [u8]) -> &mut [u8] {
    let start = if metapage[PRIMARY_BIT_OFFSET] == 0 {
        TRANSACTION_0_OFFSET
    } else {
        TRANSACTION_1_OFFSET
    };
    let end = start + TRANSACTION_SIZE;

    &mut metapage[start..end]
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

fn get_secondary_const(metapage: &[u8]) -> &[u8] {
    let start = if metapage[PRIMARY_BIT_OFFSET] == 0 {
        TRANSACTION_1_OFFSET
    } else {
        TRANSACTION_0_OFFSET
    };
    let end = start + TRANSACTION_SIZE;

    &metapage[start..end]
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub(crate) struct PageNumber {
    page_index: u64,
    page_order: u8,
}

impl PageNumber {
    // TODO: remove this
    pub(crate) fn null() -> Self {
        Self::new(0, 0)
    }

    fn new(page_index: u64, page_order: u8) -> Self {
        Self {
            page_index,
            page_order,
        }
    }

    pub(crate) fn to_be_bytes(self) -> [u8; 8] {
        let mut temp = self.page_index;
        temp |= (self.page_order as u64) << 48;
        temp.to_be_bytes()
    }

    pub(crate) fn from_be_bytes(bytes: [u8; 8]) -> Self {
        let temp = u64::from_be_bytes(bytes);
        let index = temp & 0x0000_FFFF_FFFF_FFFF;
        let order = (temp >> 48) as u8;

        Self::new(index, order)
    }

    fn address_range(&self, page_size: usize) -> Range<usize> {
        let pages = 1usize << self.page_order;
        (self.page_index as usize * pages * page_size)
            ..((self.page_index as usize + 1) * pages * page_size)
    }

    fn page_size_bytes(&self, page_size: usize) -> usize {
        let pages = 1usize << self.page_order;
        pages * page_size
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

    fn get_root_page(&self) -> Option<(PageNumber, u8)> {
        let num = PageNumber::from_be_bytes(
            self.mem[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + 8)]
                .try_into()
                .unwrap(),
        );
        let messages = self.mem[ROOT_PAGE_MESSAGES_OFFSET];
        if num.page_index == 0 {
            None
        } else {
            Some((num, messages))
        }
    }

    fn get_last_committed_transaction_id(&self) -> u128 {
        u128::from_be_bytes(
            self.mem[TRANSACTION_ID_OFFSET..(TRANSACTION_ID_OFFSET + size_of::<u128>())]
                .try_into()
                .unwrap(),
        )
    }

    #[allow(dead_code)]
    fn get_allocator_data(&self) -> (usize, usize) {
        let start = u64::from_be_bytes(
            self.mem[ALLOCATOR_STATE_PTR_OFFSET..(ALLOCATOR_STATE_PTR_OFFSET + size_of::<u64>())]
                .try_into()
                .unwrap(),
        );
        let len = u64::from_be_bytes(
            self.mem[ALLOCATOR_STATE_LEN_OFFSET..(ALLOCATOR_STATE_LEN_OFFSET + size_of::<u64>())]
                .try_into()
                .unwrap(),
        );
        (start as usize, (start + len) as usize)
    }

    fn get_allocator_dirty(&self) -> bool {
        let value = u8::from_be_bytes(
            self.mem
                [ALLOCATOR_STATE_DIRTY_OFFSET..(ALLOCATOR_STATE_DIRTY_OFFSET + size_of::<u8>())]
                .try_into()
                .unwrap(),
        );
        match value {
            0 => false,
            1 => true,
            _ => unreachable!(),
        }
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

    fn set_root_page(&mut self, page_number: PageNumber, valid_messages: u8) {
        self.mem[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + 8)]
            .copy_from_slice(&page_number.to_be_bytes());
        self.mem[ROOT_PAGE_MESSAGES_OFFSET] = valid_messages;
    }

    fn set_last_committed_transaction_id(&mut self, transaction_id: u128) {
        self.mem[TRANSACTION_ID_OFFSET..(TRANSACTION_ID_OFFSET + size_of::<u128>())]
            .copy_from_slice(&transaction_id.to_be_bytes());
    }

    fn set_allocator_data(&mut self, start: usize, len: usize) {
        self.mem[ALLOCATOR_STATE_PTR_OFFSET..(ALLOCATOR_STATE_PTR_OFFSET + size_of::<u64>())]
            .copy_from_slice(&(start as u64).to_be_bytes());
        self.mem[ALLOCATOR_STATE_LEN_OFFSET..(ALLOCATOR_STATE_LEN_OFFSET + size_of::<u64>())]
            .copy_from_slice(&(len as u64).to_be_bytes());
    }

    fn get_allocator_data(&self) -> (usize, usize) {
        let start = u64::from_be_bytes(
            self.mem[ALLOCATOR_STATE_PTR_OFFSET..(ALLOCATOR_STATE_PTR_OFFSET + size_of::<u64>())]
                .try_into()
                .unwrap(),
        );
        let len = u64::from_be_bytes(
            self.mem[ALLOCATOR_STATE_LEN_OFFSET..(ALLOCATOR_STATE_LEN_OFFSET + size_of::<u64>())]
                .try_into()
                .unwrap(),
        );
        (start as usize, (start + len) as usize)
    }

    fn set_allocator_dirty(&mut self, dirty: bool) {
        if dirty {
            self.mem[ALLOCATOR_STATE_DIRTY_OFFSET] = 1;
        } else {
            self.mem[ALLOCATOR_STATE_DIRTY_OFFSET] = 0;
        }
    }

    fn get_allocator_dirty(&self) -> bool {
        let value = u8::from_be_bytes(
            self.mem
                [ALLOCATOR_STATE_DIRTY_OFFSET..(ALLOCATOR_STATE_DIRTY_OFFSET + size_of::<u8>())]
                .try_into()
                .unwrap(),
        );
        match value {
            0 => false,
            1 => true,
            _ => unreachable!(),
        }
    }

    fn into_guard(self) -> MutexGuard<'a, MetapageGuard> {
        self._guard
    }
}

pub(crate) trait Page {
    fn memory(&self) -> &[u8];

    fn get_page_number(&self) -> PageNumber;
}

pub struct PageImpl<'a> {
    mem: &'a [u8],
    page_number: PageNumber,
}

impl<'a> Debug for PageImpl<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("PageImpl: page_number={:?}", self.page_number))
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

pub(crate) struct PageMut<'a> {
    mem: &'a mut [u8],
    page_number: PageNumber,
    open_pages: &'a RefCell<HashSet<PageNumber>>,
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

impl<'a> Drop for PageMut<'a> {
    fn drop(&mut self) {
        self.open_pages.borrow_mut().remove(&self.page_number);
    }
}

pub(crate) struct TransactionalMemory {
    // Pages allocated since the last commit
    allocated_since_commit: RefCell<HashSet<PageNumber>>,
    freed_since_commit: RefCell<Vec<PageNumber>>,
    // Metapage guard lock should be held when using this to modify the page allocator state
    // May be None, if the allocator state was corrupted when the file was opened
    page_allocator: Option<BuddyAllocator>,
    mmap: Mmap,
    // We use unsafe to access the metapage (page 0), and so guard it with this mutex
    // It would be nice if this was a RefCell<&[u8]> on the metapage. However, that would be
    // self-referential, since we also hold the mmap object
    metapage_guard: Mutex<MetapageGuard>,
    // The number of PageMut which are outstanding
    open_dirty_pages: RefCell<HashSet<PageNumber>>,
    // Indicates that a non-durable commit has been made, so reads should be served from the secondary meta page
    read_from_secondary: AtomicBool,
    page_size: usize,
}

impl TransactionalMemory {
    fn calculate_usable_order(mmap_size: usize, page_size: usize) -> usize {
        let total_pages = mmap_size / page_size;
        // Require at least 10 of the highest order pages
        let largest_order_in_pages = total_pages / 10;
        assert!(largest_order_in_pages > 0);
        let max_order = (64 - largest_order_in_pages.leading_zeros() - 1) as usize;
        min(MAX_PAGE_ORDER, max_order)
    }

    fn calculate_usable_pages(mmap_size: usize, page_size: usize, max_order: usize) -> usize {
        let mut guess = mmap_size / page_size;
        let mut new_guess =
            (mmap_size - 2 * BuddyAllocator::required_space(guess, max_order)) / page_size;
        // Make sure we don't loop forever. This might not converge if it oscillates
        let mut i = 0;
        while guess != new_guess && i < 1000 {
            guess = new_guess;
            new_guess =
                (mmap_size - 2 * BuddyAllocator::required_space(guess, max_order)) / page_size;
            i += 1;
        }

        guess
    }

    pub(crate) fn new(mmap: MmapRaw, requested_page_size: Option<usize>) -> Result<Self, Error> {
        let mut mmap = Mmap::new(mmap);
        let mutex = Mutex::new(MetapageGuard {});
        if mmap[0..MAGICNUMBER.len()] != MAGICNUMBER {
            let page_size = requested_page_size.unwrap_or_else(get_page_size);
            // Ensure that the database metadata fits into the first page
            assert!(page_size >= DB_METAPAGE_SIZE);
            assert!(page_size.is_power_of_two());

            let max_order = Self::calculate_usable_order(mmap.len(), page_size);
            let usable_pages = Self::calculate_usable_pages(mmap.len(), page_size, max_order);

            // Explicitly zero the memory
            mmap[0..DB_METAPAGE_SIZE].copy_from_slice(&[0; DB_METAPAGE_SIZE]);
            for i in &mut mmap[(usable_pages * page_size)..] {
                *i = 0
            }

            let allocator_state_size = BuddyAllocator::required_space(usable_pages, max_order);

            // Store the page & db size. These are immutable
            mmap[PAGE_SIZE_OFFSET] = page_size.trailing_zeros() as u8;
            let length = mmap.len() as u64;
            mmap[DB_SIZE_OFFSET..(DB_SIZE_OFFSET + size_of::<u64>())]
                .copy_from_slice(&length.to_be_bytes());

            // Set to 1, so that we can mutate the first transaction state
            mmap[PRIMARY_BIT_OFFSET] = 1;
            let start = mmap.len() - 2 * allocator_state_size;
            let mut mutator =
                TransactionMutator::new(get_secondary(&mut mmap), mutex.lock().unwrap());
            mutator.set_root_page(PageNumber::new(0, 0), 0);
            mutator.set_last_committed_transaction_id(0);
            mutator.set_allocator_dirty(false);
            mutator.set_allocator_data(start, allocator_state_size);
            drop(mutator);
            let allocator = BuddyAllocator::init_new(
                &mut mmap[start..(start + allocator_state_size)],
                usable_pages,
                max_order,
            );
            allocator.record_alloc(
                &mut mmap[start..(start + allocator_state_size)],
                DB_METADATA_PAGE,
                0,
            );
            // Make the state we just wrote the primary
            mmap[PRIMARY_BIT_OFFSET] = 0;

            // Initialize the secondary allocator state
            let start = mmap.len() - allocator_state_size;
            let mut mutator =
                TransactionMutator::new(get_secondary(&mut mmap), mutex.lock().unwrap());
            mutator.set_allocator_dirty(false);
            mutator.set_allocator_data(start, allocator_state_size);
            drop(mutator);
            let allocator = BuddyAllocator::init_new(
                &mut mmap[start..(start + allocator_state_size)],
                usable_pages,
                max_order,
            );
            allocator.record_alloc(
                &mut mmap[start..(start + allocator_state_size)],
                DB_METADATA_PAGE,
                0,
            );

            mmap[VERSION_OFFSET] = 1;

            mmap.flush()?;
            // Write the magic number only after the data structure is initialized and written to disk
            // to ensure that it's crash safe
            mmap[0..MAGICNUMBER.len()].copy_from_slice(&MAGICNUMBER);
            mmap.flush()?;
        }

        let page_size = (1 << mmap[PAGE_SIZE_OFFSET]) as usize;
        if let Some(size) = requested_page_size {
            assert_eq!(page_size, size);
        }
        assert_eq!(
            u64::from_be_bytes(
                mmap[DB_SIZE_OFFSET..(DB_SIZE_OFFSET + size_of::<u64>())]
                    .try_into()
                    .unwrap()
            ) as usize,
            mmap.len()
        );

        let accessor = TransactionAccessor::new(get_primary(&mmap), mutex.lock().unwrap());
        let mut allocator_dirty = accessor.get_allocator_dirty();
        drop(accessor);
        let accessor = TransactionAccessor::new(get_secondary(&mut mmap), mutex.lock().unwrap());
        allocator_dirty |= accessor.get_allocator_dirty();
        drop(accessor);

        let page_allocator = if allocator_dirty {
            None
        } else {
            let max_order = Self::calculate_usable_order(mmap.len(), page_size);
            let usable_pages = Self::calculate_usable_pages(mmap.len(), page_size, max_order);
            Some(BuddyAllocator::new(usable_pages, max_order))
        };

        Ok(TransactionalMemory {
            allocated_since_commit: RefCell::new(HashSet::new()),
            freed_since_commit: RefCell::new(vec![]),
            page_allocator,
            mmap,
            metapage_guard: mutex,
            open_dirty_pages: RefCell::new(HashSet::new()),
            read_from_secondary: AtomicBool::new(false),
            page_size,
        })
    }

    pub(crate) fn needs_repair(&self) -> Result<bool, Error> {
        let (mmap, guard) = self.acquire_mutable_metapage()?;
        let accessor = TransactionAccessor::new(get_primary(mmap), guard);
        let mut allocator_dirty = accessor.get_allocator_dirty();
        drop(accessor);

        let (mmap, guard) = self.acquire_mutable_metapage()?;
        let accessor = TransactionAccessor::new(get_secondary(mmap), guard);
        allocator_dirty |= accessor.get_allocator_dirty();

        Ok(allocator_dirty)
    }

    // Returns true if the repair is complete. If false, this method must be called again
    pub(crate) fn repair_allocator(
        &self,
        allocated_pages: impl Iterator<Item = PageNumber>,
    ) -> Result<bool, Error> {
        // TODO: clean up all the duplicated code in this function
        let (mmap, guard) = self.acquire_mutable_metapage()?;
        let mut mutator = TransactionMutator::new(get_secondary(mmap), guard);
        if mutator.get_allocator_dirty() {
            let (mem, guard) = self.acquire_mutable_page_allocator(mutator)?;

            let max_order = Self::calculate_usable_order(self.mmap.len(), self.page_size);
            let usable_pages =
                Self::calculate_usable_pages(self.mmap.len(), self.page_size, max_order);
            let allocator = BuddyAllocator::init_new(mem, usable_pages, max_order);
            allocator.record_alloc(mem, DB_METADATA_PAGE, 0);
            for page in allocated_pages {
                allocator.record_alloc(mem, page.page_index, page.page_order as usize);
            }
            self.mmap.flush()?;

            drop(guard);
            let (mmap, guard) = self.acquire_mutable_metapage()?;
            let mut mutator = TransactionMutator::new(get_secondary(mmap), guard);
            mutator.set_allocator_dirty(false);
            self.mmap.flush()?;

            Ok(false)
        } else {
            // Repair the primary instead
            drop(mutator);
            let (mmap, guard) = self.acquire_mutable_metapage()?;
            mutator = TransactionMutator::new(get_primary_mut(mmap), guard);

            let (mem, guard) = self.acquire_mutable_page_allocator(mutator)?;

            let max_order = Self::calculate_usable_order(self.mmap.len(), self.page_size);
            let usable_pages =
                Self::calculate_usable_pages(self.mmap.len(), self.page_size, max_order);
            let allocator = BuddyAllocator::init_new(mem, usable_pages, max_order);
            // TODO: make the metapage not part of the allocator. This caused a bug, and also prevents
            // the first high order pages from being ever allocated
            allocator.record_alloc(mem, DB_METADATA_PAGE, 0);
            for page in allocated_pages {
                allocator.record_alloc(mem, page.page_index, page.page_order as usize);
            }
            self.mmap.flush()?;

            drop(guard);
            let (mmap, guard) = self.acquire_mutable_metapage()?;
            mutator = TransactionMutator::new(get_primary_mut(mmap), guard);
            mutator.set_allocator_dirty(false);
            self.mmap.flush()?;

            Ok(true)
        }
    }

    pub(crate) fn finalize_repair_allocator(&mut self) -> Result<(), Error> {
        assert!(!self.needs_repair()?);
        let max_order = Self::calculate_usable_order(self.mmap.len(), self.page_size);
        let usable_pages = Self::calculate_usable_pages(self.mmap.len(), self.page_size, max_order);
        let allocator = BuddyAllocator::new(usable_pages, max_order);
        self.page_allocator = Some(allocator);

        Ok(())
    }

    fn acquire_mutable_metapage(&self) -> Result<(&mut [u8], MutexGuard<MetapageGuard>), Error> {
        let guard = self.metapage_guard.lock().unwrap();
        // Safety: we acquire the metapage lock and only access the metapage
        let mem = unsafe { self.mmap.get_memory_mut(0..DB_METAPAGE_SIZE) };

        Ok((mem, guard))
    }

    fn acquire_mutable_page_allocator<'a>(
        &self,
        mut mutator: TransactionMutator<'a>,
    ) -> Result<(&mut [u8], MutexGuard<'a, MetapageGuard>), Error> {
        // The allocator is a cache, and therefore can only be modified when it's marked dirty
        if !mutator.get_allocator_dirty() {
            mutator.set_allocator_dirty(true);
            self.mmap.flush()?
        }

        // Safety: we have the metapage lock and only access the metapage
        // (page allocator state is logically part of the metapage)
        let (start, end) = mutator.get_allocator_data();
        assert!(end <= self.mmap.len());
        // Safety: the allocator state is considered part of the metapage, and we hold the lock
        let mem = unsafe { self.mmap.get_memory_mut(start..end) };

        Ok((mem, mutator.into_guard()))
    }

    // Commit all outstanding changes and make them visible as the primary
    pub(crate) fn commit(&self, transaction_id: u128) -> Result<(), Error> {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        assert!(self.open_dirty_pages.borrow().is_empty());
        assert!(self.page_allocator.is_some());

        let (mmap, guard) = self.acquire_mutable_metapage()?;
        let mut mutator = TransactionMutator::new(get_secondary(mmap), guard);
        mutator.set_last_committed_transaction_id(transaction_id);
        drop(mutator);

        self.mmap.flush()?;

        let next = match self.mmap.get_memory(0..DB_METAPAGE_SIZE)[PRIMARY_BIT_OFFSET] {
            0 => 1,
            1 => 0,
            _ => unreachable!(),
        };
        let (mmap, guard) = self.acquire_mutable_metapage()?;
        let mut mutator = TransactionMutator::new(get_secondary(mmap), guard);
        mutator.set_allocator_dirty(false);
        drop(mutator);
        let (mmap, guard) = self.acquire_mutable_metapage()?;

        mmap[PRIMARY_BIT_OFFSET] = next;
        // Dirty the current primary (we just switched them on the previous line)
        let mut mutator = TransactionMutator::new(get_secondary(mmap), guard);
        mutator.set_allocator_dirty(true);
        self.mmap.flush()?;

        let (mem, guard) = self.acquire_mutable_page_allocator(mutator)?;
        for page_number in self.allocated_since_commit.borrow_mut().drain() {
            self.page_allocator.as_ref().unwrap().record_alloc(
                mem,
                page_number.page_index,
                page_number.page_order as usize,
            );
        }
        for page_number in self.freed_since_commit.borrow_mut().drain(..) {
            self.page_allocator.as_ref().unwrap().free(
                mem,
                page_number.page_index,
                page_number.page_order as usize,
            );
        }
        drop(guard); // Ensure the guard lives past all the writes to the page allocator state
        self.read_from_secondary.store(false, Ordering::SeqCst);

        Ok(())
    }

    // Make changes visible, without a durability guarantee
    pub(crate) fn non_durable_commit(&self, transaction_id: u128) -> Result<(), Error> {
        // All mutable pages must be dropped, this ensures that when a transaction completes
        // no more writes can happen to the pages it allocated. Thus it is safe to make them visible
        // to future read transactions
        assert!(self.open_dirty_pages.borrow().is_empty());
        assert!(self.page_allocator.is_some());

        let (mmap, guard) = self.acquire_mutable_metapage()?;
        let mut mutator = TransactionMutator::new(get_secondary(mmap), guard);
        mutator.set_last_committed_transaction_id(transaction_id);
        drop(mutator);

        // Ensure the dirty bit is set on the primary page, so that the following updates to it are safe
        let mut primary_mutator =
            TransactionMutator::new(get_primary_mut(mmap), self.metapage_guard.lock().unwrap());
        if !primary_mutator.get_allocator_dirty() {
            primary_mutator.set_allocator_dirty(true);
            // Must fsync this, even though we're in a non-durable commit. Because we're dirtying
            // the primary allocator state
            self.mmap.flush()?;
        }

        // Modify the primary allocator state directly. This is only safe because we first set the dirty bit
        let (mem, guard) = self.acquire_mutable_page_allocator(primary_mutator)?;
        for page_number in self.allocated_since_commit.borrow_mut().drain() {
            self.page_allocator.as_ref().unwrap().record_alloc(
                mem,
                page_number.page_index,
                page_number.page_order as usize,
            );
        }
        assert!(self.freed_since_commit.borrow().is_empty());
        drop(guard); // Ensure the guard lives past all the writes to the page allocator state
        self.read_from_secondary.store(true, Ordering::SeqCst);

        Ok(())
    }

    pub(crate) fn rollback_uncommited_writes(&self) -> Result<(), Error> {
        assert!(self.open_dirty_pages.borrow().is_empty());
        let (metamem, guard) = self.acquire_mutable_metapage()?;
        let mutator = TransactionMutator::new(get_secondary(metamem), guard);
        let (mem, guard) = self.acquire_mutable_page_allocator(mutator)?;
        for page_number in self.allocated_since_commit.borrow_mut().drain() {
            self.page_allocator.as_ref().unwrap().free(
                mem,
                page_number.page_index,
                page_number.page_order as usize,
            );
        }
        for page_number in self.freed_since_commit.borrow_mut().drain(..) {
            self.page_allocator.as_ref().unwrap().record_alloc(
                mem,
                page_number.page_index,
                page_number.page_order as usize,
            );
        }
        // Drop guard only after page_allocator calls are completed
        drop(guard);

        Ok(())
    }

    pub(crate) fn get_page(&self, page_number: PageNumber) -> PageImpl {
        // We must not retrieve an immutable reference to a page which already has a mutable ref to it
        assert!(
            !self.open_dirty_pages.borrow().contains(&page_number),
            "{:?}",
            page_number
        );

        PageImpl {
            mem: self
                .mmap
                .get_memory(page_number.address_range(self.page_size)),
            page_number,
        }
    }

    // Safety: the caller must ensure that no references to the memory in `page` exist
    pub(crate) unsafe fn get_page_mut(&self, page_number: PageNumber) -> PageMut {
        self.open_dirty_pages.borrow_mut().insert(page_number);

        // TODO: change this to take a NodeHandle, and check that future get_page() calls don't
        // request valid_message bytes after this request. Otherwise, we could get a race.
        // Immutable references are allowed, they just need to be to a strict subset of the
        // valid delta message bytes

        let mem = self
            .mmap
            .get_memory_mut(page_number.address_range(self.page_size));

        PageMut {
            mem,
            page_number,
            open_pages: &self.open_dirty_pages,
        }
    }

    pub(crate) fn get_primary_root_page(&self) -> Option<(PageNumber, u8)> {
        if self.read_from_secondary.load(Ordering::SeqCst) {
            TransactionAccessor::new(
                get_secondary_const(self.mmap.get_memory(0..DB_METAPAGE_SIZE)),
                self.metapage_guard.lock().unwrap(),
            )
            .get_root_page()
        } else {
            TransactionAccessor::new(
                get_primary(self.mmap.get_memory(0..DB_METAPAGE_SIZE)),
                self.metapage_guard.lock().unwrap(),
            )
            .get_root_page()
        }
    }

    pub(crate) fn get_last_committed_transaction_id(&self) -> Result<u128, Error> {
        let id = if self.read_from_secondary.load(Ordering::SeqCst) {
            TransactionAccessor::new(
                get_secondary_const(self.mmap.get_memory(0..DB_METAPAGE_SIZE)),
                self.metapage_guard.lock()?,
            )
            .get_last_committed_transaction_id()
        } else {
            TransactionAccessor::new(
                get_primary(self.mmap.get_memory(0..DB_METAPAGE_SIZE)),
                self.metapage_guard.lock()?,
            )
            .get_last_committed_transaction_id()
        };

        Ok(id)
    }

    // TODO: valid_message_bytes kind of breaks the separation of concerns for the PageManager.
    // It's only used by the delta message protocol of the b-tree
    pub(crate) fn set_secondary_root_page(
        &self,
        root_page: PageNumber,
        valid_messages: u8,
    ) -> Result<(), Error> {
        let (mmap, guard) = self.acquire_mutable_metapage()?;
        let mut mutator = TransactionMutator::new(get_secondary(mmap), guard);
        mutator.set_root_page(root_page, valid_messages);

        Ok(())
    }

    // Safety: the caller must ensure that no references to the memory in `page` exist
    pub(crate) unsafe fn free(&self, page: PageNumber) -> Result<(), Error> {
        let (mmap, guard) = self.acquire_mutable_metapage()?;
        let mutator = TransactionMutator::new(get_secondary(mmap), guard);
        let (mem, guard) = self.acquire_mutable_page_allocator(mutator)?;
        self.page_allocator
            .as_ref()
            .unwrap()
            .free(mem, page.page_index, page.page_order as usize);
        drop(guard);
        self.freed_since_commit.borrow_mut().push(page);

        Ok(())
    }

    // Frees the page if it was allocated since the last commit. Returns true, if the page was freed
    // Safety: the caller must ensure that no references to the memory in `page` exist
    pub(crate) unsafe fn free_if_uncommitted(&self, page: PageNumber) -> Result<bool, Error> {
        if self.allocated_since_commit.borrow_mut().remove(&page) {
            let (mmap, guard) = self.acquire_mutable_metapage()?;
            let mutator = TransactionMutator::new(get_secondary(mmap), guard);
            let (mem, guard) = self.acquire_mutable_page_allocator(mutator)?;
            self.page_allocator.as_ref().unwrap().free(
                mem,
                page.page_index,
                page.page_order as usize,
            );
            drop(guard);

            Ok(true)
        } else {
            Ok(false)
        }
    }

    // Page has not been committed
    pub(crate) fn uncommitted(&self, page: PageNumber) -> bool {
        self.allocated_since_commit.borrow().contains(&page)
    }

    pub(crate) fn allocate(&self, allocation_size: usize) -> Result<PageMut, Error> {
        let required_pages = (allocation_size + self.page_size - 1) / self.page_size;
        let required_order = if required_pages.is_power_of_two() {
            // TODO: use .log2() when it's stable
            required_pages.trailing_zeros()
        } else {
            // TODO: use .log2() when it's stable
            required_pages.next_power_of_two().trailing_zeros()
        } as usize;

        let (mmap, guard) = self.acquire_mutable_metapage()?;
        let mutator = TransactionMutator::new(get_secondary(mmap), guard);
        let (mem, guard) = self.acquire_mutable_page_allocator(mutator)?;
        let page_number = PageNumber::new(
            self.page_allocator
                .as_ref()
                .unwrap()
                .alloc(mem, required_order)?,
            required_order as u8,
        );
        // Drop guard only after page_allocator.alloc() is completed
        drop(guard);

        self.allocated_since_commit.borrow_mut().insert(page_number);
        self.open_dirty_pages.borrow_mut().insert(page_number);

        let address_range = page_number.address_range(self.page_size);
        assert!(address_range.end <= self.mmap.len());
        // Safety:
        // The address range we're returning was just allocated, so no other references exist
        let mem = unsafe { self.mmap.get_memory_mut(address_range) };
        // Zero the memory
        mem.copy_from_slice(&vec![0u8; page_number.page_size_bytes(self.page_size)]);
        debug_assert!(mem.len() >= allocation_size);

        assert_ne!(page_number.page_index, 0);

        Ok(PageMut {
            mem,
            page_number,
            open_pages: &self.open_dirty_pages,
        })
    }

    pub(crate) fn count_free_pages(&self) -> Result<usize, Error> {
        let (mmap, guard) = self.acquire_mutable_metapage()?;
        // TODO: this is a read-only operation, so should be able to use an accessor
        // and avoid dirtying the allocator state
        let mutator = TransactionMutator::new(get_secondary(mmap), guard);
        let (mem, guard) = self.acquire_mutable_page_allocator(mutator).unwrap();
        let count = self.page_allocator.as_ref().unwrap().count_free_pages(mem);
        // Drop guard only after page_allocator.count_free() is completed
        drop(guard);

        Ok(count)
    }
}

impl Drop for TransactionalMemory {
    fn drop(&mut self) {
        // Commit any non-durable transactions that are outstanding
        if self.read_from_secondary.load(Ordering::SeqCst) {
            if let Ok(non_durable_transaction_id) = self.get_last_committed_transaction_id() {
                if self.commit(non_durable_transaction_id).is_err() {
                    eprintln!(
                        "Failure while finalizing non-durable commit. Database may have rolled back"
                    );
                }
            } else {
                eprintln!(
                    "Failure while finalizing non-durable commit. Database may have rolled back"
                );
            }
        }
        if self.mmap.flush().is_ok() && self.page_allocator.is_some() {
            if let Ok((metamem, guard)) = self.acquire_mutable_metapage() {
                let mut mutator = TransactionMutator::new(get_secondary(metamem), guard);
                mutator.set_allocator_dirty(false);
                let _ = self.mmap.flush();
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::page_store::page_manager::{
        get_primary_mut, get_secondary, MetapageGuard, TransactionMutator,
    };
    use crate::tree_store::page_store::TransactionalMemory;
    use crate::{Database, Table};
    use memmap2::{MmapMut, MmapRaw};
    use std::fs::OpenOptions;
    use std::sync::Mutex;
    use tempfile::NamedTempFile;

    #[test]
    fn repair_allocator() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        let write_txn = db.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        table.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();
        let free_pages = db.stats().unwrap().free_pages();
        drop(db);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(tmpfile.path())
            .unwrap();

        let mut mmap = unsafe { MmapMut::map_mut(&file) }.unwrap();

        let mutex = Mutex::new(MetapageGuard {});
        let mut mutator = TransactionMutator::new(get_secondary(&mut mmap), mutex.lock().unwrap());
        mutator.set_allocator_dirty(true);
        drop(mutator);
        mmap.flush().unwrap();

        let mut mutator =
            TransactionMutator::new(get_primary_mut(&mut mmap), mutex.lock().unwrap());
        mutator.set_allocator_dirty(true);
        drop(mutator);
        mmap.flush().unwrap();

        drop(mmap);

        let mmap = MmapRaw::map_raw(&file).unwrap();

        assert!(TransactionalMemory::new(mmap, None)
            .unwrap()
            .needs_repair()
            .unwrap());

        let db2 = unsafe { Database::open(tmpfile.path(), 1024 * 1024).unwrap() };
        assert_eq!(free_pages, db2.stats().unwrap().free_pages());
        let write_txn = db2.begin_write().unwrap();
        let mut table: Table<[u8], [u8]> = write_txn.open_table(b"x").unwrap();
        table.insert(b"hello2", b"world2").unwrap();
        write_txn.commit().unwrap();
    }
}
