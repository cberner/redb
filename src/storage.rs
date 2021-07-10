use crate::btree::{lookup_in_raw, tree_size, BtreeBuilder};
use crate::page_manager::{Page, PageManager, ALL_MEMORY_HACK, DB_METADATA_PAGE};
use crate::Error;
use memmap2::MmapMut;
use std::convert::TryInto;

const MAGICNUMBER: [u8; 4] = [b'r', b'e', b'd', b'b'];
const ALLOCATOR_STATE_OFFSET: usize = MAGICNUMBER.len();
const ROOT_PAGE_OFFSET: usize = ALLOCATOR_STATE_OFFSET + PageManager::state_size();
const DATA_LEN_OFFSET: usize = ROOT_PAGE_OFFSET + PageManager::state_size();
const DATA_OFFSET: usize = DATA_LEN_OFFSET + 8;
const DB_METADATA_SIZE: usize = DATA_OFFSET;

const ENTRY_DELETED: u8 = 1;

// Provides a simple zero-copy way to access entries
//
// Entry format is:
// * (1 byte) flags: 1 = DELETED
// * (8 bytes) key_size
// * (key_size bytes) key_data
// * (8 bytes) value_size
// * (value_size bytes) value_data
struct EntryAccessor<'a> {
    raw: &'a [u8],
}

impl<'a> EntryAccessor<'a> {
    fn new(raw: &'a [u8]) -> Self {
        EntryAccessor { raw }
    }

    fn is_deleted(&self) -> bool {
        self.raw[0] & ENTRY_DELETED != 0
    }

    fn key_len(&self) -> usize {
        u64::from_be_bytes(self.raw[1..9].try_into().unwrap()) as usize
    }

    fn key(&self) -> &'a [u8] {
        &self.raw[9..(9 + self.key_len())]
    }

    fn value_len(&self) -> usize {
        let key_len = self.key_len();
        u64::from_be_bytes(
            self.raw[(9 + key_len)..(9 + key_len + 8)]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn value(&self) -> &'a [u8] {
        let value_offset = 1 + 8 + self.key_len() + 8;
        &self.raw[value_offset..(value_offset + self.value_len())]
    }

    fn raw_len(&self) -> usize {
        1 + 8 + self.key_len() + 8 + self.value_len()
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
struct EntryMutator<'a> {
    raw: &'a mut [u8],
}

impl<'a> EntryMutator<'a> {
    fn new(raw: &'a mut [u8]) -> Self {
        EntryMutator { raw }
    }

    fn raw_len(&self) -> usize {
        EntryAccessor::new(self.raw).raw_len()
    }

    fn write_flags(&mut self, flags: u8) {
        self.raw[0] = flags;
    }

    fn write_key(&mut self, key: &[u8]) {
        self.raw[1..9].copy_from_slice(&(key.len() as u64).to_be_bytes());
        self.raw[9..(9 + key.len())].copy_from_slice(key);
    }

    fn write_value(&mut self, value: &[u8]) {
        let value_offset = 9 + EntryAccessor::new(self.raw).key_len();
        self.raw[value_offset..(value_offset + 8)]
            .copy_from_slice(&(value.len() as u64).to_be_bytes());
        self.raw[(value_offset + 8)..(value_offset + 8 + value.len())].copy_from_slice(value);
    }
}

pub(in crate) struct Storage {
    mem: PageManager,
}

impl Storage {
    pub(in crate) fn new(mut mmap: MmapMut) -> Result<Storage, Error> {
        // Ensure that the database metadata fits into the first page
        assert!(page_size::get() >= DB_METADATA_SIZE);

        if mmap[0..MAGICNUMBER.len()] != MAGICNUMBER {
            PageManager::initialize(
                &mut mmap
                    [ALLOCATOR_STATE_OFFSET..(ALLOCATOR_STATE_OFFSET + PageManager::state_size())],
            );
            mmap[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + 8)].copy_from_slice(&0u64.to_be_bytes());
            mmap[DATA_LEN_OFFSET..(DATA_LEN_OFFSET + 8)].copy_from_slice(&0u64.to_be_bytes());
            mmap.flush()?;
            // Write the magic number only after the data structure is initialized and written to disk
            // to ensure that it's crash safe
            mmap[0..MAGICNUMBER.len()].copy_from_slice(&MAGICNUMBER);
            mmap.flush()?;
        }

        Ok(Storage {
            mem: PageManager::restore(mmap, ALLOCATOR_STATE_OFFSET),
        })
    }

    pub(in crate) fn append(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let mut all_mem = self.mem.get_page_mut(ALL_MEMORY_HACK);
        let mmap = all_mem.memory_mut();

        let mut data_len = u64::from_be_bytes(
            mmap[DATA_LEN_OFFSET..(DATA_LEN_OFFSET + 8)]
                .try_into()
                .unwrap(),
        ) as usize;

        let mut index = DATA_OFFSET + data_len;

        // Append the new key & value
        let mut mutator = EntryMutator::new(&mut mmap[index..]);
        mutator.write_flags(0);
        mutator.write_key(key);
        mutator.write_value(value);
        index += mutator.raw_len();

        data_len = index - DATA_OFFSET;

        mmap[DATA_LEN_OFFSET..(DATA_LEN_OFFSET + 8)].copy_from_slice(&data_len.to_be_bytes());
        Ok(())
    }

    pub(in crate) fn len(&self) -> Result<usize, Error> {
        if let Some(root) = self.get_root_page() {
            Ok(tree_size(root, &self.mem))
        } else {
            Ok(0)
        }
    }

    fn get_root_page(&self) -> Option<Page> {
        let metapage = self.mem.get_page(DB_METADATA_PAGE);
        let mmap = metapage.memory();
        let root_page_number = u64::from_be_bytes(
            mmap[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + 8)]
                .try_into()
                .unwrap(),
        );
        if root_page_number == 0 {
            None
        } else {
            Some(self.mem.get_page(root_page_number))
        }
    }

    pub(in crate) fn fsync(&self) -> Result<(), Error> {
        let mut builder = BtreeBuilder::new();
        let all_mem = self.mem.get_page(ALL_MEMORY_HACK);
        let mmap = all_mem.memory();

        let data_len = u64::from_be_bytes(
            mmap[DATA_LEN_OFFSET..(DATA_LEN_OFFSET + 8)]
                .try_into()
                .unwrap(),
        ) as usize;

        let mut index = DATA_OFFSET;
        while index < (DATA_OFFSET + data_len) {
            let entry = EntryAccessor::new(&mmap[index..]);
            if !entry.is_deleted() {
                builder.add(entry.key(), entry.value());
            }
            index += entry.raw_len();
        }

        let node = builder.build();
        self.mem
            .hack_set_free_page_to_next_after((DATA_OFFSET + data_len) as u64);
        // Need to drop this because to_bytes() needs to be able to acquire mutable memory
        // TODO: fix the allocator so that mutable and immutable pages can co-exist,
        // as long as they are different pages
        drop(all_mem);
        let root_page = node.to_bytes(&self.mem);

        let mut all_mem = self.mem.get_page_mut(ALL_MEMORY_HACK);
        let mmap = all_mem.memory_mut();
        mmap[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + 8)].copy_from_slice(&root_page.to_be_bytes());

        self.mem.store_state(
            &mut mmap[ALLOCATOR_STATE_OFFSET..(ALLOCATOR_STATE_OFFSET + PageManager::state_size())],
        );

        drop(all_mem);
        self.mem.fsync()?;
        Ok(())
    }

    pub(in crate) fn get(&self, key: &[u8]) -> Result<Option<AccessGuard>, Error> {
        if let Some(root_page) = self.get_root_page() {
            if let Some((page, offset, len)) = lookup_in_raw(root_page, key, &self.mem) {
                return Ok(Some(AccessGuard::PageBacked(page, offset, len)));
            }
        }
        Ok(None)
    }

    // Returns a boolean indicating if an entry was removed
    pub(in crate) fn remove(&self, key: &[u8]) -> Result<bool, Error> {
        if let Some(root_page) = self.get_root_page() {
            if lookup_in_raw(root_page, key, &self.mem).is_some() {
                let mut all_mem = self.mem.get_page_mut(ALL_MEMORY_HACK);
                let mmap = all_mem.memory_mut();
                // Delete the entry from the entry space
                let data_len = u64::from_be_bytes(
                    mmap[DATA_LEN_OFFSET..(DATA_LEN_OFFSET + 8)]
                        .try_into()
                        .unwrap(),
                ) as usize;

                let mut index = DATA_OFFSET;
                while index < (DATA_OFFSET + data_len) {
                    let entry = EntryAccessor::new(&mmap[index..]);
                    if entry.key() == key {
                        drop(entry);
                        let mut entry = EntryMutator::new(&mut mmap[index..]);
                        entry.write_flags(ENTRY_DELETED);
                        break;
                    }
                    index += entry.raw_len();
                }

                drop(all_mem);
                self.mem.fsync()?;
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub enum AccessGuard<'a> {
    PageBacked(Page<'a>, usize, usize),
    Local(&'a [u8]),
}

impl<'a> AsRef<[u8]> for AccessGuard<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            AccessGuard::PageBacked(page, offset, len) => &page.memory()[*offset..(*offset + *len)],
            AccessGuard::Local(data_ref) => data_ref,
        }
    }
}
