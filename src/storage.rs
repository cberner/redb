use crate::btree::{
    lookup_in_raw, tree_delete, tree_insert, BtreeBuilder, BtreeEntry, BtreeRangeIter,
};
use crate::page_manager::{Page, PageManager, DB_METADATA_PAGE};
use crate::types::RedbKey;
use crate::Error;
use memmap2::MmapMut;
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::{RangeBounds, RangeFull};

const MAGICNUMBER: [u8; 4] = [b'r', b'e', b'd', b'b'];
const ALLOCATOR_STATE_OFFSET: usize = MAGICNUMBER.len();
const ROOT_PAGE_OFFSET: usize = ALLOCATOR_STATE_OFFSET + PageManager::state_size();
const DB_METADATA_SIZE: usize = ROOT_PAGE_OFFSET;

// The table of name -> table_id mappings
const TABLE_TABLE_ID: u64 = 0;

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

    pub(in crate) fn get_or_create_table(&self, name: &[u8]) -> Result<u64, Error> {
        if let Some(found) = self.get::<[u8]>(TABLE_TABLE_ID, name, self.get_root_page_number())? {
            return Ok(u64::from_be_bytes(found.as_ref().try_into().unwrap()));
        }
        let mut iter = self.get_range_reversed::<RangeFull, [u8]>(
            TABLE_TABLE_ID,
            ..,
            self.get_root_page_number(),
        )?;
        let largest_id = iter
            .next()
            .map(|x| u64::from_be_bytes(x.value().try_into().unwrap()))
            .unwrap_or(TABLE_TABLE_ID);
        drop(iter);
        let new_id = largest_id + 1;
        self.insert::<[u8]>(TABLE_TABLE_ID, name, &new_id.to_be_bytes())?;
        Ok(new_id)
    }

    pub(in crate) fn insert<K: RedbKey + ?Sized>(
        &self,
        table_id: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Error> {
        let new_root = if let Some(root) = self.get_root_page() {
            tree_insert::<K>(root, table_id, key, value, &self.mem)
        } else {
            let mut builder = BtreeBuilder::new();
            builder.add(table_id, key, value);
            builder.build::<K>().to_bytes(&self.mem)
        };
        self.set_root_page(Some(new_root));
        Ok(())
    }

    pub(in crate) fn bulk_insert<K: RedbKey + ?Sized>(
        &self,
        table_id: u64,
        entries: HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(), Error> {
        // Assume that rewriting half the tree is about the same cost as building a completely new one
        if entries.len() <= self.len(table_id, self.get_root_page_number())? / 2 {
            for (key, value) in entries.iter() {
                self.insert::<K>(table_id, key, value)?;
            }
        } else {
            let mut builder = BtreeBuilder::new();
            // Copy all the existing entries
            let mut tables_iter = BtreeRangeIter::<RangeFull, [u8]>::new(
                self.get_root_page(),
                TABLE_TABLE_ID,
                ..,
                &self.mem,
            );
            while let Some(table_entry) = tables_iter.next() {
                let id = u64::from_be_bytes(table_entry.value().try_into().unwrap());
                // Copy the table entry
                builder.add(
                    table_entry.table_id(),
                    table_entry.key(),
                    table_entry.value(),
                );
                // Copy the contents of the table
                let mut iter =
                    BtreeRangeIter::<RangeFull, [u8]>::new(self.get_root_page(), id, .., &self.mem);
                while let Some(x) = iter.next() {
                    // TODO: THIS IS BROKEN. .contains_key() doesn't respect any custom ordering of the keys
                    if table_id != x.table_id() || !entries.contains_key(x.key()) {
                        builder.add(x.table_id(), x.key(), x.value());
                    }
                }
            }
            for (key, value) in entries {
                builder.add(table_id, &key, &value);
            }

            let new_root = builder.build::<K>().to_bytes(&self.mem);
            self.set_root_page(Some(new_root));
        }
        Ok(())
    }

    pub(in crate) fn len(&self, table: u64, root_page: Option<u64>) -> Result<usize, Error> {
        let mut iter = BtreeRangeIter::<RangeFull, [u8]>::new(
            root_page.map(|p| self.mem.get_page(p)),
            table,
            ..,
            &self.mem,
        );
        let mut count = 0;
        while iter.next().is_some() {
            count += 1;
        }
        Ok(count)
    }

    pub(in crate) fn get_root_page_number(&self) -> Option<u64> {
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
            Some(root_page_number)
        }
    }

    fn get_root_page(&self) -> Option<Page> {
        self.get_root_page_number().map(|p| self.mem.get_page(p))
    }

    fn set_root_page(&self, root_page: Option<u64>) {
        let mut meta = self.mem.get_metapage_mut();
        let mmap = meta.memory_mut();
        mmap[ROOT_PAGE_OFFSET..(ROOT_PAGE_OFFSET + 8)]
            .copy_from_slice(&root_page.unwrap_or(0).to_be_bytes());
    }

    pub(in crate) fn fsync(&self) -> Result<(), Error> {
        let mut meta = self.mem.get_metapage_mut();
        let mmap = meta.memory_mut();

        self.mem.store_state(
            &mut mmap[ALLOCATOR_STATE_OFFSET..(ALLOCATOR_STATE_OFFSET + PageManager::state_size())],
        );

        drop(meta);
        self.mem.fsync()?;
        Ok(())
    }

    pub(in crate) fn get<K: RedbKey + ?Sized>(
        &self,
        table_id: u64,
        key: &[u8],
        root_page_number: Option<u64>,
    ) -> Result<Option<AccessGuard>, Error> {
        if let Some(root_page) = root_page_number.map(|p| self.mem.get_page(p)) {
            if let Some((page, offset, len)) =
                lookup_in_raw::<K>(root_page, table_id, key, &self.mem)
            {
                return Ok(Some(AccessGuard::PageBacked(page, offset, len)));
            }
        }
        Ok(None)
    }

    pub(in crate) fn get_range<'a, T: RangeBounds<&'a K>, K: RedbKey + ?Sized + 'a>(
        &'a self,
        table_id: u64,
        range: T,
        root_page: Option<u64>,
    ) -> Result<BtreeRangeIter<T, K>, Error> {
        Ok(BtreeRangeIter::new(
            root_page.map(|p| self.mem.get_page(p)),
            table_id,
            range,
            &self.mem,
        ))
    }

    pub(in crate) fn get_range_reversed<'a, T: RangeBounds<&'a K>, K: RedbKey + ?Sized + 'a>(
        &'a self,
        table_id: u64,
        range: T,
        root_page: Option<u64>,
    ) -> Result<BtreeRangeIter<T, K>, Error> {
        Ok(BtreeRangeIter::new_reversed(
            root_page.map(|p| self.mem.get_page(p)),
            table_id,
            range,
            &self.mem,
        ))
    }

    // Returns a boolean indicating if an entry was removed
    pub(in crate) fn remove<K: RedbKey + ?Sized>(
        &self,
        table_id: u64,
        key: &[u8],
    ) -> Result<bool, Error> {
        if let Some(root_page) = self.get_root_page() {
            let old_root = root_page.get_page_number();
            let new_root = tree_delete::<K>(root_page, table_id, key, &self.mem);
            self.set_root_page(new_root);
            return Ok(old_root == new_root.unwrap_or(0));
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
