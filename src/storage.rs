use crate::btree::{
    lookup_in_raw, make_single_leaf, tree_delete, tree_insert, BtreeEntry, BtreeRangeIter,
};
use crate::page_manager::{Page, PageNumber, TransactionalMemory};
use crate::types::RedbKey;
use crate::Error;
use memmap2::MmapMut;
use std::convert::TryInto;
use std::ops::{RangeBounds, RangeFull};

// The table of name -> table_id mappings
const TABLE_TABLE_ID: u64 = 0;

pub(in crate) struct Storage {
    mem: TransactionalMemory,
}

impl Storage {
    pub(in crate) fn new(mmap: MmapMut) -> Result<Storage, Error> {
        let mem = TransactionalMemory::new(mmap)?;

        Ok(Storage { mem })
    }

    // Returns a tuple of the table id and the new root page
    pub(in crate) fn get_or_create_table(
        &self,
        name: &[u8],
        root_page: Option<PageNumber>,
    ) -> Result<(u64, PageNumber), Error> {
        if let Some(found) = self.get::<[u8]>(TABLE_TABLE_ID, name, root_page)? {
            let table_id = u64::from_be_bytes(found.as_ref().try_into().unwrap());
            return Ok((table_id, root_page.unwrap()));
        }
        let mut iter = self.get_range_reversed::<RangeFull, [u8]>(TABLE_TABLE_ID, .., root_page)?;
        let largest_id = iter
            .next()
            .map(|x| u64::from_be_bytes(x.value().try_into().unwrap()))
            .unwrap_or(TABLE_TABLE_ID);
        drop(iter);
        let new_id = largest_id + 1;
        let new_root =
            self.insert::<[u8]>(TABLE_TABLE_ID, name, &new_id.to_be_bytes(), root_page)?;
        Ok((new_id, new_root))
    }

    // Returns the new root page number
    pub(in crate) fn insert<K: RedbKey + ?Sized>(
        &self,
        table_id: u64,
        key: &[u8],
        value: &[u8],
        root_page: Option<PageNumber>,
    ) -> Result<PageNumber, Error> {
        let new_root = if let Some(root) = root_page.map(|p| self.mem.get_page(p)) {
            tree_insert::<K>(root, table_id, key, value, &self.mem)
        } else {
            make_single_leaf(table_id, key, value, &self.mem)
        };
        Ok(new_root)
    }

    pub(in crate) fn len(&self, table: u64, root_page: Option<PageNumber>) -> Result<usize, Error> {
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

    pub(in crate) fn get_root_page_number(&self) -> Option<PageNumber> {
        self.mem.get_primary_root_page()
    }

    pub(in crate) fn commit(&self, new_root: Option<PageNumber>) -> Result<(), Error> {
        self.mem
            .set_secondary_root_page(new_root.unwrap_or(PageNumber(0)));
        self.mem.commit()?;
        Ok(())
    }

    pub(in crate) fn get<K: RedbKey + ?Sized>(
        &self,
        table_id: u64,
        key: &[u8],
        root_page_number: Option<PageNumber>,
    ) -> Result<Option<AccessGuard>, Error> {
        if let Some(root_page) = root_page_number.map(|p| self.mem.get_page(p)) {
            if let Some((page, offset, len)) =
                lookup_in_raw::<K>(root_page, table_id, key, &self.mem)
            {
                return Ok(Some(AccessGuard::new(page, offset, len)));
            }
        }
        Ok(None)
    }

    pub(in crate) fn get_range<'a, T: RangeBounds<&'a K>, K: RedbKey + ?Sized + 'a>(
        &'a self,
        table_id: u64,
        range: T,
        root_page: Option<PageNumber>,
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
        root_page: Option<PageNumber>,
    ) -> Result<BtreeRangeIter<T, K>, Error> {
        Ok(BtreeRangeIter::new_reversed(
            root_page.map(|p| self.mem.get_page(p)),
            table_id,
            range,
            &self.mem,
        ))
    }

    // Returns the new root page. To determine if an entry was remove test whether equal to root_page
    pub(in crate) fn remove<K: RedbKey + ?Sized>(
        &self,
        table_id: u64,
        key: &[u8],
        page_number: Option<PageNumber>,
    ) -> Result<Option<PageNumber>, Error> {
        if let Some(root_page) = page_number.map(|p| self.mem.get_page(p)) {
            let new_root = tree_delete::<K>(root_page, table_id, key, &self.mem);
            return Ok(new_root);
        }
        Ok(page_number)
    }
}

pub struct AccessGuard<'a> {
    page: Page<'a>,
    offset: usize,
    len: usize,
}

impl<'a> AccessGuard<'a> {
    fn new(page: Page<'a>, offset: usize, len: usize) -> Self {
        Self { page, offset, len }
    }
}

impl<'a> AsRef<[u8]> for AccessGuard<'a> {
    fn as_ref(&self) -> &[u8] {
        &self.page.memory()[self.offset..(self.offset + self.len)]
    }
}
