use crate::tree_store::btree_utils::{
    find_key, make_mut_single_leaf, print_tree, tree_delete, tree_insert,
};
use crate::tree_store::page_store::TransactionalMemory;
use crate::tree_store::{AccessGuardMut, BtreeRangeIter, PageNumber};
use crate::types::{RedbKey, RedbValue, WithLifetime};
use crate::{AccessGuard, Result};
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::ops::{RangeBounds, RangeFull};

pub(crate) struct BtreeMut<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    // TODO: make these private?
    pub(crate) mem: &'a TransactionalMemory,
    pub(crate) root: Option<PageNumber>,
    // TODO: storing these freed_pages is very error prone, because they need to be copied out into
    // the Storage object before the BtreeMut is destroyed
    pub(crate) freed_pages: Vec<PageNumber>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> BtreeMut<'a, K, V> {
    pub(crate) fn new(root: Option<PageNumber>, mem: &'a TransactionalMemory) -> Self {
        Self {
            mem,
            root,
            freed_pages: vec![],
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    // Safety: caller must ensure that no uncommitted data is accessed within this tree, from other references
    pub(crate) unsafe fn insert(&mut self, key: &K, value: &V) -> Result {
        if let Some(p) = self.root {
            let root = self.mem.get_page(p);
            // Safety: Caller guaranteed that no uncommitted data will be used
            let (new_root, _, freed) = tree_insert::<K>(
                root,
                key.as_bytes().as_ref(),
                value.as_bytes().as_ref(),
                self.mem,
            )?;
            self.freed_pages.extend_from_slice(&freed);
            self.root = Some(new_root);
        } else {
            let (new_root, _) =
                make_mut_single_leaf(key.as_bytes().as_ref(), value.as_bytes().as_ref(), self.mem)?;
            self.root = Some(new_root);
        }
        Ok(())
    }

    /// Reserve space to insert a key-value pair
    /// The returned reference will have length equal to value_length
    // Safety: caller must ensure that no uncommitted data is accessed within this tree, from other references
    pub(crate) unsafe fn insert_reserve(
        &mut self,
        key: &K,
        value_length: usize,
    ) -> Result<AccessGuardMut> {
        let value = vec![0u8; value_length];
        let (new_root, guard) = if let Some(p) = self.root {
            let root = self.mem.get_page(p);
            // Safety: Caller guaranteed that no uncommitted data will be used
            let (new_root, guard, freed) =
                tree_insert::<K>(root, key.as_bytes().as_ref(), &value, self.mem)?;
            self.freed_pages.extend_from_slice(&freed);
            (new_root, guard)
        } else {
            make_mut_single_leaf(key.as_bytes().as_ref(), &value, self.mem)?
        };
        self.root = Some(new_root);
        Ok(guard)
    }

    // Special version of insert_reserve for usage in the freed table
    // Safety: caller must ensure that no uncommitted data is accessed within this tree, from other references
    pub(crate) unsafe fn insert_reserve_special(
        &mut self,
        key: &K,
        value_length: usize,
    ) -> Result<(AccessGuardMut, Option<PageNumber>, Vec<PageNumber>)> {
        let value = vec![0u8; value_length];
        let (new_root, guard) = if let Some(p) = self.root {
            let root = self.mem.get_page(p);
            // Safety: Caller guaranteed that no uncommitted data will be used
            let (new_root, guard, freed) =
                tree_insert::<K>(root, key.as_bytes().as_ref(), &value, self.mem)?;
            self.freed_pages.extend_from_slice(&freed);
            (new_root, guard)
        } else {
            make_mut_single_leaf(key.as_bytes().as_ref(), &value, self.mem)?
        };
        self.root = Some(new_root);
        Ok((guard, self.root, std::mem::take(&mut self.freed_pages)))
    }

    // Safety: caller must ensure that no uncommitted data is accessed within this tree, from other references
    pub(crate) unsafe fn remove(&mut self, key: &K) -> Result<Option<AccessGuard<V>>> {
        if let Some(p) = self.root {
            let root_page = self.mem.get_page(p);
            // Safety: Caller guaranteed that no uncommitted data will be used
            let (new_root, found, freed) =
                tree_delete::<K, V>(root_page, key.as_bytes().as_ref(), true, self.mem)?;
            self.freed_pages.extend_from_slice(&freed);
            self.root = new_root;
            Ok(found)
        } else {
            Ok(None)
        }
    }

    // Like remove(), but does not free uncommitted data
    pub(crate) fn remove_retain_uncommitted(&mut self, key: &K) -> Result<Option<AccessGuard<V>>> {
        if let Some(p) = self.root {
            let root_page = self.mem.get_page(p);
            // Safety: free_uncommitted is false
            let (new_root, found, freed) = unsafe {
                tree_delete::<K, V>(root_page, key.as_bytes().as_ref(), false, self.mem)?
            };
            self.freed_pages.extend_from_slice(&freed);
            self.root = new_root;
            Ok(found)
        } else {
            Ok(None)
        }
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self) {
        if let Some(p) = self.root {
            print_tree::<K>(self.mem.get_page(p), self.mem);
        }
    }

    fn read_tree(&self) -> Btree<K, V> {
        Btree::new(self.root, self.mem)
    }

    pub(crate) fn get(
        &self,
        key: &K,
    ) -> Result<Option<<<V as RedbValue>::View as WithLifetime>::Out>> {
        self.read_tree().get(key)
    }

    pub(crate) fn range<T: RangeBounds<KR>, KR: Borrow<K> + 'a>(
        &self,
        range: T,
    ) -> Result<BtreeRangeIter<K, V>> {
        self.read_tree().range(range)
    }

    pub(crate) fn len(&self) -> Result<usize> {
        self.read_tree().len()
    }
}

impl<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Drop for BtreeMut<'a, K, V> {
    fn drop(&mut self) {
        debug_assert!(self.freed_pages.is_empty());
        if !self.freed_pages.is_empty() {
            eprintln!("{} pages leaked by BtreeMut", self.freed_pages.len());
        }
    }
}

pub(crate) struct Btree<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    mem: &'a TransactionalMemory,
    root: Option<PageNumber>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized, V: RedbValue + ?Sized> Btree<'a, K, V> {
    pub(crate) fn new(root: Option<PageNumber>, mem: &'a TransactionalMemory) -> Self {
        Self {
            mem,
            root,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub(crate) fn get(
        &self,
        key: &K,
    ) -> Result<Option<<<V as RedbValue>::View as WithLifetime<'a>>::Out>> {
        if let Some(p) = self.root {
            let root_page = self.mem.get_page(p);
            return Ok(find_key::<K, V>(
                root_page,
                key.as_bytes().as_ref(),
                self.mem,
            ));
        }
        Ok(None)
    }

    pub(crate) fn range<T: RangeBounds<KR>, KR: Borrow<K> + 'a>(
        &self,
        range: T,
    ) -> Result<BtreeRangeIter<'a, K, V>> {
        Ok(BtreeRangeIter::new(range, self.root, self.mem))
    }

    pub(crate) fn len(&self) -> Result<usize> {
        let mut iter: BtreeRangeIter<[u8], [u8]> =
            BtreeRangeIter::new::<RangeFull, [u8]>(.., self.root, self.mem);
        let mut count = 0;
        while iter.next().is_some() {
            count += 1;
        }
        Ok(count)
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self) {
        if let Some(p) = self.root {
            print_tree::<K>(self.mem.get_page(p), self.mem);
        }
    }
}
