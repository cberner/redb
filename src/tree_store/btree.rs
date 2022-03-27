use crate::tree_store::btree_utils::find_key;
use crate::tree_store::page_store::TransactionalMemory;
use crate::tree_store::{BtreeRangeIter, PageNumber};
use crate::types::{RedbKey, RedbValue, WithLifetime};
use crate::Result;
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::ops::{RangeBounds, RangeFull};

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
}
