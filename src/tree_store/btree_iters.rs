use crate::Result;
use crate::tree_store::btree_base::{BRANCH, LEAF};
use crate::tree_store::btree_base::{BranchAccessor, LeafAccessor};
use crate::tree_store::page_store::{Page, PageHint, PageImpl};
use crate::tree_store::{PageNumber, PageResolver};
use crate::types::{Key, Value};
use Bound::{Excluded, Included, Unbounded};
use std::borrow::Borrow;
use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::{Range, RangeBounds};

pub(super) fn lower_bound_entry<K: Key>(accessor: &LeafAccessor<'_>, bound: Bound<&[u8]>) -> usize {
    match bound {
        Included(query) | Excluded(query) => {
            let (mut position, found) = accessor.position::<K>(query);
            if matches!(bound, Excluded(_)) && found {
                position += 1;
            }
            position
        }
        Unbounded => 0,
    }
}

pub(super) fn child_to_visit<K: Key>(
    accessor: &BranchAccessor<'_, '_, PageImpl>,
    bound: Bound<&[u8]>,
    reverse: bool,
) -> usize {
    match bound {
        Included(query) | Excluded(query) => accessor.child_for_key::<K>(query).0,
        Unbounded => {
            if reverse {
                accessor.count_children() - 1
            } else {
                0
            }
        }
    }
}

pub(crate) struct EntryGuard<K: Key, V: Value> {
    page: PageImpl,
    key_range: Range<usize>,
    value_range: Range<usize>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<K: Key, V: Value> EntryGuard<K, V> {
    pub(super) fn new(page: PageImpl, key_range: Range<usize>, value_range: Range<usize>) -> Self {
        Self {
            page,
            key_range,
            value_range,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    pub(super) fn key_bytes(&self) -> &[u8] {
        &self.page.memory()[self.key_range.clone()]
    }

    pub(crate) fn key_data(&self) -> Vec<u8> {
        self.page.memory()[self.key_range.clone()].to_vec()
    }

    pub(crate) fn key(&self) -> K::SelfType<'_> {
        K::from_bytes(&self.page.memory()[self.key_range.clone()])
    }

    pub(crate) fn value(&self) -> V::SelfType<'_> {
        V::from_bytes(&self.page.memory()[self.value_range.clone()])
    }

    pub(crate) fn into_raw(self) -> (PageImpl, Range<usize>, Range<usize>) {
        (self.page, self.key_range, self.value_range)
    }
}

pub(crate) struct AllPageNumbersBtreeIter {
    pending: Vec<PageNumber>,
    fixed_key_size: Option<usize>,
    manager: PageResolver,
    hint: PageHint,
}

impl AllPageNumbersBtreeIter {
    pub(crate) fn new(
        root: PageNumber,
        fixed_key_size: Option<usize>,
        manager: PageResolver,
        hint: PageHint,
    ) -> Self {
        Self {
            pending: vec![root],
            fixed_key_size,
            manager,
            hint,
        }
    }
}

impl Iterator for AllPageNumbersBtreeIter {
    type Item = Result<PageNumber>;

    fn next(&mut self) -> Option<Self::Item> {
        let page_number = self.pending.pop()?;
        let page = match self.manager.get_page(page_number, self.hint) {
            Ok(page) => page,
            Err(err) => return Some(Err(err)),
        };
        match page.memory()[0] {
            LEAF => {}
            BRANCH => {
                let accessor = BranchAccessor::new(&page, self.fixed_key_size);
                // Push in reverse so children are popped left-to-right.
                for child in (0..accessor.count_children()).rev() {
                    self.pending.push(accessor.child_page(child).unwrap());
                }
            }
            _ => unreachable!(),
        }
        Some(Ok(page_number))
    }
}

pub(super) fn range_is_empty<
    'a,
    K: Key + 'static,
    KR: Borrow<K::SelfType<'a>>,
    T: RangeBounds<KR>,
>(
    range: &T,
) -> bool {
    match (range.start_bound(), range.end_bound()) {
        (Unbounded, _) | (_, Unbounded) => false,
        (Included(start), Excluded(end)) | (Excluded(start), Included(end) | Excluded(end)) => {
            let start_tmp = K::as_bytes(start.borrow());
            let start_value = start_tmp.as_ref();
            let end_tmp = K::as_bytes(end.borrow());
            let end_value = end_tmp.as_ref();
            K::compare(start_value, end_value).is_ge()
        }
        (Included(start), Included(end)) => {
            let start_tmp = K::as_bytes(start.borrow());
            let start_value = start_tmp.as_ref();
            let end_tmp = K::as_bytes(end.borrow());
            let end_value = end_tmp.as_ref();
            K::compare(start_value, end_value).is_gt()
        }
    }
}
