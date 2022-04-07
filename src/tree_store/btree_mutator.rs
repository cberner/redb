use crate::tree_store::btree_base::{FreePolicy, LeafBuilder2};
use crate::tree_store::btree_utils::{
    make_index, make_mut_single_leaf, tree_delete_helper, tree_insert_helper, DeletionResult,
};
use crate::tree_store::page_store::Page;
use crate::tree_store::{AccessGuardMut, PageNumber, TransactionalMemory};
use crate::types::{RedbKey, RedbValue};
use crate::{AccessGuard, Result};
use std::marker::PhantomData;

pub(crate) struct MutateHelper<'a, 'b, K: RedbKey + ?Sized, V: RedbValue + ?Sized> {
    root: &'b mut Option<PageNumber>,
    free_policy: FreePolicy,
    mem: &'a TransactionalMemory,
    freed: &'b mut Vec<PageNumber>,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, 'b, K: RedbKey + ?Sized, V: RedbValue + ?Sized> MutateHelper<'a, 'b, K, V> {
    pub(crate) fn new(
        root: &'b mut Option<PageNumber>,
        free_policy: FreePolicy,
        mem: &'a TransactionalMemory,
        freed: &'b mut Vec<PageNumber>,
    ) -> Self {
        Self {
            root,
            free_policy,
            mem,
            freed,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    pub(crate) fn safe_delete(&mut self, key: &K) -> Result<Option<AccessGuard<'a, V>>> {
        assert_eq!(self.free_policy, FreePolicy::Never);
        // Safety: we asserted that the free policy is Never
        unsafe { self.delete(key) }
    }

    // Safety: caller must ensure that no references to uncommitted pages in this table exist
    pub(crate) unsafe fn delete(&mut self, key: &K) -> Result<Option<AccessGuard<'a, V>>> {
        if let Some(p) = self.root {
            let (deletion_result, found) = tree_delete_helper::<K, V>(
                self.mem.get_page(*p),
                key.as_bytes().as_ref(),
                self.free_policy,
                self.freed,
                self.mem,
            )?;
            let new_root = match deletion_result {
                DeletionResult::Subtree(page) => Some(page),
                DeletionResult::PartialLeaf(entries) => {
                    if entries.is_empty() {
                        None
                    } else {
                        let mut builder = LeafBuilder2::new(self.mem);
                        for (key, value) in entries.iter() {
                            builder.push(key, value);
                        }
                        Some(builder.build()?.get_page_number())
                    }
                }
                DeletionResult::PartialInternal(pages, keys) => {
                    assert_eq!(pages.len(), 1);
                    assert!(keys.is_empty());
                    Some(pages[0])
                }
            };
            *self.root = new_root;
            Ok(found)
        } else {
            Ok(None)
        }
    }

    // Safety: caller must ensure that no references to uncommitted pages in this tree exist
    pub(crate) unsafe fn insert(&mut self, key: &K, value: &V) -> Result<AccessGuardMut<'a>> {
        let (new_root, guard) = if let Some(p) = self.root {
            let (page1, more, guard) = tree_insert_helper::<K>(
                self.mem.get_page(*p),
                key.as_bytes().as_ref(),
                value.as_bytes().as_ref(),
                self.freed,
                self.free_policy,
                self.mem,
            )?;

            let new_root = if let Some((key, page2)) = more {
                make_index(&[page1, page2], &[&key], self.mem)?
            } else {
                page1
            };
            (new_root, guard)
        } else {
            make_mut_single_leaf(key.as_bytes().as_ref(), value.as_bytes().as_ref(), self.mem)?
        };
        *self.root = Some(new_root);
        Ok(guard)
    }
}
