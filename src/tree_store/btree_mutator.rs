use crate::tree_store::btree_base::FreePolicy;
use crate::tree_store::btree_utils::{tree_delete, tree_insert};
use crate::tree_store::{make_mut_single_leaf, AccessGuardMut, PageNumber, TransactionalMemory};
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

    pub(crate) unsafe fn delete(&mut self, key: &K) -> Result<Option<AccessGuard<'a, V>>> {
        if let Some(p) = self.root {
            let (new_root, found, mut freed) = tree_delete::<K, V>(
                self.mem.get_page(*p),
                key.as_bytes().as_ref(),
                self.free_policy,
                self.mem,
            )?;
            self.freed.append(&mut freed);
            *self.root = new_root;
            Ok(found)
        } else {
            Ok(None)
        }
    }

    pub(crate) unsafe fn insert(&mut self, key: &K, value: &V) -> Result<AccessGuardMut<'a>> {
        let (new_root, guard) = if let Some(p) = self.root {
            let (new_root, guard, mut freed) = tree_insert::<K>(
                self.mem.get_page(*p),
                key.as_bytes().as_ref(),
                value.as_bytes().as_ref(),
                self.free_policy,
                self.mem,
            )?;
            self.freed.append(&mut freed);
            (new_root, guard)
        } else {
            make_mut_single_leaf(key.as_bytes().as_ref(), value.as_bytes().as_ref(), self.mem)?
        };
        *self.root = Some(new_root);
        Ok(guard)
    }
}
