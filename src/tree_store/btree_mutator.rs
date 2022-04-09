use crate::tree_store::btree_base::{
    FreePolicy, IndexBuilder, InternalAccessor, InternalMutator, LeafAccessor, LeafBuilder,
    LeafBuilder2, INTERNAL, LEAF,
};
use crate::tree_store::btree_utils::{make_index, tree_delete_helper, DeletionResult};
use crate::tree_store::page_store::{Page, PageImpl};
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
                    if keys.is_empty() {
                        Some(pages[0])
                    } else {
                        Some(make_index(&pages, &keys, self.mem)?)
                    }
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
        let (new_root, guard) = if let Some(p) = *self.root {
            let (page1, more, guard) = self.insert_helper(
                self.mem.get_page(p),
                key.as_bytes().as_ref(),
                value.as_bytes().as_ref(),
            )?;

            let new_root = if let Some((key, page2)) = more {
                let mut builder = IndexBuilder::new(self.mem);
                builder.push_child(page1);
                builder.push_key(&key);
                builder.push_child(page2);
                builder.build()?.get_page_number()
            } else {
                page1
            };
            (new_root, guard)
        } else {
            let key_bytes = key.as_bytes();
            let value_bytes = value.as_bytes();
            let key_bytes = key_bytes.as_ref();
            let value_bytes = value_bytes.as_ref();
            let mut page = self.mem.allocate(LeafBuilder::required_bytes(
                1,
                key_bytes.len() + value_bytes.len(),
            ))?;
            let mut builder = LeafBuilder::new(&mut page, 1, key_bytes.len());
            builder.append(key_bytes, value_bytes);
            drop(builder);

            let accessor = LeafAccessor::new(&page);
            let offset = accessor.offset_of_first_value();
            let page_num = page.get_page_number();
            let guard = AccessGuardMut::new(page, offset, value_bytes.len());

            (page_num, guard)
        };
        *self.root = Some(new_root);
        Ok(guard)
    }

    #[allow(clippy::type_complexity)]
    // Safety: caller must ensure that no references to uncommitted pages in this table exist
    unsafe fn insert_helper(
        &mut self,
        page: PageImpl<'a>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(
        PageNumber,
        Option<(Vec<u8>, PageNumber)>,
        AccessGuardMut<'a>,
    )> {
        let node_mem = page.memory();
        Ok(match node_mem[0] {
            LEAF => {
                let accessor = LeafAccessor::new(&page);
                let (position, found) = accessor.position::<K>(key);
                let mut builder = LeafBuilder2::new(self.mem);
                for i in 0..accessor.num_pairs() {
                    if i == position {
                        builder.push(key, value);
                    }
                    if !found || i != position {
                        let entry = accessor.entry(i).unwrap();
                        builder.push(entry.key(), entry.value());
                    }
                }
                if accessor.num_pairs() == position {
                    builder.push(key, value);
                }
                if !builder.should_split() {
                    let new_page = builder.build()?;

                    let page_number = page.get_page_number();
                    drop(page);
                    self.free_policy
                        .conditional_free(page_number, self.freed, self.mem)?;

                    let new_page_number = new_page.get_page_number();
                    let accessor = LeafAccessor::new(&new_page);
                    let offset = accessor.offset_of_value(position).unwrap();
                    let guard = AccessGuardMut::new(new_page, offset, value.len());

                    (new_page_number, None, guard)
                } else {
                    let (new_page1, new_page2) = builder.build_split()?;
                    let page_number = page.get_page_number();
                    drop(page);
                    self.free_policy
                        .conditional_free(page_number, self.freed, self.mem)?;

                    let new_page_number = new_page1.get_page_number();
                    let new_page_number2 = new_page2.get_page_number();
                    let accessor = LeafAccessor::new(&new_page1);
                    let division = accessor.num_pairs();
                    let split_key = accessor.last_entry().key().to_vec();
                    let guard = if position < division {
                        let accessor = LeafAccessor::new(&new_page1);
                        let offset = accessor.offset_of_value(position).unwrap();
                        AccessGuardMut::new(new_page1, offset, value.len())
                    } else {
                        let accessor = LeafAccessor::new(&new_page2);
                        let offset = accessor.offset_of_value(position - division).unwrap();
                        AccessGuardMut::new(new_page2, offset, value.len())
                    };

                    (new_page_number, Some((split_key, new_page_number2)), guard)
                }
            }
            INTERNAL => {
                let accessor = InternalAccessor::new(&page);
                let (child_index, child_page) = accessor.child_for_key::<K>(key);
                let (page1, more, guard) =
                    self.insert_helper(self.mem.get_page(child_page), key, value)?;

                if more.is_none() {
                    // Check fast-path if no children were added
                    if page1 == child_page {
                        // NO-OP. One of our descendants is uncommitted, so there was no change
                        return Ok((page.get_page_number(), None, guard));
                    } else if self.mem.uncommitted(page.get_page_number()) {
                        let page_number = page.get_page_number();
                        drop(page);
                        // Safety: Since the page is uncommitted, no other transactions could have it open
                        // and we just dropped our reference to it, on the line above
                        let mut mutpage = self.mem.get_page_mut(page_number);
                        let mut mutator = InternalMutator::new(&mut mutpage);
                        mutator.write_child_page(child_index, page1);
                        return Ok((mutpage.get_page_number(), None, guard));
                    }
                }

                // A child was added, or we couldn't use the fast-path above
                let mut builder = IndexBuilder::new(self.mem);
                if child_index == 0 {
                    builder.push_child(page1);
                    if let Some((ref index_key2, page2)) = more {
                        builder.push_key(index_key2);
                        builder.push_child(page2);
                    }
                } else {
                    builder.push_child(accessor.child_page(0).unwrap());
                }
                for i in 1..accessor.count_children() {
                    if let Some(key) = accessor.key(i - 1) {
                        builder.push_key(key);
                        if i == child_index {
                            builder.push_child(page1);
                            if let Some((ref index_key2, page2)) = more {
                                builder.push_key(index_key2);
                                builder.push_child(page2);
                            }
                        } else {
                            builder.push_child(accessor.child_page(i).unwrap());
                        }
                    } else {
                        unreachable!();
                    }
                }

                let result = if builder.should_split() {
                    let (new_page1, split_key, new_page2) = builder.build_split()?;
                    (
                        new_page1.get_page_number(),
                        Some((split_key, new_page2.get_page_number())),
                        guard,
                    )
                } else {
                    let new_page = builder.build()?;
                    (new_page.get_page_number(), None, guard)
                };
                // Free the original page, since we've replaced it
                let page_number = page.get_page_number();
                drop(page);
                // Safety: If the page is uncommitted, no other transactions can have references to it,
                // and we just dropped ours on the line above
                self.free_policy
                    .conditional_free(page_number, self.freed, self.mem)?;

                result
            }
            _ => unreachable!(),
        })
    }
}
