use crate::Result;
use crate::tree_store::btree_base::{BRANCH, LEAF};
use crate::tree_store::btree_base::{BranchAccessor, LeafAccessor};
use crate::tree_store::btree_iters::RangeIterState::{BranchChild, Enter, Leaf};
use crate::tree_store::page_store::{Page, PageHint, PageImpl};
use crate::tree_store::{PageNumber, PageResolver};
use crate::types::{Key, Value};
use Bound::{Excluded, Included, Unbounded};
use std::borrow::Borrow;
use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::{Range, RangeBounds};

#[derive(Debug, Clone)]
enum RangeIterState {
    Enter {
        page: PageImpl,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        parent: Option<Box<RangeIterState>>,
    },
    Leaf {
        page: PageImpl,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        entry: usize,
        start: usize,
        end: usize,
        parent: Option<Box<RangeIterState>>,
    },
    BranchChild {
        page: PageImpl,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        child: usize,
        parent: Option<Box<RangeIterState>>,
    },
}

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

fn upper_bound_entry<K: Key>(accessor: &LeafAccessor<'_>, bound: Bound<&[u8]>) -> usize {
    match bound {
        Included(query) | Excluded(query) => {
            let (mut position, found) = accessor.position::<K>(query);
            if matches!(bound, Included(_)) && found {
                position += 1;
            }
            position
        }
        Unbounded => accessor.num_pairs(),
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

fn leaf_entries<K: Key>(
    accessor: &LeafAccessor<'_>,
    left_bound: Bound<&[u8]>,
    right_bound: Bound<&[u8]>,
) -> Range<usize> {
    let start = lower_bound_entry::<K>(accessor, left_bound);
    let end = upper_bound_entry::<K>(accessor, right_bound);
    start..end
}

impl RangeIterState {
    fn page_number(&self) -> PageNumber {
        match self {
            Enter { page, .. } | Leaf { page, .. } | BranchChild { page, .. } => {
                page.get_page_number()
            }
        }
    }

    fn next<K: Key>(
        self,
        left_bound: Bound<&[u8]>,
        right_bound: Bound<&[u8]>,
        reverse: bool,
        manager: &PageResolver,
        hint: PageHint,
    ) -> Result<Option<RangeIterState>> {
        match self {
            Enter {
                page,
                fixed_key_size,
                fixed_value_size,
                parent,
            } => match page.memory()[0] {
                LEAF => {
                    let accessor =
                        LeafAccessor::new(page.memory(), fixed_key_size, fixed_value_size);
                    let entry_count = accessor.num_pairs();
                    // TODO: Track when a descended subtree is fully inside the
                    // range, so interior leaves can skip these bound searches.
                    let entries = leaf_entries::<K>(&accessor, left_bound, right_bound);
                    Ok(if entries.start < entries.end {
                        let entry = if reverse {
                            entries.end - 1
                        } else {
                            entries.start
                        };
                        Some(Leaf {
                            page,
                            fixed_key_size,
                            fixed_value_size,
                            entry,
                            start: entries.start,
                            end: entries.end,
                            parent,
                        })
                    } else if (!reverse && !matches!(right_bound, Unbounded) && entries.end == 0)
                        || (reverse
                            && !matches!(left_bound, Unbounded)
                            && entries.start == entry_count)
                    {
                        None
                    } else {
                        parent.map(|x| *x)
                    })
                }
                BRANCH => {
                    let accessor = BranchAccessor::new(&page, fixed_key_size);
                    let seek_bound = if reverse { right_bound } else { left_bound };
                    let child = child_to_visit::<K>(&accessor, seek_bound, reverse);
                    Ok(Some(BranchChild {
                        child,
                        page,
                        fixed_key_size,
                        fixed_value_size,
                        parent,
                    }))
                }
                _ => unreachable!(),
            },
            Leaf {
                page,
                fixed_key_size,
                fixed_value_size,
                entry,
                start,
                end,
                parent,
            } => {
                let next_entry = if reverse {
                    entry.checked_sub(1).filter(|entry| *entry >= start)
                } else {
                    let next_entry = entry + 1;
                    (next_entry < end).then_some(next_entry)
                };
                if let Some(entry) = next_entry {
                    Ok(Some(Leaf {
                        page,
                        fixed_key_size,
                        fixed_value_size,
                        entry,
                        start,
                        end,
                        parent,
                    }))
                } else {
                    Ok(parent.map(|x| *x))
                }
            }
            BranchChild {
                page,
                fixed_key_size,
                fixed_value_size,
                child,
                parent,
            } => {
                let (child_page, child_count) = {
                    let accessor = BranchAccessor::new(&page, fixed_key_size);
                    let child_count = accessor.count_children();
                    let child_page = manager.get_page(accessor.child_page(child).unwrap(), hint)?;
                    (child_page, child_count)
                };
                let parent = Self::next_branch_child(
                    BranchChild {
                        page,
                        fixed_key_size,
                        fixed_value_size,
                        child,
                        parent,
                    },
                    child_count,
                    reverse,
                );
                Ok(Some(Enter {
                    page: child_page,
                    fixed_key_size,
                    fixed_value_size,
                    parent,
                }))
            }
        }
    }

    fn next_branch_child(
        state: RangeIterState,
        child_count: usize,
        reverse: bool,
    ) -> Option<Box<RangeIterState>> {
        let BranchChild {
            page,
            fixed_key_size,
            fixed_value_size,
            child,
            parent,
        } = state
        else {
            unreachable!("next branch child requires a branch child state");
        };
        let next_child = if reverse {
            child.checked_sub(1)
        } else {
            let next_child = child + 1;
            (next_child < child_count).then_some(next_child)
        };
        if let Some(child) = next_child {
            Some(Box::new(BranchChild {
                page,
                fixed_key_size,
                fixed_value_size,
                child,
                parent,
            }))
        } else {
            parent
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
    next: Option<RangeIterState>,
    manager: PageResolver,
    hint: PageHint,
}

impl AllPageNumbersBtreeIter {
    pub(crate) fn new(
        root: PageNumber,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        manager: PageResolver,
        hint: PageHint,
    ) -> Result<Self> {
        let root_page = manager.get_page(root, hint)?;
        let start = Enter {
            page: root_page,
            fixed_key_size,
            fixed_value_size,
            parent: None,
        };
        Ok(Self {
            next: Some(start),
            manager,
            hint,
        })
    }
}

impl Iterator for AllPageNumbersBtreeIter {
    type Item = Result<PageNumber>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let state = self.next.take()?;
            let value = state.page_number();
            // Only return each page number once
            let once = match &state {
                Enter {
                    page,
                    fixed_key_size,
                    fixed_value_size,
                    ..
                } => match page.memory()[0] {
                    BRANCH => true,
                    LEAF => {
                        LeafAccessor::new(page.memory(), *fixed_key_size, *fixed_value_size)
                            .num_pairs()
                            == 0
                    }
                    _ => unreachable!(),
                },
                Leaf { entry, .. } => *entry == 0,
                BranchChild { .. } => false,
            };
            match state.next::<()>(Unbounded, Unbounded, false, &self.manager, self.hint) {
                Ok(next) => {
                    self.next = next;
                }
                Err(err) => {
                    return Some(Err(err));
                }
            }
            if once {
                return Some(Ok(value));
            }
        }
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
