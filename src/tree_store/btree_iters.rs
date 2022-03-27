use crate::tree_store::btree_base::{EntryAccessor, InternalAccessor, LeafAccessor};
use crate::tree_store::btree_base::{INTERNAL, LEAF};
use crate::tree_store::btree_iters::RangeIterState::{Internal, Leaf};
use crate::tree_store::page_store::{Page, PageImpl, TransactionalMemory};
use crate::tree_store::PageNumber;
use crate::types::{RedbKey, RedbValue};
use std::borrow::Borrow;
use std::collections::Bound;
use std::marker::PhantomData;
use std::ops::RangeBounds;

#[derive(Debug)]
pub enum RangeIterState<'a> {
    Leaf {
        page: PageImpl<'a>,
        entry: usize,
        parent: Option<Box<RangeIterState<'a>>>,
    },
    Internal {
        page: PageImpl<'a>,
        child: usize,
        parent: Option<Box<RangeIterState<'a>>>,
    },
}

impl<'a> RangeIterState<'a> {
    fn page_number(&self) -> PageNumber {
        match self {
            Leaf { page, .. } => page.get_page_number(),
            Internal { page, .. } => page.get_page_number(),
        }
    }

    fn next(self, reverse: bool, manager: &'a TransactionalMemory) -> Option<RangeIterState> {
        match self {
            Leaf {
                page,
                entry,
                parent,
            } => {
                let accessor = LeafAccessor::new(&page);
                let direction = if reverse { -1 } else { 1 };
                let next_entry = entry as isize + direction;
                if 0 <= next_entry && next_entry < accessor.num_pairs() as isize {
                    Some(Leaf {
                        page,
                        entry: next_entry as usize,
                        parent,
                    })
                } else {
                    parent.map(|x| *x)
                }
            }
            Internal {
                page,
                child,
                mut parent,
            } => {
                let accessor = InternalAccessor::new(&page);
                let child_page = accessor.child_page(child).unwrap();
                let child_page = manager.get_page(child_page);
                let direction = if reverse { -1 } else { 1 };
                let next_child = child as isize + direction;
                if 0 <= next_child && next_child < accessor.count_children() as isize {
                    parent = Some(Box::new(Internal {
                        page,
                        child: next_child as usize,
                        parent,
                    }));
                }
                match child_page.memory()[0] {
                    LEAF => {
                        let child_accessor = LeafAccessor::new(&child_page);
                        let entry = if reverse {
                            child_accessor.num_pairs() - 1
                        } else {
                            0
                        };
                        Some(Leaf {
                            page: child_page,
                            entry,
                            parent,
                        })
                    }
                    INTERNAL => {
                        let child_accessor = InternalAccessor::new(&child_page);
                        let child = if reverse {
                            child_accessor.count_children() - 1
                        } else {
                            0
                        };
                        Some(Internal {
                            page: child_page,
                            child,
                            parent,
                        })
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    fn get_entry(&self) -> Option<EntryAccessor> {
        match self {
            Leaf { page, entry, .. } => LeafAccessor::new(page).entry(*entry),
            _ => None,
        }
    }
}

pub(crate) struct AllPageNumbersBtreeIter<'a> {
    next: Option<RangeIterState<'a>>,
    manager: &'a TransactionalMemory,
}

impl<'a> AllPageNumbersBtreeIter<'a> {
    pub(crate) fn new(start: RangeIterState<'a>, manager: &'a TransactionalMemory) -> Self {
        match start {
            Leaf { entry, .. } => {
                assert_eq!(entry, 0)
            }
            Internal { child, .. } => {
                assert_eq!(child, 0)
            }
        }
        Self {
            next: Some(start),
            manager,
        }
    }
}

impl<'a> Iterator for AllPageNumbersBtreeIter<'a> {
    type Item = PageNumber;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let state = self.next.take()?;
            let value = state.page_number();
            // Only return each page number once
            let once = match state {
                Leaf { entry, .. } => entry == 0,
                Internal { child, .. } => child == 0,
            };
            self.next = state.next(false, self.manager);
            if once {
                return Some(value);
            }
        }
    }
}

pub(crate) fn page_numbers_iter_start_state(page: PageImpl) -> RangeIterState {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => Leaf {
            page,
            entry: 0,
            parent: None,
        },
        INTERNAL => Internal {
            page,
            child: 0,
            parent: None,
        },
        _ => unreachable!(),
    }
}

pub struct BtreeRangeIter<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> {
    left: Option<RangeIterState<'a>>, // Exclusive. The previous element returned
    right: Option<RangeIterState<'a>>, // Exclusive. The previous element returned
    include_left: bool,               // left is inclusive, instead of exclusive
    include_right: bool,              // right is inclusive, instead of exclusive
    reversed: bool,
    manager: &'a TransactionalMemory,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: RedbKey + ?Sized + 'a, V: RedbValue + ?Sized + 'a> BtreeRangeIter<'a, K, V> {
    pub(crate) fn new<T: RangeBounds<KR>, KR: Borrow<K> + ?Sized + 'a>(
        query_range: T,
        table_root: Option<PageNumber>,
        manager: &'a TransactionalMemory,
    ) -> Self {
        if let Some(root) = table_root {
            let (include_left, left) = match query_range.start_bound() {
                Bound::Included(k) => find_iter_left::<K>(
                    manager.get_page(root),
                    None,
                    k.borrow().as_bytes().as_ref(),
                    true,
                    manager,
                ),
                Bound::Excluded(k) => find_iter_left::<K>(
                    manager.get_page(root),
                    None,
                    k.borrow().as_bytes().as_ref(),
                    false,
                    manager,
                ),
                Bound::Unbounded => {
                    let state = find_iter_unbounded_left(manager.get_page(root), None, manager);
                    (true, state)
                }
            };
            let (include_right, right) = match query_range.end_bound() {
                Bound::Included(k) => find_iter_right::<K>(
                    manager.get_page(root),
                    None,
                    k.borrow().as_bytes().as_ref(),
                    true,
                    manager,
                ),
                Bound::Excluded(k) => find_iter_right::<K>(
                    manager.get_page(root),
                    None,
                    k.borrow().as_bytes().as_ref(),
                    false,
                    manager,
                ),
                Bound::Unbounded => {
                    let state = find_iter_unbounded_right(manager.get_page(root), None, manager);
                    (true, state)
                }
            };
            Self {
                left,
                right,
                include_left,
                include_right,
                reversed: false,
                manager,
                _key_type: Default::default(),
                _value_type: Default::default(),
            }
        } else {
            Self {
                left: None,
                right: None,
                include_left: false,
                include_right: false,
                reversed: false,
                manager,
                _key_type: Default::default(),
                _value_type: Default::default(),
            }
        }
    }

    pub(crate) fn reverse(self) -> Self {
        Self {
            left: self.left,
            right: self.right,
            include_left: self.include_left,
            include_right: self.include_right,
            reversed: !self.reversed,
            manager: self.manager,
            _key_type: Default::default(),
            _value_type: Default::default(),
        }
    }

    // TODO: we need generic-associated-types to implement Iterator
    pub fn next(&mut self) -> Option<EntryAccessor> {
        if let (
            Some(Leaf {
                page: left_page,
                entry: left_entry,
                parent: _,
            }),
            Some(Leaf {
                page: right_page,
                entry: right_entry,
                parent: _,
            }),
        ) = (&self.left, &self.right)
        {
            if left_page.get_page_number() == right_page.get_page_number()
                && left_entry >= right_entry
                && !(self.include_left && self.include_right)
            {
                return None;
            }
        }

        loop {
            if !self.reversed {
                if !self.include_left {
                    self.left = self.left.take()?.next(self.reversed, self.manager);
                }
                // Return None if the next state is None
                self.left.as_ref()?;

                if let (
                    Some(Leaf {
                        page: left_page,
                        entry: left_entry,
                        parent: _,
                    }),
                    Some(Leaf {
                        page: right_page,
                        entry: right_entry,
                        parent: _,
                    }),
                ) = (&self.left, &self.right)
                {
                    if left_page.get_page_number() == right_page.get_page_number()
                        && left_entry >= right_entry
                        && !self.include_right
                    {
                        return None;
                    }
                }

                self.include_left = false;
                if self.left.as_ref().unwrap().get_entry().is_some() {
                    return self.left.as_ref().map(|s| s.get_entry().unwrap());
                }
            } else {
                if !self.include_right {
                    self.right = self.right.take()?.next(self.reversed, self.manager);
                }
                // Return None if the next state is None
                self.right.as_ref()?;

                if let (
                    Some(Leaf {
                        page: left_page,
                        entry: left_entry,
                        parent: _,
                    }),
                    Some(Leaf {
                        page: right_page,
                        entry: right_entry,
                        parent: _,
                    }),
                ) = (&self.left, &self.right)
                {
                    if left_page.get_page_number() == right_page.get_page_number()
                        && left_entry >= right_entry
                        && !self.include_left
                    {
                        return None;
                    }
                }

                self.include_right = false;
                if self.right.as_ref().unwrap().get_entry().is_some() {
                    return self.right.as_ref().map(|s| s.get_entry().unwrap());
                }
            }
        }
    }
}

fn find_iter_unbounded_left<'a>(
    page: PageImpl<'a>,
    mut parent: Option<Box<RangeIterState<'a>>>,
    manager: &'a TransactionalMemory,
) -> Option<RangeIterState<'a>> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => Some(Leaf {
            page,
            entry: 0,
            parent,
        }),
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let child_page_number = accessor.child_page(0).unwrap();
            let child_page = manager.get_page(child_page_number);
            parent = Some(Box::new(Internal {
                page,
                child: 1,
                parent,
            }));
            find_iter_unbounded_left(child_page, parent, manager)
        }
        _ => unreachable!(),
    }
}

fn find_iter_unbounded_right<'a>(
    page: PageImpl<'a>,
    mut parent: Option<Box<RangeIterState<'a>>>,
    manager: &'a TransactionalMemory,
) -> Option<RangeIterState<'a>> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            let entry = accessor.num_pairs() - 1;
            Some(Leaf {
                page,
                entry,
                parent,
            })
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let child_index = accessor.count_children() - 1;
            let child_page_number = accessor.child_page(child_index).unwrap();
            let child_page = manager.get_page(child_page_number);
            if child_index > 0 && accessor.child_page(child_index - 1).is_some() {
                parent = Some(Box::new(Internal {
                    page,
                    child: child_index - 1,
                    parent,
                }));
            }
            find_iter_unbounded_right(child_page, parent, manager)
        }
        _ => unreachable!(),
    }
}

// Returns a bool indicating whether the first entry pointed to by the state is included in the
// queried range
fn find_iter_left<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    mut parent: Option<Box<RangeIterState<'a>>>,
    query: &[u8],
    include_query: bool,
    manager: &'a TransactionalMemory,
) -> (bool, Option<RangeIterState<'a>>) {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            let (position, found) = accessor.position::<K>(query);
            let include = position < accessor.num_pairs() && (include_query || !found);
            let result = Leaf {
                page,
                entry: position,
                parent,
            };
            (include, Some(result))
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let (child_index, child_page_number) = accessor.child_for_key::<K>(query);
            let child_page = manager.get_page(child_page_number);
            if child_index < accessor.count_children() - 1 {
                parent = Some(Box::new(Internal {
                    page,
                    child: child_index + 1,
                    parent,
                }));
            }
            find_iter_left::<K>(child_page, parent, query, include_query, manager)
        }
        _ => unreachable!(),
    }
}

fn find_iter_right<'a, K: RedbKey + ?Sized>(
    page: PageImpl<'a>,
    mut parent: Option<Box<RangeIterState<'a>>>,
    query: &[u8],
    include_query: bool,
    manager: &'a TransactionalMemory,
) -> (bool, Option<RangeIterState<'a>>) {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            let (position, found) = accessor.position::<K>(query);
            let result = Leaf {
                page,
                entry: position,
                parent,
            };
            (include_query && found, Some(result))
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let (child_index, child_page_number) = accessor.child_for_key::<K>(query);
            let child_page = manager.get_page(child_page_number);
            if child_index > 0 && accessor.child_page(child_index - 1).is_some() {
                parent = Some(Box::new(Internal {
                    page,
                    child: child_index - 1,
                    parent,
                }));
            }
            find_iter_right::<K>(child_page, parent, query, include_query, manager)
        }
        _ => unreachable!(),
    }
}
