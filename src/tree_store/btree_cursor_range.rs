use crate::Result;
use crate::tree_store::btree_cursor::Cursor;
use crate::tree_store::btree_iters::{EntryGuard, range_is_empty};
use crate::tree_store::page_store::PageHint;
use crate::tree_store::{PageNumber, PageResolver};
use crate::types::{Key, Value};
use Bound::{Excluded, Included, Unbounded};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::Bound;
use std::ops::RangeBounds;

#[derive(Clone)]
enum Slot<C> {
    Uninitialized,
    Active(C),
    Exhausted,
}

impl<C> Slot<C> {
    fn active(&self) -> Option<&C> {
        match self {
            Slot::Active(cursor) => Some(cursor),
            Slot::Uninitialized | Slot::Exhausted => None,
        }
    }

    fn active_mut(&mut self) -> Option<&mut C> {
        match self {
            Slot::Active(cursor) => Some(cursor),
            Slot::Uninitialized | Slot::Exhausted => None,
        }
    }

    fn is_uninitialized(&self) -> bool {
        matches!(self, Slot::Uninitialized)
    }
}

#[derive(Copy, Clone)]
enum Side {
    Front,
    Back,
}

#[derive(Clone)]
pub(crate) struct BtreeCursorRange<K: Key + 'static, V: Value + 'static> {
    root: Option<PageNumber>,
    lower_bound: Bound<Vec<u8>>,
    upper_bound: Bound<Vec<u8>>,
    manager: PageResolver,
    hint: PageHint,
    front: Slot<Cursor<K, V>>,
    back: Slot<Cursor<K, V>>,
}

impl<K: Key + 'static, V: Value + 'static> BtreeCursorRange<K, V> {
    pub(crate) fn new<'a, T: RangeBounds<KR>, KR: Borrow<K::SelfType<'a>>>(
        query_range: &'_ T,
        table_root: Option<PageNumber>,
        manager: PageResolver,
        hint: PageHint,
    ) -> Result<Self> {
        if table_root.is_none() || range_is_empty::<K, KR, T>(query_range) {
            return Ok(Self::empty(manager, hint));
        }

        let lower_bound = query_range
            .start_bound()
            .map(|key| K::as_bytes(key.borrow()).as_ref().to_vec());
        let upper_bound = query_range
            .end_bound()
            .map(|key| K::as_bytes(key.borrow()).as_ref().to_vec());
        Ok(Self {
            root: table_root,
            lower_bound,
            upper_bound,
            manager,
            hint,
            front: Slot::Uninitialized,
            back: Slot::Uninitialized,
        })
    }

    fn empty(manager: PageResolver, hint: PageHint) -> Self {
        Self {
            root: None,
            lower_bound: Unbounded,
            upper_bound: Unbounded,
            manager,
            hint,
            front: Slot::Exhausted,
            back: Slot::Exhausted,
        }
    }

    fn close(&mut self) {
        self.root = None;
        self.front = Slot::Exhausted;
        self.back = Slot::Exhausted;
    }

    fn initialize_side(&mut self, side: Side) -> Result {
        assert!(matches!(self.slot(side), Slot::Uninitialized));
        let root = self.root.expect("range cursor must have a root");
        let mut cursor = Cursor::new(root, self.manager.clone(), self.hint);
        match side {
            Side::Front => {
                cursor.seek_to(self.lower_bound.as_ref().map(Vec::as_slice))?;
            }
            Side::Back => match self.upper_bound.as_ref().map(Vec::as_slice) {
                Included(key) => cursor.seek_to(Excluded(key))?,
                Excluded(key) => cursor.seek_to(Included(key))?,
                Unbounded => cursor.seek_to_end()?,
            },
        }
        *self.slot_mut(side) = Slot::Active(cursor);
        Ok(())
    }

    fn prepare(&mut self, side: Side) -> Result<bool> {
        if matches!(self.slot(side), Slot::Uninitialized) {
            self.initialize_side(side)?;
        }
        Ok(self.slot(side).active().is_some())
    }

    fn slot(&self, side: Side) -> &Slot<Cursor<K, V>> {
        match side {
            Side::Front => &self.front,
            Side::Back => &self.back,
        }
    }

    fn slot_mut(&mut self, side: Side) -> &mut Slot<Cursor<K, V>> {
        match side {
            Side::Front => &mut self.front,
            Side::Back => &mut self.back,
        }
    }

    fn opposite_slot(&self, side: Side) -> &Slot<Cursor<K, V>> {
        match side {
            Side::Front => &self.back,
            Side::Back => &self.front,
        }
    }

    fn active_cursor_mut(&mut self, side: Side) -> &mut Cursor<K, V> {
        self.slot_mut(side)
            .active_mut()
            .expect("cursor must be active")
    }

    fn cursors_have_remaining(&mut self) -> Result<bool> {
        // The gap after one leaf is the same logical position as the gap before
        // the next leaf. Keep the front cursor canonical so path comparison can
        // detect when alternating iteration has consumed the whole range.
        if let (Slot::Active(front), Slot::Active(_)) = (&mut self.front, &self.back) {
            front.normalize_forward_gap()?;
        }

        match (&self.front, &self.back) {
            (Slot::Active(front), Slot::Active(back)) => {
                Ok(front.compare_position(back) == Ordering::Less)
            }
            (Slot::Exhausted, _) | (_, Slot::Exhausted) => Ok(false),
            (Slot::Uninitialized, _) | (_, Slot::Uninitialized) => Ok(true),
        }
    }

    fn advance_cursor(&mut self, side: Side) -> Result<Option<EntryGuard<K, V>>> {
        match side {
            Side::Front => self.active_cursor_mut(side).next(),
            Side::Back => self.active_cursor_mut(side).prev(),
        }
    }

    fn entry_within_open_bound(&self, entry: &EntryGuard<K, V>, side: Side) -> bool {
        let key = entry.key_bytes();
        match side {
            Side::Front => match self.upper_bound.as_ref().map(Vec::as_slice) {
                Included(bound) => K::compare(key, bound).is_le(),
                Excluded(bound) => K::compare(key, bound).is_lt(),
                Unbounded => true,
            },
            Side::Back => match self.lower_bound.as_ref().map(Vec::as_slice) {
                Included(bound) => K::compare(key, bound).is_ge(),
                Excluded(bound) => K::compare(key, bound).is_gt(),
                Unbounded => true,
            },
        }
    }

    fn next_from(&mut self, side: Side) -> Option<Result<EntryGuard<K, V>>> {
        match self.prepare(side) {
            Ok(true) => {}
            Ok(false) => return None,
            Err(err) => return Some(Err(err)),
        }
        match self.cursors_have_remaining() {
            Ok(true) => {}
            Ok(false) => {
                self.close();
                return None;
            }
            Err(err) => return Some(Err(err)),
        }

        let entry = match self.advance_cursor(side) {
            Ok(Some(entry)) => entry,
            Ok(None) => {
                self.close();
                return None;
            }
            Err(err) => return Some(Err(err)),
        };
        if self.opposite_slot(side).is_uninitialized()
            && !self.entry_within_open_bound(&entry, side)
        {
            self.close();
            return None;
        }
        Some(Ok(entry))
    }
}

impl<K: Key + 'static, V: Value + 'static> Iterator for BtreeCursorRange<K, V> {
    type Item = Result<EntryGuard<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_from(Side::Front)
    }
}

impl<K: Key + 'static, V: Value + 'static> DoubleEndedIterator for BtreeCursorRange<K, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.next_from(Side::Back)
    }
}
