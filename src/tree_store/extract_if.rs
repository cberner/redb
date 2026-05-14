use crate::tree_store::btree_cursor::{CursorMut, EntryRef, Position};
use crate::tree_store::{BtreeHeader, PageAllocator, PageNumber, PageTrackerPolicy};
use crate::types::{Key, Value};
use crate::{AccessGuard, Result};
use std::collections::Bound;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

type ExtractItem<'a, K, V> = (AccessGuard<'a, K>, AccessGuard<'a, V>);

struct ExtractScan<'s, K, V, F>
where
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
{
    front_bound: &'s mut Bound<Vec<u8>>,
    back_bound: &'s mut Bound<Vec<u8>>,
    predicate: &'s mut F,
    predicate_running: &'s mut bool,
    _type: PhantomData<(K, V)>,
}

impl<K, V, F> ExtractScan<'_, K, V, F>
where
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
{
    fn next<'a>(
        &mut self,
        cursor: &mut CursorMut<'a, '_, K, V>,
    ) -> Result<Option<ExtractItem<'a, K, V>>> {
        cursor.seek_to(front_seek_position(self.front_bound))?;
        while let Some(entry) = cursor.peek_next()? {
            if !self.before_back_bound(entry.key_bytes()) {
                return Ok(None);
            }

            let key = entry.key_bytes().to_vec();
            if self.predicate_matches(&entry) {
                let result = cursor
                    .remove_next_detached()?
                    .expect("peeked cursor entry must be removable");
                *self.front_bound = Excluded(key);
                return Ok(Some(result));
            }
            cursor.next()?;
            *self.front_bound = Excluded(key);
        }
        Ok(None)
    }

    fn next_back<'a>(
        &mut self,
        cursor: &mut CursorMut<'a, '_, K, V>,
    ) -> Result<Option<ExtractItem<'a, K, V>>> {
        cursor.seek_to(back_seek_position(self.back_bound))?;
        while let Some(entry) = cursor.peek_prev()? {
            if !self.after_front_bound(entry.key_bytes()) {
                return Ok(None);
            }

            let key = entry.key_bytes().to_vec();
            if self.predicate_matches(&entry) {
                let result = cursor
                    .remove_prev_detached()?
                    .expect("peeked cursor entry must be removable");
                *self.back_bound = Excluded(key);
                return Ok(Some(result));
            }
            cursor.prev()?;
            *self.back_bound = Excluded(key);
        }
        Ok(None)
    }

    fn predicate_matches(&mut self, entry: &EntryRef<'_, K, V>) -> bool {
        assert!(!*self.predicate_running);
        *self.predicate_running = true;
        let result = (self.predicate)(entry.key(), entry.value());
        *self.predicate_running = false;
        result
    }

    fn before_back_bound(&self, key: &[u8]) -> bool {
        match bound_as_ref(self.back_bound) {
            Included(bound) => K::compare(key, bound).is_le(),
            Excluded(bound) => K::compare(key, bound).is_lt(),
            Unbounded => true,
        }
    }

    fn after_front_bound(&self, key: &[u8]) -> bool {
        match bound_as_ref(self.front_bound) {
            Included(bound) => K::compare(key, bound).is_ge(),
            Excluded(bound) => K::compare(key, bound).is_gt(),
            Unbounded => true,
        }
    }
}

pub(crate) struct BtreeExtractIf<
    'a,
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
> {
    root: &'a mut Option<BtreeHeader>,
    front_bound: Bound<Vec<u8>>,
    back_bound: Bound<Vec<u8>>,
    done: bool,
    predicate: F,
    predicate_running: bool,
    free_on_drop: Vec<PageNumber>,
    master_free_list: Arc<Mutex<Vec<PageNumber>>>,
    allocated: Arc<Mutex<PageTrackerPolicy>>,
    page_allocator: PageAllocator,
    _type: PhantomData<(&'a K, &'a V)>,
}

impl<'a, K: Key + 'static, V: Value + 'static, F> BtreeExtractIf<'a, K, V, F>
where
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
{
    pub(crate) fn new(
        root: &'a mut Option<BtreeHeader>,
        front_bound: Bound<Vec<u8>>,
        back_bound: Bound<Vec<u8>>,
        predicate: F,
        master_free_list: Arc<Mutex<Vec<PageNumber>>>,
        allocated: Arc<Mutex<PageTrackerPolicy>>,
        page_allocator: PageAllocator,
    ) -> Self {
        Self {
            root,
            front_bound,
            back_bound,
            done: false,
            predicate,
            predicate_running: false,
            free_on_drop: vec![],
            master_free_list,
            allocated,
            page_allocator,
            _type: PhantomData,
        }
    }

    pub(crate) fn predicate_panicked(&self) -> bool {
        self.predicate_running
    }

    pub(crate) fn close(&mut self) -> Result {
        self.done = true;
        self.free_pending_pages();
        Ok(())
    }

    fn next_inner(&mut self) -> Result<Option<ExtractItem<'a, K, V>>> {
        // TODO: keep the front cursor live across calls and only reload it when
        // the back cursor mutates the tree.
        let mut front_bound = self.front_bound.clone();
        let mut back_bound = self.back_bound.clone();
        let result = {
            let Self {
                root,
                predicate,
                predicate_running,
                free_on_drop,
                allocated,
                page_allocator,
                ..
            } = self;
            let mut cursor: CursorMut<'a, '_, K, V> =
                CursorMut::new(root, page_allocator, free_on_drop, allocated);
            let mut scan = ExtractScan {
                front_bound: &mut front_bound,
                back_bound: &mut back_bound,
                predicate,
                predicate_running,
                _type: PhantomData,
            };
            scan.next(&mut cursor)
        };

        self.front_bound = front_bound;
        self.back_bound = back_bound;
        self.free_pending_pages();
        if matches!(result, Ok(None)) {
            self.done = true;
        }
        result
    }

    fn next_back_inner(&mut self) -> Result<Option<ExtractItem<'a, K, V>>> {
        // TODO: keep the back cursor live across calls and only reload it when
        // the front cursor mutates the tree.
        let mut front_bound = self.front_bound.clone();
        let mut back_bound = self.back_bound.clone();
        let result = {
            let Self {
                root,
                predicate,
                predicate_running,
                free_on_drop,
                allocated,
                page_allocator,
                ..
            } = self;
            let mut cursor: CursorMut<'a, '_, K, V> =
                CursorMut::new(root, page_allocator, free_on_drop, allocated);
            let mut scan = ExtractScan {
                front_bound: &mut front_bound,
                back_bound: &mut back_bound,
                predicate,
                predicate_running,
                _type: PhantomData,
            };
            scan.next_back(&mut cursor)
        };

        self.front_bound = front_bound;
        self.back_bound = back_bound;
        self.free_pending_pages();
        if matches!(result, Ok(None)) {
            self.done = true;
        }
        result
    }

    fn free_pending_pages(&mut self) {
        let mut master_free_list = self.master_free_list.lock().unwrap();
        let mut allocated = self.allocated.lock().unwrap();
        for page in self.free_on_drop.drain(..) {
            if !self
                .page_allocator
                .free_if_uncommitted(page, &mut allocated)
            {
                master_free_list.push(page);
            }
        }
    }
}

fn bound_as_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    bound.as_ref().map(Vec::as_slice)
}

fn front_seek_position(bound: &Bound<Vec<u8>>) -> Position<'_> {
    match bound {
        Included(bound) => Position::Before(bound.as_slice()),
        Excluded(bound) => Position::After(bound.as_slice()),
        Unbounded => Position::Start,
    }
}

fn back_seek_position(bound: &Bound<Vec<u8>>) -> Position<'_> {
    match bound {
        Included(bound) => Position::After(bound.as_slice()),
        Excluded(bound) => Position::Before(bound.as_slice()),
        Unbounded => Position::End,
    }
}

impl<'a, K: Key + 'static, V: Value + 'static, F> Iterator for BtreeExtractIf<'a, K, V, F>
where
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
{
    type Item = Result<ExtractItem<'a, K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        self.next_inner().transpose()
    }
}

impl<K: Key + 'static, V: Value + 'static, F> DoubleEndedIterator for BtreeExtractIf<'_, K, V, F>
where
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        self.next_back_inner().transpose()
    }
}

impl<K: Key + 'static, V: Value + 'static, F> Drop for BtreeExtractIf<'_, K, V, F>
where
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
{
    fn drop(&mut self) {
        let _ = self.close();
    }
}
