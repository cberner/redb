use crate::Result;
use crate::tree_store::btree_iters::{BtreeRangeIter, EntryGuard};
use crate::tree_store::btree_mutator::MutateHelper;
use crate::tree_store::{BtreeHeader, PageAllocator, PageNumber, PageTrackerPolicy};
use crate::types::{Key, Value};
use std::sync::{Arc, Mutex};

pub(crate) struct BtreeExtractIf<
    'a,
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
> {
    root: &'a mut Option<BtreeHeader>,
    inner: BtreeRangeIter<K, V>,
    predicate: F,
    predicate_running: bool,
    free_on_drop: Vec<PageNumber>,
    master_free_list: Arc<Mutex<Vec<PageNumber>>>,
    allocated: Arc<Mutex<PageTrackerPolicy>>,
    page_allocator: PageAllocator,
}

impl<'a, K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>
    BtreeExtractIf<'a, K, V, F>
{
    pub(crate) fn new(
        root: &'a mut Option<BtreeHeader>,
        inner: BtreeRangeIter<K, V>,
        predicate: F,
        master_free_list: Arc<Mutex<Vec<PageNumber>>>,
        allocated: Arc<Mutex<PageTrackerPolicy>>,
        page_allocator: PageAllocator,
    ) -> Self {
        Self {
            root,
            inner,
            predicate,
            predicate_running: false,
            free_on_drop: vec![],
            master_free_list,
            allocated,
            page_allocator,
        }
    }

    pub(crate) fn predicate_panicked(&self) -> bool {
        self.predicate_running
    }

    fn predicate_matches(&mut self, entry: &EntryGuard<K, V>) -> bool {
        assert!(!self.predicate_running);
        self.predicate_running = true;
        let result = (self.predicate)(entry.key(), entry.value());
        self.predicate_running = false;
        result
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool> Iterator
    for BtreeExtractIf<'_, K, V, F>
{
    type Item = Result<EntryGuard<K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut item = self.inner.next();
        while let Some(Ok(ref entry)) = item {
            if self.predicate_matches(entry) {
                let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new_do_not_modify(
                    self.root,
                    self.page_allocator.clone(),
                    &mut self.free_on_drop,
                    self.allocated.clone(),
                );
                match operation.delete(&entry.key()) {
                    Ok(x) => {
                        assert!(x.is_some());
                    }
                    Err(x) => {
                        return Some(Err(x));
                    }
                }
                break;
            }
            item = self.inner.next();
        }
        item
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>
    DoubleEndedIterator for BtreeExtractIf<'_, K, V, F>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let mut item = self.inner.next_back();
        while let Some(Ok(ref entry)) = item {
            if self.predicate_matches(entry) {
                let mut operation: MutateHelper<'_, '_, K, V> = MutateHelper::new_do_not_modify(
                    self.root,
                    self.page_allocator.clone(),
                    &mut self.free_on_drop,
                    self.allocated.clone(),
                );
                match operation.delete(&entry.key()) {
                    Ok(x) => {
                        assert!(x.is_some());
                    }
                    Err(x) => {
                        return Some(Err(x));
                    }
                }
                break;
            }
            item = self.inner.next_back();
        }
        item
    }
}

impl<K: Key, V: Value, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool> Drop
    for BtreeExtractIf<'_, K, V, F>
{
    fn drop(&mut self) {
        self.inner.close();
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
