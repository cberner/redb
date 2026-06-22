use crate::tree_store::btree_cursor::RangeMut;
use crate::tree_store::{BtreeHeader, PageAllocator, PageNumber, PageTrackerPolicy};
use crate::types::{Key, Value};
use crate::{AccessGuard, Result, StorageError};
use std::collections::Bound;
use std::sync::{Arc, Mutex};

type ExtractItem<'a, K, V> = (AccessGuard<'a, K>, AccessGuard<'a, V>);
type AdvanceInner<T, R> = fn(&mut T) -> Result<Option<R>>;

#[derive(Copy, Clone, Eq, PartialEq)]
enum ExtractState {
    Running,
    // Exhausted or explicitly closed: iteration returns None.
    Closed,
    // An iteration error was returned: later calls re-raise instead of
    // continuing, since the failure is not recoverable.
    Errored,
}

pub(crate) struct BtreeExtractIf<
    'a,
    K: Key + 'static,
    V: Value + 'static,
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
> {
    range: RangeMut<'a, K, V>,
    predicate: F,
    predicate_running: bool,
    state: ExtractState,
    close_failed: bool,
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
            range: RangeMut::new(
                root,
                front_bound,
                back_bound,
                page_allocator,
                master_free_list,
                allocated,
            ),
            predicate,
            predicate_running: false,
            state: ExtractState::Running,
            close_failed: false,
        }
    }

    pub(crate) fn predicate_panicked(&self) -> bool {
        self.predicate_running
    }

    pub(crate) fn close(&mut self) -> Result {
        if self.state != ExtractState::Running {
            // Already closed, possibly by latch_error: an explicit close()
            // must still re-raise an iteration error or a failed
            // finalization, so Ok always means the scan completed cleanly.
            return if self.close_failed || self.state == ExtractState::Errored {
                Err(StorageError::PreviousIo)
            } else {
                Ok(())
            };
        }
        self.state = ExtractState::Closed;
        let result = self.range.close();
        if result.is_err() || self.range.poisoned() {
            self.close_failed = true;
        }
        result
    }

    // True if entries that were already yielded as removed may remain in the
    // tree. The write transaction must be poisoned so they cannot be
    // committed. Errors surfaced by the iterator can lose removals too, so
    // this is not implied by an explicit close() returning an error.
    pub(crate) fn close_failed(&self) -> bool {
        self.close_failed
    }

    // Latches the error state: an end whose position was lost may otherwise
    // park with a bound that un-consumes entries it already yielded or
    // tested, letting the other end see them again. close() applies both
    // ends' pending work while their state is still coherent, and later
    // calls re-raise instead of continuing. The errors reaching here are
    // unrecoverable: the storage layer has latched the underlying failure.
    fn latch_error(&mut self) {
        let _ = self.close();
        self.state = ExtractState::Errored;
    }

    fn next_inner(&mut self) -> Result<Option<ExtractItem<'a, K, V>>> {
        loop {
            // The entry borrows the front cursor, so the predicate state and
            // closure are accessed as disjoint fields while it is live.
            let matched = {
                let Some(entry) = self.range.peek_next()? else {
                    break;
                };
                assert!(!self.predicate_running);
                self.predicate_running = true;
                let matched = (self.predicate)(entry.key(), entry.value());
                self.predicate_running = false;
                matched
            };
            if matched {
                let result = self
                    .range
                    .remove_next()?
                    .expect("peeked cursor entry must be removable");
                return Ok(Some(result));
            }
            self.range.next()?;
        }
        self.close()?;
        Ok(None)
    }

    fn next_back_inner(&mut self) -> Result<Option<ExtractItem<'a, K, V>>> {
        loop {
            let matched = {
                let Some(entry) = self.range.peek_prev()? else {
                    break;
                };
                assert!(!self.predicate_running);
                self.predicate_running = true;
                let matched = (self.predicate)(entry.key(), entry.value());
                self.predicate_running = false;
                matched
            };
            if matched {
                let result = self
                    .range
                    .remove_prev()?
                    .expect("peeked cursor entry must be removable");
                return Ok(Some(result));
            }
            self.range.prev()?;
        }
        self.close()?;
        Ok(None)
    }

    // One copy of the latch protocol, shared by both iteration ends so their
    // error behavior cannot diverge.
    fn advance(
        &mut self,
        inner: AdvanceInner<Self, ExtractItem<'a, K, V>>,
    ) -> Option<Result<ExtractItem<'a, K, V>>> {
        match self.state {
            ExtractState::Errored => return Some(Err(StorageError::PreviousIo)),
            ExtractState::Closed => return None,
            ExtractState::Running => {}
        }
        let result = inner(self);
        if result.is_err() {
            self.latch_error();
        }
        result.transpose()
    }
}

impl<'a, K: Key + 'static, V: Value + 'static, F> Iterator for BtreeExtractIf<'a, K, V, F>
where
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
{
    type Item = Result<ExtractItem<'a, K, V>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.advance(Self::next_inner)
    }
}

impl<K: Key + 'static, V: Value + 'static, F> DoubleEndedIterator for BtreeExtractIf<'_, K, V, F>
where
    F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.advance(Self::next_back_inner)
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
