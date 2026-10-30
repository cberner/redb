//! A write transaction's ownership of the redb tables opened through it.
//!
//! A redb table borrows its transaction, which Python's garbage collector cannot be made to
//! respect: a table object may outlive the `with` block, or be dropped after the transaction,
//! in any order. So the transaction owns its open tables, cached by name, and a Python table
//! object is only a name's binding to one. Each cached table keeps the transaction alive with
//! an `Arc`, which is what lets the one unsafe block extend the table's borrow, and the
//! transaction leaves its `Arc`, to commit or abort, only once every table is gone.
//!
//! The cache is generic over the transaction so that its tests run under Miri, which cannot
//! execute pyo3's calls into the Python interpreter; `just miri_py` runs them.

// Nothing uses the cache until the next change wires it into `WriteTransaction`
#![allow(dead_code)]

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A type whose values borrow a transaction, such as a redb table
pub(crate) trait Borrowing: 'static {
    type Of<'txn>: Send + Sync;
}

/// A table kept open across calls, with the transaction it borrows
pub(crate) struct Open<T> {
    table: Box<dyn Any + Send + Sync>,
    // Dropped after the table, which borrows it
    _txn: Arc<T>,
}

impl<T> Open<T> {
    /// The table, as the type it was opened as; None if it was opened as another
    pub(crate) fn downcast_mut<U: Any>(&mut self) -> Option<&mut U> {
        self.table.downcast_mut()
    }
}

/// A cached table, locked for the duration of each operation on it
pub(crate) type Cached<T> = Arc<Mutex<Open<T>>>;

/// A transaction and the tables open on it
pub(crate) struct TableCache<T> {
    // Dropped before `txn`, which they borrow
    tables: HashMap<String, Cached<T>>,
    txn: Arc<T>,
}

impl<T: Send + Sync + 'static> TableCache<T> {
    pub(crate) fn new(txn: T) -> Self {
        Self {
            tables: HashMap::new(),
            txn: Arc::new(txn),
        }
    }

    /// The table `name`, opened with `open` on first use.
    ///
    /// `open` borrows the transaction for a lifetime of its own, so the borrow can leave it only
    /// inside the table it returns, which the cache keeps alive no longer than the transaction.
    pub(crate) fn cached<B: Borrowing, E>(
        &mut self,
        name: &str,
        open: impl for<'txn> FnOnce(&'txn T) -> Result<B::Of<'txn>, E>,
    ) -> Result<Cached<T>, E> {
        if let Some(cached) = self.tables.get(name) {
            return Ok(cached.clone());
        }
        let txn = self.txn.clone();
        let table = Box::new(open(&txn)?);
        // SAFETY: only the table's lifetime parameter changes. The table borrows the allocation
        // behind `txn`, which the `Open` below holds for as long as the table and drops after
        // it, and the transaction leaves that allocation, by `into_inner`, only once every
        // `Open` is gone. `downcast_mut` is the one way at the table, for as long as the `Open`
        // is borrowed.
        let table: Box<B::Of<'static>> = unsafe { Box::from_raw(Box::into_raw(table).cast()) };
        let cached = Arc::new(Mutex::new(Open { table, _txn: txn }));
        self.tables.insert(name.to_string(), cached.clone());
        Ok(cached)
    }

    /// Closes every table
    pub(crate) fn close(&mut self) {
        self.tables.clear();
    }

    /// The transaction, once every table is closed and no operation still holds one
    pub(crate) fn into_inner(mut self) -> Result<T, Self> {
        self.close();
        Arc::try_unwrap(self.txn).map_err(|txn| Self {
            tables: HashMap::new(),
            txn,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Stands in for a transaction: like redb's, it is changed through a shared borrow
    struct Txn(AtomicUsize);

    /// Stands in for a table: borrows its transaction
    struct Table<'txn>(&'txn Txn);

    struct Live;

    impl Borrowing for Live {
        type Of<'txn> = Table<'txn>;
    }

    fn open(txn: &Txn) -> Result<Table<'_>, ()> {
        Ok(Table(txn))
    }

    /// An operation on a cached table: locks it, then reaches the transaction through it
    fn bump(cached: &Cached<Txn>) -> usize {
        let mut open = cached.lock().unwrap();
        let table = open.downcast_mut::<Table<'static>>().unwrap();
        table.0.0.fetch_add(1, Ordering::Relaxed) + 1
    }

    #[test]
    fn tables_and_their_cache_drop_in_any_order() {
        let mut cache = TableCache::new(Txn(AtomicUsize::new(0)));
        let a = cache.cached::<Live, _>("a", open).unwrap();
        assert!(Arc::ptr_eq(
            &a,
            &cache.cached::<Live, _>("a", open).unwrap()
        ));
        let b = cache.cached::<Live, _>("b", open).unwrap();
        assert_eq!(bump(&a), 1);
        assert_eq!(bump(&b), 2);
        drop(cache); // the tables keep the transaction alive
        assert_eq!(bump(&a), 3);
        drop(a);
        assert_eq!(bump(&b), 4);
        drop(b); // the last table goes, then the transaction
    }

    #[test]
    fn into_inner_refuses_while_a_table_is_held() {
        let mut cache = TableCache::new(Txn(AtomicUsize::new(0)));
        let a = cache.cached::<Live, _>("a", open).unwrap();
        let Err(cache) = cache.into_inner() else {
            panic!("a table is still held")
        };
        assert_eq!(bump(&a), 1);
        drop(a);
        let Ok(txn) = cache.into_inner() else {
            panic!("every table is gone")
        };
        assert_eq!(txn.0.into_inner(), 1);
    }

    #[test]
    fn a_table_in_use_on_another_thread() {
        let mut cache = TableCache::new(Txn(AtomicUsize::new(0)));
        let a = cache.cached::<Live, _>("a", open).unwrap();
        let txn = std::thread::scope(|scope| {
            let worker = scope.spawn(move || bump(&a));
            // Whether or not the worker has finished, the transaction stays put until `a` drops
            let cache = match cache.into_inner() {
                Ok(txn) => return txn,
                Err(cache) => cache,
            };
            assert_eq!(worker.join().unwrap(), 1);
            let Ok(txn) = cache.into_inner() else {
                panic!("the worker dropped its table")
            };
            txn
        });
        assert_eq!(txn.0.into_inner(), 1);
    }
}
