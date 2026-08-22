use crate::tree_store::page_store::base::PageNumber;
use crate::tree_store::page_store::fast_hash::{PageNumberHashMap, Shrink};
use alloc::rc::Rc;
#[cfg(not(redb_no_std))]
use core::cell::RefCell;

/// The snapshot a cached page belongs to.
#[derive(Copy, Clone, Eq, PartialEq)]
pub(crate) struct CacheTag {
    /// Assigned by `TransactionalMemory`, to distinguish databases and reloads of one
    pub(crate) instance: u64,
    /// The read transaction's id
    pub(crate) generation: u64,
}

/// A thread's copies of the hottest btree pages, so that lookups in read transactions can skip
/// the shared page cache, whose lock stripe, LRU flag and `Arc` refcount are all writes to
/// memory other cores read. Being thread local, it needs no locks or atomics.
///
/// Every entry belongs to the snapshot named by `tag`, and reading a different one empties the
/// cache. That is what keeps entries from going stale: a transaction is given generation `G`
/// only while `G` is the last committed transaction, and pins it, and releasing a page for
/// reuse takes commits past `G` plus a `process_freed_pages()` that stops at the oldest live
/// read.
///
/// Only the level below the root is cached, which is small enough that a snapshot's pages
/// normally all fit. Once `max_pages` are held the cache stops admitting, rather than replacing
/// entries: admitting copies a page, so a level that does not fit would otherwise pay that copy
/// on nearly every lookup, which costs far more than the shared cache lookup it saves.
#[derive(Default)]
pub(crate) struct ThreadLocalPageCache {
    tag: Option<CacheTag>,
    max_pages: usize,
    pages: PageNumberHashMap<Rc<[u8]>>,
}

#[cfg(not(redb_no_std))]
thread_local! {
    static PAGE_CACHE: RefCell<ThreadLocalPageCache> = RefCell::default();
}

/// Runs `f` with this thread's page cache, or `None` when it is unavailable: a reentrant call,
/// or a thread being torn down. Lookups then run uncached rather than panicking.
#[cfg(not(redb_no_std))]
pub(crate) fn with_thread_local_page_cache<R>(
    f: impl FnOnce(Option<&mut ThreadLocalPageCache>) -> R,
) -> R {
    let mut f = Some(f);
    PAGE_CACHE
        .try_with(|cell| {
            let f = f.take().unwrap();
            match cell.try_borrow_mut() {
                Ok(mut cache) => f(Some(&mut cache)),
                Err(_) => f(None),
            }
        })
        .unwrap_or_else(|_| (f.take().unwrap())(None))
}

/// Always runs `f` without a cache: no_std has no thread local storage to keep one in.
#[cfg(redb_no_std)]
pub(crate) fn with_thread_local_page_cache<R>(
    f: impl FnOnce(Option<&mut ThreadLocalPageCache>) -> R,
) -> R {
    f(None)
}

/// Releases this thread's cached pages. A transaction dropped on a different thread than it
/// read from leaves that thread's pages until it caches another snapshot.
pub(crate) fn release_thread_local_page_cache() {
    with_thread_local_page_cache(|cache| {
        if let Some(cache) = cache {
            cache.clear();
        }
    });
}

impl ThreadLocalPageCache {
    pub(crate) fn get(&self, tag: CacheTag, page_number: PageNumber) -> Option<Rc<[u8]>> {
        if self.tag != Some(tag) {
            return None;
        }
        self.pages.get(&page_number).cloned()
    }

    /// Caches a copy of `bytes`, holding at most `max_pages` of them, and empties the cache
    /// first if it holds another snapshot's. Interleaving two snapshots on one thread therefore
    /// keeps refilling it: correct, but a page copy per lookup
    pub(crate) fn insert(
        &mut self,
        tag: CacheTag,
        max_pages: usize,
        page_number: PageNumber,
        bytes: &[u8],
    ) {
        // Caching a multi-page value would break this cache's memory bound
        assert_eq!(page_number.page_order, 0);
        if self.tag != Some(tag) {
            self.clear();
            self.tag = Some(tag);
            self.max_pages = max_pages;
        }
        if self.pages.len() < self.max_pages {
            self.pages.insert(page_number, Rc::from(bytes));
        }
    }

    pub(crate) fn clear(&mut self) {
        self.tag = None;
        self.max_pages = 0;
        self.pages.clear();
        self.pages.shrink();
    }
}

#[cfg(test)]
mod test {
    use super::{CacheTag, ThreadLocalPageCache};
    use crate::tree_store::page_store::base::PageNumber;

    const MAX_PAGES: usize = 128;

    fn page(index: u32) -> PageNumber {
        PageNumber::new(0, index, 0)
    }

    fn tag(instance: u64, generation: u64) -> CacheTag {
        CacheTag {
            instance,
            generation,
        }
    }

    #[test]
    fn hit_requires_matching_tag() {
        let mut cache = ThreadLocalPageCache::default();

        assert!(cache.get(tag(1, 7), page(1)).is_none());
        cache.insert(tag(1, 7), MAX_PAGES, page(1), &[1, 2, 3]);
        assert_eq!(cache.get(tag(1, 7), page(1)).unwrap().as_ref(), [1, 2, 3]);
        assert!(cache.get(tag(1, 7), page(2)).is_none());
        // Another generation of this database, and the same generation of another database
        assert!(cache.get(tag(1, 8), page(1)).is_none());
        assert!(cache.get(tag(2, 7), page(1)).is_none());
    }

    #[test]
    fn reading_another_snapshot_empties_the_cache() {
        let mut cache = ThreadLocalPageCache::default();
        cache.insert(tag(1, 7), MAX_PAGES, page(1), &[1]);
        cache.insert(tag(1, 7), MAX_PAGES, page(2), &[2]);

        cache.insert(tag(1, 8), MAX_PAGES, page(3), &[3]);
        assert_eq!(cache.pages.len(), 1);
        assert_eq!(cache.get(tag(1, 8), page(3)).unwrap().as_ref(), [3]);
        assert!(cache.get(tag(1, 8), page(1)).is_none());
    }

    #[test]
    fn bounded() {
        let mut cache = ThreadLocalPageCache::default();
        let count = u32::try_from(3 * MAX_PAGES).unwrap();
        for i in 0..count {
            cache.insert(tag(1, 1), MAX_PAGES, page(i), &i.to_le_bytes());
            assert!(cache.pages.len() <= MAX_PAGES);
        }

        let mut hits = 0;
        for i in 0..count {
            if let Some(bytes) = cache.get(tag(1, 1), page(i)) {
                assert_eq!(bytes.as_ref(), i.to_le_bytes());
                hits += 1;
            }
        }
        assert_eq!(hits, MAX_PAGES);
    }

    #[test]
    fn clear_releases_entries() {
        let mut cache = ThreadLocalPageCache::default();
        cache.insert(tag(1, 1), MAX_PAGES, page(1), &[1]);
        cache.clear();
        assert!(cache.pages.is_empty());
        assert!(cache.get(tag(1, 1), page(1)).is_none());
    }
}
