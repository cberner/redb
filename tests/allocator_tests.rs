use redb::{
    BuddyAllocatorFactory, Builder, Database, PageAllocator, PageAllocatorFactory,
    ReadableDatabase, ReadableTableMetadata, TableDefinition,
};
use std::sync::Arc;

const TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("test_data");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

/// A thin wrapper around BuddyAllocatorFactory that delegates all methods.
/// Used to prove that the pluggable allocator interface works end-to-end.
struct DelegatingAllocatorFactory {
    inner: BuddyAllocatorFactory,
}

impl DelegatingAllocatorFactory {
    fn new() -> Self {
        Self {
            inner: BuddyAllocatorFactory,
        }
    }
}

impl PageAllocatorFactory for DelegatingAllocatorFactory {
    fn create(&self, num_pages: u32, max_page_capacity: u32) -> Box<dyn PageAllocator> {
        self.inner.create(num_pages, max_page_capacity)
    }

    fn deserialize(&self, data: &[u8]) -> Box<dyn PageAllocator> {
        self.inner.deserialize(data)
    }
}

/// Baseline: creating a database without set_allocator_factory works normally.
#[test]
fn test_default_allocator_works() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        table.insert("key1", b"value1".as_slice()).unwrap();
        table.insert("key2", b"value2".as_slice()).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    assert_eq!(table.get("key1").unwrap().unwrap().value(), b"value1");
    assert_eq!(table.get("key2").unwrap().unwrap().value(), b"value2");
    assert_eq!(table.len().unwrap(), 2);
}

/// Proves the trait plumbing works: a delegating wrapper around BuddyAllocator,
/// set via Builder, produces a fully functional database.
#[test]
fn test_custom_allocator_factory() {
    let tmpfile = create_tempfile();
    let factory: Arc<dyn PageAllocatorFactory> = Arc::new(DelegatingAllocatorFactory::new());

    let db = Builder::new()
        .set_allocator_factory(factory)
        .create(tmpfile.path())
        .unwrap();

    // Insert
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        table.insert("alpha", b"one".as_slice()).unwrap();
        table.insert("beta", b"two".as_slice()).unwrap();
        table.insert("gamma", b"three".as_slice()).unwrap();
    }
    write_txn.commit().unwrap();

    // Read back
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    assert_eq!(table.get("alpha").unwrap().unwrap().value(), b"one");
    assert_eq!(table.get("beta").unwrap().unwrap().value(), b"two");
    assert_eq!(table.get("gamma").unwrap().unwrap().value(), b"three");
    assert_eq!(table.len().unwrap(), 3);
    drop(table);
    drop(read_txn);

    // Delete
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE).unwrap();
        table.remove("beta").unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE).unwrap();
    assert!(table.get("beta").unwrap().is_none());
    assert_eq!(table.len().unwrap(), 2);
}

/// Creates a database with a custom factory, closes it, reopens with the same factory,
/// and verifies data persists.
#[test]
fn test_allocator_factory_persists_across_reopen() {
    let tmpfile = create_tempfile();

    // Create and populate
    {
        let factory: Arc<dyn PageAllocatorFactory> =
            Arc::new(DelegatingAllocatorFactory::new());
        let db = Builder::new()
            .set_allocator_factory(factory)
            .create(tmpfile.path())
            .unwrap();

        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            table.insert("persist_key", b"persist_value".as_slice()).unwrap();
        }
        write_txn.commit().unwrap();
        // db dropped here, closing the database
    }

    // Reopen with the same factory type and verify data
    {
        let factory: Arc<dyn PageAllocatorFactory> =
            Arc::new(DelegatingAllocatorFactory::new());
        let db = Builder::new()
            .set_allocator_factory(factory)
            .open(tmpfile.path())
            .unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(TABLE).unwrap();
        assert_eq!(
            table.get("persist_key").unwrap().unwrap().value(),
            b"persist_value"
        );
    }
}

/// Creates a BuddyAllocator via the factory, allocates some pages, serializes
/// with to_vec(), deserializes with factory.deserialize(), and verifies the
/// state matches.
#[test]
fn test_allocator_serialization_roundtrip() {
    let factory = BuddyAllocatorFactory;

    // Create an allocator with 256 pages
    let mut allocator = factory.create(256, 256);

    // Allocate several pages at different orders
    let page_order0 = allocator.alloc(0).expect("should allocate order-0 page");
    let page_order1 = allocator.alloc(1).expect("should allocate order-1 page");
    let page_order2 = allocator.alloc(2).expect("should allocate order-2 page");

    let allocated_before = allocator.count_allocated_pages();
    let free_before = allocator.count_free_pages();
    let hash_before = allocator.xxh3_hash();

    // Serialize
    let serialized = allocator.to_vec();
    assert!(!serialized.is_empty());

    // Deserialize
    let restored = factory.deserialize(&serialized);

    // Verify state matches
    assert_eq!(restored.count_allocated_pages(), allocated_before);
    assert_eq!(restored.count_free_pages(), free_before);
    assert_eq!(restored.xxh3_hash(), hash_before);
    assert_eq!(restored.len(), 256);
    assert_eq!(restored.get_max_order(), allocator.get_max_order());

    // Verify the deserialized allocator is functional: free and re-allocate
    let mut restored_mut = factory.deserialize(&serialized);
    restored_mut.free(page_order0, 0);
    restored_mut.free(page_order1, 1);
    restored_mut.free(page_order2, 2);
    assert_eq!(restored_mut.count_allocated_pages(), 0);
}

/// Verifies BuddyAllocatorFactory creates working allocators directly.
#[test]
fn test_buddy_allocator_factory_default() {
    let factory = BuddyAllocatorFactory;

    // Create via factory
    let mut allocator = factory.create(128, 128);
    assert_eq!(allocator.len(), 128);
    assert_eq!(allocator.count_allocated_pages(), 0);
    assert_eq!(allocator.count_free_pages(), 128);
    assert!(!allocator.is_empty());

    // Allocate all order-0 pages
    for _ in 0..128 {
        allocator.alloc(0).expect("should have free pages");
    }
    assert_eq!(allocator.count_allocated_pages(), 128);
    assert_eq!(allocator.count_free_pages(), 0);
    assert!(allocator.alloc(0).is_none(), "should be exhausted");

    // highest_free_order should be None when fully allocated
    assert!(allocator.highest_free_order().is_none());
}

/// Verifies that creating an allocator with zero pages panics.
/// BuddyAllocator requires at least 1 page because calculate_usable_order
/// performs `32 - pages.leading_zeros() - 1` which underflows at 0.
#[test]
#[should_panic]
fn test_allocator_empty_panics() {
    let factory = BuddyAllocatorFactory;
    let _allocator = factory.create(0, 0);
}

/// Verifies that resize works through the trait interface.
#[test]
fn test_allocator_resize() {
    let factory = BuddyAllocatorFactory;
    let mut allocator = factory.create(64, 256);
    assert_eq!(allocator.len(), 64);
    assert_eq!(allocator.count_free_pages(), 64);

    // Grow
    allocator.resize(128);
    assert_eq!(allocator.len(), 128);
    assert_eq!(allocator.count_free_pages(), 128);

    // Allocate a page, then shrink (only after freeing pages beyond new size)
    let page = allocator.alloc(0).unwrap();
    allocator.free(page, 0);

    // Shrink back
    allocator.resize(64);
    assert_eq!(allocator.len(), 64);
    assert_eq!(allocator.count_free_pages(), 64);
}

/// Verifies record_alloc works through the trait interface.
#[test]
fn test_allocator_record_alloc() {
    let factory = BuddyAllocatorFactory;
    let mut allocator = factory.create(64, 64);

    // Record some allocations (simulating recovery)
    allocator.record_alloc(0, 0);
    allocator.record_alloc(1, 0);
    allocator.record_alloc(2, 0);

    assert_eq!(allocator.count_allocated_pages(), 3);
    assert_eq!(allocator.count_free_pages(), 61);

    // Free them back
    allocator.free(0, 0);
    allocator.free(1, 0);
    allocator.free(2, 0);
    assert_eq!(allocator.count_allocated_pages(), 0);
}

/// Verifies alloc_lowest returns the lowest available page index.
#[test]
fn test_allocator_alloc_lowest() {
    let factory = BuddyAllocatorFactory;
    let mut allocator = factory.create(64, 64);

    // alloc_lowest should return page 0 first
    let first = allocator.alloc_lowest(0).unwrap();
    assert_eq!(first, 0);

    // Next lowest should be page 1
    let second = allocator.alloc_lowest(0).unwrap();
    assert_eq!(second, 1);

    allocator.free(first, 0);
    allocator.free(second, 0);
}

/// Verifies serialization of an allocator with zero allocations roundtrips correctly.
#[test]
fn test_serialization_empty_allocator_roundtrip() {
    let factory = BuddyAllocatorFactory;
    let allocator = factory.create(128, 128);

    let hash_before = allocator.xxh3_hash();
    let serialized = allocator.to_vec();
    let restored = factory.deserialize(&serialized);

    assert_eq!(restored.xxh3_hash(), hash_before);
    assert_eq!(restored.count_allocated_pages(), 0);
    assert_eq!(restored.count_free_pages(), 128);
    assert_eq!(restored.len(), 128);
}

/// Verifies trailing_free_pages works through the trait interface.
#[test]
fn test_allocator_trailing_free_pages() {
    let factory = BuddyAllocatorFactory;
    let mut allocator = factory.create(64, 64);

    // All pages free: trailing free pages should equal total
    assert_eq!(allocator.trailing_free_pages(), 64);

    // Allocate page 0 — trailing free pages should still be 63
    // (since the allocated page is at the beginning, not the end)
    allocator.record_alloc(0, 0);
    assert_eq!(allocator.trailing_free_pages(), 63);
}
