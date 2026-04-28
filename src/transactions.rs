use crate::db::TransactionGuard;
use crate::error::CommitError;
use crate::multimap_table::ReadOnlyUntypedMultimapTable;
use crate::sealed::Sealed;
use crate::table::ReadOnlyUntypedTable;
use crate::transaction_tracker::{SavepointId, TransactionId, TransactionTracker};
use crate::tree_store::{
    AllocationPolicy, Btree, BtreeHeader, BtreeMut, InternalTableDefinition, MAX_PAIR_LENGTH,
    MAX_VALUE_LENGTH, Page, PageAllocator, PageHint, PageListMut, PageNumber, PageResolver,
    PageTrackerPolicy, SerializedSavepoint, ShrinkPolicy, TableTree, TableTreeMut, TableType,
    TransactionalMemory,
};
use crate::types::{Key, Value};
use crate::{
    AccessGuard, AccessGuardMutInPlace, ExtractIf, MultimapTable, MultimapTableDefinition,
    MultimapTableHandle, MutInPlaceValue, Range, ReadOnlyMultimapTable, ReadOnlyTable, Result,
    Savepoint, SavepointError, SetDurabilityError, StorageError, Table, TableDefinition,
    TableError, TableHandle, TransactionError, TypeName, UntypedMultimapTableHandle,
    UntypedTableHandle,
};
#[cfg(feature = "logging")]
use log::{debug, warn};
use std::borrow::Borrow;
use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{RangeBounds, RangeFull};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{panic, thread};

const MAX_PAGES_PER_COMPACTION: usize = 1_000_000;
const NEXT_SAVEPOINT_TABLE: SystemTableDefinition<(), SavepointId> =
    SystemTableDefinition::new("next_savepoint_id");
pub(crate) const SAVEPOINT_TABLE: SystemTableDefinition<SavepointId, SerializedSavepoint> =
    SystemTableDefinition::new("persistent_savepoints");
// Pages that were allocated in the data tree by a given transaction. Only updated when a savepoint
// exists
pub(crate) const DATA_ALLOCATED_TABLE: SystemTableDefinition<
    TransactionIdWithPagination,
    PageList,
> = SystemTableDefinition::new("data_pages_allocated");
// Pages in the data tree that are in the pending free state: i.e., they are unreachable from the
// root as of the given transaction.
pub(crate) const DATA_FREED_TABLE: SystemTableDefinition<TransactionIdWithPagination, PageList> =
    SystemTableDefinition::new("data_pages_unreachable");
// Pages in the system tree that are in the pending free state: i.e., they are unreachable from the
// root as of the given transaction.
pub(crate) const SYSTEM_FREED_TABLE: SystemTableDefinition<TransactionIdWithPagination, PageList> =
    SystemTableDefinition::new("system_pages_unreachable");
// The allocator state table is stored in the system table tree, but it's accessed using
// raw btree operations rather than open_system_table(), so there's no SystemTableDefinition
pub(crate) const ALLOCATOR_STATE_TABLE_NAME: &str = "allocator_state";
pub(crate) type AllocatorStateTree = Btree<AllocatorStateKey, &'static [u8]>;
pub(crate) type AllocatorStateTreeMut = BtreeMut<AllocatorStateKey, &'static [u8]>;
pub(crate) type SystemFreedTree = BtreeMut<TransactionIdWithPagination, PageList<'static>>;

// Format:
// 2 bytes: length
// length * size_of(PageNumber): array of page numbers
#[derive(Debug)]
pub(crate) struct PageList<'a> {
    data: &'a [u8],
}

impl PageList<'_> {
    fn required_bytes(len: usize) -> usize {
        2 + PageNumber::serialized_size() * len
    }

    pub(crate) fn len(&self) -> usize {
        u16::from_le_bytes(self.data[..size_of::<u16>()].try_into().unwrap()).into()
    }

    pub(crate) fn get(&self, index: usize) -> PageNumber {
        let start = size_of::<u16>() + PageNumber::serialized_size() * index;
        PageNumber::from_le_bytes(
            self.data[start..(start + PageNumber::serialized_size())]
                .try_into()
                .unwrap(),
        )
    }
}

impl Value for PageList<'_> {
    type SelfType<'a>
        = PageList<'a>
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        PageList { data }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'b [u8]
    where
        Self: 'b,
    {
        value.data
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::PageList")
    }
}

impl MutInPlaceValue for PageList<'_> {
    type BaseRefType = PageListMut;

    fn initialize(data: &mut [u8]) {
        assert!(data.len() >= 8);
        // Set the length to zero
        data[..8].fill(0);
    }

    fn from_bytes_mut(data: &mut [u8]) -> &mut Self::BaseRefType {
        unsafe { &mut *(std::ptr::from_mut::<[u8]>(data) as *mut PageListMut) }
    }
}

#[derive(Debug)]
pub(crate) struct TransactionIdWithPagination {
    pub(crate) transaction_id: u64,
    pub(crate) pagination_id: u64,
}

impl Value for TransactionIdWithPagination {
    type SelfType<'a>
        = TransactionIdWithPagination
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; 2 * size_of::<u64>()]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(2 * size_of::<u64>())
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self
    where
        Self: 'a,
    {
        let transaction_id = u64::from_le_bytes(data[..size_of::<u64>()].try_into().unwrap());
        let pagination_id = u64::from_le_bytes(data[size_of::<u64>()..].try_into().unwrap());
        Self {
            transaction_id,
            pagination_id,
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> [u8; 2 * size_of::<u64>()]
    where
        Self: 'b,
    {
        let mut result = [0u8; 2 * size_of::<u64>()];
        result[..size_of::<u64>()].copy_from_slice(&value.transaction_id.to_le_bytes());
        result[size_of::<u64>()..].copy_from_slice(&value.pagination_id.to_le_bytes());
        result
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::TransactionIdWithPagination")
    }
}

impl Key for TransactionIdWithPagination {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        let value1 = Self::from_bytes(data1);
        let value2 = Self::from_bytes(data2);

        match value1.transaction_id.cmp(&value2.transaction_id) {
            std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
            std::cmp::Ordering::Equal => value1.pagination_id.cmp(&value2.pagination_id),
            std::cmp::Ordering::Less => std::cmp::Ordering::Less,
        }
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub(crate) enum AllocatorStateKey {
    Deprecated,
    Region(u32),
    RegionTracker,
    TransactionId,
}

impl Value for AllocatorStateKey {
    type SelfType<'a> = Self;
    type AsBytes<'a> = [u8; 1 + size_of::<u32>()];

    fn fixed_width() -> Option<usize> {
        Some(1 + size_of::<u32>())
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        match data[0] {
            // 0, 1, 2 were used in redb 2.x and have a different format
            0..=2 => Self::Deprecated,
            3 => Self::Region(u32::from_le_bytes(data[1..].try_into().unwrap())),
            4 => Self::RegionTracker,
            5 => Self::TransactionId,
            _ => unreachable!(),
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        let mut result = Self::AsBytes::default();
        match value {
            Self::Region(region) => {
                result[0] = 3;
                result[1..].copy_from_slice(&u32::to_le_bytes(*region));
            }
            Self::RegionTracker => {
                result[0] = 4;
            }
            Self::TransactionId => {
                result[0] = 5;
            }
            AllocatorStateKey::Deprecated => {
                result[0] = 0;
            }
        }

        result
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::AllocatorStateKey")
    }
}

impl Key for AllocatorStateKey {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
    }
}

pub struct SystemTableDefinition<'a, K: Key + 'static, V: Value + 'static> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: Key + 'static, V: Value + 'static> SystemTableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }
}

impl<K: Key + 'static, V: Value + 'static> TableHandle for SystemTableDefinition<'_, K, V> {
    fn name(&self) -> &str {
        self.name
    }
}

impl<K: Key, V: Value> Sealed for SystemTableDefinition<'_, K, V> {}

impl<K: Key + 'static, V: Value + 'static> Clone for SystemTableDefinition<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K: Key + 'static, V: Value + 'static> Copy for SystemTableDefinition<'_, K, V> {}

impl<K: Key + 'static, V: Value + 'static> Display for SystemTableDefinition<'_, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}<{}, {}>",
            self.name,
            K::type_name().name(),
            V::type_name().name()
        )
    }
}

/// Informational storage stats about the database
#[derive(Debug)]
pub struct DatabaseStats {
    pub(crate) tree_height: u32,
    pub(crate) allocated_pages: u64,
    pub(crate) leaf_pages: u64,
    pub(crate) branch_pages: u64,
    pub(crate) stored_leaf_bytes: u64,
    pub(crate) metadata_bytes: u64,
    pub(crate) fragmented_bytes: u64,
    pub(crate) page_size: usize,
}

impl DatabaseStats {
    /// Maximum traversal distance to reach the deepest (key, value) pair, across all tables
    pub fn tree_height(&self) -> u32 {
        self.tree_height
    }

    /// Number of pages allocated
    pub fn allocated_pages(&self) -> u64 {
        self.allocated_pages
    }

    /// Number of leaf pages that store user data
    pub fn leaf_pages(&self) -> u64 {
        self.leaf_pages
    }

    /// Number of branch pages in btrees that store user data
    pub fn branch_pages(&self) -> u64 {
        self.branch_pages
    }

    /// Number of bytes consumed by keys and values that have been inserted.
    /// Does not include indexing overhead
    pub fn stored_bytes(&self) -> u64 {
        self.stored_leaf_bytes
    }

    /// Number of bytes consumed by keys in internal branch pages, plus other metadata
    pub fn metadata_bytes(&self) -> u64 {
        self.metadata_bytes
    }

    /// Number of bytes consumed by fragmentation, both in data pages and internal metadata tables
    pub fn fragmented_bytes(&self) -> u64 {
        self.fragmented_bytes
    }

    /// Number of bytes per page
    pub fn page_size(&self) -> usize {
        self.page_size
    }
}

#[derive(Copy, Clone, Debug)]
#[non_exhaustive]
pub enum Durability {
    /// Commits with this durability level will not be persisted to disk unless followed by a
    /// commit with [`Durability::Immediate`].
    None,
    /// Commits with this durability level are guaranteed to be persistent as soon as
    /// [`WriteTransaction::commit`] returns.
    Immediate,
}

// These are the actual durability levels used internally. `Durability::Paranoid` is translated
// to `InternalDurability::Immediate`, and also enables 2-phase commit
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum InternalDurability {
    None,
    Immediate,
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum PostCommitFree {
    Enabled,
    Disabled,
}

// Like a Table but only one may be open at a time to avoid possible races
pub struct SystemTable<'s, K: Key + 'static, V: Value + 'static> {
    name: String,
    namespace: &'s mut SystemNamespace,
    tree: BtreeMut<K, V>,
    transaction_guard: Arc<TransactionGuard>,
}

impl<'s, K: Key + 'static, V: Value + 'static> SystemTable<'s, K, V> {
    fn new(
        name: &str,
        table_root: Option<BtreeHeader>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        guard: Arc<TransactionGuard>,
        page_allocator: PageAllocator,
        namespace: &'s mut SystemNamespace,
    ) -> SystemTable<'s, K, V> {
        // No need to track allocations in the system tree. Savepoint restoration only relies on
        // freeing in the data tree
        let ignore = Arc::new(Mutex::new(PageTrackerPolicy::Ignore));
        SystemTable {
            name: name.to_string(),
            namespace,
            tree: BtreeMut::new(
                table_root,
                guard.clone(),
                page_allocator,
                freed_pages,
                ignore,
            ),
            transaction_guard: guard,
        }
    }

    fn get<'a>(&self, key: impl Borrow<K::SelfType<'a>>) -> Result<Option<AccessGuard<'_, V>>>
    where
        K: 'a,
    {
        self.tree.get(key.borrow())
    }

    fn range<'a, KR>(&self, range: impl RangeBounds<KR> + 'a) -> Result<Range<'_, K, V>>
    where
        K: 'a,
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree
            .range(&range)
            .map(|x| Range::new(x, self.transaction_guard.clone()))
    }

    pub fn extract_from_if<'a, KR, F: for<'f> FnMut(K::SelfType<'f>, V::SelfType<'f>) -> bool>(
        &mut self,
        range: impl RangeBounds<KR> + 'a,
        predicate: F,
    ) -> Result<ExtractIf<'_, K, V, F>>
    where
        KR: Borrow<K::SelfType<'a>> + 'a,
    {
        self.tree
            .extract_from_if(&range, predicate)
            .map(ExtractIf::new)
    }

    pub fn insert<'k, 'v>(
        &mut self,
        key: impl Borrow<K::SelfType<'k>>,
        value: impl Borrow<V::SelfType<'v>>,
    ) -> Result<Option<AccessGuard<'_, V>>> {
        let value_len = V::as_bytes(value.borrow()).as_ref().len();
        if value_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len));
        }
        let key_len = K::as_bytes(key.borrow()).as_ref().len();
        if key_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(key_len));
        }
        if value_len + key_len > MAX_PAIR_LENGTH {
            return Err(StorageError::ValueTooLarge(value_len + key_len));
        }
        self.tree.insert(key.borrow(), value.borrow())
    }

    pub fn remove<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
    ) -> Result<Option<AccessGuard<'_, V>>>
    where
        K: 'a,
    {
        self.tree.remove(key.borrow())
    }
}

impl<K: Key + 'static, V: MutInPlaceValue + 'static> SystemTable<'_, K, V> {
    pub fn insert_reserve<'a>(
        &mut self,
        key: impl Borrow<K::SelfType<'a>>,
        value_length: usize,
    ) -> Result<AccessGuardMutInPlace<'_, V>> {
        if value_length > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(value_length));
        }
        let key_len = K::as_bytes(key.borrow()).as_ref().len();
        if key_len > MAX_VALUE_LENGTH {
            return Err(StorageError::ValueTooLarge(key_len));
        }
        if value_length + key_len > MAX_PAIR_LENGTH {
            return Err(StorageError::ValueTooLarge(value_length + key_len));
        }
        self.tree.insert_reserve(key.borrow(), value_length)
    }
}

impl<K: Key + 'static, V: Value + 'static> Drop for SystemTable<'_, K, V> {
    fn drop(&mut self) {
        self.namespace.close_table(
            &self.name,
            &self.tree,
            self.tree.get_root().map(|x| x.length).unwrap_or_default(),
        );
    }
}

struct SystemNamespace {
    table_tree: TableTreeMut,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    transaction_guard: Arc<TransactionGuard>,
}

impl SystemNamespace {
    fn new(
        root_page: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        page_allocator: PageAllocator,
    ) -> Self {
        // No need to track allocations in the system tree. Savepoint restoration only relies on
        // freeing in the data tree
        let ignore = Arc::new(Mutex::new(PageTrackerPolicy::Ignore));
        let freed_pages = Arc::new(Mutex::new(vec![]));
        Self {
            table_tree: TableTreeMut::new(
                root_page,
                guard.clone(),
                page_allocator,
                freed_pages.clone(),
                ignore,
            ),
            freed_pages,
            transaction_guard: guard.clone(),
        }
    }

    fn system_freed_pages(&self) -> Arc<Mutex<Vec<PageNumber>>> {
        self.freed_pages.clone()
    }

    fn open_system_table<'s, K: Key + 'static, V: Value + 'static>(
        &'s mut self,
        transaction: &WriteTransaction,
        definition: SystemTableDefinition<K, V>,
    ) -> Result<SystemTable<'s, K, V>> {
        let (root, _) = self
            .table_tree
            .get_or_create_table::<K, V>(definition.name(), TableType::Normal)
            .map_err(|e| {
                e.into_storage_error_or_corrupted("Internal error. System table is corrupted")
            })?;
        transaction.dirty.store(true, Ordering::Release);

        let page_allocator = self.table_tree.page_allocator().clone();
        Ok(SystemTable::new(
            definition.name(),
            root,
            self.freed_pages.clone(),
            self.transaction_guard.clone(),
            page_allocator,
            self,
        ))
    }

    fn get_system_table_root<K: Key + 'static, V: Value + 'static>(
        &self,
        definition: SystemTableDefinition<K, V>,
    ) -> Result<Option<BtreeHeader>> {
        let table = self
            .table_tree
            .get_table::<K, V>(definition.name(), TableType::Normal)
            .map_err(|e| {
                e.into_storage_error_or_corrupted("Internal error. System table is corrupted")
            })?;
        Ok(table.and_then(|definition| match definition {
            InternalTableDefinition::Normal { table_root, .. } => table_root,
            InternalTableDefinition::Multimap { .. } => unreachable!(),
        }))
    }

    fn close_table<K: Key + 'static, V: Value + 'static>(
        &mut self,
        name: &str,
        table: &BtreeMut<K, V>,
        length: u64,
    ) {
        self.table_tree
            .stage_update_table_root(name, table.get_root(), length);
    }
}

struct TableNamespace {
    open_tables: HashMap<String, &'static panic::Location<'static>>,
    allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    table_tree: TableTreeMut,
}

impl TableNamespace {
    fn new(
        root_page: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        page_allocator: PageAllocator,
    ) -> Self {
        let allocated = Arc::new(Mutex::new(PageTrackerPolicy::new_tracking()));
        let freed_pages = Arc::new(Mutex::new(vec![]));
        let table_tree = TableTreeMut::new(
            root_page,
            guard,
            page_allocator,
            // Committed pages which are no longer reachable and will be queued for free'ing
            // These are separated from the system freed pages
            freed_pages.clone(),
            allocated.clone(),
        );
        Self {
            open_tables: HashMap::default(),
            table_tree,
            freed_pages,
            allocated_pages: allocated,
        }
    }

    fn set_dirty(&mut self, transaction: &WriteTransaction) {
        transaction.dirty.store(true, Ordering::Release);
        if !transaction.transaction_tracker.any_savepoint_exists() {
            // No savepoints exist, and we don't allow savepoints to be created in a dirty transaction
            // so we can disable allocation tracking now
            *self.allocated_pages.lock().unwrap() = PageTrackerPolicy::Ignore;
        }
    }

    fn set_root(&mut self, root: Option<BtreeHeader>) {
        assert!(self.open_tables.is_empty());
        self.table_tree.set_root(root);
    }

    #[track_caller]
    fn inner_open<K: Key + 'static, V: Value + 'static>(
        &mut self,
        name: &str,
        table_type: TableType,
    ) -> Result<(Option<BtreeHeader>, u64), TableError> {
        if let Some(location) = self.open_tables.get(name) {
            return Err(TableError::TableAlreadyOpen(name.to_string(), location));
        }

        let root = self
            .table_tree
            .get_or_create_table::<K, V>(name, table_type)?;
        self.open_tables
            .insert(name.to_string(), panic::Location::caller());

        Ok(root)
    }

    #[track_caller]
    pub fn open_multimap_table<'txn, K: Key + 'static, V: Key + 'static>(
        &mut self,
        transaction: &'txn WriteTransaction,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<MultimapTable<'txn, K, V>, TableError> {
        #[cfg(feature = "logging")]
        debug!("Opening multimap table: {definition}");
        let (root, length) = self.inner_open::<K, V>(definition.name(), TableType::Multimap)?;
        self.set_dirty(transaction);

        Ok(MultimapTable::new(
            definition.name(),
            root,
            length,
            self.freed_pages.clone(),
            self.allocated_pages.clone(),
            self.table_tree.page_allocator().clone(),
            transaction,
        ))
    }

    #[track_caller]
    pub fn open_table<'txn, K: Key + 'static, V: Value + 'static>(
        &mut self,
        transaction: &'txn WriteTransaction,
        definition: TableDefinition<K, V>,
    ) -> Result<Table<'txn, K, V>, TableError> {
        #[cfg(feature = "logging")]
        debug!("Opening table: {definition}");
        let (root, _) = self.inner_open::<K, V>(definition.name(), TableType::Normal)?;
        self.set_dirty(transaction);

        Ok(Table::new(
            definition.name(),
            root,
            self.freed_pages.clone(),
            self.allocated_pages.clone(),
            self.table_tree.page_allocator().clone(),
            transaction,
        ))
    }

    #[track_caller]
    fn inner_rename(
        &mut self,
        name: &str,
        new_name: &str,
        table_type: TableType,
    ) -> Result<(), TableError> {
        if let Some(location) = self.open_tables.get(name) {
            return Err(TableError::TableAlreadyOpen(name.to_string(), location));
        }

        self.table_tree.rename_table(name, new_name, table_type)
    }

    #[track_caller]
    fn rename_table(
        &mut self,
        transaction: &WriteTransaction,
        name: &str,
        new_name: &str,
    ) -> Result<(), TableError> {
        #[cfg(feature = "logging")]
        debug!("Renaming table: {name} to {new_name}");
        self.set_dirty(transaction);
        self.inner_rename(name, new_name, TableType::Normal)
    }

    #[track_caller]
    fn rename_multimap_table(
        &mut self,
        transaction: &WriteTransaction,
        name: &str,
        new_name: &str,
    ) -> Result<(), TableError> {
        #[cfg(feature = "logging")]
        debug!("Renaming multimap table: {name} to {new_name}");
        self.set_dirty(transaction);
        self.inner_rename(name, new_name, TableType::Multimap)
    }

    #[track_caller]
    fn inner_delete(&mut self, name: &str, table_type: TableType) -> Result<bool, TableError> {
        if let Some(location) = self.open_tables.get(name) {
            return Err(TableError::TableAlreadyOpen(name.to_string(), location));
        }

        self.table_tree.delete_table(name, table_type)
    }

    #[track_caller]
    fn delete_table(
        &mut self,
        transaction: &WriteTransaction,
        name: &str,
    ) -> Result<bool, TableError> {
        #[cfg(feature = "logging")]
        debug!("Deleting table: {name}");
        self.set_dirty(transaction);
        self.inner_delete(name, TableType::Normal)
    }

    #[track_caller]
    fn delete_multimap_table(
        &mut self,
        transaction: &WriteTransaction,
        name: &str,
    ) -> Result<bool, TableError> {
        #[cfg(feature = "logging")]
        debug!("Deleting multimap table: {name}");
        self.set_dirty(transaction);
        self.inner_delete(name, TableType::Multimap)
    }

    pub(crate) fn close_table<K: Key + 'static, V: Value + 'static>(
        &mut self,
        name: &str,
        table: &BtreeMut<K, V>,
        length: u64,
    ) {
        self.open_tables.remove(name).unwrap();
        self.table_tree
            .stage_update_table_root(name, table.get_root(), length);
    }

    pub(crate) fn close_table_without_update(&mut self, name: &str) {
        self.open_tables.remove(name).unwrap();
    }
}

// Transaction-local savepoint lifecycle state.
#[derive(Default)]
struct SavepointTransactionState {
    created_persistent: HashSet<(SavepointId, TransactionId)>,
    deleted_persistent: Vec<(SavepointId, TransactionId)>,
    invalidated: BTreeSet<SavepointId>,
}

impl SavepointTransactionState {
    fn record_created(&mut self, id: SavepointId, transaction_id: TransactionId) {
        self.created_persistent.insert((id, transaction_id));
    }

    fn record_deleted(&mut self, id: SavepointId, transaction_id: TransactionId) {
        self.deleted_persistent.push((id, transaction_id));
    }

    fn record_invalidated(&mut self, ids: impl IntoIterator<Item = SavepointId>) {
        self.invalidated.extend(ids);
    }

    fn is_invalidated(&self, id: SavepointId) -> bool {
        self.invalidated.contains(&id)
    }

    fn has_created_or_deleted(&self) -> bool {
        !self.created_persistent.is_empty() || !self.deleted_persistent.is_empty()
    }

    fn apply_on_commit(&mut self, tracker: &TransactionTracker) {
        // Persistent savepoints whose on-disk entry was deleted: release their
        // tracker refcount now that the deletion is durable.
        for (savepoint, transaction) in self.deleted_persistent.drain(..) {
            tracker.deallocate_savepoint(savepoint, transaction);
        }
        // Savepoints that restore_savepoint() invalidated: remove them from the
        // shared valid_savepoints map. For persistent savepoints,
        // deallocate_savepoint above has already removed them; for ephemeral,
        // the user's Savepoint handle still owns the live_read_transactions
        // refcount and will release it on drop.
        tracker.invalidate_savepoints(std::mem::take(&mut self.invalidated));
        // Persistent savepoints created during this transaction stay live:
        // drop them from our bookkeeping without releasing tracker state.
        self.created_persistent.clear();
    }

    fn apply_on_abort(&mut self, tracker: &TransactionTracker) {
        // Persistent savepoints created during this transaction: their
        // on-disk entries will be rolled back by rollback_uncommitted_writes(),
        // but the shared tracker registration must be released explicitly.
        for (savepoint, transaction) in self.created_persistent.drain() {
            tracker.deallocate_savepoint(savepoint, transaction);
        }
        // Deleted-persistent entries will be rolled back on disk, so the
        // tracker state must NOT be released (it is still valid).
        self.deleted_persistent.clear();
        // Invalidations were only staged in this struct and never touched the
        // shared tracker, so dropping them is sufficient.
        self.invalidated.clear();
    }
}

/// A read/write transaction
///
/// Only a single [`WriteTransaction`] may exist at a time
pub struct WriteTransaction {
    transaction_tracker: Arc<TransactionTracker>,
    mem: Arc<TransactionalMemory>,
    transaction_guard: Arc<TransactionGuard>,
    transaction_id: TransactionId,
    tables: Mutex<TableNamespace>,
    system_tables: Mutex<SystemNamespace>,
    completed: bool,
    dirty: AtomicBool,
    poisoned: AtomicBool,
    durability: InternalDurability,
    two_phase_commit: bool,
    shrink_policy: ShrinkPolicy,
    quick_repair: bool,
    post_commit_free: PostCommitFree,
    // All transaction-local savepoint lifecycle state. See
    // `SavepointTransactionState` for the commit/abort contract.
    savepoint_state: Mutex<SavepointTransactionState>,
}

impl WriteTransaction {
    pub(crate) fn new(
        guard: TransactionGuard,
        transaction_tracker: Arc<TransactionTracker>,
        mem: Arc<TransactionalMemory>,
        allocation_policy: AllocationPolicy,
    ) -> Result<Self> {
        let transaction_id = guard.id();
        let guard = Arc::new(guard);

        let root_page = mem.get_data_root();
        let system_page = mem.get_system_root();

        let page_allocator = PageAllocator::new(mem.clone(), allocation_policy);
        let tables = TableNamespace::new(root_page, guard.clone(), page_allocator.clone());
        let system_tables = SystemNamespace::new(system_page, guard.clone(), page_allocator);

        Ok(Self {
            transaction_tracker,
            mem: mem.clone(),
            transaction_guard: guard.clone(),
            transaction_id,
            tables: Mutex::new(tables),
            system_tables: Mutex::new(system_tables),
            completed: false,
            dirty: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            durability: InternalDurability::Immediate,
            two_phase_commit: false,
            quick_repair: false,
            post_commit_free: PostCommitFree::Enabled,
            shrink_policy: ShrinkPolicy::Default,
            savepoint_state: Mutex::new(SavepointTransactionState::default()),
        })
    }

    pub(crate) fn set_shrink_policy(&mut self, shrink_policy: ShrinkPolicy) {
        self.shrink_policy = shrink_policy;
    }

    pub(crate) fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
    }

    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    // A PageAllocator for this transaction. All clones share the same
    // allocated-since-commit set so they agree on which pages this
    // transaction has allocated.
    fn page_allocator(&self) -> PageAllocator {
        self.tables
            .lock()
            .unwrap()
            .table_tree
            .page_allocator()
            .clone()
    }

    fn read_existing_system_table<K: Key + 'static, V: Value + 'static, T>(
        &self,
        definition: SystemTableDefinition<K, V>,
        read: impl FnOnce(&Btree<K, V>) -> Result<T>,
    ) -> Result<Option<T>> {
        let system_tables = self.system_tables.lock().unwrap();
        let Some(root) = system_tables.get_system_table_root(definition)? else {
            return Ok(None);
        };
        let table = Btree::new(
            Some(root),
            PageHint::None,
            self.transaction_guard.clone(),
            PageResolver::new(self.mem.clone()),
        )?;
        read(&table).map(Some)
    }

    pub(crate) fn pending_free_pages(&self) -> Result<bool> {
        let system_tables = self.system_tables.lock().unwrap();
        Ok(system_tables
            .get_system_table_root(DATA_FREED_TABLE)?
            .is_some()
            || system_tables
                .get_system_table_root(SYSTEM_FREED_TABLE)?
                .is_some())
    }

    #[cfg(debug_assertions)]
    pub fn print_allocated_page_debug(&self) {
        let mut all_allocated: HashSet<PageNumber> =
            HashSet::from_iter(self.mem.all_allocated_pages());

        self.mem.debug_check_allocator_consistency();

        let mut table_pages = vec![];
        self.tables
            .lock()
            .unwrap()
            .table_tree
            .visit_all_pages(|path| {
                table_pages.push(path.page_number());
                Ok(())
            })
            .unwrap();
        println!("Tables");
        for p in table_pages {
            assert!(all_allocated.remove(&p));
            println!("{p:?}");
        }

        let mut system_table_pages = vec![];
        self.system_tables
            .lock()
            .unwrap()
            .table_tree
            .visit_all_pages(|path| {
                system_table_pages.push(path.page_number());
                Ok(())
            })
            .unwrap();
        println!("System tables");
        for p in system_table_pages {
            assert!(all_allocated.remove(&p));
            println!("{p:?}");
        }

        {
            println!("Pending free (in data freed table)");
            let mut system_tables = self.system_tables.lock().unwrap();
            let data_freed = system_tables
                .open_system_table(self, DATA_FREED_TABLE)
                .unwrap();
            for entry in data_freed.range::<TransactionIdWithPagination>(..).unwrap() {
                let (_, entry) = entry.unwrap();
                let value = entry.value();
                for i in 0..value.len() {
                    let p = value.get(i);
                    assert!(all_allocated.remove(&p));
                    println!("{p:?}");
                }
            }
        }
        {
            println!("Pending free (in system freed table)");
            let mut system_tables = self.system_tables.lock().unwrap();
            let system_freed = system_tables
                .open_system_table(self, SYSTEM_FREED_TABLE)
                .unwrap();
            for entry in system_freed
                .range::<TransactionIdWithPagination>(..)
                .unwrap()
            {
                let (_, entry) = entry.unwrap();
                let value = entry.value();
                for i in 0..value.len() {
                    let p = value.get(i);
                    assert!(all_allocated.remove(&p));
                    println!("{p:?}");
                }
            }
        }
        {
            let tables = self.tables.lock().unwrap();
            let pages = tables.freed_pages.lock().unwrap();
            if !pages.is_empty() {
                println!("Pages in in-memory data freed_pages");
                for p in pages.iter() {
                    println!("{p:?}");
                    assert!(all_allocated.remove(p));
                }
            }
        }
        {
            let system_tables = self.system_tables.lock().unwrap();
            let pages = system_tables.freed_pages.lock().unwrap();
            if !pages.is_empty() {
                println!("Pages in in-memory system freed_pages");
                for p in pages.iter() {
                    println!("{p:?}");
                    assert!(all_allocated.remove(p));
                }
            }
        }
        if !all_allocated.is_empty() {
            println!("Leaked pages");
            for p in all_allocated {
                println!("{p:?}");
            }
        }
    }

    /// Creates a snapshot of the current database state, which can be used to rollback the database.
    /// This savepoint will exist until it is deleted with `[delete_savepoint()]`.
    ///
    /// Note that while a savepoint exists, pages that become unused after it was created are not freed.
    /// Therefore, the lifetime of a savepoint should be minimized.
    ///
    /// Returns `[SavepointError::InvalidSavepoint`], if the transaction is "dirty" (any tables have been opened),
    /// or `[SavepointError::ImmediateDurabilityRequired]` if the transaction's durability is less than
    /// `[Durability::Immediate]`
    pub fn persistent_savepoint(&self) -> Result<u64, SavepointError> {
        if self.durability != InternalDurability::Immediate {
            return Err(SavepointError::ImmediateDurabilityRequired);
        }

        let mut savepoint = self.ephemeral_savepoint()?;

        let mut system_tables = self.system_tables.lock().unwrap();

        let mut next_table = system_tables.open_system_table(self, NEXT_SAVEPOINT_TABLE)?;
        next_table.insert((), savepoint.get_id().next())?;
        drop(next_table);

        let mut savepoint_table = system_tables.open_system_table(self, SAVEPOINT_TABLE)?;
        savepoint_table.insert(
            savepoint.get_id(),
            SerializedSavepoint::from_savepoint(&savepoint),
        )?;

        savepoint.set_persistent();

        self.savepoint_state
            .lock()
            .unwrap()
            .record_created(savepoint.get_id(), savepoint.get_transaction_id());

        Ok(savepoint.get_id().0)
    }

    pub(crate) fn transaction_guard(&self) -> Arc<TransactionGuard> {
        self.transaction_guard.clone()
    }

    pub(crate) fn next_persistent_savepoint_id(&self) -> Result<Option<SavepointId>> {
        let Some(value) = self.read_existing_system_table(NEXT_SAVEPOINT_TABLE, |next_table| {
            let value = next_table.get(&())?;
            Ok(value.map(|next_id| next_id.value()))
        })?
        else {
            return Ok(None);
        };
        Ok(value)
    }

    /// Get a persistent savepoint given its id
    pub fn get_persistent_savepoint(&self, id: u64) -> Result<Savepoint, SavepointError> {
        let Some(value) = self.read_existing_system_table(SAVEPOINT_TABLE, |table| {
            let value = table.get(&SavepointId(id))?;
            Ok(value.map(|x| x.value().to_savepoint(self.transaction_tracker.clone())))
        })?
        else {
            return Err(SavepointError::InvalidSavepoint);
        };
        value.ok_or(SavepointError::InvalidSavepoint)
    }

    /// Delete the given persistent savepoint.
    ///
    /// Note that if the transaction is `abort()`'ed this deletion will be rolled back.
    ///
    /// Returns `true` if the savepoint existed
    /// Returns `[SavepointError::ImmediateDurabilityRequired]` if the transaction's durability
    /// is less than `[Durability::Immediate]`
    pub fn delete_persistent_savepoint(&self, id: u64) -> Result<bool, SavepointError> {
        if self.durability != InternalDurability::Immediate {
            return Err(SavepointError::ImmediateDurabilityRequired);
        }
        let mut system_tables = self.system_tables.lock().unwrap();
        if system_tables
            .get_system_table_root(SAVEPOINT_TABLE)?
            .is_none()
        {
            return Ok(false);
        }
        let mut table = system_tables.open_system_table(self, SAVEPOINT_TABLE)?;
        let savepoint = table.remove(SavepointId(id))?;
        if let Some(serialized) = savepoint {
            let savepoint = serialized
                .value()
                .to_savepoint(self.transaction_tracker.clone());
            self.savepoint_state
                .lock()
                .unwrap()
                .record_deleted(savepoint.get_id(), savepoint.get_transaction_id());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List all persistent savepoints
    pub fn list_persistent_savepoints(&self) -> Result<impl Iterator<Item = u64>> {
        let Some(savepoints) = self.read_existing_system_table(SAVEPOINT_TABLE, |table| {
            let mut savepoints = vec![];
            for savepoint in table.range::<RangeFull, SavepointId>(&..)? {
                savepoints.push(savepoint?.key().0);
            }
            Ok(savepoints)
        })?
        else {
            return Ok(vec![].into_iter());
        };
        Ok(savepoints.into_iter())
    }

    fn allocate_savepoint(&self) -> Result<(SavepointId, TransactionId)> {
        let transaction_id = self
            .transaction_tracker
            .register_read_transaction(&self.mem)?;
        let id = self.transaction_tracker.allocate_savepoint(transaction_id);
        Ok((id, transaction_id))
    }

    /// Creates a snapshot of the current database state, which can be used to rollback the database
    ///
    /// This savepoint will be freed as soon as the returned `[Savepoint]` is dropped.
    ///
    /// Returns `[SavepointError::InvalidSavepoint`], if the transaction is "dirty" (any tables have been opened)
    pub fn ephemeral_savepoint(&self) -> Result<Savepoint, SavepointError> {
        if self.dirty.load(Ordering::Acquire) {
            return Err(SavepointError::InvalidSavepoint);
        }

        let (id, transaction_id) = self.allocate_savepoint()?;
        #[cfg(feature = "logging")]
        debug!("Creating savepoint id={id:?}, txn_id={transaction_id:?}");

        let root = self.mem.get_data_root();
        let savepoint = Savepoint::new_ephemeral(
            &self.mem,
            self.transaction_tracker.clone(),
            id,
            transaction_id,
            root,
        );

        Ok(savepoint)
    }

    /// Restore the state of the database to the given [`Savepoint`]
    ///
    /// Calling this method invalidates all [`Savepoint`]s created after savepoint
    pub fn restore_savepoint(&mut self, savepoint: &Savepoint) -> Result<(), SavepointError> {
        // Reject a Savepoint that is from a different Database
        if std::ptr::from_ref(self.transaction_tracker.as_ref()) != savepoint.db_address() {
            return Err(SavepointError::InvalidSavepoint);
        }

        if !self
            .transaction_tracker
            .is_valid_savepoint(savepoint.get_id())
            || self
                .savepoint_state
                .lock()
                .unwrap()
                .is_invalidated(savepoint.get_id())
        {
            return Err(SavepointError::InvalidSavepoint);
        }

        if self.durability != InternalDurability::Immediate
            && self
                .list_persistent_savepoints()?
                .any(|id| id > savepoint.get_id().0)
        {
            return Err(SavepointError::ImmediateDurabilityRequired);
        }
        #[cfg(feature = "logging")]
        debug!(
            "Beginning savepoint restore (id={:?}) in transaction id={:?}",
            savepoint.get_id(),
            self.transaction_id
        );
        // Restoring a savepoint that reverted a file format or checksum type change could corrupt
        // the database
        assert_eq!(self.mem.get_version(), savepoint.get_version());
        self.dirty.store(true, Ordering::Release);

        // Restoring a savepoint needs to accomplish the following:
        // 1) restore the table tree. This is trivial, since we have the old root
        // 1a) we also filter the freed tree to remove any pages referenced by the old root
        // 2) free all pages that were allocated since the savepoint and are unreachable
        //    from the restored table tree root. Here we diff the reachable pages from the old
        //    and new roots
        // 3) update the system tree to remove invalid persistent savepoints.

        // 1) restore the table tree
        {
            self.tables
                .lock()
                .unwrap()
                .set_root(savepoint.get_user_root());
        }

        // 1a) purge all transactions that happened after the savepoint from the data freed tree
        let txn_id = savepoint.get_transaction_id().next().raw_id();
        {
            let lower = TransactionIdWithPagination {
                transaction_id: txn_id,
                pagination_id: 0,
            };
            let mut system_tables = self.system_tables.lock().unwrap();
            let mut data_freed = system_tables.open_system_table(self, DATA_FREED_TABLE)?;
            for entry in data_freed.extract_from_if(lower.., |_, _| true)? {
                entry?;
            }
            // No need to process the system freed table, because it only rolls forward
        }

        // 2) queue all pages that became unreachable
        {
            let tables = self.tables.lock().unwrap();
            let page_allocator = tables.table_tree.page_allocator();
            for page in tables.allocated_pages.lock().unwrap().reset() {
                debug_assert!(page_allocator.uncommitted(page));
                debug_assert!(self.mem.is_allocated(page));
                page_allocator.free(page, &mut PageTrackerPolicy::Ignore);
            }
            let mut data_freed_pages = tables.freed_pages.lock().unwrap();
            data_freed_pages.clear();
            let mut system_tables = self.system_tables.lock().unwrap();
            let data_allocated = system_tables.open_system_table(self, DATA_ALLOCATED_TABLE)?;
            let lower = TransactionIdWithPagination {
                transaction_id: txn_id,
                pagination_id: 0,
            };
            for entry in data_allocated.range(lower..)? {
                let (_, value) = entry?;
                for i in 0..value.value().len() {
                    data_freed_pages.push(value.value().get(i));
                }
            }
            // Also queue unpersisted allocations from non-durable commits after the savepoint.
            // These are tracked in memory rather than in DATA_ALLOCATED_TABLE. We don't remove
            // them from the map here: if this transaction aborts, the in-memory map must still
            // reflect the full history. durable_commit() will empty the map.
            for page in self
                .mem
                .unpersisted_allocations_after(savepoint.get_transaction_id())
            {
                data_freed_pages.push(page);
            }
        }

        // 3) Mark all savepoints newer than the restored one as invalidated for this
        // transaction, to prevent the user from later trying to restore a savepoint
        // "on another timeline". The invalidation is purely per-transaction state -
        // the shared `valid_savepoints` map is only updated if/when commit_inner()
        // runs, so an abort implicitly reverts the invalidation by dropping this set.
        let invalidated = self
            .transaction_tracker
            .list_savepoints_after(savepoint.get_id());
        self.savepoint_state
            .lock()
            .unwrap()
            .record_invalidated(invalidated);
        for persistent_savepoint in self.list_persistent_savepoints()? {
            if persistent_savepoint > savepoint.get_id().0 {
                self.delete_persistent_savepoint(persistent_savepoint)?;
            }
        }

        Ok(())
    }

    /// Set the desired durability level for writes made in this transaction
    /// Defaults to [`Durability::Immediate`]
    ///
    /// If a persistent savepoint has been created or deleted, in this transaction, the durability may not
    /// be reduced below [`Durability::Immediate`]
    pub fn set_durability(&mut self, durability: Durability) -> Result<(), SetDurabilityError> {
        let persistent_modified = self
            .savepoint_state
            .lock()
            .unwrap()
            .has_created_or_deleted();
        if persistent_modified && !matches!(durability, Durability::Immediate) {
            return Err(SetDurabilityError::PersistentSavepointModified);
        }

        self.durability = match durability {
            Durability::None => InternalDurability::None,
            Durability::Immediate => InternalDurability::Immediate,
        };

        Ok(())
    }

    /// Enable or disable 2-phase commit (defaults to disabled)
    ///
    /// By default, data is written using the following 1-phase commit algorithm:
    ///
    /// 1. Update the inactive commit slot with the new database state
    /// 2. Flip the god byte primary bit to activate the newly updated commit slot
    /// 3. Call `fsync` to ensure all writes have been persisted to disk
    ///
    /// All data is written with checksums. When opening the database after a crash, the most
    /// recent of the two commit slots with a valid checksum is used.
    ///
    /// Security considerations: The checksum used is xxhash, a fast, non-cryptographic hash
    /// function with close to perfect collision resistance when used with non-malicious input. An
    /// attacker with an extremely high degree of control over the database's workload, including
    /// the ability to cause the database process to crash, can cause invalid data to be written
    /// with a valid checksum, leaving the database in an invalid, attacker-controlled state.
    ///
    /// Alternatively, you can enable 2-phase commit, which writes data like this:
    ///
    /// 1. Update the inactive commit slot with the new database state
    /// 2. Call `fsync` to ensure the database slate and commit slot update have been persisted
    /// 3. Flip the god byte primary bit to activate the newly updated commit slot
    /// 4. Call `fsync` to ensure the write to the god byte has been persisted
    ///
    /// This mitigates a theoretical attack where an attacker who
    /// 1. can control the order in which pages are flushed to disk
    /// 2. can introduce crashes during `fsync`,
    /// 3. has knowledge of the database file contents, and
    /// 4. can include arbitrary data in a write transaction
    ///
    /// could cause a transaction to partially commit (some but not all of the data is written).
    /// This is described in the design doc in futher detail.
    ///
    /// Security considerations: Many hard disk drives and SSDs do not actually guarantee that data
    /// has been persisted to disk after calling `fsync`. Even with 2-phase commit, an attacker with
    /// a high degree of control over the database's workload, including the ability to cause the
    /// database process to crash, can cause the database to crash with the god byte primary bit
    /// pointing to an invalid commit slot, leaving the database in an invalid, potentially attacker-
    /// controlled state.
    pub fn set_two_phase_commit(&mut self, enabled: bool) {
        self.two_phase_commit = enabled;
    }

    /// Enable or disable quick-repair (defaults to disabled)
    ///
    /// By default, when reopening the database after a crash, redb needs to do a full repair.
    /// This involves walking the entire database to verify the checksums and reconstruct the
    /// allocator state, so it can be very slow if the database is large.
    ///
    /// Alternatively, you can enable quick-repair. In this mode, redb saves the allocator state
    /// as part of each commit (so it doesn't need to be reconstructed), and enables 2-phase commit
    /// (which guarantees that the primary commit slot is valid without needing to look at the
    /// checksums). This means commits are slower, but recovery after a crash is almost instant.
    pub fn set_quick_repair(&mut self, enabled: bool) {
        self.quick_repair = enabled;
    }

    pub(crate) fn disable_post_commit_free(&mut self) {
        self.post_commit_free = PostCommitFree::Disabled;
    }

    /// Open the given table
    ///
    /// The table will be created if it does not exist
    #[track_caller]
    pub fn open_table<'txn, K: Key + 'static, V: Value + 'static>(
        &'txn self,
        definition: TableDefinition<K, V>,
    ) -> Result<Table<'txn, K, V>, TableError> {
        self.tables.lock().unwrap().open_table(self, definition)
    }

    /// Open the given table
    ///
    /// The table will be created if it does not exist
    #[track_caller]
    pub fn open_multimap_table<'txn, K: Key + 'static, V: Key + 'static>(
        &'txn self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<MultimapTable<'txn, K, V>, TableError> {
        self.tables
            .lock()
            .unwrap()
            .open_multimap_table(self, definition)
    }

    pub(crate) fn close_table<K: Key + 'static, V: Value + 'static>(
        &self,
        name: &str,
        table: &BtreeMut<K, V>,
        length: u64,
    ) {
        let mut tables = self.tables.lock().unwrap();
        if self.is_poisoned() {
            tables.close_table_without_update(name);
        } else {
            tables.close_table(name, table, length);
        }
    }

    /// Rename the given table
    pub fn rename_table(
        &self,
        definition: impl TableHandle,
        new_name: impl TableHandle,
    ) -> Result<(), TableError> {
        let name = definition.name().to_string();
        // Drop the definition so that callers can pass in a `Table` to rename, without getting a TableAlreadyOpen error
        drop(definition);
        self.tables
            .lock()
            .unwrap()
            .rename_table(self, &name, new_name.name())
    }

    /// Rename the given multimap table
    pub fn rename_multimap_table(
        &self,
        definition: impl MultimapTableHandle,
        new_name: impl MultimapTableHandle,
    ) -> Result<(), TableError> {
        let name = definition.name().to_string();
        // Drop the definition so that callers can pass in a `MultimapTable` to rename, without getting a TableAlreadyOpen error
        drop(definition);
        self.tables
            .lock()
            .unwrap()
            .rename_multimap_table(self, &name, new_name.name())
    }

    /// Delete the given table
    ///
    /// Returns a bool indicating whether the table existed
    pub fn delete_table(&self, definition: impl TableHandle) -> Result<bool, TableError> {
        let name = definition.name().to_string();
        // Drop the definition so that callers can pass in a `Table` or `MultimapTable` to delete, without getting a TableAlreadyOpen error
        drop(definition);
        self.tables.lock().unwrap().delete_table(self, &name)
    }

    /// Delete the given table
    ///
    /// Returns a bool indicating whether the table existed
    pub fn delete_multimap_table(
        &self,
        definition: impl MultimapTableHandle,
    ) -> Result<bool, TableError> {
        let name = definition.name().to_string();
        // Drop the definition so that callers can pass in a `Table` or `MultimapTable` to delete, without getting a TableAlreadyOpen error
        drop(definition);
        self.tables
            .lock()
            .unwrap()
            .delete_multimap_table(self, &name)
    }

    /// List all the tables
    pub fn list_tables(&self) -> Result<impl Iterator<Item = UntypedTableHandle> + '_> {
        self.tables
            .lock()
            .unwrap()
            .table_tree
            .list_tables(TableType::Normal)
            .map(|x| x.into_iter().map(UntypedTableHandle::new))
    }

    /// List all the multimap tables
    pub fn list_multimap_tables(
        &self,
    ) -> Result<impl Iterator<Item = UntypedMultimapTableHandle> + '_> {
        self.tables
            .lock()
            .unwrap()
            .table_tree
            .list_tables(TableType::Multimap)
            .map(|x| x.into_iter().map(UntypedMultimapTableHandle::new))
    }

    /// Commit the transaction
    ///
    /// All writes performed in this transaction will be visible to future transactions, and are
    /// durable as consistent with the [`Durability`] level set by [`Self::set_durability`]
    ///
    /// Returns [`CommitError::TransactionPoisoned`] if a previous operation panicked and left the
    /// transaction unable to commit.
    pub fn commit(mut self) -> Result<(), CommitError> {
        // Set completed flag first, so that we don't go through the abort() path on drop, if this fails
        self.completed = true;
        if self.is_poisoned() {
            self.abort_inner()?;
            return Err(CommitError::TransactionPoisoned);
        }
        self.commit_inner()
    }

    fn commit_inner(&mut self) -> Result<(), CommitError> {
        // Quick-repair requires 2-phase commit
        if self.quick_repair {
            self.two_phase_commit = true;
        }

        let (user_root, allocated_pages, data_freed) =
            self.tables.lock().unwrap().table_tree.flush_and_close()?;

        let stored_data_freed_pages = self.store_data_freed_pages(data_freed)?;

        #[cfg(feature = "logging")]
        debug!(
            "Committing transaction id={:?} with durability={:?} two_phase={} quick_repair={}",
            self.transaction_id, self.durability, self.two_phase_commit, self.quick_repair
        );
        let allocated_pages: Vec<PageNumber> = allocated_pages.into_iter().collect();
        match self.durability {
            InternalDurability::None => {
                self.non_durable_commit(user_root, allocated_pages, stored_data_freed_pages)?;
                self.apply_savepoint_state_on_commit();
            }
            InternalDurability::Immediate => self.durable_commit(user_root, allocated_pages)?,
        }

        assert!(
            self.system_tables
                .lock()
                .unwrap()
                .system_freed_pages()
                .lock()
                .unwrap()
                .is_empty()
        );
        assert!(
            self.tables
                .lock()
                .unwrap()
                .freed_pages
                .lock()
                .unwrap()
                .is_empty()
        );

        #[cfg(feature = "logging")]
        debug!(
            "Finished commit of transaction id={:?}",
            self.transaction_id
        );

        Ok(())
    }

    fn apply_savepoint_state_on_commit(&self) {
        self.savepoint_state
            .lock()
            .unwrap()
            .apply_on_commit(&self.transaction_tracker);
    }

    fn store_data_freed_pages(&self, mut freed_pages: Vec<PageNumber>) -> Result<bool> {
        let stored_pages = !freed_pages.is_empty();
        let mut system_tables = self.system_tables.lock().unwrap();
        let mut freed_table = system_tables.open_system_table(self, DATA_FREED_TABLE)?;
        let mut pagination_counter = 0;
        #[cfg(debug_assertions)]
        let page_allocator = self.page_allocator();
        while !freed_pages.is_empty() {
            let chunk_size = 400;
            let buffer_size = PageList::required_bytes(chunk_size);
            let key = TransactionIdWithPagination {
                transaction_id: self.transaction_id.raw_id(),
                pagination_id: pagination_counter,
            };
            let mut access_guard = freed_table.insert_reserve(&key, buffer_size)?;

            let len = freed_pages.len();
            access_guard.as_mut().clear();
            for page in freed_pages.drain(len - min(len, chunk_size)..) {
                // Make sure that the page is currently allocated
                debug_assert!(
                    self.mem.is_allocated(page),
                    "Page is not allocated: {page:?}"
                );
                #[cfg(debug_assertions)]
                debug_assert!(
                    !page_allocator.uncommitted(page),
                    "Page is uncommitted: {page:?}"
                );
                access_guard.as_mut().push_back(page);
            }

            pagination_counter += 1;
        }

        Ok(stored_pages)
    }

    // Flushes this transaction's data-tree allocations to DATA_ALLOCATED_TABLE, along with any
    // previously unpersisted allocations that are now becoming durable. Must be called AFTER
    // `process_freed_pages` so that pages reclaimed during this commit are already dropped from
    // the in-memory `unpersisted_allocations` map and not written to disk as stale records.
    fn flush_data_allocated_pages(&self, data_allocated_pages: Vec<PageNumber>) -> Result {
        // Catch scenarios like a page getting allocated and then deallocated within the same
        // transaction, but errantly left in the allocated pages list.
        #[cfg(debug_assertions)]
        {
            let page_allocator = self.page_allocator();
            for page in &data_allocated_pages {
                debug_assert!(
                    self.mem.is_allocated(*page),
                    "Page is not allocated: {page:?}"
                );
                debug_assert!(
                    page_allocator.uncommitted(*page),
                    "Page is committed: {page:?}"
                );
            }
        }

        let unpersisted = self.mem.take_unpersisted_allocations();
        let mut system_tables = self.system_tables.lock().unwrap();
        let mut allocated_table = system_tables.open_system_table(self, DATA_ALLOCATED_TABLE)?;
        for (txn_id, pages) in unpersisted {
            Self::write_allocated_pages_entry(
                &mut allocated_table,
                txn_id,
                pages.into_iter().collect(),
            )?;
        }
        Self::write_allocated_pages_entry(
            &mut allocated_table,
            self.transaction_id,
            data_allocated_pages,
        )?;

        // Purge any transactions that are no longer referenced
        let oldest = self
            .transaction_tracker
            .oldest_savepoint()
            .map_or(u64::MAX, |(_, x)| x.raw_id());
        let key = TransactionIdWithPagination {
            transaction_id: oldest,
            pagination_id: 0,
        };
        for entry in allocated_table.extract_from_if(..key, |_, _| true)? {
            entry?;
        }

        Ok(())
    }

    fn write_allocated_pages_entry(
        allocated_table: &mut SystemTable<'_, TransactionIdWithPagination, PageList<'static>>,
        transaction_id: TransactionId,
        mut pages: Vec<PageNumber>,
    ) -> Result {
        let mut pagination_counter = 0;
        while !pages.is_empty() {
            let chunk_size = 400;
            let buffer_size = PageList::required_bytes(chunk_size);
            let key = TransactionIdWithPagination {
                transaction_id: transaction_id.raw_id(),
                pagination_id: pagination_counter,
            };
            let mut access_guard = allocated_table.insert_reserve(&key, buffer_size)?;

            let len = pages.len();
            access_guard.as_mut().clear();
            for page in pages.drain(len - min(len, chunk_size)..) {
                access_guard.as_mut().push_back(page);
            }

            pagination_counter += 1;
        }
        Ok(())
    }

    /// Abort the transaction
    ///
    /// All writes performed in this transaction will be rolled back
    pub fn abort(mut self) -> Result {
        // Set completed flag first, so that we don't go through the abort() path on drop, if this fails
        self.completed = true;
        self.abort_inner()
    }

    fn abort_inner(&mut self) -> Result {
        #[cfg(feature = "logging")]
        debug!("Aborting transaction id={:?}", self.transaction_id);
        self.tables
            .lock()
            .unwrap()
            .table_tree
            .clear_root_updates_and_close();
        // Release all transaction-local savepoint state. The on-disk mutations
        // (SAVEPOINT_TABLE / NEXT_SAVEPOINT_TABLE) are reverted by the
        // `PageAllocator::rollback_all` call below; `apply_on_abort` handles
        // the in-memory `TransactionTracker` state that rollback cannot see.
        self.savepoint_state
            .lock()
            .unwrap()
            .apply_on_abort(&self.transaction_tracker);
        self.mem.check_io_errors()?;
        self.page_allocator().rollback_all();
        #[cfg(feature = "logging")]
        debug!("Finished abort of transaction id={:?}", self.transaction_id);
        Ok(())
    }

    pub(crate) fn durable_commit(
        &mut self,
        user_root: Option<BtreeHeader>,
        allocated_pages: Vec<PageNumber>,
    ) -> Result {
        let free_until_transaction = self
            .transaction_tracker
            .oldest_live_read_transaction()
            .map_or(self.transaction_id, |x| x.next());
        self.process_freed_pages(free_until_transaction)?;
        // Flush allocated pages (including previously unpersisted allocations that are now
        // becoming durable) AFTER process_freed_pages, so that any pages reclaimed here have
        // already been dropped from the in-memory `unpersisted_allocations` map.
        self.flush_data_allocated_pages(allocated_pages)?;

        let mut system_tables = self.system_tables.lock().unwrap();
        let system_freed_pages = system_tables.system_freed_pages();
        let system_root = {
            let system_tree = system_tables.table_tree.flush_table_root_updates()?;
            system_tree
                .delete_table(ALLOCATOR_STATE_TABLE_NAME, TableType::Normal)
                .map_err(|e| e.into_storage_error_or_corrupted("Unexpected TableError"))?;

            if self.quick_repair {
                system_tree.create_table_and_flush_table_root(
                    ALLOCATOR_STATE_TABLE_NAME,
                    |system_tree_ref, tree: &mut AllocatorStateTreeMut| {
                        loop {
                            let num_regions = self
                                .mem
                                .reserve_allocator_state(tree, self.transaction_id)?;

                            // The allocator snapshot must match the committed system root. Pages
                            // freed while building that root stay allocated in the snapshot and are
                            // recorded in SYSTEM_FREED_TABLE before the allocator state is saved.
                            self.store_system_freed_pages(
                                system_tree_ref,
                                self.transaction_id,
                                system_freed_pages.clone(),
                                None,
                            )?;

                            if self.mem.try_save_allocator_state(tree, num_regions)? {
                                return Ok(());
                            }

                            // Clear out the table before retrying, just in case the number of regions
                            // has somehow shrunk. Don't use retain_in() for this, since it doesn't
                            // free the pages immediately -- we need to reuse those pages to guarantee
                            // that our retry loop will eventually terminate
                            while let Some(guards) = tree.last()? {
                                let key = guards.0.value();
                                drop(guards);
                                tree.remove(&key)?;
                            }
                        }
                    },
                )?;
            }

            system_tree.finalize_dirty_checksums()?
        };

        let page_allocator = self.page_allocator();
        self.mem.commit(
            user_root,
            system_root,
            self.transaction_id,
            self.two_phase_commit,
            self.shrink_policy,
        )?;
        // All of this transaction's allocations are durable; discard the per-txn tracker.
        let _ = page_allocator.take_allocated_since_commit();

        // Mark any pending non-durable commits as fully committed.
        self.transaction_tracker.clear_pending_non_durable_commits();

        // Immediately free the pages that were freed from the system-tree. These are only
        // accessed by write transactions, so it's safe to free them as soon as the commit is done.
        for page in system_freed_pages.lock().unwrap().drain(..) {
            page_allocator.free(page, &mut PageTrackerPolicy::Ignore);
        }

        drop(system_tables);

        self.apply_savepoint_state_on_commit();

        if self.post_commit_free == PostCommitFree::Enabled {
            self.process_data_freed_pages_after_commit(user_root, &page_allocator)?;
        }

        Ok(())
    }

    fn process_data_freed_pages_after_commit(
        &self,
        user_root: Option<BtreeHeader>,
        page_allocator: &PageAllocator,
    ) -> Result {
        let epilogue_transaction = self.transaction_id.next();
        let free_until = self
            .transaction_tracker
            .oldest_live_read_transaction()
            .map_or(epilogue_transaction, |x| x.next());

        let mut freed_any = false;
        let (system_root, stored_system_freed_pages, extracted_data_transactions) = {
            let mut system_tables = self.system_tables.lock().unwrap();
            let system_freed_pages = system_tables.system_freed_pages();
            let extracted_data_transactions = self.extract_freed_pages(
                &mut system_tables,
                DATA_FREED_TABLE,
                free_until,
                |page| {
                    freed_any = true;
                    // See process_freed_pages(): free_until excludes pages that are still needed
                    // by a live reader or pending non-durable commit.
                    debug_assert!(!self.mem.unpersisted(page));
                    page_allocator.free(page, &mut PageTrackerPolicy::Ignore);
                },
            )?;
            if !freed_any {
                return Ok(());
            }

            let system_tree = system_tables.table_tree.flush_table_root_updates()?;
            let stored_system_freed_pages = self.store_system_freed_pages(
                system_tree,
                epilogue_transaction,
                system_freed_pages,
                None,
            )?;
            (
                system_tree.finalize_dirty_checksums()?,
                stored_system_freed_pages,
                extracted_data_transactions,
            )
        };

        let epilogue_allocations = page_allocator.take_allocated_since_commit();
        self.mem.non_durable_commit(
            user_root,
            system_root,
            epilogue_transaction,
            epilogue_allocations,
        )?;
        self.transaction_tracker
            .reserve_transaction_id(epilogue_transaction, self.transaction_id);
        self.transaction_tracker.register_non_durable_commit(
            epilogue_transaction,
            self.transaction_id,
            stored_system_freed_pages,
        );
        // The epilogue only extracts DATA_FREED_TABLE entries. It is still correct to clear these
        // ids from the non-durable scan set: ordinary non-durable commits filter unpersisted
        // system pages before writing SYSTEM_FREED_TABLE, and durable commits process any
        // remaining persisted system entries without consulting this tracker.
        self.transaction_tracker
            .mark_non_durable_freed_pages_processed(extracted_data_transactions);

        Ok(())
    }

    // Commit without a durability guarantee
    pub(crate) fn non_durable_commit(
        &mut self,
        user_root: Option<BtreeHeader>,
        allocated_pages: Vec<PageNumber>,
        stored_data_freed_pages: bool,
    ) -> Result {
        let free_until_transaction = self
            .transaction_tracker
            .oldest_live_read_nondurable_transaction()
            .map_or(self.transaction_id, |x| x.next());
        self.process_freed_pages_nondurable(free_until_transaction)?;

        let mut post_commit_frees = vec![];

        let (system_root, stored_system_freed_pages) = {
            let mut system_tables = self.system_tables.lock().unwrap();
            let system_freed_pages = system_tables.system_freed_pages();
            system_tables.table_tree.flush_table_root_updates()?;
            for page in system_freed_pages
                .lock()
                .unwrap()
                .extract_if(.., |p| self.mem.unpersisted(*p))
            {
                post_commit_frees.push(page);
            }
            // Store all freed pages for a future commit(), since we can't free pages during a
            // non-durable commit (it's non-durable, so could be rolled back anytime in the future)
            let stored_system_freed_pages = self.store_system_freed_pages(
                &mut system_tables.table_tree,
                self.transaction_id,
                system_freed_pages,
                Some(&mut post_commit_frees),
            )?;

            let system_root = system_tables
                .table_tree
                .flush_table_root_updates()?
                .finalize_dirty_checksums()?;
            (system_root, stored_system_freed_pages)
        };

        let newly_unpersisted = self.page_allocator().take_allocated_since_commit();
        self.mem.non_durable_commit(
            user_root,
            system_root,
            self.transaction_id,
            newly_unpersisted,
        )?;
        // Record the data-tree pages allocated in this transaction in the in-memory map.
        self.mem
            .record_unpersisted_allocations(self.transaction_id, allocated_pages);
        let stored_freed_pages = stored_data_freed_pages || stored_system_freed_pages;
        // Register this as a non-durable transaction to ensure that freed pages are only processed
        // after this transaction has been persisted.
        self.transaction_tracker.register_non_durable_commit(
            self.transaction_id,
            self.mem.get_last_durable_transaction_id()?,
            stored_freed_pages,
        );

        for page in post_commit_frees {
            let removed = self
                .mem
                .free_if_unpersisted(page, &mut PageTrackerPolicy::Ignore);
            assert!(removed);
        }

        Ok(())
    }

    // Relocate pages to lower number regions/pages
    // Returns true if a page(s) was moved
    pub(crate) fn compact_pages(&mut self) -> Result<bool> {
        let mut progress = false;

        // Find the 1M highest pages
        let mut highest_pages = BTreeMap::new();
        let mut tables = self.tables.lock().unwrap();
        let table_tree = &mut tables.table_tree;
        table_tree.highest_index_pages(MAX_PAGES_PER_COMPACTION, &mut highest_pages)?;
        let mut system_tables = self.system_tables.lock().unwrap();
        let system_table_tree = &mut system_tables.table_tree;
        system_table_tree.highest_index_pages(MAX_PAGES_PER_COMPACTION, &mut highest_pages)?;

        let page_allocator = table_tree.page_allocator().clone();

        // Calculate how many of them can be relocated to lower pages, starting from the last page
        let mut relocation_map = HashMap::new();
        for path in highest_pages.into_values().rev() {
            if relocation_map.contains_key(&path.page_number()) {
                continue;
            }
            let old_page = page_allocator.get_page(path.page_number(), PageHint::None)?;
            let mut new_page = page_allocator
                .allocate_lowest(old_page.memory().len(), &mut PageTrackerPolicy::Ignore)?;
            let new_page_number = new_page.get_page_number();
            // We have to copy at least the page type into the new page.
            // Otherwise its cache priority will be calculated incorrectly
            new_page.memory_mut()[0] = old_page.memory()[0];
            drop(new_page);
            // We're able to move this to a lower page, so insert it and rewrite all its parents
            if new_page_number < path.page_number() {
                relocation_map.insert(path.page_number(), new_page_number);
                for parent in path.parents() {
                    if relocation_map.contains_key(parent) {
                        continue;
                    }
                    let old_parent = page_allocator.get_page(*parent, PageHint::None)?;
                    let mut new_page = page_allocator.allocate_lowest(
                        old_parent.memory().len(),
                        &mut PageTrackerPolicy::Ignore,
                    )?;
                    let new_page_number = new_page.get_page_number();
                    // We have to copy at least the page type into the new page.
                    // Otherwise its cache priority will be calculated incorrectly
                    new_page.memory_mut()[0] = old_parent.memory()[0];
                    drop(new_page);
                    relocation_map.insert(*parent, new_page_number);
                }
            } else {
                page_allocator.free(new_page_number, &mut PageTrackerPolicy::Ignore);
                break;
            }
        }

        if !relocation_map.is_empty() {
            progress = true;
        }

        table_tree.relocate_tables(&relocation_map)?;
        system_table_tree.relocate_tables(&relocation_map)?;

        Ok(progress)
    }

    // NOTE: must be called before store_system_freed_pages() during commit, since this can create
    // more pages freed by the current transaction
    fn process_freed_pages(&mut self, free_until: TransactionId) -> Result {
        // We assume below that PageNumber is length 8
        assert_eq!(PageNumber::serialized_size(), 8);

        let page_allocator = self.page_allocator();
        let mut free_page = |page| {
            // These pages cannot be unpersisted: free_until is bounded by
            // oldest_live_read_transaction, which pins back to the durable_ancestor of every
            // pending non-durable commit (see register_non_durable_commit). As a result, no entry
            // whose pages are still unpersisted is eligible for processing here.
            debug_assert!(!self.mem.unpersisted(page));
            page_allocator.free(page, &mut PageTrackerPolicy::Ignore);
        };

        let extracted_transactions = {
            let mut system_tables = self.system_tables.lock().unwrap();
            let mut extracted_transactions = self.extract_freed_pages(
                &mut system_tables,
                DATA_FREED_TABLE,
                free_until,
                &mut free_page,
            )?;
            extracted_transactions.extend(self.extract_freed_pages(
                &mut system_tables,
                SYSTEM_FREED_TABLE,
                free_until,
                &mut free_page,
            )?);
            extracted_transactions
        };
        self.transaction_tracker
            .mark_non_durable_freed_pages_processed(extracted_transactions);

        Ok(())
    }

    fn extract_freed_pages(
        &self,
        system_tables: &mut SystemNamespace,
        definition: SystemTableDefinition<TransactionIdWithPagination, PageList>,
        free_until: TransactionId,
        mut process_page: impl FnMut(PageNumber),
    ) -> Result<Vec<TransactionId>> {
        if system_tables.get_system_table_root(definition)?.is_none() {
            return Ok(vec![]);
        }

        let mut freed = system_tables.open_system_table(self, definition)?;
        let key = TransactionIdWithPagination {
            transaction_id: free_until.raw_id(),
            pagination_id: 0,
        };
        let mut extracted_transactions = vec![];
        for entry in freed.extract_from_if(..key, |_, _| true)? {
            let (key, page_list) = entry?;
            let transaction_id = TransactionId::new(key.value().transaction_id);
            if extracted_transactions.last().copied() != Some(transaction_id) {
                extracted_transactions.push(transaction_id);
            }
            let page_list = page_list.value();
            for i in 0..page_list.len() {
                process_page(page_list.get(i));
            }
        }

        Ok(extracted_transactions)
    }

    fn process_freed_pages_nondurable_helper(
        &mut self,
        free_until: TransactionId,
        definition: SystemTableDefinition<TransactionIdWithPagination, PageList>,
    ) -> Result<Vec<TransactionId>> {
        let mut processed = vec![];
        let mut system_tables = self.system_tables.lock().unwrap();

        let last_key = TransactionIdWithPagination {
            transaction_id: free_until.raw_id(),
            pagination_id: 0,
        };
        let oldest_unprocessed = self
            .transaction_tracker
            .oldest_unprocessed_non_durable_commit()
            .map_or(free_until.raw_id(), |x| x.raw_id());
        let first_key = TransactionIdWithPagination {
            transaction_id: oldest_unprocessed,
            pagination_id: 0,
        };
        let mut data_freed = system_tables.open_system_table(self, definition)?;

        let mut candidate_transactions = vec![];
        for entry in data_freed.range(first_key..last_key)? {
            let (key, _) = entry?;
            let transaction_id = TransactionId::new(key.value().transaction_id);
            if self
                .transaction_tracker
                .is_unprocessed_non_durable_commit(transaction_id)
                && candidate_transactions.last().copied() != Some(transaction_id)
            {
                candidate_transactions.push(transaction_id);
            }
        }
        for transaction_id in candidate_transactions {
            let mut key = TransactionIdWithPagination {
                transaction_id: transaction_id.raw_id(),
                pagination_id: 0,
            };
            loop {
                let Some(entry) = data_freed.get(&key)? else {
                    break;
                };
                let pages = entry.value();
                let mut new_pages = vec![];
                for i in 0..pages.len() {
                    let page = pages.get(i);
                    if !self
                        .mem
                        .free_if_unpersisted(page, &mut PageTrackerPolicy::Ignore)
                    {
                        new_pages.push(page);
                    }
                }
                if new_pages.len() != pages.len() {
                    drop(entry);
                    if new_pages.is_empty() {
                        data_freed.remove(&key)?;
                    } else {
                        let required = PageList::required_bytes(new_pages.len());
                        let mut page_list_mut = data_freed.insert_reserve(&key, required)?;
                        for page in new_pages {
                            page_list_mut.as_mut().push_back(page);
                        }
                    }
                }
                key.pagination_id += 1;
            }
            processed.push(transaction_id);
        }

        Ok(processed)
    }

    // NOTE: must be called before store_system_freed_pages() during commit, since this can create
    // more pages freed by the current transaction
    //
    // This method only frees pages that are unpersisted, in non-durable transactions, since
    // it is called from a non-durable commit() and therefore can't modify anything that the
    // on-disk state in the last durable transaction might reference.
    fn process_freed_pages_nondurable(&mut self, free_until: TransactionId) -> Result {
        // We assume below that PageNumber is length 8
        assert_eq!(PageNumber::serialized_size(), 8);

        // Handle the data freed tree
        let mut processed =
            self.process_freed_pages_nondurable_helper(free_until, DATA_FREED_TABLE)?;

        // Handle the system freed tree
        processed
            .extend(self.process_freed_pages_nondurable_helper(free_until, SYSTEM_FREED_TABLE)?);

        for transaction_id in processed {
            self.transaction_tracker
                .mark_non_durable_freed_pages_processed([transaction_id]);
        }

        Ok(())
    }

    fn store_system_freed_pages(
        &self,
        system_tree: &mut TableTreeMut,
        transaction_id: TransactionId,
        system_freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        mut unpersisted_pages: Option<&mut Vec<PageNumber>>,
    ) -> Result<bool> {
        assert_eq!(PageNumber::serialized_size(), 8); // We assume below that PageNumber is length 8
        if system_freed_pages.lock().unwrap().is_empty() {
            return Ok(false);
        }
        let mut stored_pages = false;

        system_tree.open_table_and_flush_table_root(
            SYSTEM_FREED_TABLE.name(),
            |system_freed_tree: &mut SystemFreedTree| {
                let mut pagination_id =
                    Self::next_system_freed_pagination_id(system_freed_tree, transaction_id)?;
                while !system_freed_pages.lock().unwrap().is_empty() {
                    let chunk_size = 200;
                    let buffer_size = PageList::required_bytes(chunk_size);
                    let key = TransactionIdWithPagination {
                        transaction_id: transaction_id.raw_id(),
                        pagination_id,
                    };
                    let mut access_guard = system_freed_tree.insert_reserve(&key, buffer_size)?;

                    let mut freed_pages = system_freed_pages.lock().unwrap();
                    let len = freed_pages.len();
                    access_guard.as_mut().clear();
                    for page in freed_pages.drain(len - min(len, chunk_size)..) {
                        if let Some(ref mut unpersisted_pages) = unpersisted_pages
                            && self.mem.unpersisted(page)
                        {
                            unpersisted_pages.push(page);
                        } else {
                            access_guard.as_mut().push_back(page);
                            stored_pages = true;
                        }
                    }
                    drop(access_guard);

                    pagination_id += 1;
                }
                Ok(())
            },
        )?;

        Ok(stored_pages)
    }

    fn next_system_freed_pagination_id(
        system_freed_tree: &SystemFreedTree,
        transaction_id: TransactionId,
    ) -> Result<u64> {
        let first_key = TransactionIdWithPagination {
            transaction_id: transaction_id.raw_id(),
            pagination_id: 0,
        };
        let next_transaction_key = TransactionIdWithPagination {
            transaction_id: transaction_id.next().raw_id(),
            pagination_id: 0,
        };
        let transaction_range = first_key..next_transaction_key;
        let mut existing_entries = system_freed_tree.range(&transaction_range)?;
        Ok(existing_entries
            .next_back()
            .transpose()?
            .map_or(0, |entry| entry.key().pagination_id + 1))
    }

    /// Retrieves information about storage usage in the database
    pub fn stats(&self) -> Result<DatabaseStats> {
        let tables = self.tables.lock().unwrap();
        let table_tree = &tables.table_tree;
        let data_tree_stats = table_tree.stats()?;

        let system_tables = self.system_tables.lock().unwrap();
        let system_table_tree = &system_tables.table_tree;
        let system_tree_stats = system_table_tree.stats()?;

        let total_metadata_bytes = data_tree_stats.metadata_bytes()
            + system_tree_stats.metadata_bytes
            + system_tree_stats.stored_leaf_bytes;
        let total_fragmented = data_tree_stats.fragmented_bytes()
            + system_tree_stats.fragmented_bytes
            + self.mem.count_free_pages()? * (self.mem.get_page_size() as u64);

        Ok(DatabaseStats {
            tree_height: data_tree_stats.tree_height(),
            allocated_pages: self.mem.count_allocated_pages()?,
            leaf_pages: data_tree_stats.leaf_pages(),
            branch_pages: data_tree_stats.branch_pages(),
            stored_leaf_bytes: data_tree_stats.stored_bytes(),
            metadata_bytes: total_metadata_bytes,
            fragmented_bytes: total_fragmented,
            page_size: self.mem.get_page_size(),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn print_debug(&self) -> Result {
        // Flush any pending updates to make sure we get the latest root
        let mut tables = self.tables.lock().unwrap();
        if let Some(page) = tables
            .table_tree
            .flush_table_root_updates()
            .unwrap()
            .finalize_dirty_checksums()
            .unwrap()
        {
            eprintln!("Master tree:");
            let master_tree: Btree<&str, InternalTableDefinition> = Btree::new(
                Some(page),
                PageHint::None,
                self.transaction_guard.clone(),
                PageResolver::new(self.mem.clone()),
            )?;
            master_tree.print_debug(true)?;
        }

        // Flush any pending updates to make sure we get the latest root
        let mut system_tables = self.system_tables.lock().unwrap();
        if let Some(page) = system_tables
            .table_tree
            .flush_table_root_updates()
            .unwrap()
            .finalize_dirty_checksums()
            .unwrap()
        {
            eprintln!("System tree:");
            let master_tree: Btree<&str, InternalTableDefinition> = Btree::new(
                Some(page),
                PageHint::None,
                self.transaction_guard.clone(),
                PageResolver::new(self.mem.clone()),
            )?;
            master_tree.print_debug(true)?;
        }

        Ok(())
    }
}

impl Drop for WriteTransaction {
    fn drop(&mut self) {
        if !self.completed && !thread::panicking() && !self.mem.storage_failure() {
            #[allow(unused_variables)]
            if let Err(error) = self.abort_inner() {
                #[cfg(feature = "logging")]
                warn!("Failure automatically aborting transaction: {error}");
            }
        } else if !self.completed && self.mem.storage_failure() {
            self.tables
                .lock()
                .unwrap()
                .table_tree
                .clear_root_updates_and_close();
        }
    }
}

/// A read-only transaction
///
/// Read-only transactions may exist concurrently with writes
pub struct ReadTransaction {
    mem: Arc<TransactionalMemory>,
    tree: TableTree,
}

impl ReadTransaction {
    pub(crate) fn new(
        mem: Arc<TransactionalMemory>,
        guard: TransactionGuard,
    ) -> Result<Self, TransactionError> {
        let root_page = mem.get_data_root();
        let guard = Arc::new(guard);
        let resolver = PageResolver::new(mem.clone());
        Ok(Self {
            mem,
            tree: TableTree::new(root_page, PageHint::Clean, guard, resolver)
                .map_err(TransactionError::Storage)?,
        })
    }

    /// Open the given table
    pub fn open_table<K: Key + 'static, V: Value + 'static>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<ReadOnlyTable<K, V>, TableError> {
        let header = self
            .tree
            .get_table::<K, V>(definition.name(), TableType::Normal)?
            .ok_or_else(|| TableError::TableDoesNotExist(definition.name().to_string()))?;

        match header {
            InternalTableDefinition::Normal { table_root, .. } => Ok(ReadOnlyTable::new(
                definition.name().to_string(),
                table_root,
                PageHint::Clean,
                self.tree.transaction_guard().clone(),
                PageResolver::new(self.mem.clone()),
            )?),
            InternalTableDefinition::Multimap { .. } => unreachable!(),
        }
    }

    /// Open the given table without a type
    pub fn open_untyped_table(
        &self,
        handle: impl TableHandle,
    ) -> Result<ReadOnlyUntypedTable, TableError> {
        let header = self
            .tree
            .get_table_untyped(handle.name(), TableType::Normal)?
            .ok_or_else(|| TableError::TableDoesNotExist(handle.name().to_string()))?;

        match header {
            InternalTableDefinition::Normal {
                table_root,
                fixed_key_size,
                fixed_value_size,
                ..
            } => Ok(ReadOnlyUntypedTable::new(
                table_root,
                PageHint::Clean,
                fixed_key_size,
                fixed_value_size,
                PageResolver::new(self.mem.clone()),
            )),
            InternalTableDefinition::Multimap { .. } => unreachable!(),
        }
    }

    /// Open the given table
    pub fn open_multimap_table<K: Key + 'static, V: Key + 'static>(
        &self,
        definition: MultimapTableDefinition<K, V>,
    ) -> Result<ReadOnlyMultimapTable<K, V>, TableError> {
        let header = self
            .tree
            .get_table::<K, V>(definition.name(), TableType::Multimap)?
            .ok_or_else(|| TableError::TableDoesNotExist(definition.name().to_string()))?;

        match header {
            InternalTableDefinition::Normal { .. } => unreachable!(),
            InternalTableDefinition::Multimap {
                table_root,
                table_length,
                ..
            } => Ok(ReadOnlyMultimapTable::new(
                table_root,
                table_length,
                PageHint::Clean,
                self.tree.transaction_guard().clone(),
                PageResolver::new(self.mem.clone()),
            )?),
        }
    }

    /// Open the given table without a type
    pub fn open_untyped_multimap_table(
        &self,
        handle: impl MultimapTableHandle,
    ) -> Result<ReadOnlyUntypedMultimapTable, TableError> {
        let header = self
            .tree
            .get_table_untyped(handle.name(), TableType::Multimap)?
            .ok_or_else(|| TableError::TableDoesNotExist(handle.name().to_string()))?;

        match header {
            InternalTableDefinition::Normal { .. } => unreachable!(),
            InternalTableDefinition::Multimap {
                table_root,
                table_length,
                fixed_key_size,
                fixed_value_size,
                ..
            } => Ok(ReadOnlyUntypedMultimapTable::new(
                table_root,
                table_length,
                PageHint::Clean,
                fixed_key_size,
                fixed_value_size,
                PageResolver::new(self.mem.clone()),
            )),
        }
    }

    /// List all the tables
    pub fn list_tables(&self) -> Result<impl Iterator<Item = UntypedTableHandle>> {
        self.tree
            .list_tables(TableType::Normal)
            .map(|x| x.into_iter().map(UntypedTableHandle::new))
    }

    /// List all the multimap tables
    pub fn list_multimap_tables(&self) -> Result<impl Iterator<Item = UntypedMultimapTableHandle>> {
        self.tree
            .list_tables(TableType::Multimap)
            .map(|x| x.into_iter().map(UntypedMultimapTableHandle::new))
    }

    /// Close the transaction
    ///
    /// Transactions are automatically closed when they and all objects referencing them have been dropped,
    /// so this method does not normally need to be called.
    /// This method can be used to ensure that there are no outstanding objects remaining.
    ///
    /// Returns `ReadTransactionStillInUse` error if a table or other object retrieved from the transaction still references this transaction
    pub fn close(self) -> Result<(), TransactionError> {
        if Arc::strong_count(self.tree.transaction_guard()) > 1 {
            return Err(TransactionError::ReadTransactionStillInUse(Box::new(self)));
        }
        // No-op, just drop ourself
        Ok(())
    }
}

impl Debug for ReadTransaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReadTransaction")
    }
}

#[cfg(test)]
mod test {
    use crate::{Database, TableDefinition};

    const X: TableDefinition<&str, &str> = TableDefinition::new("x");
    const BIG_VALUE: TableDefinition<u64, &[u8]> = TableDefinition::new("big_value");

    #[test]
    fn transaction_id_persistence() {
        let tmpfile = crate::create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(X).unwrap();
            table.insert("hello", "world").unwrap();
        }
        let first_txn_id = write_txn.transaction_id;
        write_txn.commit().unwrap();
        drop(db);

        let db2 = Database::create(tmpfile.path()).unwrap();
        let write_txn = db2.begin_write().unwrap();
        assert!(write_txn.transaction_id > first_txn_id);
    }

    #[test]
    fn post_commit_epilogue_reserves_transaction_id() {
        let tmpfile = crate::create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();
        let value = vec![0; 512 * 1024];

        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(BIG_VALUE).unwrap();
            table.insert(0, value.as_slice()).unwrap();
        }
        write_txn.commit().unwrap();

        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(BIG_VALUE).unwrap();
            table.remove(0).unwrap();
        }
        let remove_txn_id = write_txn.transaction_id;
        write_txn.commit().unwrap();

        let write_txn = db.begin_write().unwrap();
        assert!(write_txn.transaction_id > remove_txn_id.next());
    }
}
