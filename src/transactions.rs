use crate::db::TransactionGuard;
use crate::error::CommitError;
use crate::multimap_table::ReadOnlyUntypedMultimapTable;
use crate::sealed::Sealed;
use crate::table::ReadOnlyUntypedTable;
use crate::transaction_tracker::{SavepointId, TransactionId, TransactionTracker};
use crate::tree_store::{
    Btree, BtreeHeader, BtreeMut, InternalTableDefinition, MAX_PAIR_LENGTH, MAX_VALUE_LENGTH, Page,
    PageHint, PageListMut, PageNumber, PageTrackerPolicy, SerializedSavepoint, TableTree,
    TableTreeMut, TableType, TransactionalMemory,
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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::RangeBounds;
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
pub(crate) type AllocatorStateTreeMut<'a> = BtreeMut<'a, AllocatorStateKey, &'static [u8]>;
pub(crate) type SystemFreedTree<'a> = BtreeMut<'a, TransactionIdWithPagination, PageList<'static>>;

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
            0 => Self::Region(u32::from_le_bytes(data[1..].try_into().unwrap())),
            1 => Self::RegionTracker,
            2 => Self::TransactionId,
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
                result[0] = 0;
                result[1..].copy_from_slice(&u32::to_le_bytes(*region));
            }
            Self::RegionTracker => {
                result[0] = 1;
            }
            Self::TransactionId => {
                result[0] = 2;
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
    /// commit with a higher durability level.
    ///
    /// Note: Pages are only freed during commits with higher durability levels. Exclusively using
    /// this durability level will result in rapid growth of the database file.
    None,
    /// Commits with this durability level have been queued for persitance to disk, and should be
    /// persistent some time after [`WriteTransaction::commit`] returns.
    Eventual,
    /// Commits with this durability level are guaranteed to be persistent as soon as
    /// [`WriteTransaction::commit`] returns.
    Immediate,
}

// These are the actual durability levels used internally. `Durability::Paranoid` is translated
// to `InternalDurability::Immediate`, and also enables 2-phase commit
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum InternalDurability {
    None,
    Eventual,
    Immediate,
}

// Like a Table but only one may be open at a time to avoid possible races
pub struct SystemTable<'db, 's, K: Key + 'static, V: Value + 'static> {
    name: String,
    namespace: &'s mut SystemNamespace<'db>,
    tree: BtreeMut<'s, K, V>,
    transaction_guard: Arc<TransactionGuard>,
}

impl<'db, 's, K: Key + 'static, V: Value + 'static> SystemTable<'db, 's, K, V> {
    fn new(
        name: &str,
        table_root: Option<BtreeHeader>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
        namespace: &'s mut SystemNamespace<'db>,
    ) -> SystemTable<'db, 's, K, V> {
        // No need to track allocations in the system tree. Savepoint restoration only relies on
        // freeing in the data tree
        let ignore = Arc::new(Mutex::new(PageTrackerPolicy::Ignore));
        SystemTable {
            name: name.to_string(),
            namespace,
            tree: BtreeMut::new(table_root, guard.clone(), mem, freed_pages, ignore),
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

impl<K: Key + 'static, V: MutInPlaceValue + 'static> SystemTable<'_, '_, K, V> {
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

impl<K: Key + 'static, V: Value + 'static> Drop for SystemTable<'_, '_, K, V> {
    fn drop(&mut self) {
        self.namespace.close_table(
            &self.name,
            &self.tree,
            self.tree.get_root().map(|x| x.length).unwrap_or_default(),
        );
    }
}

struct SystemNamespace<'db> {
    table_tree: TableTreeMut<'db>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    transaction_guard: Arc<TransactionGuard>,
}

impl<'db> SystemNamespace<'db> {
    fn new(
        root_page: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Self {
        // No need to track allocations in the system tree. Savepoint restoration only relies on
        // freeing in the data tree
        let ignore = Arc::new(Mutex::new(PageTrackerPolicy::Ignore));
        let freed_pages = Arc::new(Mutex::new(vec![]));
        Self {
            table_tree: TableTreeMut::new(
                root_page,
                guard.clone(),
                mem,
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

    fn open_system_table<'txn, 's, K: Key + 'static, V: Value + 'static>(
        &'s mut self,
        transaction: &'txn WriteTransaction,
        definition: SystemTableDefinition<K, V>,
    ) -> Result<SystemTable<'db, 's, K, V>> {
        let (root, _) = self
            .table_tree
            .get_or_create_table::<K, V>(definition.name(), TableType::Normal)
            .map_err(|e| {
                e.into_storage_error_or_corrupted("Internal error. System table is corrupted")
            })?;
        transaction.dirty.store(true, Ordering::Release);

        Ok(SystemTable::new(
            definition.name(),
            root,
            self.freed_pages.clone(),
            self.transaction_guard.clone(),
            transaction.mem.clone(),
            self,
        ))
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

struct TableNamespace<'db> {
    open_tables: HashMap<String, &'static panic::Location<'static>>,
    allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    table_tree: TableTreeMut<'db>,
}

impl TableNamespace<'_> {
    fn new(
        root_page: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Self {
        let allocated = Arc::new(Mutex::new(PageTrackerPolicy::new_tracking()));
        let freed_pages = Arc::new(Mutex::new(vec![]));
        let table_tree = TableTreeMut::new(
            root_page,
            guard,
            mem,
            // Committed pages which are no longer reachable and will be queued for free'ing
            // These are separated from the system freed pages
            freed_pages.clone(),
            allocated.clone(),
        );
        Self {
            open_tables: Default::default(),
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
            transaction.mem.clone(),
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
            transaction.mem.clone(),
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
}

/// A read/write transaction
///
/// Only a single [`WriteTransaction`] may exist at a time
pub struct WriteTransaction {
    transaction_tracker: Arc<TransactionTracker>,
    mem: Arc<TransactionalMemory>,
    transaction_guard: Arc<TransactionGuard>,
    transaction_id: TransactionId,
    tables: Mutex<TableNamespace<'static>>,
    system_tables: Mutex<SystemNamespace<'static>>,
    completed: bool,
    dirty: AtomicBool,
    durability: InternalDurability,
    two_phase_commit: bool,
    quick_repair: bool,
    // Persistent savepoints created during this transaction
    created_persistent_savepoints: Mutex<HashSet<SavepointId>>,
    deleted_persistent_savepoints: Mutex<Vec<(SavepointId, TransactionId)>>,
}

impl WriteTransaction {
    pub(crate) fn new(
        guard: TransactionGuard,
        transaction_tracker: Arc<TransactionTracker>,
        mem: Arc<TransactionalMemory>,
    ) -> Result<Self> {
        let transaction_id = guard.id();
        let guard = Arc::new(guard);

        let root_page = mem.get_data_root();
        let system_page = mem.get_system_root();

        let tables = TableNamespace::new(root_page, guard.clone(), mem.clone());
        let system_tables = SystemNamespace::new(system_page, guard.clone(), mem.clone());

        Ok(Self {
            transaction_tracker,
            mem: mem.clone(),
            transaction_guard: guard.clone(),
            transaction_id,
            tables: Mutex::new(tables),
            system_tables: Mutex::new(system_tables),
            completed: false,
            dirty: AtomicBool::new(false),
            durability: InternalDurability::Immediate,
            two_phase_commit: false,
            quick_repair: false,
            created_persistent_savepoints: Mutex::new(Default::default()),
            deleted_persistent_savepoints: Mutex::new(vec![]),
        })
    }

    pub(crate) fn pending_free_pages(&self) -> Result<bool> {
        let mut system_tables = self.system_tables.lock().unwrap();
        if system_tables
            .open_system_table(self, DATA_FREED_TABLE)?
            .tree
            .get_root()
            .is_some()
        {
            return Ok(true);
        }
        if system_tables
            .open_system_table(self, SYSTEM_FREED_TABLE)?
            .tree
            .get_root()
            .is_some()
        {
            return Ok(true);
        }

        Ok(false)
    }

    #[cfg(any(test, fuzzing))]
    pub fn print_allocated_page_debug(&self) {
        let mut all_allocated: HashSet<PageNumber> =
            HashSet::from_iter(self.mem.all_allocated_pages());

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
            all_allocated.remove(&p);
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
            all_allocated.remove(&p);
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
                    all_allocated.remove(&p);
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
                    all_allocated.remove(&p);
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
                    all_allocated.remove(p);
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
                    all_allocated.remove(p);
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
    /// Returns `[SavepointError::InvalidSavepoint`], if the transaction is "dirty" (any tables have been opened)
    /// or if the transaction's durability is less than `[Durability::Immediate]`
    pub fn persistent_savepoint(&self) -> Result<u64, SavepointError> {
        if self.durability != InternalDurability::Immediate {
            return Err(SavepointError::InvalidSavepoint);
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

        self.created_persistent_savepoints
            .lock()
            .unwrap()
            .insert(savepoint.get_id());

        Ok(savepoint.get_id().0)
    }

    pub(crate) fn transaction_guard(&self) -> Arc<TransactionGuard> {
        self.transaction_guard.clone()
    }

    pub(crate) fn next_persistent_savepoint_id(&self) -> Result<Option<SavepointId>> {
        let mut system_tables = self.system_tables.lock().unwrap();
        let next_table = system_tables.open_system_table(self, NEXT_SAVEPOINT_TABLE)?;
        let value = next_table.get(())?;
        if let Some(next_id) = value {
            Ok(Some(next_id.value()))
        } else {
            Ok(None)
        }
    }

    /// Get a persistent savepoint given its id
    pub fn get_persistent_savepoint(&self, id: u64) -> Result<Savepoint, SavepointError> {
        let mut system_tables = self.system_tables.lock().unwrap();
        let table = system_tables.open_system_table(self, SAVEPOINT_TABLE)?;
        let value = table.get(SavepointId(id))?;

        value
            .map(|x| x.value().to_savepoint(self.transaction_tracker.clone()))
            .ok_or(SavepointError::InvalidSavepoint)
    }

    /// Delete the given persistent savepoint.
    ///
    /// Note that if the transaction is `abort()`'ed this deletion will be rolled back.
    ///
    /// Returns `true` if the savepoint existed
    /// Returns `[SavepointError::InvalidSavepoint`] if the transaction's durability is less than `[Durability::Immediate]`
    pub fn delete_persistent_savepoint(&self, id: u64) -> Result<bool, SavepointError> {
        if self.durability != InternalDurability::Immediate {
            return Err(SavepointError::InvalidSavepoint);
        }
        let mut system_tables = self.system_tables.lock().unwrap();
        let mut table = system_tables.open_system_table(self, SAVEPOINT_TABLE)?;
        let savepoint = table.remove(SavepointId(id))?;
        if let Some(serialized) = savepoint {
            let savepoint = serialized
                .value()
                .to_savepoint(self.transaction_tracker.clone());
            self.deleted_persistent_savepoints
                .lock()
                .unwrap()
                .push((savepoint.get_id(), savepoint.get_transaction_id()));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List all persistent savepoints
    pub fn list_persistent_savepoints(&self) -> Result<impl Iterator<Item = u64>> {
        let mut system_tables = self.system_tables.lock().unwrap();
        let table = system_tables.open_system_table(self, SAVEPOINT_TABLE)?;
        let mut savepoints = vec![];
        for savepoint in table.range::<SavepointId>(..)? {
            savepoints.push(savepoint?.0.value().0);
        }
        Ok(savepoints.into_iter())
    }

    // TODO: deduplicate this with the one in Database
    fn allocate_read_transaction(&self) -> Result<TransactionGuard> {
        let id = self
            .transaction_tracker
            .register_read_transaction(&self.mem)?;

        Ok(TransactionGuard::new_read(
            id,
            self.transaction_tracker.clone(),
        ))
    }

    fn allocate_savepoint(&self) -> Result<(SavepointId, TransactionId)> {
        let transaction_id = self.allocate_read_transaction()?.leak();
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
        // Ensure that user does not try to restore a Savepoint that is from a different Database
        assert_eq!(
            std::ptr::from_ref(self.transaction_tracker.as_ref()),
            savepoint.db_address()
        );

        if !self
            .transaction_tracker
            .is_valid_savepoint(savepoint.get_id())
        {
            return Err(SavepointError::InvalidSavepoint);
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
            let mut data_freed_pages = tables.freed_pages.lock().unwrap();
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
        }

        // 3) Invalidate all savepoints that are newer than the one being applied to prevent the user
        // from later trying to restore a savepoint "on another timeline"
        self.transaction_tracker
            .invalidate_savepoints_after(savepoint.get_id());
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
        let created = !self
            .created_persistent_savepoints
            .lock()
            .unwrap()
            .is_empty();
        let deleted = !self
            .deleted_persistent_savepoints
            .lock()
            .unwrap()
            .is_empty();
        if (created || deleted) && !matches!(durability, Durability::Immediate) {
            return Err(SetDurabilityError::PersistentSavepointModified);
        }

        self.durability = match durability {
            Durability::None => InternalDurability::None,
            Durability::Eventual => InternalDurability::Eventual,
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
        self.tables.lock().unwrap().close_table(name, table, length);
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
    pub fn commit(mut self) -> Result<(), CommitError> {
        // Set completed flag first, so that we don't go through the abort() path on drop, if this fails
        self.completed = true;
        self.commit_inner()
    }

    fn commit_inner(&mut self) -> Result<(), CommitError> {
        // Quick-repair requires 2-phase commit
        if self.quick_repair {
            self.two_phase_commit = true;
        }

        let (user_root, allocated_pages, data_freed) =
            self.tables.lock().unwrap().table_tree.flush_and_close()?;

        self.store_data_freed_pages(data_freed)?;
        self.store_allocated_pages(allocated_pages.into_iter().collect())?;

        #[cfg(feature = "logging")]
        debug!(
            "Committing transaction id={:?} with durability={:?} two_phase={} quick_repair={}",
            self.transaction_id, self.durability, self.two_phase_commit, self.quick_repair
        );
        match self.durability {
            InternalDurability::None => self.non_durable_commit(user_root)?,
            InternalDurability::Eventual => self.durable_commit(user_root, true)?,
            InternalDurability::Immediate => self.durable_commit(user_root, false)?,
        }

        for (savepoint, transaction) in self.deleted_persistent_savepoints.lock().unwrap().iter() {
            self.transaction_tracker
                .deallocate_savepoint(*savepoint, *transaction);
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

    fn store_data_freed_pages(&self, mut freed_pages: Vec<PageNumber>) -> Result {
        let mut system_tables = self.system_tables.lock().unwrap();
        let mut freed_table = system_tables.open_system_table(self, DATA_FREED_TABLE)?;
        let mut pagination_counter = 0;
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
                debug_assert!(!self.mem.uncommitted(page), "Page is uncommitted: {page:?}");
                access_guard.as_mut().push_back(page);
            }

            pagination_counter += 1;
        }

        Ok(())
    }

    fn store_allocated_pages(&self, mut data_allocated_pages: Vec<PageNumber>) -> Result {
        let mut system_tables = self.system_tables.lock().unwrap();
        let mut allocated_table = system_tables.open_system_table(self, DATA_ALLOCATED_TABLE)?;
        let mut pagination_counter = 0;
        while !data_allocated_pages.is_empty() {
            let chunk_size = 400;
            let buffer_size = PageList::required_bytes(chunk_size);
            let key = TransactionIdWithPagination {
                transaction_id: self.transaction_id.raw_id(),
                pagination_id: pagination_counter,
            };
            let mut access_guard = allocated_table.insert_reserve(&key, buffer_size)?;

            let len = data_allocated_pages.len();
            access_guard.as_mut().clear();
            for page in data_allocated_pages.drain(len - min(len, chunk_size)..) {
                // Make sure that the page is currently allocated. This is to catch scenarios like
                // a page getting allocated, and then deallocated within the same transaction,
                // but errantly being left in the allocated pages list
                debug_assert!(
                    self.mem.is_allocated(page),
                    "Page is not allocated: {page:?}"
                );
                debug_assert!(self.mem.uncommitted(page), "Page is committed: {page:?}");
                access_guard.as_mut().push_back(page);
            }

            pagination_counter += 1;
        }

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
        for savepoint in self.created_persistent_savepoints.lock().unwrap().iter() {
            match self.delete_persistent_savepoint(savepoint.0) {
                Ok(_) => {}
                Err(err) => match err {
                    SavepointError::InvalidSavepoint => {
                        unreachable!();
                    }
                    SavepointError::Storage(storage_err) => {
                        return Err(storage_err);
                    }
                },
            }
        }
        self.mem.rollback_uncommitted_writes()?;
        #[cfg(feature = "logging")]
        debug!("Finished abort of transaction id={:?}", self.transaction_id);
        Ok(())
    }

    pub(crate) fn durable_commit(
        &mut self,
        user_root: Option<BtreeHeader>,
        eventual: bool,
    ) -> Result {
        let free_until_transaction = self
            .transaction_tracker
            .oldest_live_read_transaction()
            .map_or(self.transaction_id, |x| x.next());
        self.process_freed_pages(free_until_transaction)?;

        let mut system_tables = self.system_tables.lock().unwrap();
        let system_freed_pages = system_tables.system_freed_pages();
        let system_tree = system_tables.table_tree.flush_table_root_updates()?;
        system_tree
            .delete_table(ALLOCATOR_STATE_TABLE_NAME, TableType::Normal)
            .map_err(|e| e.into_storage_error_or_corrupted("Unexpected TableError"))?;

        if self.quick_repair {
            system_tree.create_table_and_flush_table_root(
                ALLOCATOR_STATE_TABLE_NAME,
                |system_tree_ref, tree: &mut AllocatorStateTreeMut| {
                    let mut pagination_counter = 0;

                    loop {
                        let num_regions = self
                            .mem
                            .reserve_allocator_state(tree, self.transaction_id)?;

                        // We can't free pages after the commit, because that would invalidate our
                        // saved allocator state. Everything needs to go through the transactional
                        // free mechanism
                        self.store_system_freed_pages(
                            system_tree_ref,
                            system_freed_pages.clone(),
                            None,
                            &mut pagination_counter,
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

        let system_root = system_tree.finalize_dirty_checksums()?;

        self.mem.commit(
            user_root,
            system_root,
            self.transaction_id,
            eventual,
            self.two_phase_commit,
        )?;

        // Mark any pending non-durable commits as fully committed.
        self.transaction_tracker.clear_pending_non_durable_commits();

        // Immediately free the pages that were freed from the system-tree. These are only
        // accessed by write transactions, so it's safe to free them as soon as the commit is done.
        for page in system_freed_pages.lock().unwrap().drain(..) {
            self.mem.free(page, &mut PageTrackerPolicy::Ignore);
        }

        Ok(())
    }

    // Commit without a durability guarantee
    pub(crate) fn non_durable_commit(&mut self, user_root: Option<BtreeHeader>) -> Result {
        let free_until_transaction = self
            .transaction_tracker
            .oldest_live_read_nondurable_transaction()
            .map_or(self.transaction_id, |x| x.next());
        self.process_freed_pages_nondurable(free_until_transaction)?;

        let mut post_commit_frees = vec![];

        let system_root = {
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
            self.store_system_freed_pages(
                &mut system_tables.table_tree,
                system_freed_pages,
                Some(&mut post_commit_frees),
                &mut 0,
            )?;

            system_tables
                .table_tree
                .flush_table_root_updates()?
                .finalize_dirty_checksums()?
        };

        self.mem
            .non_durable_commit(user_root, system_root, self.transaction_id)?;
        // Register this as a non-durable transaction to ensure that the freed pages we just pushed
        // are only processed after this has been persisted
        self.transaction_tracker.register_non_durable_commit(
            self.transaction_id,
            self.mem.get_last_durable_transaction_id()?,
        );

        for page in post_commit_frees {
            self.mem.free(page, &mut PageTrackerPolicy::Ignore);
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

        // Calculate how many of them can be relocated to lower pages, starting from the last page
        let mut relocation_map = HashMap::new();
        for path in highest_pages.into_values().rev() {
            if relocation_map.contains_key(&path.page_number()) {
                continue;
            }
            let old_page = self.mem.get_page(path.page_number())?;
            let mut new_page = self.mem.allocate_lowest(old_page.memory().len())?;
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
                    let old_parent = self.mem.get_page(*parent)?;
                    let mut new_page = self.mem.allocate_lowest(old_parent.memory().len())?;
                    let new_page_number = new_page.get_page_number();
                    // We have to copy at least the page type into the new page.
                    // Otherwise its cache priority will be calculated incorrectly
                    new_page.memory_mut()[0] = old_parent.memory()[0];
                    drop(new_page);
                    relocation_map.insert(*parent, new_page_number);
                }
            } else {
                self.mem
                    .free(new_page_number, &mut PageTrackerPolicy::Ignore);
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

        // Handle the data freed tree
        let mut system_tables = self.system_tables.lock().unwrap();
        {
            let mut data_freed = system_tables.open_system_table(self, DATA_FREED_TABLE)?;
            let key = TransactionIdWithPagination {
                transaction_id: free_until.raw_id(),
                pagination_id: 0,
            };
            for entry in data_freed.extract_from_if(..key, |_, _| true)? {
                let (_, page_list) = entry?;
                for i in 0..page_list.value().len() {
                    self.mem
                        .free(page_list.value().get(i), &mut PageTrackerPolicy::Ignore);
                }
            }
        }

        // Handle the system freed tree
        {
            let mut system_freed = system_tables.open_system_table(self, SYSTEM_FREED_TABLE)?;
            let key = TransactionIdWithPagination {
                transaction_id: free_until.raw_id(),
                pagination_id: 0,
            };
            for entry in system_freed.extract_from_if(..key, |_, _| true)? {
                let (_, page_list) = entry?;
                for i in 0..page_list.value().len() {
                    self.mem
                        .free(page_list.value().get(i), &mut PageTrackerPolicy::Ignore);
                }
            }
        }

        Ok(())
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

        let mut processed = vec![];

        // Handle the data freed tree
        let mut system_tables = self.system_tables.lock().unwrap();
        {
            let last_key = TransactionIdWithPagination {
                transaction_id: free_until.raw_id(),
                pagination_id: 0,
            };
            let mut data_freed = system_tables.open_system_table(self, DATA_FREED_TABLE)?;
            let mut candidate_transactions = vec![];
            for entry in data_freed.range(..last_key)? {
                let (key, _) = entry?;
                let transaction_id = TransactionId::new(key.value().transaction_id);
                if self
                    .transaction_tracker
                    .is_unprocessed_non_durable_commit(transaction_id)
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
        }

        // Handle the system freed tree
        {
            let last_key = TransactionIdWithPagination {
                transaction_id: free_until.raw_id(),
                pagination_id: 0,
            };
            let mut data_freed = system_tables.open_system_table(self, SYSTEM_FREED_TABLE)?;
            let mut candidate_transactions = vec![];
            for entry in data_freed.range(..last_key)? {
                let (key, _) = entry?;
                let transaction_id = TransactionId::new(key.value().transaction_id);
                if self
                    .transaction_tracker
                    .is_unprocessed_non_durable_commit(transaction_id)
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
        }

        for transaction_id in processed {
            self.transaction_tracker
                .mark_unprocessed_non_durable_commit(transaction_id);
        }

        Ok(())
    }

    fn store_system_freed_pages(
        &self,
        system_tree: &mut TableTreeMut,
        system_freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        mut unpersisted_pages: Option<&mut Vec<PageNumber>>,
        pagination_counter: &mut u64,
    ) -> Result {
        assert_eq!(PageNumber::serialized_size(), 8); // We assume below that PageNumber is length 8

        system_tree.open_table_and_flush_table_root(
            SYSTEM_FREED_TABLE.name(),
            |system_freed_tree: &mut SystemFreedTree| {
                while !system_freed_pages.lock().unwrap().is_empty() {
                    let chunk_size = 200;
                    let buffer_size = PageList::required_bytes(chunk_size);
                    let key = TransactionIdWithPagination {
                        transaction_id: self.transaction_id.raw_id(),
                        pagination_id: *pagination_counter,
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
                        }
                    }
                    drop(access_guard);

                    *pagination_counter += 1;
                }
                Ok(())
            },
        )?;

        Ok(())
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
                self.mem.clone(),
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
                self.mem.clone(),
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
        Ok(Self {
            mem: mem.clone(),
            tree: TableTree::new(root_page, PageHint::Clean, guard, mem)
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
                self.mem.clone(),
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
                fixed_key_size,
                fixed_value_size,
                self.mem.clone(),
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
                self.mem.clone(),
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
                fixed_key_size,
                fixed_value_size,
                self.mem.clone(),
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
}
