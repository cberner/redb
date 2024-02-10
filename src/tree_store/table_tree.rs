use crate::db::TransactionGuard;
use crate::error::TableError;
use crate::multimap_table::{
    finalize_tree_and_subtree_checksums, multimap_btree_stats, verify_tree_and_subtree_checksums,
};
use crate::tree_store::btree::{btree_stats, UntypedBtreeMut};
use crate::tree_store::btree_base::BtreeHeader;
use crate::tree_store::btree_iters::AllPageNumbersBtreeIter;
use crate::tree_store::{
    Btree, BtreeMut, BtreeRangeIter, PageHint, PageNumber, RawBtree, TransactionalMemory,
};
use crate::types::{Key, MutInPlaceValue, TypeName, Value};
use crate::{DatabaseStats, Result};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::mem::size_of;
use std::ops::RangeFull;
use std::sync::{Arc, Mutex};

// Forward compatibility feature in case alignment can be supported in the future
// See https://github.com/cberner/redb/issues/360
const ALIGNMENT: usize = 1;

#[derive(Debug)]
pub(crate) struct FreedTableKey {
    pub(crate) transaction_id: u64,
    pub(crate) pagination_id: u64,
}

impl Value for FreedTableKey {
    type SelfType<'a> = FreedTableKey
    where
        Self: 'a;
    type AsBytes<'a> = [u8; 2 * size_of::<u64>()]
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
        Self: 'a,
        Self: 'b,
    {
        let mut result = [0u8; 2 * size_of::<u64>()];
        result[..size_of::<u64>()].copy_from_slice(&value.transaction_id.to_le_bytes());
        result[size_of::<u64>()..].copy_from_slice(&value.pagination_id.to_le_bytes());
        result
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::FreedTableKey")
    }
}

impl Key for FreedTableKey {
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

// Format:
// 2 bytes: length
// length * size_of(PageNumber): array of page numbers
#[derive(Debug)]
pub(crate) struct FreedPageList<'a> {
    data: &'a [u8],
}

impl<'a> FreedPageList<'a> {
    pub(crate) fn required_bytes(len: usize) -> usize {
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

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct FreedPageListMut {
    data: [u8],
}

impl FreedPageListMut {
    pub(crate) fn push_back(&mut self, value: PageNumber) {
        let len = u16::from_le_bytes(self.data[..size_of::<u16>()].try_into().unwrap());
        self.data[..size_of::<u16>()].copy_from_slice(&(len + 1).to_le_bytes());
        let len: usize = len.into();
        let start = size_of::<u16>() + PageNumber::serialized_size() * len;
        self.data[start..(start + PageNumber::serialized_size())]
            .copy_from_slice(&value.to_le_bytes());
    }

    pub(crate) fn clear(&mut self) {
        self.data[..size_of::<u16>()].fill(0);
    }
}

impl Value for FreedPageList<'_> {
    type SelfType<'a> = FreedPageList<'a>
        where
            Self: 'a;
    type AsBytes<'a> = &'a [u8]
        where
            Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        FreedPageList { data }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> &'b [u8]
    where
        Self: 'a,
        Self: 'b,
    {
        value.data
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::FreedPageList")
    }
}

impl MutInPlaceValue for FreedPageList<'_> {
    type BaseRefType = FreedPageListMut;

    fn initialize(data: &mut [u8]) {
        assert!(data.len() >= 8);
        // Set the length to zero
        data[..8].fill(0);
    }

    fn from_bytes_mut(data: &mut [u8]) -> &mut Self::BaseRefType {
        unsafe { mem::transmute(data) }
    }
}

#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug)]
pub(crate) enum TableType {
    Normal,
    Multimap,
}

impl TableType {
    fn is_legacy(value: u8) -> bool {
        value == 1 || value == 2
    }
}

#[allow(clippy::from_over_into)]
impl Into<u8> for TableType {
    fn into(self) -> u8 {
        match self {
            // 1 & 2 were used in the v1 file format
            // TableType::Normal => 1,
            // TableType::Multimap => 2,
            TableType::Normal => 3,
            TableType::Multimap => 4,
        }
    }
}

impl From<u8> for TableType {
    fn from(value: u8) -> Self {
        match value {
            3 => TableType::Normal,
            4 => TableType::Multimap,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct InternalTableDefinition {
    table_root: Option<BtreeHeader>,
    table_type: TableType,
    table_length: u64,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    key_alignment: usize,
    value_alignment: usize,
    key_type: TypeName,
    value_type: TypeName,
}

impl InternalTableDefinition {
    pub(crate) fn get_root(&self) -> Option<BtreeHeader> {
        self.table_root
    }

    pub(crate) fn get_length(&self) -> u64 {
        self.table_length
    }

    pub(crate) fn get_fixed_key_size(&self) -> Option<usize> {
        self.fixed_key_size
    }

    pub(crate) fn get_fixed_value_size(&self) -> Option<usize> {
        self.fixed_value_size
    }

    pub(crate) fn get_key_alignment(&self) -> usize {
        self.key_alignment
    }

    pub(crate) fn get_value_alignment(&self) -> usize {
        self.value_alignment
    }

    pub(crate) fn get_type(&self) -> TableType {
        self.table_type
    }
}

impl Value for InternalTableDefinition {
    type SelfType<'a> = InternalTableDefinition;
    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self
    where
        Self: 'a,
    {
        debug_assert!(data.len() > 22);
        let mut offset = 0;
        let legacy = TableType::is_legacy(data[offset]);
        assert!(!legacy);
        let table_type = TableType::from(data[offset]);
        offset += 1;

        let table_length = u64::from_le_bytes(
            data[offset..(offset + size_of::<u64>())]
                .try_into()
                .unwrap(),
        );
        offset += size_of::<u64>();

        let non_null = data[offset] != 0;
        offset += 1;
        let table_root = if non_null {
            Some(BtreeHeader::from_le_bytes(
                data[offset..(offset + BtreeHeader::serialized_size())]
                    .try_into()
                    .unwrap(),
            ))
        } else {
            None
        };
        offset += BtreeHeader::serialized_size();

        let non_null = data[offset] != 0;
        offset += 1;
        let fixed_key_size = if non_null {
            let fixed = u32::from_le_bytes(
                data[offset..(offset + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            Some(fixed)
        } else {
            None
        };
        offset += size_of::<u32>();

        let non_null = data[offset] != 0;
        offset += 1;
        let fixed_value_size = if non_null {
            let fixed = u32::from_le_bytes(
                data[offset..(offset + size_of::<u32>())]
                    .try_into()
                    .unwrap(),
            ) as usize;
            Some(fixed)
        } else {
            None
        };
        offset += size_of::<u32>();
        let key_alignment = u32::from_le_bytes(
            data[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += size_of::<u32>();
        let value_alignment = u32::from_le_bytes(
            data[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += size_of::<u32>();

        let key_type_len = u32::from_le_bytes(
            data[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += size_of::<u32>();
        let key_type = TypeName::from_bytes(&data[offset..(offset + key_type_len)]);
        offset += key_type_len;
        let value_type = TypeName::from_bytes(&data[offset..]);

        InternalTableDefinition {
            table_root,
            table_type,
            table_length,
            fixed_key_size,
            fixed_value_size,
            key_alignment,
            value_alignment,
            key_type,
            value_type,
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
    where
        Self: 'a,
        Self: 'b,
    {
        let mut result = vec![value.table_type.into()];
        result.extend_from_slice(&value.table_length.to_le_bytes());
        if let Some(header) = value.table_root {
            result.push(1);
            result.extend_from_slice(&header.to_le_bytes());
        } else {
            result.push(0);
            result.extend_from_slice(&[0; BtreeHeader::serialized_size()]);
        }
        if let Some(fixed) = value.fixed_key_size {
            result.push(1);
            result.extend_from_slice(&u32::try_from(fixed).unwrap().to_le_bytes());
        } else {
            result.push(0);
            result.extend_from_slice(&[0; size_of::<u32>()])
        }
        if let Some(fixed) = value.fixed_value_size {
            result.push(1);
            result.extend_from_slice(&u32::try_from(fixed).unwrap().to_le_bytes());
        } else {
            result.push(0);
            result.extend_from_slice(&[0; size_of::<u32>()])
        }
        result.extend_from_slice(&u32::try_from(value.key_alignment).unwrap().to_le_bytes());
        result.extend_from_slice(&u32::try_from(value.value_alignment).unwrap().to_le_bytes());
        let key_type_bytes = value.key_type.to_bytes();
        result.extend_from_slice(&u32::try_from(key_type_bytes.len()).unwrap().to_le_bytes());
        result.extend_from_slice(&key_type_bytes);
        result.extend_from_slice(&value.value_type.to_bytes());

        result
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::InternalTableDefinition")
    }
}

pub struct TableNameIter {
    inner: BtreeRangeIter<&'static str, InternalTableDefinition>,
    table_type: TableType,
}

impl Iterator for TableNameIter {
    type Item = Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        for entry in self.inner.by_ref() {
            match entry {
                Ok(entry) => {
                    if entry.value().table_type == self.table_type {
                        return Some(Ok(entry.key().to_string()));
                    }
                }
                Err(err) => {
                    return Some(Err(err));
                }
            }
        }
        None
    }
}

pub(crate) struct TableTree {
    tree: Btree<&'static str, InternalTableDefinition>,
}

impl TableTree {
    pub(crate) fn new(
        master_root: Option<BtreeHeader>,
        page_hint: PageHint,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Result<Self> {
        Ok(Self {
            tree: Btree::new(master_root, page_hint, guard, mem)?,
        })
    }

    // root_page: the root of the master table
    pub(crate) fn list_tables(&self, table_type: TableType) -> Result<Vec<String>> {
        let iter = self.tree.range::<RangeFull, &str>(&(..))?;
        let iter = TableNameIter {
            inner: iter,
            table_type,
        };
        let mut result = vec![];
        for table in iter {
            result.push(table?);
        }
        Ok(result)
    }

    pub(crate) fn get_table_untyped(
        &self,
        name: &str,
        table_type: TableType,
    ) -> Result<Option<InternalTableDefinition>, TableError> {
        if let Some(guard) = self.tree.get(&name)? {
            let definition = guard.value();
            if definition.get_type() != table_type {
                return if definition.get_type() == TableType::Multimap {
                    Err(TableError::TableIsMultimap(name.to_string()))
                } else {
                    Err(TableError::TableIsNotMultimap(name.to_string()))
                };
            }
            if definition.get_key_alignment() != ALIGNMENT {
                return Err(TableError::TypeDefinitionChanged {
                    name: definition.key_type.clone(),
                    alignment: definition.get_key_alignment(),
                    width: definition.get_fixed_key_size(),
                });
            }
            if definition.get_value_alignment() != ALIGNMENT {
                return Err(TableError::TypeDefinitionChanged {
                    name: definition.value_type.clone(),
                    alignment: definition.get_value_alignment(),
                    width: definition.get_fixed_value_size(),
                });
            }

            Ok(Some(definition))
        } else {
            Ok(None)
        }
    }

    // root_page: the root of the master table
    pub(crate) fn get_table<K: Key, V: Value>(
        &self,
        name: &str,
        table_type: TableType,
    ) -> Result<Option<InternalTableDefinition>, TableError> {
        Ok(
            if let Some(definition) = self.get_table_untyped(name, table_type)? {
                // Do additional checks on the types to be sure they match
                if definition.key_type != K::type_name() || definition.value_type != V::type_name()
                {
                    return Err(TableError::TableTypeMismatch {
                        table: name.to_string(),
                        key: definition.key_type,
                        value: definition.value_type,
                    });
                }
                if definition.get_fixed_key_size() != K::fixed_width() {
                    return Err(TableError::TypeDefinitionChanged {
                        name: K::type_name(),
                        alignment: definition.get_key_alignment(),
                        width: definition.get_fixed_key_size(),
                    });
                }
                if definition.get_fixed_value_size() != V::fixed_width() {
                    return Err(TableError::TypeDefinitionChanged {
                        name: V::type_name(),
                        alignment: definition.get_value_alignment(),
                        width: definition.get_fixed_value_size(),
                    });
                }
                Some(definition)
            } else {
                None
            },
        )
    }
}

pub(crate) struct TableTreeMut<'txn> {
    tree: BtreeMut<'txn, &'static str, InternalTableDefinition>,
    guard: Arc<TransactionGuard>,
    mem: Arc<TransactionalMemory>,
    // Cached updates from tables that have been closed. These must be flushed to the btree
    pending_table_updates: HashMap<String, (Option<BtreeHeader>, u64)>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
}

impl<'txn> TableTreeMut<'txn> {
    pub(crate) fn new(
        master_root: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    ) -> Self {
        Self {
            tree: BtreeMut::new(master_root, guard.clone(), mem.clone(), freed_pages.clone()),
            guard,
            mem,
            pending_table_updates: Default::default(),
            freed_pages,
        }
    }

    pub(crate) fn all_referenced_pages(&self) -> Result<HashSet<PageNumber>> {
        // All the pages in the table tree itself
        let mut result = HashSet::new();
        if let Some(iter) = self.tree.all_pages_iter()? {
            for page in iter {
                result.insert(page?);
            }
        }

        // All the normal tables
        for entry in self.list_tables(TableType::Normal)? {
            let definition = self
                .get_table_untyped(&entry, TableType::Normal)
                .map_err(|e| e.into_storage_error_or_corrupted("Internal corruption"))?
                .unwrap();
            if let Some(header) = definition.get_root() {
                let table_pages_iter = AllPageNumbersBtreeIter::new(
                    header.root,
                    definition.get_fixed_key_size(),
                    definition.get_fixed_value_size(),
                    self.mem.clone(),
                )?;

                for page in table_pages_iter {
                    result.insert(page?);
                }
            }
        }

        if !self.list_tables(TableType::Multimap)?.is_empty() {
            unimplemented!("Walking all multimap references is not currently supported");
        }

        Ok(result)
    }

    // Queues an update to the table root
    pub(crate) fn stage_update_table_root(
        &mut self,
        name: &str,
        table_root: Option<BtreeHeader>,
        length: u64,
    ) {
        self.pending_table_updates
            .insert(name.to_string(), (table_root, length));
    }

    pub(crate) fn clear_table_root_updates(&mut self) {
        self.pending_table_updates.clear();
    }

    pub(crate) fn verify_checksums(&self) -> Result<bool> {
        assert!(self.pending_table_updates.is_empty());
        if !self.tree.verify_checksum()? {
            return Ok(false);
        }

        for entry in self.tree.range::<RangeFull, &str>(&(..))? {
            let entry = entry?;
            let definition = entry.value();
            // TODO: all these matches on table_type() are pretty errorprone, because you can call .get_fixed_value_size() on either.
            // Maybe InternalTableDefinition should be an enum for normal and multimap, instead
            match definition.get_type() {
                TableType::Normal => {
                    if let Some(header) = definition.get_root() {
                        if !RawBtree::new(
                            Some(header),
                            definition.get_fixed_key_size(),
                            definition.get_fixed_value_size(),
                            self.mem.clone(),
                        )
                        .verify_checksum()?
                        {
                            return Ok(false);
                        }
                    }
                }
                TableType::Multimap => {
                    if !verify_tree_and_subtree_checksums(
                        definition.get_root(),
                        definition.get_fixed_key_size(),
                        definition.get_fixed_value_size(),
                        self.mem.clone(),
                    )? {
                        return Ok(false);
                    }
                }
            }
        }

        Ok(true)
    }

    pub(crate) fn flush_table_root_updates(&mut self) -> Result<Option<BtreeHeader>> {
        for (name, (table_root, table_length)) in self.pending_table_updates.drain() {
            // Bypass .get_table() since the table types are dynamic
            let mut definition = self.tree.get(&name.as_str())?.unwrap().value();
            // No-op if the root has not changed
            if definition.table_root == table_root {
                continue;
            }
            definition.table_length = table_length;
            // Finalize any dirty checksums
            if definition.table_type == TableType::Normal {
                let mut tree = UntypedBtreeMut::new(
                    table_root,
                    self.mem.clone(),
                    self.freed_pages.clone(),
                    definition.fixed_key_size,
                    definition.fixed_value_size,
                );
                tree.finalize_dirty_checksums()?;
                definition.table_root = tree.get_root();
            } else {
                definition.table_root = finalize_tree_and_subtree_checksums(
                    table_root,
                    definition.fixed_key_size,
                    definition.fixed_value_size,
                    self.mem.clone(),
                )?;
            }
            self.tree.insert(&name.as_str(), &definition)?;
        }
        self.tree.finalize_dirty_checksums()?;
        Ok(self.tree.get_root())
    }

    // root_page: the root of the master table
    pub(crate) fn list_tables(&self, table_type: TableType) -> Result<Vec<String>> {
        let tree = TableTree::new(
            self.tree.get_root(),
            PageHint::None,
            self.guard.clone(),
            self.mem.clone(),
        )?;
        tree.list_tables(table_type)
    }

    pub(crate) fn get_table_untyped(
        &self,
        name: &str,
        table_type: TableType,
    ) -> Result<Option<InternalTableDefinition>, TableError> {
        let tree = TableTree::new(
            self.tree.get_root(),
            PageHint::None,
            self.guard.clone(),
            self.mem.clone(),
        )?;
        let mut result = tree.get_table_untyped(name, table_type);

        if let Ok(Some(definition)) = result.as_mut() {
            if let Some((updated_root, updated_length)) = self.pending_table_updates.get(name) {
                definition.table_root = *updated_root;
                definition.table_length = *updated_length;
            }
        }

        result
    }

    // root_page: the root of the master table
    pub(crate) fn get_table<K: Key, V: Value>(
        &self,
        name: &str,
        table_type: TableType,
    ) -> Result<Option<InternalTableDefinition>, TableError> {
        let tree = TableTree::new(
            self.tree.get_root(),
            PageHint::None,
            self.guard.clone(),
            self.mem.clone(),
        )?;
        let mut result = tree.get_table::<K, V>(name, table_type);

        if let Ok(Some(definition)) = result.as_mut() {
            if let Some((updated_root, updated_length)) = self.pending_table_updates.get(name) {
                definition.table_root = *updated_root;
                definition.table_length = *updated_length;
            }
        }

        result
    }

    // root_page: the root of the master table
    pub(crate) fn delete_table(
        &mut self,
        name: &str,
        table_type: TableType,
    ) -> Result<bool, TableError> {
        if let Some(definition) = self.get_table_untyped(name, table_type)? {
            if let Some(header) = definition.get_root() {
                let iter = AllPageNumbersBtreeIter::new(
                    header.root,
                    definition.fixed_key_size,
                    definition.fixed_value_size,
                    self.mem.clone(),
                )?;
                let mut freed_pages = self.freed_pages.lock().unwrap();
                for page_number in iter {
                    freed_pages.push(page_number?);
                }
            }

            self.pending_table_updates.remove(name);

            let found = self.tree.remove(&name)?.is_some();
            return Ok(found);
        }

        Ok(false)
    }

    // Returns a tuple of the table id and the new root page
    // root_page: the root of the master table
    pub(crate) fn get_or_create_table<K: Key, V: Value>(
        &mut self,
        name: &str,
        table_type: TableType,
    ) -> Result<InternalTableDefinition, TableError> {
        if let Some(found) = self.get_table::<K, V>(name, table_type)? {
            return Ok(found);
        }

        let table = InternalTableDefinition {
            table_root: None,
            table_type,
            table_length: 0,
            fixed_key_size: K::fixed_width(),
            fixed_value_size: V::fixed_width(),
            key_alignment: ALIGNMENT,
            value_alignment: ALIGNMENT,
            key_type: K::type_name(),
            value_type: V::type_name(),
        };
        self.tree.insert(&name, &table)?;
        Ok(table)
    }

    pub(crate) fn compact_tables(&mut self) -> Result<bool> {
        let mut progress = false;
        for entry in self.tree.range::<RangeFull, &str>(&(..))? {
            let entry = entry?;
            let mut definition = entry.value();
            if let Some((updated_root, updated_length)) =
                self.pending_table_updates.get(entry.key())
            {
                definition.table_root = *updated_root;
                definition.table_length = *updated_length;
            }

            let mut tree = UntypedBtreeMut::new(
                definition.table_root,
                self.mem.clone(),
                self.freed_pages.clone(),
                definition.fixed_key_size,
                definition.fixed_value_size,
            );
            if tree.relocate()? {
                progress = true;
                self.pending_table_updates.insert(
                    entry.key().to_string(),
                    (tree.get_root(), definition.table_length),
                );
            }
        }

        if self.tree.relocate()? {
            progress = true;
        }

        Ok(progress)
    }

    pub fn stats(&self) -> Result<DatabaseStats> {
        let master_tree_stats = self.tree.stats()?;
        let mut max_subtree_height = 0;
        let mut total_stored_bytes = 0;
        // Count the master tree leaf pages as branches, since they point to the data trees
        let mut branch_pages = master_tree_stats.branch_pages + master_tree_stats.leaf_pages;
        let mut leaf_pages = 0;
        // Include the master table in the overhead
        let mut total_metadata_bytes =
            master_tree_stats.metadata_bytes + master_tree_stats.stored_leaf_bytes;
        let mut total_fragmented = master_tree_stats.fragmented_bytes;

        for entry in self.tree.range::<RangeFull, &str>(&(..))? {
            let entry = entry?;
            let mut definition = entry.value();
            if let Some((updated_root, _)) = self.pending_table_updates.get(entry.key()) {
                definition.table_root = *updated_root;
            }
            match definition.get_type() {
                TableType::Normal => {
                    let subtree_stats = btree_stats(
                        definition.table_root.map(|x| x.root),
                        &self.mem,
                        definition.fixed_key_size,
                        definition.fixed_value_size,
                    )?;
                    max_subtree_height = max(max_subtree_height, subtree_stats.tree_height);
                    total_stored_bytes += subtree_stats.stored_leaf_bytes;
                    total_metadata_bytes += subtree_stats.metadata_bytes;
                    total_fragmented += subtree_stats.fragmented_bytes;
                    branch_pages += subtree_stats.branch_pages;
                    leaf_pages += subtree_stats.leaf_pages;
                }
                TableType::Multimap => {
                    let subtree_stats = multimap_btree_stats(
                        definition.table_root.map(|x| x.root),
                        &self.mem,
                        definition.fixed_key_size,
                        definition.fixed_value_size,
                    )?;
                    max_subtree_height = max(max_subtree_height, subtree_stats.tree_height);
                    total_stored_bytes += subtree_stats.stored_leaf_bytes;
                    total_metadata_bytes += subtree_stats.metadata_bytes;
                    total_fragmented += subtree_stats.fragmented_bytes;
                    branch_pages += subtree_stats.branch_pages;
                    leaf_pages += subtree_stats.leaf_pages;
                }
            }
        }
        Ok(DatabaseStats {
            tree_height: master_tree_stats.tree_height + max_subtree_height,
            allocated_pages: self.mem.count_allocated_pages()?,
            leaf_pages,
            branch_pages,
            stored_leaf_bytes: total_stored_bytes,
            metadata_bytes: total_metadata_bytes,
            fragmented_bytes: total_fragmented,
            page_size: self.mem.get_page_size(),
        })
    }
}

#[cfg(test)]
mod test {
    use crate::tree_store::{InternalTableDefinition, TableType};
    use crate::types::TypeName;
    use crate::Value;

    #[test]
    fn round_trip() {
        let x = InternalTableDefinition {
            table_root: None,
            table_type: TableType::Multimap,
            table_length: 0,
            fixed_key_size: None,
            fixed_value_size: Some(5),
            key_alignment: 6,
            value_alignment: 7,
            key_type: TypeName::new("test::Key"),
            value_type: TypeName::new("test::Value"),
        };
        let y = InternalTableDefinition::from_bytes(InternalTableDefinition::as_bytes(&x).as_ref());
        assert_eq!(x, y);
    }
}
