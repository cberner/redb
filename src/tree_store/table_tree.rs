use crate::db::TransactionGuard;
use crate::error::TableError;
use crate::multimap_table::{
    finalize_tree_and_subtree_checksums, multimap_btree_stats, verify_tree_and_subtree_checksums,
};
use crate::tree_store::btree::{btree_stats, UntypedBtreeMut};
use crate::tree_store::btree_base::BtreeHeader;
use crate::tree_store::page_store::{new_allocators, BuddyAllocator};
use crate::tree_store::{
    Btree, BtreeMut, BtreeRangeIter, InternalTableDefinition, PageHint, PageNumber, PagePath,
    RawBtree, TableType, TransactionalMemory,
};
use crate::types::{Key, MutInPlaceValue, TypeName, Value};
use crate::{DatabaseStats, Result};
use std::cmp::max;
use std::collections::HashMap;
use std::mem;
use std::mem::size_of;
use std::ops::RangeFull;
use std::sync::{Arc, Mutex};

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
                    if entry.value().get_type() == self.table_type {
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

    pub(crate) fn transaction_guard(&self) -> &Arc<TransactionGuard> {
        self.tree.transaction_guard()
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
            definition.check_match_untyped(table_type, name)?;
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
                definition.check_match::<K, V>(table_type, name)?;
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

    pub(crate) fn all_referenced_pages(&self) -> Result<Vec<BuddyAllocator>> {
        let mut result = new_allocators(self.mem.get_layout());

        self.visit_all_pages(|path| {
            let page = path.page_number();
            result[page.region as usize].record_alloc(page.page_index, page.page_order);
            Ok(())
        })?;

        Ok(result)
    }

    pub(crate) fn visit_all_pages<F>(&self, mut visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result,
    {
        // All the pages in the table tree itself
        self.tree.visit_all_pages(&mut visitor)?;

        // All the normal tables
        for entry in self.list_tables(TableType::Normal)? {
            let definition = self
                .get_table_untyped(&entry, TableType::Normal)
                .map_err(|e| e.into_storage_error_or_corrupted("Internal corruption"))?
                .unwrap();
            definition.visit_all_pages(self.mem.clone(), |path| visitor(path))?;
        }

        for entry in self.list_tables(TableType::Multimap)? {
            let definition = self
                .get_table_untyped(&entry, TableType::Multimap)
                .map_err(|e| e.into_storage_error_or_corrupted("Internal corruption"))?
                .unwrap();
            definition.visit_all_pages(self.mem.clone(), |path| visitor(path))?;
        }

        Ok(())
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
            match definition {
                InternalTableDefinition::Normal {
                    table_root,
                    fixed_key_size,
                    fixed_value_size,
                    ..
                } => {
                    if let Some(header) = table_root {
                        if !RawBtree::new(
                            Some(header),
                            fixed_key_size,
                            fixed_value_size,
                            self.mem.clone(),
                        )
                        .verify_checksum()?
                        {
                            return Ok(false);
                        }
                    }
                }
                InternalTableDefinition::Multimap {
                    table_root,
                    fixed_key_size,
                    fixed_value_size,
                    ..
                } => {
                    if !verify_tree_and_subtree_checksums(
                        table_root,
                        fixed_key_size,
                        fixed_value_size,
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
        for (name, (new_root, new_length)) in self.pending_table_updates.drain() {
            // Bypass .get_table() since the table types are dynamic
            let mut definition = self.tree.get(&name.as_str())?.unwrap().value();
            // No-op if the root has not changed
            match definition {
                InternalTableDefinition::Normal { table_root, .. }
                | InternalTableDefinition::Multimap { table_root, .. } => {
                    if table_root == new_root {
                        continue;
                    }
                }
            }
            // Finalize any dirty checksums
            match definition {
                InternalTableDefinition::Normal {
                    ref mut table_root,
                    ref mut table_length,
                    fixed_key_size,
                    fixed_value_size,
                    ..
                } => {
                    let mut tree = UntypedBtreeMut::new(
                        new_root,
                        self.mem.clone(),
                        self.freed_pages.clone(),
                        fixed_key_size,
                        fixed_value_size,
                    );
                    tree.finalize_dirty_checksums()?;
                    *table_root = tree.get_root();
                    *table_length = new_length;
                }
                InternalTableDefinition::Multimap {
                    ref mut table_root,
                    ref mut table_length,
                    fixed_key_size,
                    fixed_value_size,
                    ..
                } => {
                    *table_root = finalize_tree_and_subtree_checksums(
                        new_root,
                        fixed_key_size,
                        fixed_value_size,
                        self.mem.clone(),
                    )?;
                    *table_length = new_length;
                }
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
                definition.set_header(*updated_root, *updated_length);
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
                definition.set_header(*updated_root, *updated_length);
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
            let mut freed_pages = self.freed_pages.lock().unwrap();
            definition.visit_all_pages(self.mem.clone(), |path| {
                freed_pages.push(path.page_number());
                Ok(())
            })?;
            drop(freed_pages);

            self.pending_table_updates.remove(name);

            let found = self.tree.remove(&name)?.is_some();
            return Ok(found);
        }

        Ok(false)
    }

    pub(crate) fn get_or_create_table<K: Key, V: Value>(
        &mut self,
        name: &str,
        table_type: TableType,
    ) -> Result<(Option<BtreeHeader>, u64), TableError> {
        let table = if let Some(found) = self.get_table::<K, V>(name, table_type)? {
            found
        } else {
            let table = InternalTableDefinition::new::<K, V>(table_type, None, 0);
            self.tree.insert(&name, &table)?;
            table
        };

        match table {
            InternalTableDefinition::Normal {
                table_root,
                table_length,
                ..
            }
            | InternalTableDefinition::Multimap {
                table_root,
                table_length,
                ..
            } => Ok((table_root, table_length)),
        }
    }

    pub(crate) fn compact_tables(&mut self) -> Result<bool> {
        let mut progress = false;
        for entry in self.tree.range::<RangeFull, &str>(&(..))? {
            let entry = entry?;
            let mut definition = entry.value();
            if let Some((updated_root, updated_length)) =
                self.pending_table_updates.get(entry.key())
            {
                definition.set_header(*updated_root, *updated_length);
            }

            if let Some(new_root) =
                definition.relocate_tree(self.mem.clone(), self.freed_pages.clone())?
            {
                progress = true;
                self.pending_table_updates
                    .insert(entry.key().to_string(), (new_root, definition.get_length()));
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
            if let Some((updated_root, length)) = self.pending_table_updates.get(entry.key()) {
                definition.set_header(*updated_root, *length);
            }
            match definition {
                InternalTableDefinition::Normal {
                    table_root,
                    fixed_key_size,
                    fixed_value_size,
                    ..
                } => {
                    let subtree_stats = btree_stats(
                        table_root.map(|x| x.root),
                        &self.mem,
                        fixed_key_size,
                        fixed_value_size,
                    )?;
                    max_subtree_height = max(max_subtree_height, subtree_stats.tree_height);
                    total_stored_bytes += subtree_stats.stored_leaf_bytes;
                    total_metadata_bytes += subtree_stats.metadata_bytes;
                    total_fragmented += subtree_stats.fragmented_bytes;
                    branch_pages += subtree_stats.branch_pages;
                    leaf_pages += subtree_stats.leaf_pages;
                }
                InternalTableDefinition::Multimap {
                    table_root,
                    fixed_key_size,
                    fixed_value_size,
                    ..
                } => {
                    let subtree_stats = multimap_btree_stats(
                        table_root.map(|x| x.root),
                        &self.mem,
                        fixed_key_size,
                        fixed_value_size,
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
    use crate::tree_store::table_tree_base::InternalTableDefinition;
    use crate::types::TypeName;
    use crate::Value;

    #[test]
    fn round_trip() {
        let x = InternalTableDefinition::Multimap {
            table_root: None,
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
