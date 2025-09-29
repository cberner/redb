use crate::db::TransactionGuard;
use crate::error::TableError;
use crate::multimap_table::{
    finalize_tree_and_subtree_checksums, multimap_btree_stats, verify_tree_and_subtree_checksums,
};
use crate::tree_store::btree::{UntypedBtreeMut, btree_stats};
use crate::tree_store::btree_base::BtreeHeader;
use crate::tree_store::{
    Btree, BtreeMut, BtreeRangeIter, InternalTableDefinition, PageHint, PageNumber, PagePath,
    PageTrackerPolicy, RawBtree, TableType, TransactionalMemory,
};
use crate::types::{Key, Value};
use crate::{DatabaseStats, Result};
use std::cmp::max;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem::size_of;
use std::ops::RangeFull;
use std::sync::Arc;
use std::{mem, thread};
use crate::mutex::Mutex;

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct PageListMut {
    data: [u8],
}

impl PageListMut {
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
    mem: Arc<TransactionalMemory>,
}

impl TableTree {
    pub(crate) fn new(
        master_root: Option<BtreeHeader>,
        page_hint: PageHint,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
    ) -> Result<Self> {
        Ok(Self {
            tree: Btree::new(master_root, page_hint, guard, mem.clone())?,
            mem,
        })
    }

    pub(crate) fn transaction_guard(&self) -> &Arc<TransactionGuard> {
        self.tree.transaction_guard()
    }

    pub(crate) fn verify_checksums(&self) -> Result<bool> {
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
                    if let Some(header) = table_root
                        && !RawBtree::new(
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
}

pub(crate) struct TableTreeMut<'txn> {
    tree: BtreeMut<'txn, &'static str, InternalTableDefinition>,
    guard: Arc<TransactionGuard>,
    mem: Arc<TransactionalMemory>,
    // Cached updates from tables that have been closed. These must be flushed to the btree
    pending_table_updates: HashMap<String, (Option<BtreeHeader>, u64)>,
    freed_pages: Arc<Mutex<Vec<PageNumber>>>,
    allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
}

impl TableTreeMut<'_> {
    pub(crate) fn new(
        master_root: Option<BtreeHeader>,
        guard: Arc<TransactionGuard>,
        mem: Arc<TransactionalMemory>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        allocated_pages: Arc<Mutex<PageTrackerPolicy>>,
    ) -> Self {
        Self {
            tree: BtreeMut::new(
                master_root,
                guard.clone(),
                mem.clone(),
                freed_pages.clone(),
                allocated_pages.clone(),
            ),
            guard,
            mem,
            pending_table_updates: Default::default(),
            freed_pages,
            allocated_pages,
        }
    }

    pub(crate) fn set_root(&mut self, root: Option<BtreeHeader>) {
        self.tree.set_root(root);
    }

    #[cfg_attr(not(debug_assertions), expect(dead_code))]
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

    pub(crate) fn clear_root_updates_and_close(&mut self) {
        self.pending_table_updates.clear();
        self.allocated_pages.lock().close();
    }

    pub(crate) fn flush_and_close(
        &mut self,
    ) -> Result<(Option<BtreeHeader>, HashSet<PageNumber>, Vec<PageNumber>)> {
        match self.flush_inner() {
            Ok(header) => {
                let allocated = self.allocated_pages.lock().close();
                let mut old = vec![];
                let mut freed_pages = self.freed_pages.lock();
                mem::swap(freed_pages.as_mut(), &mut old);
                Ok((header, allocated, old))
            }
            Err(err) => {
                // Ensure that the allocated pages get clear. Otherwise it will cause a panic
                // when they are dropped
                self.allocated_pages.lock().close();
                Err(err)
            }
        }
    }

    fn flush_inner(&mut self) -> Result<Option<BtreeHeader>> {
        self.flush_table_root_updates()?.finalize_dirty_checksums()
    }

    pub(crate) fn flush_table_root_updates(&mut self) -> Result<&mut Self> {
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
                    *table_root = tree.finalize_dirty_checksums()?;
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
        Ok(self)
    }

    // Opens a table, calls the provided closure to insert entries into it, and then
    // flushes the table root. The flush is done using insert_inplace(), so it's guaranteed
    // that no pages will be allocated or freed after the closure returns
    pub(crate) fn open_table_and_flush_table_root<K: Key + 'static, V: Value + 'static>(
        &mut self,
        name: &str,
        f: impl FnOnce(&mut BtreeMut<K, V>) -> Result,
    ) -> Result {
        assert!(self.pending_table_updates.is_empty());

        // Reserve space in the table tree
        // TODO: maybe we should have a more explicit method, like "force_uncommitted()"
        let existing = self
            .tree
            .insert(
                &name,
                &InternalTableDefinition::new::<K, V>(TableType::Normal, None, 0),
            )?
            .map(|x| x.value());
        if let Some(existing) = existing {
            self.tree.insert(&name, &existing)?;
        }

        let table_root = match self.tree.get(&name)?.unwrap().value() {
            InternalTableDefinition::Normal { table_root, .. } => table_root,
            InternalTableDefinition::Multimap { .. } => {
                unreachable!()
            }
        };

        // Open the table and call the provided closure on it
        let mut tree: BtreeMut<K, V> = BtreeMut::new(
            table_root,
            self.guard.clone(),
            self.mem.clone(),
            self.freed_pages.clone(),
            self.allocated_pages.clone(),
        );
        f(&mut tree)?;

        // Finalize the table's checksums
        let table_root = tree.finalize_dirty_checksums()?;
        let table_length = tree.get_root().map(|x| x.length).unwrap_or_default();

        // Flush the root to the table tree, without allocating
        self.tree.insert_inplace(
            &name,
            &InternalTableDefinition::new::<K, V>(TableType::Normal, table_root, table_length),
        )?;

        Ok(())
    }

    // Creates a new table, calls the provided closure to insert entries into it, and then
    // flushes the table root. The flush is done using insert_inplace(), so it's guaranteed
    // that no pages will be allocated or freed after the closure returns
    pub(crate) fn create_table_and_flush_table_root<K: Key + 'static, V: Value + 'static>(
        &mut self,
        name: &str,
        f: impl FnOnce(&mut Self, &mut BtreeMut<K, V>) -> Result,
    ) -> Result {
        assert!(self.pending_table_updates.is_empty());
        assert!(self.tree.get(&name)?.is_none());

        // Reserve space in the table tree
        self.tree.insert(
            &name,
            &InternalTableDefinition::new::<K, V>(TableType::Normal, None, 0),
        )?;

        // Create an empty table and call the provided closure on it
        let mut tree: BtreeMut<K, V> = BtreeMut::new(
            None,
            self.guard.clone(),
            self.mem.clone(),
            self.freed_pages.clone(),
            self.allocated_pages.clone(),
        );
        f(self, &mut tree)?;

        // Finalize the table's checksums
        let table_root = tree.finalize_dirty_checksums()?;
        let table_length = tree.get_root().map(|x| x.length).unwrap_or_default();

        // Flush the root to the table tree, without allocating
        self.tree.insert_inplace(
            &name,
            &InternalTableDefinition::new::<K, V>(TableType::Normal, table_root, table_length),
        )?;

        Ok(())
    }

    pub(crate) fn finalize_dirty_checksums(&mut self) -> Result<Option<BtreeHeader>> {
        self.tree.finalize_dirty_checksums()
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

        if let Ok(Some(definition)) = result.as_mut()
            && let Some((updated_root, updated_length)) = self.pending_table_updates.get(name)
        {
            definition.set_header(*updated_root, *updated_length);
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

        if let Ok(Some(definition)) = result.as_mut()
            && let Some((updated_root, updated_length)) = self.pending_table_updates.get(name)
        {
            definition.set_header(*updated_root, *updated_length);
        }

        result
    }

    pub(crate) fn rename_table(
        &mut self,
        name: &str,
        new_name: &str,
        table_type: TableType,
    ) -> Result<(), TableError> {
        if let Some(definition) = self.get_table_untyped(name, table_type)? {
            if self.get_table_untyped(new_name, table_type)?.is_some() {
                return Err(TableError::TableExists(new_name.to_string()));
            }
            if let Some(update) = self.pending_table_updates.remove(name) {
                self.pending_table_updates
                    .insert(new_name.to_string(), update);
            }
            assert!(self.tree.remove(&name)?.is_some());
            assert!(self.tree.insert(&new_name, &definition)?.is_none());
        } else {
            return Err(TableError::TableDoesNotExist(name.to_string()));
        }

        Ok(())
    }

    pub(crate) fn delete_table(
        &mut self,
        name: &str,
        table_type: TableType,
    ) -> Result<bool, TableError> {
        if let Some(definition) = self.get_table_untyped(name, table_type)? {
            let mut freed_pages = self.freed_pages.lock();
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

    // Returns the paths to the n pages that are closest to the end of the database
    // The return value is sorted, according to path.page_number()'s Ord
    pub(crate) fn highest_index_pages(
        &self,
        n: usize,
        output: &mut BTreeMap<PageNumber, PagePath>,
    ) -> Result {
        for entry in self.tree.range::<RangeFull, &str>(&(..))? {
            let entry = entry?;
            let mut definition = entry.value();
            if let Some((updated_root, updated_length)) =
                self.pending_table_updates.get(entry.key())
            {
                definition.set_header(*updated_root, *updated_length);
            }

            definition.visit_all_pages(self.mem.clone(), |path| {
                output.insert(path.page_number(), path.clone());
                while output.len() > n {
                    output.pop_first();
                }
                Ok(())
            })?;
        }

        self.tree.visit_all_pages(|path| {
            output.insert(path.page_number(), path.clone());
            while output.len() > n {
                output.pop_first();
            }
            Ok(())
        })?;

        Ok(())
    }

    pub(crate) fn relocate_tables(
        &mut self,
        relocation_map: &HashMap<PageNumber, PageNumber>,
    ) -> Result {
        for entry in self.tree.range::<RangeFull, &str>(&(..))? {
            let entry = entry?;
            let mut definition = entry.value();
            if let Some((updated_root, updated_length)) =
                self.pending_table_updates.get(entry.key())
            {
                definition.set_header(*updated_root, *updated_length);
            }

            if let Some(new_root) = definition.relocate_tree(
                self.mem.clone(),
                self.freed_pages.clone(),
                relocation_map,
            )? {
                self.pending_table_updates.insert(
                    entry.key().to_string(),
                    (Some(new_root), definition.get_length()),
                );
            }
        }

        self.tree.relocate(relocation_map)?;

        Ok(())
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

impl Drop for TableTreeMut<'_> {
    fn drop(&mut self) {
        if thread::panicking() {
            return;
        }
        assert!(self.allocated_pages.lock().is_empty());
    }
}

#[cfg(test)]
mod test {
    use crate::Value;
    use crate::tree_store::table_tree_base::InternalTableDefinition;
    use crate::types::TypeName;

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
