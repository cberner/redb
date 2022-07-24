use crate::tree_store::btree::btree_stats;
use crate::tree_store::btree_base::Checksum;
use crate::tree_store::btree_iters::AllPageNumbersBtreeIter;
use crate::tree_store::{BtreeMut, BtreeRangeIter, PageNumber, TransactionalMemory};
use crate::types::{
    AsBytesWithLifetime, OwnedAsBytesLifetime, OwnedLifetime, RedbKey, RedbValue, WithLifetime,
};
use crate::{DatabaseStats, Error, Result};
use std::cell::RefCell;
use std::cmp::max;
use std::collections::HashMap;
use std::mem::size_of;
use std::ops::RangeFull;
use std::rc::Rc;

#[derive(Debug)]
pub(crate) struct FreedTableKey {
    pub(crate) transaction_id: u64,
    pub(crate) pagination_id: u64,
}

impl RedbValue for FreedTableKey {
    type View = OwnedLifetime<FreedTableKey>;
    type ToBytes = OwnedAsBytesLifetime<[u8; 2 * size_of::<u64>()]>;

    fn fixed_width() -> Option<usize> {
        Some(2 * size_of::<u64>())
    }

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        let transaction_id = u64::from_le_bytes(data[..size_of::<u64>()].try_into().unwrap());
        let pagination_id = u64::from_le_bytes(data[size_of::<u64>()..].try_into().unwrap());
        Self {
            transaction_id,
            pagination_id,
        }
    }

    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
        let mut result = [0u8; 2 * size_of::<u64>()];
        result[..size_of::<u64>()].copy_from_slice(&self.transaction_id.to_le_bytes());
        result[size_of::<u64>()..].copy_from_slice(&self.pagination_id.to_le_bytes());
        result
    }

    fn redb_type_name() -> String {
        "FreedTableKey".to_string()
    }
}

impl RedbKey for FreedTableKey {
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

#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug)]
pub(crate) enum TableType {
    Normal,
    Multimap,
}

#[allow(clippy::from_over_into)]
impl Into<u8> for TableType {
    fn into(self) -> u8 {
        match self {
            TableType::Normal => 1,
            TableType::Multimap => 2,
        }
    }
}

impl From<u8> for TableType {
    fn from(value: u8) -> Self {
        match value {
            1 => TableType::Normal,
            2 => TableType::Multimap,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct InternalTableDefinition {
    table_root: Option<(PageNumber, Checksum)>,
    table_type: TableType,
    fixed_key_size: Option<usize>,
    fixed_value_size: Option<usize>,
    key_type: String,
    value_type: String,
}

impl InternalTableDefinition {
    pub(crate) fn get_root(&self) -> Option<(PageNumber, Checksum)> {
        self.table_root
    }

    pub(crate) fn get_fixed_key_size(&self) -> Option<usize> {
        self.fixed_key_size
    }

    pub(crate) fn get_fixed_value_size(&self) -> Option<usize> {
        self.fixed_value_size
    }

    pub(crate) fn get_type(&self) -> TableType {
        self.table_type
    }
}

impl RedbValue for InternalTableDefinition {
    type View = OwnedLifetime<InternalTableDefinition>;
    type ToBytes = OwnedAsBytesLifetime<Vec<u8>>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        debug_assert!(data.len() > 22);
        let mut offset = 0;
        let table_type = TableType::from(data[offset]);
        offset += 1;

        let non_null = data[offset] != 0;
        offset += 1;
        let table_root = if non_null {
            let table_root = PageNumber::from_le_bytes(
                data[offset..(offset + PageNumber::serialized_size())]
                    .try_into()
                    .unwrap(),
            );
            offset += PageNumber::serialized_size();
            let checksum = Checksum::from_le_bytes(
                data[offset..(offset + size_of::<Checksum>())]
                    .try_into()
                    .unwrap(),
            );
            offset += size_of::<Checksum>();
            Some((table_root, checksum))
        } else {
            offset += PageNumber::serialized_size();
            offset += size_of::<Checksum>();
            None
        };

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

        let key_type_len = u32::from_le_bytes(
            data[offset..(offset + size_of::<u32>())]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += size_of::<u32>();
        let key_type = std::str::from_utf8(&data[offset..(offset + key_type_len)])
            .unwrap()
            .to_string();
        offset += key_type_len;
        let value_type = std::str::from_utf8(&data[offset..]).unwrap().to_string();

        InternalTableDefinition {
            table_root,
            table_type,
            fixed_key_size,
            fixed_value_size,
            key_type,
            value_type,
        }
    }

    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
        let mut result = vec![self.table_type.into()];
        if let Some((root, checksum)) = self.table_root {
            result.push(1);
            result.extend_from_slice(&root.to_le_bytes());
            result.extend_from_slice(&checksum.to_le_bytes());
        } else {
            result.push(0);
            result.extend_from_slice(&[0; PageNumber::serialized_size()]);
            result.extend_from_slice(&[0; size_of::<Checksum>()]);
        }
        if let Some(fixed) = self.fixed_key_size {
            result.push(1);
            result.extend_from_slice(&(fixed as u32).to_le_bytes());
        } else {
            result.push(0);
            result.extend_from_slice(&[0; size_of::<u32>()])
        }
        if let Some(fixed) = self.fixed_value_size {
            result.push(1);
            result.extend_from_slice(&(fixed as u32).to_le_bytes());
        } else {
            result.push(0);
            result.extend_from_slice(&[0; size_of::<u32>()])
        }
        result.extend_from_slice(&(self.key_type.as_bytes().len() as u32).to_le_bytes());
        result.extend_from_slice(self.key_type.as_bytes());
        result.extend_from_slice(self.value_type.as_bytes());

        result
    }

    fn redb_type_name() -> String {
        "InternalTableDefinition".to_string()
    }
}

pub struct TableNameIter<'a> {
    inner: BtreeRangeIter<'a, str, InternalTableDefinition>,
    table_type: TableType,
}

impl<'a> Iterator for TableNameIter<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.inner.next() {
            if InternalTableDefinition::from_bytes(entry.value()).table_type == self.table_type {
                return Some(str::from_bytes(entry.key()).to_string());
            }
        }
        None
    }
}

pub(crate) struct TableTree<'txn> {
    tree: BtreeMut<'txn, str, InternalTableDefinition>,
    mem: &'txn TransactionalMemory,
    // Cached updates from tables that have been closed. These must be flushed to the btree
    pending_table_updates: HashMap<String, Option<(PageNumber, Checksum)>>,
    freed_pages: Rc<RefCell<Vec<PageNumber>>>,
}

impl<'txn> TableTree<'txn> {
    pub(crate) fn new(
        master_root: Option<(PageNumber, Checksum)>,
        mem: &'txn TransactionalMemory,
        freed_pages: Rc<RefCell<Vec<PageNumber>>>,
    ) -> Self {
        Self {
            tree: BtreeMut::new(master_root, mem, freed_pages.clone()),
            mem,
            pending_table_updates: Default::default(),
            freed_pages,
        }
    }

    // Queues an update to the table root
    pub(crate) fn stage_update_table_root(
        &mut self,
        name: &str,
        table_root: Option<(PageNumber, Checksum)>,
    ) {
        self.pending_table_updates
            .insert(name.to_string(), table_root);
    }

    pub(crate) fn clear_table_root_updates(&mut self) {
        self.pending_table_updates.clear();
    }

    pub(crate) fn flush_table_root_updates(&mut self) -> Result<Option<(PageNumber, Checksum)>> {
        for (name, table_root) in self.pending_table_updates.drain() {
            // Bypass .get_table() since the table types are dynamic
            // TODO: optimize away this get()
            let mut definition = self.tree.get(&name).unwrap().unwrap();
            // No-op if the root has not changed
            if definition.table_root == table_root {
                continue;
            }
            definition.table_root = table_root;
            // Safety: References into the master table are never returned to the user
            unsafe {
                self.tree.insert(&name, &definition)?;
            }
        }
        Ok(self.tree.get_root())
    }

    // root_page: the root of the master table
    pub(crate) fn list_tables(&self, table_type: TableType) -> Result<Vec<String>> {
        let iter = self.tree.range::<RangeFull, &str>(..)?;
        let iter = TableNameIter {
            inner: iter,
            table_type,
        };
        Ok(iter.collect())
    }

    // root_page: the root of the master table
    pub(crate) fn get_table<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &self,
        name: &str,
        table_type: TableType,
    ) -> Result<Option<InternalTableDefinition>> {
        if let Some(mut definition) = self.tree.get(name)? {
            if definition.get_type() != table_type {
                return Err(Error::TableTypeMismatch(format!(
                    "{:?} is not of type {:?}",
                    name, table_type
                )));
            }
            if definition.key_type != K::redb_type_name()
                || definition.value_type != V::redb_type_name()
            {
                return Err(Error::TableTypeMismatch(format!(
                    "{} is of type Table<{}, {}> not Table<{}, {}>",
                    name,
                    &definition.key_type,
                    &definition.value_type,
                    K::redb_type_name(),
                    V::redb_type_name()
                )));
            }

            if let Some(updated_root) = self.pending_table_updates.get(name) {
                definition.table_root = *updated_root;
            }

            Ok(Some(definition))
        } else {
            Ok(None)
        }
    }

    // root_page: the root of the master table
    pub(crate) fn delete_table<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &mut self,
        name: &str,
        table_type: TableType,
    ) -> Result<bool> {
        if let Some(definition) = self.get_table::<K, V>(name, table_type)? {
            if let Some((table_root, _)) = definition.get_root() {
                let iter = AllPageNumbersBtreeIter::new(
                    table_root,
                    K::fixed_width(),
                    V::fixed_width(),
                    self.mem,
                );
                let mut freed_pages = self.freed_pages.borrow_mut();
                for page_number in iter {
                    freed_pages.push(page_number);
                }
            }

            self.pending_table_updates.remove(name);

            // Safety: References into the master table are never returned to the user
            let found = unsafe { self.tree.remove(name)?.is_some() };
            return Ok(found);
        }

        Ok(false)
    }

    // Returns a tuple of the table id and the new root page
    // root_page: the root of the master table
    pub(crate) fn get_or_create_table<K: RedbKey + ?Sized, V: RedbValue + ?Sized>(
        &mut self,
        name: &str,
        table_type: TableType,
    ) -> Result<InternalTableDefinition> {
        if let Some(found) = self.get_table::<K, V>(name, table_type)? {
            return Ok(found);
        }

        let table = InternalTableDefinition {
            table_root: None,
            table_type,
            fixed_key_size: K::fixed_width(),
            fixed_value_size: V::fixed_width(),
            key_type: K::redb_type_name(),
            value_type: V::redb_type_name(),
        };
        // Safety: References into the master table are never returned to the user
        unsafe { self.tree.insert(name, &table)? };
        Ok(table)
    }

    pub fn stats(&self) -> Result<DatabaseStats> {
        let master_tree_stats = self.tree.stats();
        let mut max_subtree_height = 0;
        let mut total_stored_bytes = 0;
        // Count the master tree leaf pages as branches, since they point to the data trees
        let mut branch_pages = master_tree_stats.branch_pages + master_tree_stats.leaf_pages;
        let mut leaf_pages = 0;
        // Include the master table in the overhead
        let mut total_metadata_bytes =
            master_tree_stats.metadata_bytes + master_tree_stats.stored_leaf_bytes;
        let mut total_fragmented = master_tree_stats.fragmented_bytes;

        let mut iter = self.tree.range::<RangeFull, &str>(..)?;
        while let Some(entry) = iter.next() {
            let mut definition = InternalTableDefinition::from_bytes(entry.value());
            if let Some(updated_root) = self.pending_table_updates.get(str::from_bytes(entry.key()))
            {
                definition.table_root = *updated_root;
            }
            let subtree_stats = btree_stats(
                definition.table_root.map(|(p, _)| p),
                self.mem,
                definition.fixed_key_size,
                definition.fixed_value_size,
            );
            max_subtree_height = max(max_subtree_height, subtree_stats.tree_height);
            total_stored_bytes += subtree_stats.stored_leaf_bytes;
            total_metadata_bytes += subtree_stats.metadata_bytes;
            total_fragmented += subtree_stats.fragmented_bytes;
            branch_pages += subtree_stats.branch_pages;
            leaf_pages += subtree_stats.leaf_pages;
        }
        Ok(DatabaseStats {
            tree_height: master_tree_stats.tree_height + max_subtree_height,
            free_pages: self.mem.count_free_pages()?,
            leaf_pages,
            branch_pages,
            stored_leaf_bytes: total_stored_bytes,
            metadata_bytes: total_metadata_bytes,
            fragmented_bytes: total_fragmented,
            page_size: self.mem.get_page_size(),
        })
    }
}
