use crate::tree_store::btree_iters::{page_numbers_iter_start_state, AllPageNumbersBtreeIter};
use crate::tree_store::{BtreeMut, BtreeRangeIter, PageNumber, TransactionalMemory};
use crate::types::{
    AsBytesWithLifetime, OwnedAsBytesLifetime, OwnedLifetime, RedbKey, RedbValue, WithLifetime,
};
use crate::{Error, Result};
use std::mem::size_of;
use std::ops::RangeFull;

// The table of freed pages by transaction. FreedTableKey -> binary.
// The binary blob is a length-prefixed array of PageNumber
// TODO: create a separate root in the metapage for this
pub(crate) const FREED_TABLE: &str = "$$internal$$freed";

#[derive(Debug)]
pub(crate) struct FreedTableKey {
    pub(crate) transaction_id: u64,
    pub(crate) pagination_id: u64,
}

impl RedbValue for FreedTableKey {
    type View = OwnedLifetime<FreedTableKey>;
    type ToBytes = OwnedAsBytesLifetime<[u8; 2 * size_of::<u64>()]>;

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

    fn redb_type_name() -> &'static str {
        "FreedTableKey"
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
    // TODO: make private
    pub(crate) table_root: Option<PageNumber>,
    pub(crate) table_type: TableType,
    pub(crate) key_type: String,
    pub(crate) value_type: String,
}

impl InternalTableDefinition {
    pub(crate) fn get_root(&self) -> Option<PageNumber> {
        self.table_root
    }

    pub(crate) fn get_type(&self) -> TableType {
        self.table_type
    }
}

impl RedbValue for InternalTableDefinition {
    type View = OwnedLifetime<InternalTableDefinition>;
    type ToBytes = OwnedAsBytesLifetime<Vec<u8>>;

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        debug_assert!(data.len() > 14);
        let table_type = TableType::from(data[0]);
        let table_root = PageNumber::from_le_bytes(data[1..9].try_into().unwrap());
        let table_root = if table_root == PageNumber::null() {
            None
        } else {
            Some(table_root)
        };
        let key_type_len = u32::from_le_bytes(data[9..13].try_into().unwrap()) as usize;
        let key_type = std::str::from_utf8(&data[13..(13 + key_type_len)])
            .unwrap()
            .to_string();
        let value_type = std::str::from_utf8(&data[(13 + key_type_len)..])
            .unwrap()
            .to_string();

        InternalTableDefinition {
            table_root,
            table_type,
            key_type,
            value_type,
        }
    }

    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
        let mut result = vec![self.table_type.into()];
        result.extend_from_slice(
            &self
                .table_root
                .unwrap_or_else(PageNumber::null)
                .to_le_bytes(),
        );
        result.extend_from_slice(&(self.key_type.as_bytes().len() as u32).to_le_bytes());
        result.extend_from_slice(self.key_type.as_bytes());
        result.extend_from_slice(self.value_type.as_bytes());

        result
    }

    fn redb_type_name() -> &'static str {
        "InternalTableDefinition"
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
            if str::from_bytes(entry.key()) == FREED_TABLE {
                continue;
            }
            if InternalTableDefinition::from_bytes(entry.value()).table_type == self.table_type {
                return Some(str::from_bytes(entry.key()).to_string());
            }
        }
        None
    }
}

pub(crate) struct TableTree<'txn> {
    // TODO: make private?
    pub(crate) tree: BtreeMut<'txn, str, InternalTableDefinition>,
    mem: &'txn TransactionalMemory,
    // TODO: unify with the freed pages in self.tree
    pub(crate) extra_freed_pages: Vec<PageNumber>,
}

impl<'txn> TableTree<'txn> {
    pub(crate) fn new(master_root: Option<PageNumber>, mem: &'txn TransactionalMemory) -> Self {
        Self {
            tree: BtreeMut::new(master_root, mem),
            mem,
            extra_freed_pages: vec![],
        }
    }

    pub(crate) fn update_table_root(
        &mut self,
        name: &str,
        table_root: Option<PageNumber>,
    ) -> Result {
        // Bypass .get_table() since the table types are dynamic
        // TODO: optimize way this get()
        let mut definition = self.tree.get(name).unwrap().unwrap();
        // No-op if the root has not changed
        if definition.table_root == table_root {
            return Ok(());
        }
        definition.table_root = table_root;
        // Safety: References into the master table are never returned to the user
        unsafe {
            self.tree.insert(name, &definition)?;
        }
        Ok(())
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
        if let Some(definition) = self.tree.get(name)? {
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
            if let Some(table_root) = definition.get_root() {
                let page = self.mem.get_page(table_root);
                let start = page_numbers_iter_start_state(page);
                let iter = AllPageNumbersBtreeIter::new(start, self.mem);
                for page_number in iter {
                    self.extra_freed_pages.push(page_number);
                }
            }

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
            key_type: K::redb_type_name().to_string(),
            value_type: V::redb_type_name().to_string(),
        };
        // Safety: References into the master table are never returned to the user
        unsafe { self.tree.insert(name, &table)? };
        Ok(table)
    }
}
