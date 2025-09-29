use crate::multimap_table::{UntypedMultiBtree, relocate_subtrees};
use crate::tree_store::{
    BtreeHeader, PageNumber, PagePath, TransactionalMemory, UntypedBtree, UntypedBtreeMut,
};
use crate::{Key, Result, TableError, TypeName, Value};
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use crate::mutex::Mutex;

// Forward compatibility feature in case alignment can be supported in the future
// See https://github.com/cberner/redb/issues/360
const ALIGNMENT: usize = 1;

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
pub(crate) enum InternalTableDefinition {
    Normal {
        table_root: Option<BtreeHeader>,
        table_length: u64,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        key_alignment: usize,
        value_alignment: usize,
        key_type: TypeName,
        value_type: TypeName,
    },
    Multimap {
        table_root: Option<BtreeHeader>,
        table_length: u64,
        fixed_key_size: Option<usize>,
        fixed_value_size: Option<usize>,
        key_alignment: usize,
        value_alignment: usize,
        key_type: TypeName,
        value_type: TypeName,
    },
}

impl InternalTableDefinition {
    pub(super) fn new<K: Key, V: Value>(
        table_type: TableType,
        table_root: Option<BtreeHeader>,
        table_length: u64,
    ) -> Self {
        match table_type {
            TableType::Normal => InternalTableDefinition::Normal {
                table_root,
                table_length,
                fixed_key_size: K::fixed_width(),
                fixed_value_size: V::fixed_width(),
                key_alignment: ALIGNMENT,
                value_alignment: ALIGNMENT,
                key_type: K::type_name(),
                value_type: V::type_name(),
            },
            TableType::Multimap => InternalTableDefinition::Multimap {
                table_root,
                table_length,
                fixed_key_size: K::fixed_width(),
                fixed_value_size: V::fixed_width(),
                key_alignment: ALIGNMENT,
                value_alignment: ALIGNMENT,
                key_type: K::type_name(),
                value_type: V::type_name(),
            },
        }
    }

    pub(super) fn set_header(&mut self, root: Option<BtreeHeader>, length: u64) {
        match self {
            InternalTableDefinition::Normal {
                table_root,
                table_length,
                ..
            }
            | InternalTableDefinition::Multimap {
                table_root,
                table_length,
                ..
            } => {
                *table_root = root;
                *table_length = length;
            }
        }
    }

    pub(super) fn check_match_untyped(
        &self,
        table_type: TableType,
        name: &str,
    ) -> Result<(), TableError> {
        if self.get_type() != table_type {
            return if self.get_type() == TableType::Multimap {
                Err(TableError::TableIsMultimap(name.to_string()))
            } else {
                Err(TableError::TableIsNotMultimap(name.to_string()))
            };
        }
        if self.private_get_key_alignment() != ALIGNMENT {
            return Err(TableError::TypeDefinitionChanged {
                name: self.private_key_type(),
                alignment: self.private_get_key_alignment(),
                width: self.private_get_fixed_key_size(),
            });
        }
        if self.private_get_value_alignment() != ALIGNMENT {
            return Err(TableError::TypeDefinitionChanged {
                name: self.private_value_type(),
                alignment: self.private_get_value_alignment(),
                width: self.private_get_fixed_value_size(),
            });
        }

        Ok(())
    }

    pub(super) fn check_match<K: Key, V: Value>(
        &self,
        table_type: TableType,
        name: &str,
    ) -> Result<(), TableError> {
        self.check_match_untyped(table_type, name)?;

        if self.private_key_type() != K::type_name() || self.private_value_type() != V::type_name()
        {
            return Err(TableError::TableTypeMismatch {
                table: name.to_string(),
                key: self.private_key_type(),
                value: self.private_value_type(),
            });
        }
        if self.private_get_fixed_key_size() != K::fixed_width() {
            return Err(TableError::TypeDefinitionChanged {
                name: K::type_name(),
                alignment: self.private_get_key_alignment(),
                width: self.private_get_fixed_key_size(),
            });
        }
        if self.private_get_fixed_value_size() != V::fixed_width() {
            return Err(TableError::TypeDefinitionChanged {
                name: V::type_name(),
                alignment: self.private_get_value_alignment(),
                width: self.private_get_fixed_value_size(),
            });
        }

        Ok(())
    }

    pub(crate) fn visit_all_pages<'a, F>(&self, mem: Arc<TransactionalMemory>, visitor: F) -> Result
    where
        F: FnMut(&PagePath) -> Result + 'a,
    {
        match self {
            InternalTableDefinition::Normal {
                table_root,
                fixed_key_size,
                fixed_value_size,
                ..
            } => {
                let tree = UntypedBtree::new(*table_root, mem, *fixed_key_size, *fixed_value_size);
                tree.visit_all_pages(visitor)?;
            }
            InternalTableDefinition::Multimap {
                table_root,
                fixed_key_size,
                fixed_value_size,
                ..
            } => {
                let tree =
                    UntypedMultiBtree::new(*table_root, mem, *fixed_key_size, *fixed_value_size);
                tree.visit_all_pages(visitor)?;
            }
        }

        Ok(())
    }

    pub(crate) fn relocate_tree(
        &mut self,
        mem: Arc<TransactionalMemory>,
        freed_pages: Arc<Mutex<Vec<PageNumber>>>,
        relocation_map: &HashMap<PageNumber, PageNumber>,
    ) -> Result<Option<BtreeHeader>> {
        let original_root = self.private_get_root();
        let relocated_root = match self {
            InternalTableDefinition::Normal { table_root, .. } => *table_root,
            InternalTableDefinition::Multimap {
                table_root,
                fixed_key_size,
                fixed_value_size,
                ..
            } => {
                if let Some(header) = table_root {
                    let (page_number, checksum) = relocate_subtrees(
                        (header.root, header.checksum),
                        *fixed_key_size,
                        *fixed_value_size,
                        mem.clone(),
                        freed_pages.clone(),
                        relocation_map,
                    )?;
                    Some(BtreeHeader::new(page_number, checksum, header.length))
                } else {
                    None
                }
            }
        };
        let mut tree = UntypedBtreeMut::new(
            relocated_root,
            mem,
            freed_pages,
            self.private_get_fixed_key_size(),
            self.private_get_fixed_value_size(),
        );
        tree.relocate(relocation_map)?;
        if tree.get_root() != original_root {
            self.set_header(tree.get_root(), self.get_length());
            Ok(tree.get_root())
        } else {
            Ok(None)
        }
    }

    fn private_get_root(&self) -> Option<BtreeHeader> {
        match self {
            InternalTableDefinition::Normal { table_root, .. }
            | InternalTableDefinition::Multimap { table_root, .. } => *table_root,
        }
    }

    pub(crate) fn get_length(&self) -> u64 {
        match self {
            InternalTableDefinition::Normal { table_length, .. }
            | InternalTableDefinition::Multimap { table_length, .. } => *table_length,
        }
    }

    fn private_get_fixed_key_size(&self) -> Option<usize> {
        match self {
            InternalTableDefinition::Normal { fixed_key_size, .. }
            | InternalTableDefinition::Multimap { fixed_key_size, .. } => *fixed_key_size,
        }
    }

    fn private_get_fixed_value_size(&self) -> Option<usize> {
        match self {
            InternalTableDefinition::Normal {
                fixed_value_size, ..
            }
            | InternalTableDefinition::Multimap {
                fixed_value_size, ..
            } => *fixed_value_size,
        }
    }

    fn private_get_key_alignment(&self) -> usize {
        match self {
            InternalTableDefinition::Normal { key_alignment, .. }
            | InternalTableDefinition::Multimap { key_alignment, .. } => *key_alignment,
        }
    }

    fn private_get_value_alignment(&self) -> usize {
        match self {
            InternalTableDefinition::Normal {
                value_alignment, ..
            }
            | InternalTableDefinition::Multimap {
                value_alignment, ..
            } => *value_alignment,
        }
    }

    pub(crate) fn get_type(&self) -> TableType {
        match self {
            InternalTableDefinition::Normal { .. } => TableType::Normal,
            InternalTableDefinition::Multimap { .. } => TableType::Multimap,
        }
    }

    fn private_key_type(&self) -> TypeName {
        match self {
            InternalTableDefinition::Normal { key_type, .. }
            | InternalTableDefinition::Multimap { key_type, .. } => key_type.clone(),
        }
    }

    fn private_value_type(&self) -> TypeName {
        match self {
            InternalTableDefinition::Normal { value_type, .. }
            | InternalTableDefinition::Multimap { value_type, .. } => value_type.clone(),
        }
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

        match table_type {
            TableType::Normal => InternalTableDefinition::Normal {
                table_root,
                table_length,
                fixed_key_size,
                fixed_value_size,
                key_alignment,
                value_alignment,
                key_type,
                value_type,
            },
            TableType::Multimap => InternalTableDefinition::Multimap {
                table_root,
                table_length,
                fixed_key_size,
                fixed_value_size,
                key_alignment,
                value_alignment,
                key_type,
                value_type,
            },
        }
    }

    // Be careful if you change this serialization format! The InternalTableDefinition for
    // a given table needs to have a consistent serialized length, regardless of the table
    // contents, so that create_table_and_flush_table_root() can update the allocator state
    // table without causing more allocations
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
    where
        Self: 'b,
    {
        let mut result = vec![value.get_type().into()];
        result.extend_from_slice(&value.get_length().to_le_bytes());
        if let Some(header) = value.private_get_root() {
            result.push(1);
            result.extend_from_slice(&header.to_le_bytes());
        } else {
            result.push(0);
            result.extend_from_slice(&[0; BtreeHeader::serialized_size()]);
        }
        if let Some(fixed) = value.private_get_fixed_key_size() {
            result.push(1);
            result.extend_from_slice(&u32::try_from(fixed).unwrap().to_le_bytes());
        } else {
            result.push(0);
            result.extend_from_slice(&[0; size_of::<u32>()]);
        }
        if let Some(fixed) = value.private_get_fixed_value_size() {
            result.push(1);
            result.extend_from_slice(&u32::try_from(fixed).unwrap().to_le_bytes());
        } else {
            result.push(0);
            result.extend_from_slice(&[0; size_of::<u32>()]);
        }
        result.extend_from_slice(
            &u32::try_from(value.private_get_key_alignment())
                .unwrap()
                .to_le_bytes(),
        );
        result.extend_from_slice(
            &u32::try_from(value.private_get_value_alignment())
                .unwrap()
                .to_le_bytes(),
        );
        let key_type_bytes = value.private_key_type().to_bytes();
        result.extend_from_slice(&u32::try_from(key_type_bytes.len()).unwrap().to_le_bytes());
        result.extend_from_slice(&key_type_bytes);
        result.extend_from_slice(&value.private_value_type().to_bytes());

        result
    }

    fn type_name() -> TypeName {
        TypeName::internal("redb::InternalTableDefinition")
    }
}
