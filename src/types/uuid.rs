use crate::{Key, TypeName, Value};
use std::cmp::Ordering;
use uuid::Uuid;

impl Value for Uuid {
    type SelfType<'a>
        = Uuid
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a uuid::Bytes
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(16)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Uuid::from_slice(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.as_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("uuid::Uuid")
    }
}

impl Key for Uuid {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Database, Key, ReadableDatabase, TableDefinition, Value};
    use tempfile::NamedTempFile;
    use uuid::Uuid;

    const UUID_TABLE: TableDefinition<Uuid, Uuid> = TableDefinition::new("table");

    #[test]
    fn test_uuid_ordering() {
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let bytes1 = <Uuid as Value>::as_bytes(&uuid1);
        let bytes2 = <Uuid as Value>::as_bytes(&uuid2);
        assert_eq!(
            uuid1.cmp(&uuid2),
            Uuid::compare(bytes1.as_slice(), bytes2.as_slice())
        );
    }

    #[test]
    fn test_uuid_table() {
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let db = Database::create(NamedTempFile::new().unwrap()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(UUID_TABLE).unwrap();
            table.insert(uuid1, uuid2).unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        {
            let table = read_txn.open_table(UUID_TABLE).unwrap();
            let value = table.get(&uuid1).unwrap().unwrap();
            assert_eq!(value.value(), uuid2);
        }
    }
}
