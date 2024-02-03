use redb::{
    Database, Error, Key, ReadableTable, Table, TableDefinition, TableHandle, Value,
    WriteTransaction,
};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;

const TABLE: TableDefinition<u64, &str> = TableDefinition::new("my_data");

struct SpecialValuesDb {
    database: Database,
    file: File,
}

impl SpecialValuesDb {
    fn new() -> Self {
        SpecialValuesDb {
            database: Database::create("index.redb").unwrap(),
            file: OpenOptions::new()
                .write(true)
                .create(true)
                .read(true)
                .open("values.dat")
                .unwrap(),
        }
    }

    fn begin_txn(&mut self) -> SpecialValuesTransaction {
        SpecialValuesTransaction {
            inner: self.database.begin_write().unwrap(),
            file: &mut self.file,
        }
    }
}

struct SpecialValuesTransaction<'db> {
    inner: WriteTransaction,
    file: &'db mut File,
}

impl<'db> SpecialValuesTransaction<'db> {
    fn open_table<K: Key + 'static, V: Value + 'static>(
        &mut self,
        table: TableDefinition<K, V>,
    ) -> SpecialValuesTable<K, V> {
        let def: TableDefinition<K, (u64, u64)> = TableDefinition::new(table.name());
        SpecialValuesTable {
            inner: self.inner.open_table(def).unwrap(),
            file: self.file,
            _value_type: Default::default(),
        }
    }

    fn commit(self) {
        self.file.sync_all().unwrap();
        self.inner.commit().unwrap();
    }
}

struct SpecialValuesTable<'txn, K: Key + 'static, V: Value + 'static> {
    inner: Table<'txn, K, (u64, u64)>,
    file: &'txn mut File,
    _value_type: PhantomData<V>,
}

impl<'txn, K: Key + 'static, V: Value + 'static> SpecialValuesTable<'txn, K, V> {
    fn insert(&mut self, key: K::SelfType<'_>, value: V::SelfType<'_>) {
        // Append to end of file
        let offset = self.file.seek(SeekFrom::End(0)).unwrap();
        let value = V::as_bytes(&value);
        self.file.write_all(value.as_ref()).unwrap();
        self.inner
            .insert(key, (offset, value.as_ref().len() as u64))
            .unwrap();
    }

    fn get(&mut self, key: K::SelfType<'_>) -> ValueAccessor<V> {
        let (offset, length) = self.inner.get(key).unwrap().unwrap().value();
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        let mut data = vec![0u8; length as usize];
        self.file.read_exact(data.as_mut_slice()).unwrap();
        ValueAccessor {
            data,
            _value_type: Default::default(),
        }
    }
}

struct ValueAccessor<V: Value + 'static> {
    data: Vec<u8>,
    _value_type: PhantomData<V>,
}

impl<V: Value + 'static> ValueAccessor<V> {
    fn value(&self) -> V::SelfType<'_> {
        V::from_bytes(&self.data)
    }
}

/// redb is not designed to support very large values, or values with special requirements (such as alignment or mutability).
/// There's a hard limit of slightly less than 4GiB per value, and performance is likely to be poor when mutating values above a few megabytes.
/// Additionally, because redb is copy-on-write, mutating a value in-place is not possible, and therefore mutating large values is slow.
/// Storing values with alignment requirements is also not supported.
///
/// This example demonstrates one way to handle such values, via a sidecar file.
fn main() -> Result<(), Error> {
    let mut db = SpecialValuesDb::new();
    let mut txn = db.begin_txn();
    {
        let mut table = txn.open_table(TABLE);
        table.insert(0, "hello world");
    }
    txn.commit();

    let mut txn = db.begin_txn();
    let mut table = txn.open_table(TABLE);
    assert_eq!(table.get(0).value(), "hello world");

    Ok(())
}
