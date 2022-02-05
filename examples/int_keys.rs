use redb::{Database, Error, ReadOnlyTable, ReadableTable, Table};

fn main() -> Result<(), Error> {
    let db = unsafe { Database::open("int_keys.redb", 1024 * 1024)? };
    let write_txn = db.begin_write()?;
    let mut table: Table<u64, u64> = write_txn.open_table("my_data")?;
    table.insert(&0, &0)?;
    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    let table: ReadOnlyTable<u64, u64> = read_txn.open_table("my_data")?;
    assert_eq!(table.get(&0)?.unwrap().to_value(), 0);

    Ok(())
}
