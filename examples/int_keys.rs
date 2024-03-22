use redb::{Database, Error, TableDefinition};

const TABLE: TableDefinition<u64, u64> = TableDefinition::new("my_data");

fn main() -> Result<(), Error> {
    let db = Database::create("int_keys.redb")?;
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        table.insert(0, 0)?;
    }
    write_txn.commit()?;

    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;
    assert_eq!(table.get(0)?.unwrap().value(), 0);

    Ok(())
}
