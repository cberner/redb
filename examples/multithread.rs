use redb::TableHandle;
use redb::{Database, Error, TableDefinition};
use std::{sync::Arc, thread, time::Duration};

fn main() -> Result<(), Error> {
    let db = Database::create("my_db.redb")?;
    let definition: TableDefinition<&str, u32> = TableDefinition::new("my_data");

    let db = Arc::new(db);

    let threads = 10;
    let mut handles = Vec::with_capacity(threads);
    for i in 0..threads {
        let db = db.clone();
        let a = std::thread::spawn(move || -> Result<(), Error> {
            // Sleep a little bit to add some non-determinism to execution order
            thread::sleep(Duration::from_millis(100));
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(definition)?;
                table.insert(&i.to_string().as_str(), i as u32)?;
                // The resulting table should have a different "a" value each time this example is run
                table.insert("a".to_string().as_str(), i as u32)?;
            }
            write_txn.commit()?;
            Ok(())
        });
        handles.push(a);
    }
    // See if there any errors were returned from the threads
    for handle in handles {
        if let Err(e) = handle.join() {
            println!("{:?}", e)
        }
    }

    // Check that the `Database` has the table (and only the table) that we created
    let read_txn = db.begin_read()?;
    let tables = read_txn.list_tables()?;
    for table in tables {
        println!("Table: {}", table.name());
        let _d = TableDefinition::<&str, u32>::new(table.name());
    }

    // Print every (key, value) pair in the table
    let table = read_txn.open_table(definition)?;
    for (k, v) in table.range("0"..)?.flatten() {
        println!("{:?}, {:?}", k.value(), v.value());
    }

    Ok(())
}
