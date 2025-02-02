use redb::TableHandle;
use redb::{Database, Error, TableDefinition};
use std::time::Instant;
use std::{sync::Arc, thread, time::Duration};

fn main() -> Result<(), Error> {
    let db = Database::create("my_db.redb")?;
    let definition: TableDefinition<&str, u32> = TableDefinition::new("my_data");

    let db = Arc::new(db);
    // Seed the database with some information
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(definition)?;
        table.insert(&0.to_string().as_str(), 0 as u32)?;
        // The resulting table should have a different "a" value each time this example is run
        table.insert("a".to_string().as_str(), 0 as u32)?;
    }
    write_txn.commit()?;

    let read_threads = 8;
    let write_threads = 2;
    let mut handles = Vec::with_capacity(read_threads + write_threads);
    for i in 0..read_threads {
        let db = db.clone();
        let h = std::thread::spawn(move || -> Result<(), Error> {
            let start = Instant::now();
            while start.elapsed() < Duration::from_millis(100) {
                let read_txn = db.begin_read()?;
                // Print every (key, value) pair in the table
                let table = read_txn.open_table(definition)?;
                for (k, v) in table.range("0"..)?.flatten() {
                    println!("From read_thread #{}: {:?}, {:?}", i, k.value(), v.value());
                }
            }
            Ok(())
        });
        handles.push(h);
    }

    for i in 0..write_threads {
        let db = db.clone();
        let h = std::thread::spawn(move || -> Result<(), Error> {
            let start = Instant::now();
            while start.elapsed() < Duration::from_millis(100) {
                let write_txn = db.begin_write()?;
                {
                    let mut table = write_txn.open_table(definition)?;
                    table.insert(&i.to_string().as_str(), i as u32)?;
                    // The resulting table should have a different "a" value each time this example is run
                    table.insert("a".to_string().as_str(), i as u32)?;
                    println!("Inserted data from write_thread #{}", i);
                }
                write_txn.commit()?;
            }
            Ok(())
        });
        handles.push(h);
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
