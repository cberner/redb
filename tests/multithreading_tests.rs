#[cfg(not(target_os = "wasi"))]
mod multithreading_test {
    use redb::{Database, ReadableTableMetadata, TableDefinition};
    use std::sync::Arc;
    use std::thread;

    fn create_tempfile() -> tempfile::NamedTempFile {
        if cfg!(target_os = "wasi") {
            tempfile::NamedTempFile::new_in("/").unwrap()
        } else {
            tempfile::NamedTempFile::new().unwrap()
        }
    }

    const TABLE: TableDefinition<&str, &str> = TableDefinition::new("x");
    #[test]
    fn len() {
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();
        let db = Arc::new(db);
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            table.insert("hello", "world").unwrap();
            table.insert("hello2", "world2").unwrap();
            table.insert("hi", "world").unwrap();
        }
        write_txn.commit().unwrap();

        let db2 = db.clone();
        let t = thread::spawn(move || {
            let read_txn = db2.begin_read().unwrap();
            let table = read_txn.open_table(TABLE).unwrap();
            assert_eq!(table.len().unwrap(), 3);
        });
        t.join().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(TABLE).unwrap();
        assert_eq!(table.len().unwrap(), 3);
    }

    #[test]
    fn multithreaded_insert() {
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        const DEF1: TableDefinition<&str, &str> = TableDefinition::new("x");
        const DEF2: TableDefinition<&str, &str> = TableDefinition::new("y");
        let write_txn = db.begin_write().unwrap();
        {
            let mut table1 = write_txn.open_table(DEF1).unwrap();
            let mut table2 = write_txn.open_table(DEF2).unwrap();

            thread::scope(|s| {
                s.spawn(|| {
                    table2.insert("hello", "world").unwrap();
                    table2.insert("hello2", "world2").unwrap();
                });
            });

            table1.insert("hello", "world").unwrap();
            table1.insert("hello2", "world2").unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(DEF1).unwrap();
        assert_eq!(table.len().unwrap(), 2);
        let table = read_txn.open_table(DEF2).unwrap();
        assert_eq!(table.len().unwrap(), 2);
    }
}
