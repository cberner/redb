#[cfg(not(target_os = "wasi"))]
mod multithreading_test {
    use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
    use std::sync::Arc;
    use std::thread;

    fn create_tempfile() -> tempfile::NamedTempFile {
        if cfg!(target_os = "wasi") {
            tempfile::NamedTempFile::new_in("/tmp").unwrap()
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

    // Stresses concurrent mutation of separate tables within one WriteTransaction: inserts
    // allocate and dirty pages concurrently, and removals of committed entries queue pages on the
    // transaction's shared freed list from multiple threads.
    #[test]
    fn multithreaded_insert_and_remove() {
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        const ELEMENTS: u64 = 1000;
        let table_defs: Vec<TableDefinition<u64, u64>> = vec![
            TableDefinition::new("a"),
            TableDefinition::new("b"),
            TableDefinition::new("c"),
            TableDefinition::new("d"),
        ];

        let write_txn = db.begin_write().unwrap();
        {
            let mut tables: Vec<_> = table_defs
                .iter()
                .map(|def| write_txn.open_table(*def).unwrap())
                .collect();
            thread::scope(|s| {
                for table in tables.iter_mut() {
                    s.spawn(move || {
                        for i in 0..ELEMENTS {
                            table.insert(i, i).unwrap();
                        }
                    });
                }
            });
        }
        write_txn.commit().unwrap();

        // Remove half the now-committed entries and insert new ones, concurrently
        let write_txn = db.begin_write().unwrap();
        {
            let mut tables: Vec<_> = table_defs
                .iter()
                .map(|def| write_txn.open_table(*def).unwrap())
                .collect();
            thread::scope(|s| {
                for table in tables.iter_mut() {
                    s.spawn(move || {
                        for i in 0..ELEMENTS {
                            if i % 2 == 0 {
                                assert_eq!(table.remove(i).unwrap().unwrap().value(), i);
                            } else {
                                table.insert(ELEMENTS + i, i).unwrap();
                            }
                        }
                    });
                }
            });
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        for def in &table_defs {
            let table = read_txn.open_table(*def).unwrap();
            assert_eq!(table.len().unwrap(), ELEMENTS);
            for i in 0..ELEMENTS {
                if i % 2 == 0 {
                    assert!(table.get(i).unwrap().is_none());
                } else {
                    assert_eq!(table.get(i).unwrap().unwrap().value(), i);
                    assert_eq!(table.get(ELEMENTS + i).unwrap().unwrap().value(), i);
                }
            }
        }
    }

    #[test]
    fn multithreaded_re_read() {
        let tmpfile = create_tempfile();
        let db = Database::create(tmpfile.path()).unwrap();

        const DEF1: TableDefinition<&str, &str> = TableDefinition::new("x");
        const DEF2: TableDefinition<&str, &str> = TableDefinition::new("y");
        const DEF3: TableDefinition<&str, &str> = TableDefinition::new("z");
        let write_txn = db.begin_write().unwrap();
        {
            let mut table1 = write_txn.open_table(DEF1).unwrap();
            let mut table2 = write_txn.open_table(DEF2).unwrap();
            let mut table3 = write_txn.open_table(DEF3).unwrap();
            table1.insert("hello", "world").unwrap();

            thread::scope(|s| {
                s.spawn(|| {
                    let value = table1.get("hello").unwrap().unwrap();
                    table2.insert("hello2", value.value()).unwrap();
                });
            });
            thread::scope(|s| {
                s.spawn(|| {
                    let value = table1.get("hello").unwrap().unwrap();
                    table3.insert("hello2", value.value()).unwrap();
                });
            });

            assert_eq!(table2.get("hello2").unwrap().unwrap().value(), "world");
            assert_eq!(table3.get("hello2").unwrap().unwrap().value(), "world");
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        let table = read_txn.open_table(DEF1).unwrap();
        assert_eq!(table.len().unwrap(), 1);
        let table = read_txn.open_table(DEF2).unwrap();
        assert_eq!(table.len().unwrap(), 1);
        let table = read_txn.open_table(DEF3).unwrap();
        assert_eq!(table.len().unwrap(), 1);
    }
}
