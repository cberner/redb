use redb::{
    BitwiseOr, BytesAppend, Database, NumericAdd, NumericMax, NumericMin, ReadableDatabase,
    ReadableTable, TableDefinition, merge_fn,
};

const TABLE_U64: TableDefinition<&str, u64> = TableDefinition::new("merge_u64");
const TABLE_U16: TableDefinition<&str, u16> = TableDefinition::new("merge_u16");
const TABLE_U8: TableDefinition<&str, u8> = TableDefinition::new("merge_u8");
const TABLE_BYTES: TableDefinition<&str, &[u8]> = TableDefinition::new("merge_bytes");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

#[test]
fn merge_add_new_key() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U64).unwrap();
        table
            .merge("counter", &100u64.to_le_bytes(), &NumericAdd)
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U64).unwrap();
    assert_eq!(table.get("counter").unwrap().unwrap().value(), 100);
}

#[test]
fn merge_add_existing() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Seed with initial value
    {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE_U64).unwrap();
            table.insert("counter", &50).unwrap();
        }
        write_txn.commit().unwrap();
    }

    // Merge-add 25
    {
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE_U64).unwrap();
            table
                .merge("counter", &25u64.to_le_bytes(), &NumericAdd)
                .unwrap();
        }
        write_txn.commit().unwrap();
    }

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U64).unwrap();
    assert_eq!(table.get("counter").unwrap().unwrap().value(), 75);
}

#[test]
fn merge_max() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U64).unwrap();
        table.insert("score", &30).unwrap();
        // Try to merge with a smaller value -- should keep 30
        table
            .merge("score", &10u64.to_le_bytes(), &NumericMax)
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U64).unwrap();
    assert_eq!(table.get("score").unwrap().unwrap().value(), 30);

    // Now merge with a larger value -- should become 50
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U64).unwrap();
        table
            .merge("score", &50u64.to_le_bytes(), &NumericMax)
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U64).unwrap();
    assert_eq!(table.get("score").unwrap().unwrap().value(), 50);
}

#[test]
fn merge_min() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U64).unwrap();
        table.insert("score", &30).unwrap();
        // Try to merge with a larger value -- should keep 30
        table
            .merge("score", &50u64.to_le_bytes(), &NumericMin)
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U64).unwrap();
    assert_eq!(table.get("score").unwrap().unwrap().value(), 30);

    // Now merge with a smaller value -- should become 10
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U64).unwrap();
        table
            .merge("score", &10u64.to_le_bytes(), &NumericMin)
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U64).unwrap();
    assert_eq!(table.get("score").unwrap().unwrap().value(), 10);
}

#[test]
fn merge_bytes_append() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_BYTES).unwrap();
        table.insert("log", b"hello".as_slice()).unwrap();
        table.merge("log", b" world", &BytesAppend).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_BYTES).unwrap();
    assert_eq!(table.get("log").unwrap().unwrap().value(), b"hello world");
}

#[test]
fn merge_bitwise_or() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U64).unwrap();
        table.insert("flags", &0b0000_1010u64).unwrap();
        table
            .merge("flags", &0b0000_0101u64.to_le_bytes(), &BitwiseOr)
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U64).unwrap();
    assert_eq!(table.get("flags").unwrap().unwrap().value(), 0b0000_1111);
}

#[test]
fn merge_fn_operator() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Custom merge: multiply existing by operand
    let multiply = merge_fn(|_key, existing, operand| {
        let a = existing.map_or(1u64, |b| u64::from_le_bytes(b.try_into().unwrap()));
        let b = u64::from_le_bytes(operand.try_into().unwrap());
        Some((a * b).to_le_bytes().to_vec())
    });

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U64).unwrap();
        table.insert("product", &6).unwrap();
        table
            .merge("product", &7u64.to_le_bytes(), &multiply)
            .unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U64).unwrap();
    assert_eq!(table.get("product").unwrap().unwrap().value(), 42);
}

#[test]
fn merge_delete_via_none() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    // Operator that always deletes
    let delete_op = merge_fn(|_key, _existing, _operand| None);

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U64).unwrap();
        table.insert("doomed", &42).unwrap();
        table.merge("doomed", &[0], &delete_op).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U64).unwrap();
    assert!(table.get("doomed").unwrap().is_none());
}

#[test]
fn merge_multiple_sequential() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U64).unwrap();
        for i in 1..=10u64 {
            table.merge("sum", &i.to_le_bytes(), &NumericAdd).unwrap();
        }
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U64).unwrap();
    // 1+2+3+...+10 = 55
    assert_eq!(table.get("sum").unwrap().unwrap().value(), 55);
}

#[test]
fn merge_in_typed() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U64).unwrap();
        table.insert("val", &10).unwrap();
        table.merge_in("val", &5u64, &NumericAdd).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U64).unwrap();
    assert_eq!(table.get("val").unwrap().unwrap().value(), 15);
}

#[test]
fn merge_preserves_other_keys() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(TABLE_U64).unwrap();
        table.insert("a", &1).unwrap();
        table.insert("b", &2).unwrap();
        table.insert("c", &3).unwrap();
        table.merge("b", &10u64.to_le_bytes(), &NumericAdd).unwrap();
    }
    write_txn.commit().unwrap();

    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(TABLE_U64).unwrap();
    assert_eq!(table.get("a").unwrap().unwrap().value(), 1);
    assert_eq!(table.get("b").unwrap().unwrap().value(), 12);
    assert_eq!(table.get("c").unwrap().unwrap().value(), 3);
}

#[test]
fn merge_different_width_types() {
    let tmpfile = create_tempfile();
    let db = Database::create(tmpfile.path()).unwrap();

    let write_txn = db.begin_write().unwrap();
    {
        // u8
        let mut table = write_txn.open_table(TABLE_U8).unwrap();
        table.insert("x", &10u8).unwrap();
        table.merge("x", &5u8.to_le_bytes(), &NumericAdd).unwrap();
        assert_eq!(table.get("x").unwrap().unwrap().value(), 15u8);
    }
    {
        // u16
        let mut table = write_txn.open_table(TABLE_U16).unwrap();
        table.insert("y", &1000u16).unwrap();
        table
            .merge("y", &234u16.to_le_bytes(), &NumericAdd)
            .unwrap();
        assert_eq!(table.get("y").unwrap().unwrap().value(), 1234u16);
    }
    {
        // u64
        let mut table = write_txn.open_table(TABLE_U64).unwrap();
        table.insert("z", &1_000_000u64).unwrap();
        table
            .merge("z", &337u64.to_le_bytes(), &NumericAdd)
            .unwrap();
        assert_eq!(table.get("z").unwrap().unwrap().value(), 1_000_337u64);
    }
    write_txn.commit().unwrap();
}
