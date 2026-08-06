//! Tests for `#[redb(crate = "...")]`, which names the crate the implementations are generated
//! for when it is not reachable as `::redb`, such as deriving against two versions of redb
//! from the same crate.
//!
//! redb 2.6 keeps `TypeName::name()` private, which the composed type name of a struct with
//! fields needs, so the old-version struct here is a unit struct.

mod old {
    use redb_derive::{Key, Value};

    #[derive(Value, Key, Debug, PartialEq, Eq, PartialOrd, Ord)]
    #[redb(crate = "redb2_6")]
    pub struct OldValue;
}

mod new {
    use redb_derive::{Key, Value};

    #[derive(Value, Key, Debug, PartialEq, Eq, PartialOrd, Ord)]
    #[redb(crate = "::redb")]
    pub struct NewValue {
        pub id: u32,
        pub name: String,
    }
}

use new::NewValue;
use old::OldValue;
use redb::ReadableDatabase;

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

const OLD_TABLE: redb2_6::TableDefinition<u32, OldValue> = redb2_6::TableDefinition::new("old");
const NEW_TABLE: redb::TableDefinition<u32, NewValue> = redb::TableDefinition::new("new");

#[test]
fn derives_for_both_redb_versions() {
    let old_file = create_tempfile();
    let old_db = redb2_6::Database::create(old_file.path()).unwrap();
    let write_txn = old_db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(OLD_TABLE).unwrap();
        table.insert(1, &OldValue).unwrap();
    }
    write_txn.commit().unwrap();
    let read_txn = old_db.begin_read().unwrap();
    let table = read_txn.open_table(OLD_TABLE).unwrap();
    assert_eq!(table.get(1).unwrap().unwrap().value(), OldValue);

    let new_file = create_tempfile();
    let new_db = redb::Database::create(new_file.path()).unwrap();
    let write_txn = new_db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(NEW_TABLE).unwrap();
        let value = NewValue {
            id: 1,
            name: "new".to_string(),
        };
        table.insert(1, &value).unwrap();
    }
    write_txn.commit().unwrap();
    let read_txn = new_db.begin_read().unwrap();
    let table = read_txn.open_table(NEW_TABLE).unwrap();
    let value = table.get(1).unwrap().unwrap().value();
    assert_eq!(
        value,
        NewValue {
            id: 1,
            name: "new".to_string(),
        }
    );
}
