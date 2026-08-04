//! Tests for `#[redb(crate = "...")]`, which names the crate the implementations are generated
//! for when it is not reachable as `::redb`, such as deriving against two versions of redb
//! from the same crate.
//!
//! The composed type name of a struct with fields reads the field types' `TypeName`s only
//! through `TypeName::new`, `name()`, and `PartialEq`, so that it compiles against every redb
//! version exposing those -- redb 3.0 on. redb 2.6 keeps `name()` private, so the 2.6 struct
//! here is a unit struct, the most that version can derive.

mod old {
    use redb_derive::{Key, Value};

    #[derive(Value, Key, Debug, PartialEq, Eq, PartialOrd, Ord)]
    #[redb(crate = "redb2_6")]
    pub struct OldValue;
}

mod old3 {
    use redb_derive::{Key, Value};

    // A user-defined field type, so the tagging in the composed type name runs against the old
    // version too.
    #[derive(Value, Key, Debug, PartialEq, Eq, PartialOrd, Ord)]
    #[redb(crate = "redb3_0")]
    pub struct Old3Inner {
        pub tag: u16,
    }

    #[derive(Value, Key, Debug, PartialEq, Eq, PartialOrd, Ord)]
    #[redb(crate = "redb3_0")]
    pub struct Old3Value {
        pub id: u32,
        pub inner: Old3Inner,
        // redb 3.0 classifies composites of user-defined types as built-in (the classification
        // bubbling is new in 4.2), so this field renders untagged, matching 3.0's own type
        // names -- which collide for such composites -- rather than 4.2's.
        pub maybe: Option<Old3Inner>,
        pub name: String,
    }
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
use old3::{Old3Inner, Old3Value};
use redb::ReadableDatabase;
#[allow(unused_imports)]
use redb::ReadableTable;
use redb3_0::ReadableDatabase as _;

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

const OLD_TABLE: redb2_6::TableDefinition<u32, OldValue> = redb2_6::TableDefinition::new("old");
const OLD3_TABLE: redb3_0::TableDefinition<u32, Old3Value> = redb3_0::TableDefinition::new("old3");
const NEW_TABLE: redb::TableDefinition<u32, NewValue> = redb::TableDefinition::new("new");

#[test]
// Reads through the inherent ReadOnlyTable::get(), which redb deprecates behind a feature flag.
// This crate cannot see that flag, so the allowance is unconditional.
#[allow(deprecated)]
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

    let old3_value = Old3Value {
        id: 7,
        inner: Old3Inner { tag: 3 },
        maybe: Some(Old3Inner { tag: 5 }),
        name: "old3".to_string(),
    };
    assert_eq!(
        <Old3Value as redb3_0::Value>::type_name().name(),
        "Old3Value {id: u32, inner: Old3Inner {tag: u16}#user, \
         maybe: Option<Old3Inner {tag: u16}>, name: String}"
    );
    let old3_file = create_tempfile();
    let old3_db = redb3_0::Database::create(old3_file.path()).unwrap();
    let write_txn = old3_db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(OLD3_TABLE).unwrap();
        table.insert(1, &old3_value).unwrap();
    }
    write_txn.commit().unwrap();
    let read_txn = old3_db.begin_read().unwrap();
    let table = read_txn.open_table(OLD3_TABLE).unwrap();
    assert_eq!(table.get(1).unwrap().unwrap().value(), old3_value);

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
