//! Tests for `#[redb(crate = "...")]`, which names the crate the implementations are generated
//! for when that crate is not written as `::redb`.
//!
//! This package depends on redb as `redb`. The derives below name that path explicitly, both as
//! `redb` and as `::redb`. `crates/redb-derive-rename-test` depends on redb only under another
//! name, so a generated `::redb` path fails to compile there.
//!
//! redb-derive 0.3 always implements `try_from_bytes` and `try_compare`, which redb 2.6 and 3.0
//! do not have. Derives for those versions stay on redb-derive 0.2.0.

mod explicit {
    use redb_derive::{Key, Value};

    #[derive(Value, Key, Debug, PartialEq, Eq, PartialOrd, Ord)]
    #[redb(crate = "::redb")]
    pub struct ExplicitValue {
        pub id: u32,
        pub name: String,
    }
}

mod plain {
    use redb_derive::{Key, Value};

    #[derive(Value, Key, Debug, PartialEq, Eq, PartialOrd, Ord)]
    #[redb(crate = "redb")]
    pub struct PlainPathValue {
        pub id: u32,
        pub name: String,
    }
}

use explicit::ExplicitValue;
use plain::PlainPathValue;
use redb::ReadableDatabase;
#[allow(unused_imports)]
use redb::ReadableTable;

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

const EXPLICIT_TABLE: redb::TableDefinition<u32, ExplicitValue> =
    redb::TableDefinition::new("explicit");
const PLAIN_TABLE: redb::TableDefinition<u32, PlainPathValue> = redb::TableDefinition::new("plain");

#[test]
fn explicit_crate_path_roundtrips() {
    let explicit = ExplicitValue {
        id: 1,
        name: "explicit".to_string(),
    };
    assert_eq!(
        <ExplicitValue as redb::Value>::type_name().name(),
        "ExplicitValue {id: u32, name: String}"
    );
    assert!(<ExplicitValue as redb::Value>::try_from_bytes(&[]).is_err());

    let file = create_tempfile();
    let db = redb::Database::create(file.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(EXPLICIT_TABLE).unwrap();
        table.insert(1, &explicit).unwrap();
    }
    write_txn.commit().unwrap();
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(EXPLICIT_TABLE).unwrap();
    assert_eq!(table.get(1).unwrap().unwrap().value(), explicit);

    let plain = PlainPathValue {
        id: 2,
        name: "plain".to_string(),
    };
    assert_eq!(
        <PlainPathValue as redb::Value>::type_name().name(),
        "PlainPathValue {id: u32, name: String}"
    );
    let small = <PlainPathValue as redb::Value>::as_bytes(&PlainPathValue {
        id: 1,
        name: "a".to_string(),
    });
    let large = <PlainPathValue as redb::Value>::as_bytes(&plain);
    assert_eq!(
        <PlainPathValue as redb::Key>::try_compare(small.as_ref(), large.as_ref()).unwrap(),
        std::cmp::Ordering::Less
    );

    let file = create_tempfile();
    let db = redb::Database::create(file.path()).unwrap();
    let write_txn = db.begin_write().unwrap();
    {
        let mut table = write_txn.open_table(PLAIN_TABLE).unwrap();
        table.insert(2, &plain).unwrap();
    }
    write_txn.commit().unwrap();
    let read_txn = db.begin_read().unwrap();
    let table = read_txn.open_table(PLAIN_TABLE).unwrap();
    assert_eq!(table.get(2).unwrap().unwrap().value(), plain);
}
