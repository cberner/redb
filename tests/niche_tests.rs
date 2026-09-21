// Tables of `Option<&str>` written by redb 2.6, before `&str` declared a niche, are opened
// through the legacy type
#![cfg(feature = "experimental-niches")]

use redb::{Database, Legacy, ReadableDatabase, ReadableTable, TableDefinition, TableError};

const NUM_ENTRIES: u64 = 1_000;

// The tables as redb 2.6 wrote them
const V26_VALUES: redb2_6::TableDefinition<u64, Option<&str>> =
    redb2_6::TableDefinition::new("values");
const V26_KEYS: redb2_6::TableDefinition<Option<&str>, u64> = redb2_6::TableDefinition::new("keys");

// The same tables under the type the niche gives, which does not open them
const VALUES: TableDefinition<u64, Option<&str>> = TableDefinition::new("values");
const KEYS: TableDefinition<Option<&str>, u64> = TableDefinition::new("keys");

// And under the legacy type, which does
const LEGACY_VALUES: TableDefinition<u64, Option<Legacy<&str>>> = TableDefinition::new("values");
const LEGACY_KEYS: TableDefinition<Option<Legacy<&str>>, u64> = TableDefinition::new("keys");

fn create_tempfile() -> tempfile::NamedTempFile {
    if cfg!(target_os = "wasi") {
        tempfile::NamedTempFile::new_in("/tmp").unwrap()
    } else {
        tempfile::NamedTempFile::new().unwrap()
    }
}

// `None`, the empty string, and unique strings of two lengths, enough of them for several leaves
fn string(i: u64) -> Option<String> {
    match i {
        0 => None,
        1 => Some(String::new()),
        _ if i.is_multiple_of(2) => Some(format!("{i:06}")),
        _ => Some(format!("{i:06}{}", "-suffix".repeat(20))),
    }
}

#[test]
fn tables_written_without_the_niche_open_through_the_legacy_type() {
    let tmpfile = create_tempfile();
    {
        let db = redb2_6::Database::builder()
            .create_with_file_format_v3(true)
            .create(tmpfile.path())
            .unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut values = txn.open_table(V26_VALUES).unwrap();
            let mut keys = txn.open_table(V26_KEYS).unwrap();
            for i in 0..NUM_ENTRIES {
                values.insert(i, string(i).as_deref()).unwrap();
                keys.insert(string(i).as_deref(), i).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    let db = Database::open(tmpfile.path()).unwrap();
    let txn = db.begin_read().unwrap();
    // `Option<&str>` is now another type, so the old tables refuse it rather than misread
    assert!(matches!(
        txn.open_table(VALUES),
        Err(TableError::TableTypeMismatch { .. })
    ));
    assert!(matches!(
        txn.open_table(KEYS),
        Err(TableError::TableTypeMismatch { .. })
    ));

    let values = txn.open_table(LEGACY_VALUES).unwrap();
    let keys = txn.open_table(LEGACY_KEYS).unwrap();
    for i in 0..NUM_ENTRIES {
        let expected = string(i);
        assert_eq!(
            values.get(&i).unwrap().unwrap().value(),
            expected.as_deref()
        );
        assert_eq!(keys.get(&expected.as_deref()).unwrap().unwrap().value(), i);
    }
}
