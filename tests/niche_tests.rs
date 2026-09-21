// Tables of `Option<&str>` and `Option<String>` written by redb 2.6, before the string types
// declared a niche, are opened through the legacy type
#![cfg(feature = "experimental-niches")]

use redb::{Database, Legacy, ReadableDatabase, ReadableTable, TableDefinition, TableError};

const NUM_ENTRIES: u64 = 1_000;

// The tables as redb 2.6 wrote them
const V26_VALUES: redb2_6::TableDefinition<u64, Option<&str>> =
    redb2_6::TableDefinition::new("values");
const V26_KEYS: redb2_6::TableDefinition<Option<&str>, u64> = redb2_6::TableDefinition::new("keys");
const V26_STRING_VALUES: redb2_6::TableDefinition<u64, Option<String>> =
    redb2_6::TableDefinition::new("string_values");
const V26_STRING_KEYS: redb2_6::TableDefinition<Option<String>, u64> =
    redb2_6::TableDefinition::new("string_keys");

// The same tables under the type the niche gives, which does not open them
const VALUES: TableDefinition<u64, Option<&str>> = TableDefinition::new("values");
const KEYS: TableDefinition<Option<&str>, u64> = TableDefinition::new("keys");
const STRING_VALUES: TableDefinition<u64, Option<String>> = TableDefinition::new("string_values");
const STRING_KEYS: TableDefinition<Option<String>, u64> = TableDefinition::new("string_keys");

// And under the legacy type, which does
const LEGACY_VALUES: TableDefinition<u64, Option<Legacy<&str>>> = TableDefinition::new("values");
const LEGACY_KEYS: TableDefinition<Option<Legacy<&str>>, u64> = TableDefinition::new("keys");
const LEGACY_STRING_VALUES: TableDefinition<u64, Option<Legacy<String>>> =
    TableDefinition::new("string_values");
const LEGACY_STRING_KEYS: TableDefinition<Option<Legacy<String>>, u64> =
    TableDefinition::new("string_keys");

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
            let mut string_values = txn.open_table(V26_STRING_VALUES).unwrap();
            let mut string_keys = txn.open_table(V26_STRING_KEYS).unwrap();
            for i in 0..NUM_ENTRIES {
                values.insert(i, string(i).as_deref()).unwrap();
                keys.insert(string(i).as_deref(), i).unwrap();
                string_values.insert(i, string(i)).unwrap();
                string_keys.insert(string(i), i).unwrap();
            }
        }
        txn.commit().unwrap();
    }

    let db = Database::open(tmpfile.path()).unwrap();
    let txn = db.begin_read().unwrap();
    // `Option` of a string is now another type, so the old tables refuse it rather than misread
    assert!(matches!(
        txn.open_table(VALUES),
        Err(TableError::TableTypeMismatch { .. })
    ));
    assert!(matches!(
        txn.open_table(KEYS),
        Err(TableError::TableTypeMismatch { .. })
    ));
    assert!(matches!(
        txn.open_table(STRING_VALUES),
        Err(TableError::TableTypeMismatch { .. })
    ));
    assert!(matches!(
        txn.open_table(STRING_KEYS),
        Err(TableError::TableTypeMismatch { .. })
    ));

    let values = txn.open_table(LEGACY_VALUES).unwrap();
    let keys = txn.open_table(LEGACY_KEYS).unwrap();
    let string_values = txn.open_table(LEGACY_STRING_VALUES).unwrap();
    let string_keys = txn.open_table(LEGACY_STRING_KEYS).unwrap();
    for i in 0..NUM_ENTRIES {
        let expected = string(i);
        assert_eq!(
            values.get(&i).unwrap().unwrap().value(),
            expected.as_deref()
        );
        assert_eq!(keys.get(&expected.as_deref()).unwrap().unwrap().value(), i);
        assert_eq!(string_values.get(&i).unwrap().unwrap().value(), expected);
        assert_eq!(string_keys.get(&expected).unwrap().unwrap().value(), i);
    }
}
