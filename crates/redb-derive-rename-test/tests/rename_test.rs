// This package depends on redb only under the name `renamed_redb`, so the derives work only
// through `#[redb(crate = "...")]`, and any stray `redb` path in the generated code fails to
// compile here.
use redb_derive::{Key, Value};

#[derive(Key, Value, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[redb(crate = "renamed_redb")]
struct Composite {
    a: u32,
    b: String,
}

#[test]
fn derive_with_renamed_redb_dependency() {
    let original = Composite {
        a: 7,
        b: "hello".to_string(),
    };
    let bytes = <Composite as renamed_redb::Value>::as_bytes(&original);
    let decoded = <Composite as renamed_redb::Value>::from_bytes(&bytes);
    assert_eq!(decoded, original);
    assert_eq!(
        <Composite as renamed_redb::Value>::type_name().name(),
        "Composite {a: u32, b: String}"
    );

    let small = <Composite as renamed_redb::Value>::as_bytes(&Composite {
        a: 1,
        b: "x".to_string(),
    });
    let large = <Composite as renamed_redb::Value>::as_bytes(&Composite {
        a: 2,
        b: "x".to_string(),
    });
    assert_eq!(
        <Composite as renamed_redb::Key>::compare(&small, &large),
        std::cmp::Ordering::Less
    );
}
