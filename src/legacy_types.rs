//! Types that open the tables written before redb 5.0, when the built-in types had no niche

use crate::{Key, TypeName, Value};
use alloc::borrow::Cow;
use core::cmp::Ordering;
use core::marker::PhantomData;

/// `T` without the niche it declares: opens a table of `Option<T>` written before version 5.0.
///
/// Declaring a niche changes the encoding of `Option<T>`, and its type name, so tables created
/// before version 5.0 no longer open as `Option<T>` in version 5.0. It opens as
/// `Option<Legacy<T>>`, at any depth of nesting, and yields plain `Option<T>` values; migrate it by
/// copying it into a table of `Option<T>`.
///
/// From redb 5.0 on, `&str` declares a niche, so a table of `Option<&str>` written by an earlier
/// version opens as `Option<Legacy<&str>>`:
///
/// ```rust
/// use redb::{Database, Legacy, ReadableTable, TableDefinition};
/// # use tempfile::NamedTempFile;
/// const OLD: TableDefinition<u64, Option<Legacy<&str>>> = TableDefinition::new("names");
/// const NEW: TableDefinition<u64, Option<&str>> = TableDefinition::new("names_v2");
///
/// # fn main() -> Result<(), redb::Error> {
/// # #[cfg(not(target_os = "wasi"))]
/// # let tmpfile = NamedTempFile::new().unwrap();
/// # #[cfg(target_os = "wasi")]
/// # let tmpfile = NamedTempFile::new_in("/tmp").unwrap();
/// # let db = Database::create(tmpfile.path())?;
/// # let txn = db.begin_write()?;
/// # txn.open_table(OLD)?.insert(1, Some("one"))?;
/// # txn.commit()?;
/// let txn = db.begin_write()?;
/// {
///     let old = txn.open_table(OLD)?;
///     let mut new = txn.open_table(NEW)?;
///     for entry in old.iter()? {
///         let (key, value) = entry?;
///         new.insert(key.value(), value.value())?;
///     }
/// }
/// txn.delete_table(OLD)?;
/// txn.commit()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Legacy<T>(PhantomData<T>);

impl<T: Value> Value for Legacy<T> {
    type SelfType<'a>
        = T::SelfType<'a>
    where
        Self: 'a;
    type AsBytes<'a>
        = T::AsBytes<'a>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        T::fixed_width()
    }

    fn from_bytes<'a>(data: &'a [u8]) -> T::SelfType<'a>
    where
        Self: 'a,
    {
        T::from_bytes(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a T::SelfType<'b>) -> T::AsBytes<'a>
    where
        Self: 'b,
    {
        T::as_bytes(value)
    }

    fn type_name() -> TypeName {
        T::type_name()
    }
}

impl<T: Key> Key for Legacy<T> {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        T::compare(data1, data2)
    }

    fn separator<'a>(left: &'a [u8], right: &'a [u8]) -> Cow<'a, [u8]> {
        T::separator(left, right)
    }

    fn min_encoded_key() -> Option<Cow<'static, [u8]>> {
        T::min_encoded_key()
    }
}

#[cfg(test)]
mod tests {
    use super::Legacy;
    use crate::{TypeName, Value};
    use core::num::NonZeroU32;

    // `Option<Legacy<T>>` has the tag byte encoding and the name `Option<T>` had before `T`
    // declared its niche
    #[test]
    fn legacy_drops_the_niche() {
        assert_eq!(<Legacy<NonZeroU32> as Value>::NICHE, None);
        assert_eq!(
            <Legacy<NonZeroU32> as Value>::type_name(),
            <NonZeroU32 as Value>::type_name()
        );
        assert_eq!(
            <Option<Legacy<NonZeroU32>> as Value>::type_name(),
            TypeName::internal("Option<NonZeroU32>").into_composite(false)
        );
        assert_eq!(
            <Option<Legacy<NonZeroU32>> as Value>::fixed_width(),
            Some(5)
        );
        assert_eq!(
            <Option<Legacy<NonZeroU32>> as Value>::as_bytes(&None),
            [0; 5]
        );
        let one = NonZeroU32::new(1).unwrap();
        assert_eq!(
            <Option<Legacy<NonZeroU32>> as Value>::as_bytes(&Some(one)),
            [1, 1, 0, 0, 0]
        );
        assert_eq!(
            <Option<Legacy<NonZeroU32>> as Value>::from_bytes(&[1, 1, 0, 0, 0]),
            Some(one)
        );
    }
}
