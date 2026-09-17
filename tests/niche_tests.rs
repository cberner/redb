use redb::backends::InMemoryBackend;
use redb::{
    Database, Key, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
    TableError, TypeName, Value,
};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::num::{
    NonZeroI8, NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI128, NonZeroU8, NonZeroU16, NonZeroU32,
    NonZeroU64, NonZeroU128,
};

macro_rules! test_nonzero {
    ($name:ident, $nonzero:ty, $primitive:ty, [$($value:expr),+]) => {
        #[test]
        fn $name() {
            type Optional = Option<$nonzero>;
            let width = size_of::<$primitive>();
            let zero = vec![0; width];
            assert_eq!(<$nonzero>::NICHE, Some(zero.as_slice()));
            assert_eq!(<$nonzero>::fixed_width(), Some(width));
            assert_eq!(Optional::fixed_width(), Some(width));
            assert_eq!(Optional::as_bytes(&None), zero);
            assert_eq!(Optional::from_bytes(&zero), None);
            assert_eq!(Optional::min_encoded_key().as_deref(), Some(zero.as_slice()));
            assert_eq!(<$nonzero>::type_name().name(), stringify!($nonzero));
            assert_ne!(<$nonzero>::type_name(), <$primitive>::type_name());

            let values: &[$primitive] = &[$($value),+];
            let mut options = vec![None];
            for &primitive in values {
                let value = <$nonzero>::new(primitive).unwrap();
                let encoded = primitive.to_le_bytes();
                assert_eq!(<$nonzero>::as_bytes(&value), encoded);
                assert_eq!(<$nonzero>::from_bytes(&encoded), value);
                assert_ne!(encoded.as_slice(), zero);
                assert_eq!(Optional::as_bytes(&Some(value)), encoded);
                assert_eq!(Optional::from_bytes(&encoded), Some(value));
                options.push(Some(value));
            }
            for left in &options {
                for right in &options {
                    let a = Optional::as_bytes(left);
                    let b = Optional::as_bytes(right);
                    assert_eq!(Optional::compare(&a, &b), left.cmp(right));
                    if let (Some(left), Some(right)) = (left, right) {
                        assert_eq!(<$nonzero>::compare(&a, &b), left.cmp(right));
                    }
                    if left < right {
                        assert_eq!(Optional::separator(&a, &b).as_ref(), a);
                    }
                }
            }
        }
    };
}

test_nonzero!(nonzero_u8, NonZeroU8, u8, [1, 128, u8::MAX]);
test_nonzero!(nonzero_u16, NonZeroU16, u16, [1, 256, u16::MAX]);
test_nonzero!(nonzero_u32, NonZeroU32, u32, [1, 256, u32::MAX]);
test_nonzero!(nonzero_u64, NonZeroU64, u64, [1, 256, u64::MAX]);
test_nonzero!(nonzero_u128, NonZeroU128, u128, [1, 256, u128::MAX]);
test_nonzero!(nonzero_i8, NonZeroI8, i8, [i8::MIN, -1, 1, i8::MAX]);
test_nonzero!(
    nonzero_i16,
    NonZeroI16,
    i16,
    [i16::MIN, -256, -1, 1, 256, i16::MAX]
);
test_nonzero!(
    nonzero_i32,
    NonZeroI32,
    i32,
    [i32::MIN, -256, -1, 1, 256, i32::MAX]
);
test_nonzero!(
    nonzero_i64,
    NonZeroI64,
    i64,
    [i64::MIN, -256, -1, 1, 256, i64::MAX]
);
test_nonzero!(
    nonzero_i128,
    NonZeroI128,
    i128,
    [i128::MIN, -256, -1, 1, 256, i128::MAX]
);

#[test]
fn nested_options_and_singleton_tuples() {
    type Nested = Option<Option<NonZeroU32>>;
    let value = NonZeroU32::new(256).unwrap();
    assert_eq!(Nested::fixed_width(), Some(5));
    assert_eq!(<Option<NonZeroU32>>::NICHE, None);
    let cases = [
        (None, [0, 0, 0, 0, 0]),
        (Some(None), [1, 0, 0, 0, 0]),
        (Some(Some(value)), [1, 0, 1, 0, 0]),
    ];
    for (value, bytes) in cases {
        assert_eq!(Nested::as_bytes(&value), bytes);
        assert_eq!(Nested::from_bytes(&bytes), value);
        for (other, other_bytes) in cases {
            assert_eq!(Nested::compare(&bytes, &other_bytes), value.cmp(&other));
        }
    }

    type TupleOption = Option<((NonZeroU32,),)>;
    assert_eq!(<(NonZeroU32,)>::NICHE, NonZeroU32::NICHE);
    assert_eq!(<((NonZeroU32,),)>::NICHE, NonZeroU32::NICHE);
    assert_eq!(TupleOption::fixed_width(), Some(4));
    assert_eq!(TupleOption::as_bytes(&None), [0; 4]);
    assert_eq!(TupleOption::from_bytes(&[0; 4]), None);
    assert_eq!(TupleOption::as_bytes(&Some(((value,),))), [0, 1, 0, 0]);
    assert_eq!(TupleOption::from_bytes(&[0, 1, 0, 0]), Some(((value,),)));
    assert_eq!(<(u32,)>::NICHE, None);
}

#[test]
fn existing_option_encodings() {
    assert_eq!(u32::NICHE, None);
    assert_eq!(bool::NICHE, None);
    assert_eq!(<&str>::NICHE, None);
    assert_eq!(<Option<u32>>::fixed_width(), Some(5));
    assert_eq!(<Option<u32>>::as_bytes(&None), [0; 5]);
    assert_eq!(<Option<u32>>::as_bytes(&Some(256)), [1, 0, 1, 0, 0]);
    assert_eq!(<Option<bool>>::fixed_width(), Some(2));
    assert_eq!(<Option<bool>>::as_bytes(&None), [0, 0]);
    assert_eq!(<Option<bool>>::as_bytes(&Some(false)), [1, 0]);
    assert_eq!(<Option<bool>>::as_bytes(&Some(true)), [1, 1]);
    assert_eq!(<Option<&str>>::fixed_width(), None);
    assert_eq!(<Option<&str>>::as_bytes(&None), [0]);
    assert_eq!(<Option<&str>>::as_bytes(&Some("")), [1]);
    assert_eq!(<Option<&str>>::as_bytes(&Some("hello")), b"\x01hello");
    assert_eq!(<Option<()>>::as_bytes(&None), [0]);
    assert_eq!(<Option<()>>::as_bytes(&Some(())), [1]);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct NicheBytes<'a, const N: usize>(&'a [u8]);

impl<const N: usize> Value for NicheBytes<'_, N> {
    const NICHE: Option<&'static [u8]> = Some(&[255; N]);

    type SelfType<'a>
        = NicheBytes<'a, N>
    where
        Self: 'a;
    type AsBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        assert_ne!(data, Self::NICHE.unwrap());
        NicheBytes(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        assert_ne!(value.0, Self::NICHE.unwrap());
        value.0
    }

    fn type_name() -> TypeName {
        TypeName::new(&format!("test::NicheBytes<{N}>"))
    }
}

impl<const N: usize> Key for NicheBytes<'_, N> {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        assert_ne!(data1, Self::NICHE.unwrap());
        assert_ne!(data2, Self::NICHE.unwrap());
        data1.cmp(data2)
    }

    fn separator<'a>(left: &'a [u8], right: &'a [u8]) -> Cow<'a, [u8]> {
        let separator = <&[u8]>::separator(left, right);
        if separator.as_ref() == Self::NICHE.unwrap() {
            Cow::Borrowed(left)
        } else {
            separator
        }
    }
}

#[test]
fn variable_width_niches() {
    fn check<const N: usize>() {
        type Optional<'a, const N: usize> = Option<NicheBytes<'a, N>>;
        let niche = NicheBytes::<N>::NICHE.unwrap();
        assert_eq!(Optional::<N>::fixed_width(), None);
        assert_eq!(Optional::<N>::as_bytes(&None), niche);
        assert_eq!(Optional::<N>::from_bytes(niche), None);
        assert_eq!(Optional::<N>::min_encoded_key().as_deref(), Some(niche));
        assert_eq!(<(NicheBytes<N>,)>::NICHE, Some(niche));
        assert_eq!(<Option<(NicheBytes<N>,)>>::as_bytes(&None), niche);
        assert_eq!(<Option<(NicheBytes<N>,)>>::from_bytes(niche), None);

        let samples: &[&[u8]] = &[
            b"",
            b"\0",
            b"\0\0",
            b"aaaa-long",
            b"bbbb-long",
            &[255],
            &[255, 255],
            &[255, 255, 255],
            &[255, 255, 255, 255],
        ];
        let mut values = vec![None];
        for &bytes in samples {
            if bytes != niche {
                let value = Some(NicheBytes::<N>(bytes));
                assert_eq!(Optional::as_bytes(&value), bytes);
                assert_eq!(Optional::<N>::from_bytes(bytes), value);
                values.push(value);
            }
        }
        for left in &values {
            for right in &values {
                let a = Optional::as_bytes(left);
                let b = Optional::as_bytes(right);
                assert_eq!(Optional::<N>::compare(&a, &b), left.cmp(right));
                if left < right {
                    let separator = Optional::<N>::separator(&a, &b);
                    assert!(separator.len() <= a.len());
                    assert!(Optional::<N>::compare(&a, &separator).is_le());
                    assert!(Optional::<N>::compare(&separator, &b).is_lt());
                    let decoded = Optional::<N>::from_bytes(&separator);
                    assert_eq!(Optional::as_bytes(&decoded), separator.as_ref());
                }
            }
        }
        assert_eq!(
            Optional::<N>::separator(b"aaaa-long", b"bbbb-long").as_ref(),
            b"b"
        );

        type Nested<'a, const N: usize> = Option<Option<NicheBytes<'a, N>>>;
        let cases = [None, Some(None), Some(Some(NicheBytes::<N>(b"\0")))];
        for value in &cases {
            let bytes = Nested::as_bytes(value);
            assert_eq!(Nested::<N>::from_bytes(&bytes), *value);
            for other in &cases {
                assert_eq!(
                    Nested::<N>::compare(&bytes, &Nested::as_bytes(other)),
                    value.cmp(other)
                );
            }
        }
    }

    check::<0>();
    check::<1>();
    check::<3>();
}

#[test]
fn nonzero_tables_reopen() {
    const BARE: TableDefinition<NonZeroI32, NonZeroU64> = TableDefinition::new("bare");
    const OPTIONAL: TableDefinition<Option<NonZeroI32>, Option<(NonZeroU64,)>> =
        TableDefinition::new("optional");
    let file = tempfile::NamedTempFile::new().unwrap();
    {
        let db = Database::create(file.path()).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut bare = txn.open_table(BARE).unwrap();
            let mut optional = txn.open_table(OPTIONAL).unwrap();
            for i in (-1024i32..=1024).rev() {
                let key = NonZeroI32::new(i);
                let value = NonZeroU64::new(u64::from(i.unsigned_abs()));
                optional.insert(key, value.map(|value| (value,))).unwrap();
                if let (Some(key), Some(value)) = (key, value) {
                    bare.insert(key, value).unwrap();
                }
            }
            assert!(bare.stats().unwrap().tree_height() > 1);
            assert!(optional.stats().unwrap().tree_height() > 1);
            assert_eq!(bare.stats().unwrap().stored_bytes(), 2048 * 12);
            assert_eq!(optional.stats().unwrap().stored_bytes(), 2049 * 12);
        }
        txn.commit().unwrap();
    }
    let mut db = Database::open(file.path()).unwrap();
    assert!(db.check_integrity().unwrap());
    let txn = db.begin_read().unwrap();
    let bare = txn.open_table(BARE).unwrap();
    let optional = txn.open_table(OPTIONAL).unwrap();
    for i in -1024i32..=1024 {
        let key = NonZeroI32::new(i);
        let value = NonZeroU64::new(u64::from(i.unsigned_abs()));
        assert_eq!(
            optional.get(key).unwrap().unwrap().value(),
            value.map(|value| (value,))
        );
        if let Some(key) = key {
            assert_eq!(bare.get(key).unwrap().unwrap().value(), value.unwrap());
        }
    }
    let mut expected: Vec<_> = (-1024..=1024).map(NonZeroI32::new).collect();
    expected.sort_unstable();
    let stored: Vec<_> = optional
        .iter()
        .unwrap()
        .map(|entry| entry.unwrap().0.value())
        .collect();
    assert_eq!(stored, expected);
}

#[test]
fn nonzero_type_identity() {
    let db = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let definition: TableDefinition<u32, u32> = TableDefinition::new("primitive");
    let wrong_key: TableDefinition<NonZeroU32, u32> = TableDefinition::new("primitive");
    let wrong_value: TableDefinition<u32, NonZeroU32> = TableDefinition::new("primitive");
    let txn = db.begin_write().unwrap();
    txn.open_table(definition).unwrap().insert(0, 0).unwrap();
    txn.commit().unwrap();
    let txn = db.begin_write().unwrap();
    assert!(matches!(
        txn.open_table(wrong_key),
        Err(TableError::TableTypeMismatch { .. })
    ));
    assert!(matches!(
        txn.open_table(wrong_value),
        Err(TableError::TableTypeMismatch { .. })
    ));
    txn.abort().unwrap();
    let txn = db.begin_read().unwrap();
    assert!(matches!(
        txn.open_table(wrong_key),
        Err(TableError::TableTypeMismatch { .. })
    ));
    assert!(matches!(
        txn.open_table(wrong_value),
        Err(TableError::TableTypeMismatch { .. })
    ));
}
