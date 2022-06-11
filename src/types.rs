use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::Debug;
use std::marker::PhantomData;

pub trait AsBytesWithLifetime<'a> {
    type Out: AsRef<[u8]>;
}

pub struct RefAsBytesLifetime<T: AsRef<[u8]> + ?Sized>(PhantomData<T>);
impl<'a, T: 'a + AsRef<[u8]> + ?Sized> AsBytesWithLifetime<'a> for RefAsBytesLifetime<T> {
    type Out = &'a T;
}

pub struct OwnedAsBytesLifetime<T: AsRef<[u8]>>(PhantomData<T>);
impl<'a, T: AsRef<[u8]> + 'a> AsBytesWithLifetime<'a> for OwnedAsBytesLifetime<T> {
    type Out = T;
}

pub trait WithLifetime<'a> {
    type Out: Debug;
}

pub struct RefLifetime<T: ?Sized>(PhantomData<T>);
impl<'a, T: 'a + Debug + ?Sized> WithLifetime<'a> for RefLifetime<T> {
    type Out = &'a T;
}

pub struct OwnedLifetime<T>(PhantomData<T>);
impl<'a, T: 'a + Debug> WithLifetime<'a> for OwnedLifetime<T> {
    type Out = T;
}

pub trait RedbValue: Debug {
    // TODO: need GATs, so that we can replace all this HRTB stuff
    type View: for<'a> WithLifetime<'a>;
    type ToBytes: for<'a> AsBytesWithLifetime<'a>;

    /// Width of a fixed type, or None for variable width
    fn fixed_width() -> Option<usize>;

    /// Deserializes data
    /// Implementations may return a view over data, or an owned type
    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out;

    /// Serialize the key to a slice
    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out;

    /// Globally unique identifier for this type
    fn redb_type_name() -> String;
}

pub trait RedbKey: RedbValue {
    /// Compare data1 with data2
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering;
}

impl RedbValue for [u8] {
    type View = RefLifetime<[u8]>;
    type ToBytes = RefAsBytesLifetime<[u8]>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        data
    }

    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
        self
    }

    fn redb_type_name() -> String {
        "[u8]".to_string()
    }
}

impl RedbKey for [u8] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

impl<const N: usize> RedbValue for [u8; N] {
    type View = RefLifetime<[u8; N]>;
    type ToBytes = RefAsBytesLifetime<[u8; N]>;

    fn fixed_width() -> Option<usize> {
        Some(N)
    }

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        data.try_into().unwrap()
    }

    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
        self
    }

    fn redb_type_name() -> String {
        format!("[u8;{}]", N)
    }
}

impl<const N: usize> RedbKey for [u8; N] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

impl RedbValue for str {
    type View = RefLifetime<str>;
    type ToBytes = RefAsBytesLifetime<str>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        std::str::from_utf8(data).unwrap()
    }

    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
        self
    }

    fn redb_type_name() -> String {
        "str".to_string()
    }
}

impl RedbKey for str {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        let str1 = str::from_bytes(data1);
        let str2 = str::from_bytes(data2);
        str1.cmp(str2)
    }
}

macro_rules! be_value {
    ($t:ty) => {
        impl RedbValue for $t {
            type View = OwnedLifetime<$t>;
            type ToBytes = OwnedAsBytesLifetime<[u8; std::mem::size_of::<$t>()]>;

            fn fixed_width() -> Option<usize> {
                Some(std::mem::size_of::<$t>())
            }

            fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
                <$t>::from_le_bytes(data.try_into().unwrap())
            }

            fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
                self.to_le_bytes()
            }

            fn redb_type_name() -> String {
                stringify!($t).to_string()
            }
        }
    };
}

macro_rules! be_impl {
    ($t:ty) => {
        be_value!($t);

        impl RedbKey for $t {
            fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
                Self::from_bytes(data1).cmp(&Self::from_bytes(data2))
            }
        }
    };
}

be_impl!(u8);
be_impl!(u16);
be_impl!(u32);
be_impl!(u64);
be_impl!(u128);
be_impl!(i8);
be_impl!(i16);
be_impl!(i32);
be_impl!(i64);
be_impl!(i128);
be_value!(f32);
be_value!(f64);
