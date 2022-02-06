use std::cmp::Ordering;
use std::convert::TryInto;
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
    type Out;
}

pub struct RefLifetime<T: ?Sized>(PhantomData<T>);
impl<'a, T: 'a + ?Sized> WithLifetime<'a> for RefLifetime<T> {
    type Out = &'a T;
}

pub struct OwnedLifetime<T>(PhantomData<T>);
impl<'a, T: 'a> WithLifetime<'a> for OwnedLifetime<T> {
    type Out = T;
}

pub trait RedbValue {
    // TODO: need GATs, so that we can replace all this HRTB stuff
    type View: for<'a> WithLifetime<'a>;
    type ToBytes: for<'a> AsBytesWithLifetime<'a>;

    /// Deserializes data
    /// Implementations may return a view over data, or an owned type
    ///
    /// Note to implementors: redb guarantees that `data` is the same byte array as that returned
    /// from as_bytes() for a value written to a specific table, but does not guarantee that
    /// the type is the same. In particular, if you create a table "x" with types K & V, write some
    /// data and then re-open it with the types K & V2, V2::from_bytes will be called on a byte
    /// array written from V::as_bytes(). Therefore, you must validate the `data` parameter to
    /// ensure this method is safe, and not assume it contains the bytes returned by as_bytes()
    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out;

    /// Serialize the key to a slice
    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out;
}

pub trait RedbKey: RedbValue {
    /// Compare data1 with data2
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering;
}

impl RedbValue for [u8] {
    type View = RefLifetime<[u8]>;
    type ToBytes = RefAsBytesLifetime<[u8]>;

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        data
    }

    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
        self
    }
}

impl RedbKey for [u8] {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

impl RedbValue for str {
    type View = RefLifetime<str>;
    type ToBytes = RefAsBytesLifetime<str>;

    fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
        std::str::from_utf8(data).unwrap()
    }

    fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
        self
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

            fn from_bytes(data: &[u8]) -> <Self::View as WithLifetime>::Out {
                <$t>::from_be_bytes(data.try_into().unwrap())
            }

            fn as_bytes(&self) -> <Self::ToBytes as AsBytesWithLifetime>::Out {
                self.to_be_bytes()
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
