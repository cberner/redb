use crate::sealed::Sealed;
use crate::tree_store::encode_bounds;
use crate::types::Key;
use alloc::vec::Vec;
use core::borrow::Borrow;
use core::ops::{Bound, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive};

/// A range of keys, accepted by the range taking methods of a table
///
/// Implemented for the standard range types over any type that borrows the table's key type, so
/// `5..10`, `&5..&10`, and `(Bound::Excluded(5), Bound::Unbounded)` are all accepted, and for `..`.
///
/// Unlike a plain [`std::ops::RangeBounds`] bound, the borrowed key type is a parameter of the implementing
/// range type rather than of the method, so `..` carries no type to infer and needs no annotation.
///
/// This trait is sealed and cannot be implemented outside of redb.
pub trait KeyRange<K: Key>: Sealed {
    /// Returns the range's bounds, as encoded by [`Key::as_bytes`]
    #[doc(hidden)]
    fn key_bounds(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>);
}

macro_rules! impl_key_range {
    ($range:ident) => {
        impl<KR> Sealed for $range<KR> {}

        impl<'a, K: Key + 'a, KR: Borrow<K::SelfType<'a>>> KeyRange<K> for $range<KR> {
            fn key_bounds(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
                encode_bounds::<K, KR, Self>(self)
            }
        }
    };
}

impl_key_range!(Range);
impl_key_range!(RangeFrom);
impl_key_range!(RangeInclusive);
impl_key_range!(RangeTo);
impl_key_range!(RangeToInclusive);

impl Sealed for RangeFull {}

impl<K: Key> KeyRange<K> for RangeFull {
    fn key_bounds(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        (Bound::Unbounded, Bound::Unbounded)
    }
}

impl<KR> Sealed for (Bound<KR>, Bound<KR>) {}

impl<'a, K: Key + 'a, KR: Borrow<K::SelfType<'a>>> KeyRange<K> for (Bound<KR>, Bound<KR>) {
    fn key_bounds(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        encode_bounds::<K, KR, Self>(self)
    }
}

impl<R: Sealed> Sealed for &R {}

impl<K: Key, R: KeyRange<K>> KeyRange<K> for &R {
    fn key_bounds(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        (*self).key_bounds()
    }
}
