use crate::complex_types::{decode_varint_len, encode_varint_len};
use crate::types::{Key, TypeName, Value};
use alloc::borrow::Cow;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::borrow::Borrow;
use core::cmp::Ordering;

fn serialize_tuple_elements_variable<const N: usize>(
    is_fixed_width: [bool; N],
    slices: [&[u8]; N],
) -> Vec<u8> {
    let total_len: usize = slices.iter().map(|x| x.len()).sum();
    let worst_case_len_overhead: usize =
        is_fixed_width.iter().map(|x| if *x { 0 } else { 5 }).sum();
    let mut output = Vec::with_capacity(total_len + worst_case_len_overhead);
    let zipped = is_fixed_width.iter().zip(slices.iter());
    for len in zipped
        .map(|(fixed, x)| if *fixed { None } else { Some(x.len()) })
        .take(slices.len() - 1)
        .flatten()
    {
        encode_varint_len(len, &mut output);
    }

    for slice in slices {
        output.extend_from_slice(slice);
    }

    debug_assert!(output.len() <= total_len + worst_case_len_overhead);

    output
}

fn serialize_tuple_elements_fixed(slices: &[&[u8]]) -> Vec<u8> {
    let total_len: usize = slices.iter().map(|x| x.len()).sum();
    let mut output = Vec::with_capacity(total_len);
    for slice in slices {
        output.extend_from_slice(slice);
    }
    output
}

fn parse_lens<const N: usize>(fixed_width: [Option<usize>; N], data: &[u8]) -> (usize, [usize; N]) {
    let mut result = [0; N];
    let mut offset = 0;
    for (i, &fixed) in fixed_width.iter().enumerate() {
        if let Some(len) = fixed {
            result[i] = len;
        } else {
            let (len, bytes_read) = decode_varint_len(&data[offset..]);
            result[i] = len;
            offset += bytes_read;
        }
    }
    (offset, result)
}

fn not_equal<T: Key>(data1: &[u8], data2: &[u8]) -> Option<Ordering> {
    match T::compare(data1, data2) {
        Ordering::Less => Some(Ordering::Less),
        Ordering::Equal => None,
        Ordering::Greater => Some(Ordering::Greater),
    }
}

// One element of a tuple, so that `tuple_separator()` can walk elements whose types it cannot name
#[derive(Clone, Copy)]
struct TupleElement {
    fixed_width: Option<usize>,
    min_encoded_key: fn() -> Option<Cow<'static, [u8]>>,
    compare: fn(&[u8], &[u8]) -> Ordering,
    separator: for<'a> fn(&'a [u8], &'a [u8]) -> Cow<'a, [u8]>,
}

impl TupleElement {
    fn of<T: Key>() -> Self {
        Self {
            fixed_width: T::fixed_width(),
            min_encoded_key: T::min_encoded_key,
            compare: T::compare,
            separator: T::separator,
        }
    }

    // A separator for this element, and whether it sorts strictly above `left`, or `None` when a
    // fixed width element leaves nothing to shorten: its encodings are compared at that width.
    fn shorten<'a>(&self, left: &'a [u8], right: &'a [u8]) -> Option<(Cow<'a, [u8]>, bool)> {
        if self.fixed_width.is_some() {
            return None;
        }
        let separator = (self.separator)(left, right);
        let strict = (self.compare)(left, &separator).is_lt();
        Some((separator, strict))
    }

    // What to store for an element the separator no longer has to distinguish, and whether that
    // sorts strictly below `right`'s.
    //
    // `unread` is whether the element the two differ in already resolves both comparisons, so
    // that nothing stored here is ever read: `left`'s bytes then serve for a type with no smallest
    // encoding, and a fixed width element, which a smallest encoding cannot shorten, is kept as it
    // is. Otherwise the comparison against `right` reaches this element, and `right`'s own bytes
    // serve instead, leaving the decision to the elements after it.
    fn discard<'a>(&self, left: &'a [u8], right: &'a [u8], unread: bool) -> (Cow<'a, [u8]>, bool) {
        if unread && self.fixed_width.is_some() {
            return (Cow::Borrowed(left), false);
        }
        match (self.min_encoded_key)() {
            // A smallest encoding sorts at or below every value, so it never sorts above `right`'s
            Some(minimum) => {
                let below = !unread && (self.compare)(&minimum, right).is_lt();
                (minimum, below)
            }
            None if unread => (Cow::Borrowed(left), false),
            None => (Cow::Borrowed(right), false),
        }
    }
}

// The separator when the two encodings differ only in their last element. It stores no length and
// has nothing after it, so the rest of the encoding is kept as it is. `offset` and `right_offset`
// are where that element starts in each.
fn last_element_separator<'a>(
    last: TupleElement,
    left: &'a [u8],
    right: &'a [u8],
    offset: usize,
    right_offset: usize,
) -> Cow<'a, [u8]> {
    let Some((separator, _)) = last.shorten(&left[offset..], &right[right_offset..]) else {
        return Cow::Borrowed(left);
    };
    if separator.len() >= left.len() - offset {
        return Cow::Borrowed(left);
    }
    let mut result = Vec::with_capacity(offset + separator.len());
    result.extend_from_slice(&left[..offset]);
    result.extend_from_slice(&separator);
    Cow::Owned(result)
}

// The separator between two encodings of a variable width tuple. `leading` describes every element
// but the last, whose length is implicit and which `last` describes.
fn tuple_separator<'a, const N: usize>(
    leading: [TupleElement; N],
    last: TupleElement,
    left: &'a [u8],
    right: &'a [u8],
) -> Cow<'a, [u8]> {
    let fixed_width: [Option<usize>; N] = core::array::from_fn(|i| leading[i].fixed_width);
    let (header, lens) = parse_lens::<N>(fixed_width, left);
    let (right_header, right_lens) = parse_lens::<N>(fixed_width, right);

    // Find the leading element the two first differ in. The ones before it are equal, and are kept
    // as they are, since they are what makes the result sort below `right` at all.
    let mut offset = header;
    let mut right_offset = right_header;
    let mut differing = None;
    for (index, element) in leading.iter().enumerate() {
        let bytes = &left[offset..(offset + lens[index])];
        let right_bytes = &right[right_offset..(right_offset + right_lens[index])];
        if !(element.compare)(bytes, right_bytes).is_eq() {
            differing = Some(index);
            break;
        }
        offset += lens[index];
        right_offset += right_lens[index];
    }
    let Some(index) = differing else {
        return last_element_separator(last, left, right, offset, right_offset);
    };

    // What to store in place of that element. A shortened one that sorts strictly above `left`'s
    // is best: both comparisons resolve there, so the elements after it are never read. `right`'s
    // own element sorts strictly above `left`'s too, so it separates as well, but it only resolves
    // the comparison against `left`: the one against `right` runs on into the elements after it.
    let bytes = &left[offset..(offset + lens[index])];
    let right_bytes = &right[right_offset..(right_offset + right_lens[index])];
    let shortened = leading[index]
        .shorten(bytes, right_bytes)
        .filter(|(_, strict)| *strict)
        .map(|(separator, _)| separator);
    let unread = shortened.is_some();

    let mut elements = Vec::with_capacity(N + 1);
    let mut below_right = unread;
    let mut cursor = header;
    let mut right_cursor = right_header;
    for (i, element) in leading.iter().enumerate() {
        let bytes = &left[cursor..(cursor + lens[i])];
        let right_bytes = &right[right_cursor..(right_cursor + right_lens[i])];
        cursor += lens[i];
        right_cursor += right_lens[i];
        elements.push(match i.cmp(&index) {
            Ordering::Less => Cow::Borrowed(bytes),
            Ordering::Equal => match &shortened {
                Some(separator) => Cow::Borrowed(separator.as_ref()),
                None => Cow::Borrowed(right_bytes),
            },
            Ordering::Greater => {
                let (stored, below) = element.discard(bytes, right_bytes, unread);
                below_right |= below;
                stored
            }
        });
    }
    let (stored, below) = last.discard(&left[cursor..], &right[right_cursor..], unread);
    below_right |= below;
    elements.push(stored);

    // Taking `right`'s element leaves the comparison against `right` to the elements after it, so
    // one of them has to sort strictly below `right`'s -- otherwise the result is `right` itself
    if !below_right {
        return Cow::Borrowed(left);
    }

    // Every length varint is rebuilt, since a shortened element's may shrink as well
    let mut result = Vec::with_capacity(left.len());
    for (i, element) in leading.iter().enumerate() {
        if element.fixed_width.is_none() {
            encode_varint_len(elements[i].len(), &mut result);
        }
    }
    // A separator longer than the whole key is no use, however it was assembled. A smallest
    // encoding longer than the element it replaces can make it longer, so this is checked
    // before copying the elements in.
    let total = elements
        .iter()
        .map(|x| x.len())
        .fold(result.len(), usize::saturating_add);
    if total >= left.len() {
        return Cow::Borrowed(left);
    }
    for element in &elements {
        result.extend_from_slice(element);
    }
    Cow::Owned(result)
}

macro_rules! fixed_width_impl {
    ( $( $t:ty ),+ ) => {
        {
            let mut sum = 0;
            $(
                sum += <$t>::fixed_width()?;
            )+
            Some(sum)
        }
    };
}

macro_rules! as_bytes_impl {
    ( $value:expr, $( $t:ty, $i:tt ),+ ) => {{
        if Self::fixed_width().is_some() {
            serialize_tuple_elements_fixed(&[
                $(
                    <$t>::as_bytes($value.$i.borrow()).as_ref(),
                )+
            ])
        } else {
            serialize_tuple_elements_variable(
            [
                $(
                    <$t>::fixed_width().is_some(),
                )+
            ],
            [
                $(
                    <$t>::as_bytes($value.$i.borrow()).as_ref(),
                )+
            ])
        }
    }};
}

macro_rules! type_name_impl {
    ( $head:ty $(,$tail:ty)+ ) => {
        {
            let mut result = String::new();
            result.push('(');
            result.push_str(&<$head>::type_name().name());
            $(
                result.push(',');
                result.push_str(&<$tail>::type_name().name());
            )+
            result.push(')');

            let any_user_defined = <$head>::type_name().is_user_defined()
                $(|| <$tail>::type_name().is_user_defined())+;

            let natural = if Self::fixed_width().is_some() {
                TypeName::internal(&result)
            } else {
                TypeName::internal2(&result)
            };
            natural.into_composite(any_user_defined)
        }
    };
}

macro_rules! from_bytes_variable_impl {
    ( $data:expr $(,$t:ty, $v:ident, $i:literal )+ | $t_last:ty, $v_last:ident, $i_last:literal ) => {
        #[allow(clippy::manual_bits)]
        {
            let (mut offset, lens) = parse_lens::<$i_last>(
                [
                    $(
                        <$t>::fixed_width(),
                    )+
                ],
                $data);
            $(
                let len = lens[$i];
                let $v = <$t>::from_bytes(&$data[offset..(offset + len)]);
                offset += len;
            )+
            let $v_last = <$t_last>::from_bytes(&$data[offset..]);
            ($(
                $v,
            )+
                $v_last
            )
        }
    };
}

macro_rules! from_bytes_fixed_impl {
    ( $data:expr $(,$t:ty, $v:ident )+ ) => {
        {
            let mut offset = 0;
            $(
                let len = <$t>::fixed_width().unwrap();
                let $v = <$t>::from_bytes(&$data[offset..(offset + len)]);
                #[allow(unused_assignments)]
                {
                    offset += len;
                }
            )+

            ($(
                $v,
            )+)
        }
    };
}

macro_rules! compare_variable_impl {
    ( $data0:expr, $data1:expr $(,$t:ty, $i:literal )+ | $t_last:ty, $i_last:literal ) => {
        #[allow(clippy::manual_bits)]
        {
            let fixed_width = [
                $(
                    <$t>::fixed_width(),
                )+
            ];
            let (mut offset0, lens0) = parse_lens::<$i_last>(fixed_width, $data0);
            let (mut offset1, lens1) = parse_lens::<$i_last>(fixed_width, $data1);
            $(
                let index = $i;
                let len0 = lens0[index];
                let len1 = lens1[index];
                if let Some(order) = not_equal::<$t>(
                    &$data0[offset0..(offset0 + len0)],
                    &$data1[offset1..(offset1 + len1)],
                ) {
                    return order;
                }
                offset0 += len0;
                offset1 += len1;
            )+

            <$t_last>::compare(&$data0[offset0..], &$data1[offset1..])
        }
    };
}

macro_rules! compare_fixed_impl {
    ( $data0:expr, $data1:expr, $($t:ty),+ ) => {
        {
            let mut offset0 = 0;
            let mut offset1 = 0;
            $(
                let len = <$t>::fixed_width().unwrap();
                if let Some(order) = not_equal::<$t>(
                    &$data0[offset0..(offset0 + len)],
                    &$data1[offset1..(offset1 + len)],
                ) {
                    return order;
                }
                #[allow(unused_assignments)]
                {
                    offset0 += len;
                    offset1 += len;
                }
            )+

            Ordering::Equal
        }
    };
}

macro_rules! tuple_impl {
    ( $($t:ident, $v:ident, $i:tt ),+ | $t_last:ident, $v_last:ident, $i_last:tt ) => {
        impl<$($t: Value,)+ $t_last: Value> Value for ($($t,)+ $t_last) {
            type SelfType<'a> = (
                $(<$t>::SelfType<'a>,)+
                <$t_last>::SelfType<'a>,
            )
            where
                Self: 'a;
            type AsBytes<'a> = Vec<u8>
            where
                Self: 'a;

            fn fixed_width() -> Option<usize> {
                fixed_width_impl!($($t,)+ $t_last)
            }

            fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
            where
                Self: 'a,
            {
                if Self::fixed_width().is_some() {
                    from_bytes_fixed_impl!(data $(,$t,$v)+, $t_last, $v_last)
                } else {
                    from_bytes_variable_impl!(data $(,$t,$v,$i)+ | $t_last, $v_last, $i_last)
                }
            }

            fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8>
            where
                Self: 'a,
                Self: 'b,
            {
                as_bytes_impl!(value, $($t,$i,)+ $t_last, $i_last)
            }

            fn type_name() -> TypeName {
                type_name_impl!($($t,)+ $t_last)
            }
        }

        impl<$($t: Key,)+ $t_last: Key> Key for ($($t,)+ $t_last) {
            fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
                if Self::fixed_width().is_some() {
                    compare_fixed_impl!(data1, data2, $($t,)+ $t_last)
                } else {
                    compare_variable_impl!(data1, data2 $(,$t,$i)+ | $t_last, $i_last)
                }
            }

            fn separator<'a>(left: &'a [u8], right: &'a [u8]) -> Cow<'a, [u8]> {
                // A fixed width tuple's encodings are compared at that width, so nothing shorter
                // than `left` may be returned
                if Self::fixed_width().is_some() {
                    return Cow::Borrowed(left);
                }
                tuple_separator::<$i_last>(
                    [$(TupleElement::of::<$t>(),)+],
                    TupleElement::of::<$t_last>(),
                    left,
                    right,
                )
            }
        }
    };
}

impl<T: Value> Value for (T,) {
    type SelfType<'a>
        = (T::SelfType<'a>,)
    where
        Self: 'a;
    type AsBytes<'a>
        = T::AsBytes<'a>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        T::fixed_width()
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        (T::from_bytes(data),)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        T::as_bytes(&value.0)
    }

    fn type_name() -> TypeName {
        let inner = T::type_name();
        TypeName::internal(&format!("({},)", inner.name())).into_composite(inner.is_user_defined())
    }
}

impl<T: Key> Key for (T,) {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        T::compare(data1, data2)
    }

    // Encoded and compared exactly as `T`, so it separates the same way
    fn separator<'a>(left: &'a [u8], right: &'a [u8]) -> Cow<'a, [u8]> {
        T::separator(left, right)
    }

    // Encoded exactly as `T`, so its smallest value encodes the same way
    fn min_encoded_key() -> Option<Cow<'static, [u8]>> {
        T::min_encoded_key()
    }
}

tuple_impl! {
    T0, t0, 0
    | T1, t1, 1
}

tuple_impl! {
    T0, t0, 0,
    T1, t1, 1
    | T2, t2, 2
}

tuple_impl! {
    T0, t0, 0,
    T1, t1, 1,
    T2, t2, 2
    | T3, t3, 3
}

tuple_impl! {
    T0, t0, 0,
    T1, t1, 1,
    T2, t2, 2,
    T3, t3, 3
    | T4, t4, 4
}

tuple_impl! {
    T0, t0, 0,
    T1, t1, 1,
    T2, t2, 2,
    T3, t3, 3,
    T4, t4, 4
    | T5, t5, 5
}

tuple_impl! {
    T0, t0, 0,
    T1, t1, 1,
    T2, t2, 2,
    T3, t3, 3,
    T4, t4, 4,
    T5, t5, 5
    | T6, t6, 6
}

tuple_impl! {
    T0, t0, 0,
    T1, t1, 1,
    T2, t2, 2,
    T3, t3, 3,
    T4, t4, 4,
    T5, t5, 5,
    T6, t6, 6
    | T7, t7, 7
}

tuple_impl! {
    T0, t0, 0,
    T1, t1, 1,
    T2, t2, 2,
    T3, t3, 3,
    T4, t4, 4,
    T5, t5, 5,
    T6, t6, 6,
    T7, t7, 7
    | T8, t8, 8
}

tuple_impl! {
    T0, t0, 0,
    T1, t1, 1,
    T2, t2, 2,
    T3, t3, 3,
    T4, t4, 4,
    T5, t5, 5,
    T6, t6, 6,
    T7, t7, 7,
    T8, t8, 8
    | T9, t9, 9
}

tuple_impl! {
    T0, t0, 0,
    T1, t1, 1,
    T2, t2, 2,
    T3, t3, 3,
    T4, t4, 4,
    T5, t5, 5,
    T6, t6, 6,
    T7, t7, 7,
    T8, t8, 8,
    T9, t9, 9
    | T10, t10, 10
}

tuple_impl! {
    T0, t0, 0,
    T1, t1, 1,
    T2, t2, 2,
    T3, t3, 3,
    T4, t4, 4,
    T5, t5, 5,
    T6, t6, 6,
    T7, t7, 7,
    T8, t8, 8,
    T9, t9, 9,
    T10, t10, 10
    | T11, t11, 11
}

#[cfg(test)]
mod test {
    use crate::types::{Key, TypeName, Value};
    use alloc::borrow::Cow;
    use alloc::format;
    use core::cmp::Ordering;

    #[test]
    fn width() {
        assert!(<(&str, u8)>::fixed_width().is_none());
        assert!(<(u16, u8, &str, u128)>::fixed_width().is_none());
        assert_eq!(<(u16,)>::fixed_width().unwrap(), 2);
        assert_eq!(<(u16, u8)>::fixed_width().unwrap(), 3);
        assert_eq!(<(u16, u8, u128)>::fixed_width().unwrap(), 19);
        assert_eq!(<(u16, u8, i8, u128)>::fixed_width().unwrap(), 20);
        // Check that length of final field is elided
        assert_eq!(
            <(u8, &str)>::as_bytes(&(1, "hello")).len(),
            "hello".len() + size_of::<u8>()
        );
        // Check that varint encoding uses only 1 byte for small strings
        assert_eq!(
            <(&str, u8)>::as_bytes(&("hello", 1)).len(),
            "hello".len() + size_of::<u8>() + size_of::<u8>()
        );
    }

    // `expected` is the tuple the separator between `left` and `right` must encode to
    fn check_separator<K: Key>(
        left: &K::SelfType<'_>,
        right: &K::SelfType<'_>,
        expected: &K::SelfType<'_>,
    ) {
        let left = K::as_bytes(left);
        let right = K::as_bytes(right);
        let separator = K::separator(left.as_ref(), right.as_ref());
        assert_eq!(separator.as_ref(), K::as_bytes(expected).as_ref());
        assert!(K::compare(left.as_ref(), &separator).is_le());
        assert!(K::compare(&separator, right.as_ref()).is_lt());
        // A separator has to be an encoding of the key type
        assert_eq!(
            K::as_bytes(&K::from_bytes(&separator)).as_ref(),
            separator.as_ref()
        );
    }

    #[test]
    fn single_element_tuple_separator() {
        // Encoded exactly as the element, so it separates the same way
        check_separator::<(&str,)>(&("abc0suffix",), &("abc1suffix",), &("abc1",));
    }

    #[test]
    fn last_element_separator() {
        // The last element stores no length, so shortening it leaves the rest of the encoding
        // untouched
        check_separator::<(u64, &str)>(&(7, "abc0suffix"), &(7, "abc1suffix"), &(7, "abc1"));
    }

    #[test]
    fn leading_element_separator() {
        // A shortened leading element's length varint is rewritten, and the elements after it
        // are `left`'s, not `right`'s
        check_separator::<(&str, u64)>(&("abc0suffix", 1), &("abc1suffix", 2), &("abc1", 1));
        // ...including when the shortened element is neither the first nor the last
        check_separator::<(u8, &str, u64)>(
            &(9, "abc0suffix", 1),
            &(9, "abc1suffix", 2),
            &(9, "abc1", 1),
        );
        // A varint that shrinks from three bytes to one is rewritten, not just overwritten
        let long = "x".repeat(300);
        check_separator::<(&str, u64)>(
            &(&format!("0{long}"), 1),
            &(&format!("1{long}"), 2),
            &("1", 1),
        );
    }

    #[test]
    fn right_element_separator() {
        // A fixed width element cannot shrink, but `right`'s own sorts strictly above `left`'s,
        // which is enough to let the elements after it collapse
        check_separator::<(u64, &str)>(&(1, "suffix"), &(2, "suffix"), &(2, ""));
        // ...as does a variable width element with no separator above `left`'s, when taking it
        // whole still costs less than the elements after it
        check_separator::<(&str, &str)>(&("a", "a long tail"), &("ab", "x"), &("ab", ""));
    }

    #[test]
    fn right_element_is_not_taken_without_a_smaller_tail() {
        // Taking `right`'s element leaves the comparison against `right` to the elements after it,
        // so `right`'s own already being the smallest encodings would produce `right` itself
        check_separator::<(u64, &str)>(&(1, "x"), &(2, ""), &(1, "x"));
        // ...and an element with no smallest encoding cannot be brought below `right`'s at all
        check_separator::<(u64, [&str; 1])>(&(1, ["x"]), &(2, ["y"]), &(1, ["x"]));
    }

    #[test]
    fn elements_after_the_shortened_one_are_discarded() {
        // The shortened element sorts strictly above `left`'s, so both comparisons resolve there
        // and the elements after it collapse to their smallest encodings
        check_separator::<(&str, &str)>(
            &("abc0suffix", "a long tail"),
            &("abc1suffix", "another"),
            &("abc1", ""),
        );
        // A variable width element with no smallest encoding of its own is kept as it is
        check_separator::<(&str, [&str; 2])>(
            &("abc0suffix", ["kept", "here"]),
            &("abc1suffix", ["other", "values"]),
            &("abc1", ["kept", "here"]),
        );
        // ...as is a fixed width one, which is compared at its width
        check_separator::<(&str, u64, &str)>(
            &("abc0suffix", 7, "a long tail"),
            &("abc1suffix", 9, "another"),
            &("abc1", 7, ""),
        );
        // `Option` discards down to its tag byte, which is what `None` encodes to
        check_separator::<(&str, Option<&str>)>(
            &("abc0suffix", Some("a long tail")),
            &("abc1suffix", Some("another")),
            &("abc1", None),
        );
    }

    #[test]
    fn separator_returns_full_key_when_nothing_is_shorter() {
        check_separator::<(&str, u64)>(&("abc", 1), &("abd-suffix", 2), &("abc", 1));
        // ...and when it is the last element that has no shorter separator
        check_separator::<(u64, &str)>(&(7, "abc"), &(7, "abd-suffix"), &(7, "abc"));
        // ...and when it is the last element and fixed width
        check_separator::<(&str, u64)>(&("same", 1), &("same", 2), &("same", 1));
        // Nothing sorts between these first elements, so only `right`'s own separates, and taking
        // it whole costs more than collapsing the tail saves
        check_separator::<(&str, &str)>(&("abc", "tail"), &("abc-suffix", "x"), &("abc", "tail"));
        // A wholly fixed width tuple is never asked for a separator at all
        check_separator::<(u64, u8)>(&(1, 1), &(2, 2), &(1, 1));
    }

    // A key type whose smallest value encodes to more bytes than an ordinary one. Nothing stops
    // an implementation from being shaped this way, so substituting the minimum into a separator
    // has to be able to make the result longer rather than shorter.
    #[derive(Debug)]
    struct WideMinimum;

    const WIDE_MINIMUM: [u8; 64] = [0; 64];

    impl Value for WideMinimum {
        type SelfType<'a> = &'a [u8];
        type AsBytes<'a>
            = &'a [u8]
        where
            Self: 'a;

        fn fixed_width() -> Option<usize> {
            None
        }

        fn from_bytes<'a>(data: &'a [u8]) -> &'a [u8]
        where
            Self: 'a,
        {
            data
        }

        fn as_bytes<'a, 'b: 'a>(value: &'a &'b [u8]) -> &'a [u8]
        where
            Self: 'b,
        {
            value
        }

        fn type_name() -> TypeName {
            TypeName::new("test::WideMinimum")
        }
    }

    impl Key for WideMinimum {
        // Sorts as its bytes, except that the smallest value sorts below everything
        fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
            match (data1 == WIDE_MINIMUM, data2 == WIDE_MINIMUM) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => data1.cmp(data2),
            }
        }

        // A prefix of `right` separates any two values that are not the smallest one, which is
        // all this type is used with
        fn separator<'a>(left: &'a [u8], right: &'a [u8]) -> Cow<'a, [u8]> {
            <&[u8] as Key>::separator(left, right)
        }

        fn min_encoded_key() -> Option<Cow<'static, [u8]>> {
            Some(Cow::Borrowed(&WIDE_MINIMUM))
        }
    }

    // Discarding an element replaces it with the smallest encoding, which can be longer than the
    // element it replaces. The whole key is kept when that would make the separator longer.
    #[test]
    fn separator_is_not_grown_by_a_wide_minimum() {
        // It really is a minimum: at or below every value, including itself
        assert!(WideMinimum::compare(&WIDE_MINIMUM, b"a").is_lt());
        assert!(WideMinimum::compare(b"a", &WIDE_MINIMUM).is_gt());
        assert!(WideMinimum::compare(&WIDE_MINIMUM, &WIDE_MINIMUM).is_eq());

        // Shortening the first element saves two bytes, but the two the tail would discard to
        // cost 64 each
        check_separator::<(WideMinimum, WideMinimum, WideMinimum)>(
            &(b"aaa", b"x", b"y"),
            &(b"bbb", b"x", b"y"),
            &(b"aaa", b"x", b"y"),
        );
    }
}
