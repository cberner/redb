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

// The `Key` operations of one element's type, so that `tuple_separator()`, which is generic only
// over the number of elements, can call them
#[derive(Clone, Copy)]
struct ElementType {
    fixed_width: Option<usize>,
    min_encoded_key: fn() -> Option<Cow<'static, [u8]>>,
    compare: fn(&[u8], &[u8]) -> Ordering,
    separator: for<'a> fn(&'a [u8], &'a [u8]) -> Cow<'a, [u8]>,
}

impl ElementType {
    fn of<T: Key>() -> Self {
        Self {
            fixed_width: T::fixed_width(),
            min_encoded_key: T::min_encoded_key,
            compare: T::compare,
            separator: T::separator,
        }
    }

    // An encoding that sorts strictly between `left` and `right`, or `None` when the type does
    // not offer one: `separator()` only guarantees a result in `[left, right)`
    fn between<'a>(&self, left: &'a [u8], right: &'a [u8]) -> Option<Cow<'a, [u8]>> {
        let separator = (self.separator)(left, right);
        if (self.compare)(left, &separator).is_lt() {
            Some(separator)
        } else {
            None
        }
    }

    // The smallest encoding of the type, or `fallback` for a type that has none
    fn smallest_or<'a>(&self, fallback: &'a [u8]) -> Cow<'a, [u8]> {
        match (self.min_encoded_key)() {
            Some(smallest) => smallest,
            None => Cow::Borrowed(fallback),
        }
    }
}

// The elements of a variable width tuple encoding: the `N` leading ones, whose lengths the header
// gives, then the last, which runs to the end
fn split_elements<const N: usize>(fixed_width: [Option<usize>; N], data: &[u8]) -> Vec<&[u8]> {
    let (header, lens) = parse_lens::<N>(fixed_width, data);
    let mut elements = Vec::with_capacity(N + 1);
    let mut offset = header;
    for len in lens {
        elements.push(&data[offset..(offset + len)]);
        offset += len;
    }
    elements.push(&data[offset..]);
    elements
}

// Lexicographically compares two tuples, given as their encoded elements
fn compare_elements<A: AsRef<[u8]>, B: AsRef<[u8]>>(
    types: &[ElementType],
    a: &[A],
    b: &[B],
) -> Ordering {
    for i in 0..types.len() {
        let ordering = (types[i].compare)(a[i].as_ref(), b[i].as_ref());
        if !ordering.is_eq() {
            return ordering;
        }
    }
    Ordering::Equal
}

// Encodes the tuple with these elements, or returns `None` when the encoding would be at least
// as long as `left`. The length is checked first, so an oversized encoding -- a smallest
// encoding can be longer than the element it replaced -- is never built.
fn encode_if_shorter(
    types: &[ElementType],
    elements: &[Cow<'_, [u8]>],
    left: &[u8],
) -> Option<Vec<u8>> {
    // A fixed width element's length is not recorded, so a replacement must be exactly that wide
    debug_assert!(types.iter().zip(elements).all(|(element_type, element)| {
        element_type
            .fixed_width
            .is_none_or(|width| element.len() == width)
    }));
    // Length varints for every variable width element but the last, whose length is implicit.
    // They are rebuilt rather than copied, since a replaced element's length may differ.
    let mut result = Vec::with_capacity(left.len());
    for i in 0..(elements.len() - 1) {
        if types[i].fixed_width.is_none() {
            encode_varint_len(elements[i].len(), &mut result);
        }
    }
    let mut total = result.len();
    for element in elements {
        total = total.saturating_add(element.len());
    }
    if total >= left.len() {
        return None;
    }
    for element in elements {
        result.extend_from_slice(element);
    }
    Some(result)
}

// The separator between two encodings of a variable width tuple. `leading` describes every
// element but the last, whose length is implicit and which `last` describes.
fn tuple_separator<'a, const N: usize>(
    leading: [ElementType; N],
    last: ElementType,
    left: &'a [u8],
    right: &'a [u8],
) -> Cow<'a, [u8]> {
    let fixed_width: [Option<usize>; N] = core::array::from_fn(|i| leading[i].fixed_width);
    let types: Vec<ElementType> = leading.into_iter().chain([last]).collect();
    let left_elements = split_elements(fixed_width, left);
    let right_elements = split_elements(fixed_width, right);

    let mut differing = None;
    for i in 0..types.len() {
        if !(types[i].compare)(left_elements[i], right_elements[i]).is_eq() {
            differing = Some(i);
            break;
        }
    }
    // `left` sorts below `right`, so some element differs
    let Some(index) = differing else {
        return Cow::Borrowed(left);
    };

    // The candidate: the elements the two keys share, then one that sorts above `left`'s --
    // between the differing pair when the type offers that, `right`'s own otherwise -- then the
    // smallest encoding of each remaining type, or `right`'s element where a type has none
    let mut elements = Vec::with_capacity(types.len());
    for &bytes in &left_elements[..index] {
        elements.push(Cow::Borrowed(bytes));
    }
    match types[index].between(left_elements[index], right_elements[index]) {
        Some(between) => elements.push(between),
        None => elements.push(Cow::Borrowed(right_elements[index])),
    }
    for (element_type, &bytes) in types[(index + 1)..]
        .iter()
        .zip(&right_elements[(index + 1)..])
    {
        elements.push(element_type.smallest_or(bytes));
    }

    // The candidate sorts above `left`, since its element at `index` does, but collapsing may
    // have left it at or above `right`
    if compare_elements(&types, &elements, &right_elements).is_ge() {
        return Cow::Borrowed(left);
    }
    match encode_if_shorter(&types, &elements, left) {
        Some(separator) => Cow::Owned(separator),
        None => Cow::Borrowed(left),
    }
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
                    [$(ElementType::of::<$t>(),)+],
                    ElementType::of::<$t_last>(),
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
        // A shortened leading element's length varint is rewritten, and a `u64`, which has no
        // smallest encoding, stays `right`'s
        check_separator::<(&str, u64)>(&("abc0suffix", 1), &("abc1suffix", 2), &("abc1", 2));
        // ...including when the shortened element is neither the first nor the last
        check_separator::<(u8, &str, u64)>(
            &(9, "abc0suffix", 1),
            &(9, "abc1suffix", 2),
            &(9, "abc1", 2),
        );
        // A varint that shrinks from three bytes to one is rewritten, not just overwritten
        let long = "x".repeat(300);
        check_separator::<(&str, u64)>(
            &(&format!("0{long}"), 1),
            &(&format!("1{long}"), 2),
            &("1", 2),
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
        // Elements whose types have no smallest encoding stay `right`'s
        check_separator::<(&str, [&str; 2])>(
            &("abc0suffix", ["kept", "here"]),
            &("abc1suffix", ["other", "values"]),
            &("abc1", ["other", "values"]),
        );
        check_separator::<(&str, u64, &str)>(
            &("abc0suffix", 7, "a long tail"),
            &("abc1suffix", 9, "another"),
            &("abc1", 9, ""),
        );
        // `Option` discards down to its tag byte, which is what `None` encodes to
        check_separator::<(&str, Option<&str>)>(
            &("abc0suffix", Some("a long tail")),
            &("abc1suffix", Some("another")),
            &("abc1", None),
        );
        // A fixed width `Option` collapses to its `None`, which is as wide as any `Some`
        check_separator::<(&str, Option<u64>)>(
            &("abc0suffix", Some(7)),
            &("abc1suffix", Some(9)),
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
        // A wholly fixed width tuple is compared at its width, so nothing shorter may be returned
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
