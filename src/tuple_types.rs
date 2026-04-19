use crate::complex_types::{decode_varint_len, encode_varint_len};
use crate::types::{Key, TypeName, Value};
use std::borrow::Borrow;
use std::cmp::Ordering;

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

            if Self::fixed_width().is_some() {
                TypeName::internal(&result)
            } else {
                TypeName::internal2(&result)
            }
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
        if Self::fixed_width().is_some() {
            TypeName::internal(&format!("({},)", T::type_name().name()))
        } else {
            TypeName::internal2(&format!("({},)", T::type_name().name()))
        }
    }
}

impl<T: Key> Key for (T,) {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        T::compare(data1, data2)
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
    use crate::types::Value;

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
}
