use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(RedbValue)]
pub fn redb_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let expanded = quote! {
        impl redb::RedbValue for #name {
            type SelfType<'a> = Self;
            type AsBytes<'a> = Vec<u8> where Self: 'a;

            fn fixed_width() -> Option<usize> {
                Some(std::mem::size_of::<Self>())
            }

            fn from_bytes<'a>(data: &'a [u8]) -> Self
            where
                Self: 'a,
            {
                redb::deserialize(data).unwrap()
            }

            fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
            where
                Self: 'a,
                Self: 'b,
            {
                redb::serialize(value).unwrap()
            }

            fn type_name() -> redb::TypeName {
                redb::TypeName::new(stringify!($t))
            }
        }
    };

    TokenStream::from(expanded)
}
