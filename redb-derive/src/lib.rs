use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(RedbValue, attributes(fixed_width, type_name))]
pub fn redb_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let mut width = quote!(None);
    let mut type_name = quote!(stringify!(#name));
    for attr in input.attrs {
        if attr.path().is_ident("fixed_width") {
            width = quote!(Some(std::mem::size_of::<Self>()));
        }
        if attr.path().is_ident("type_name") {
            type_name = attr.meta.require_list().unwrap().tokens.clone()
        }
    }

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics redb::RedbValue for #name #ty_generics #where_clause {
            type SelfType<'a> = Self;
            type AsBytes<'a> = Vec<u8> where Self: 'a;

            fn fixed_width() -> Option<usize> {
                #width
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
                redb::TypeName::new(#type_name)
            }
        }
    };

    TokenStream::from(expanded)
}
