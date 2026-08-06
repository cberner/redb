#![deny(clippy::all, clippy::pedantic, clippy::disallowed_methods)]
#![allow(
    clippy::must_use_candidate,
    clippy::redundant_closure_for_method_calls,
    clippy::similar_names,
    clippy::too_many_lines
)]

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, GenericParam, Ident, parse_macro_input};

// All paths in the generated code must be fully qualified (`::std::...`, `#redb::...`, and
// `<T as #redb::Value>::...` for trait methods, where `#redb` is the path returned by
// `redb_crate_path_for()`). Method-call syntax and bare paths resolve inherent items before
// trait items, so a field type with an inherent `from_bytes` or `as_bytes` (e.g. `uuid::Uuid`)
// would otherwise hijack the generated encoding, and names shadowed at the derive site would
// change what the code means.

// The path of the redb crate in generated code: `::redb`, unless an explicit
// `#[redb(crate = "path")]` on the deriving struct names it otherwise, as when the dependency
// is renamed (`my_redb = { package = "redb" }`) or the implementations are derived against one
// of several versions of the redb package.
fn redb_crate_path_for(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let mut path = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("redb") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("crate") {
                let value: syn::LitStr = meta.value()?.parse()?;
                path = Some(value.parse::<syn::Path>()?);
                Ok(())
            } else {
                Err(meta.error("expected `#[redb(crate = \"...\")]`"))
            }
        })?;
    }
    Ok(path.map_or_else(|| quote! { ::redb }, |path| quote! { #path }))
}

/// Derives `redb::Key` for a struct whose fields all implement `Key`.
///
/// The generated implementation refers to the redb crate as `::redb`. When it should be
/// generated for a crate under another name -- a renamed dependency
/// (`my_redb = { package = "redb" }`), or one of several versions of redb -- name that crate
/// with `#[redb(crate = "my_redb")]`.
#[proc_macro_derive(Key, attributes(redb))]
pub fn derive_key(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match generate_key_impl(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn generate_key_impl(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let Data::Struct(_) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "Key can only be derived for structs",
        ));
    };

    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let redb = redb_crate_path_for(input)?;

    Ok(quote! {
        impl #impl_generics #redb::Key for #name #ty_generics #where_clause {
            fn compare(
                data1: &[::std::primitive::u8],
                data2: &[::std::primitive::u8],
            ) -> ::std::cmp::Ordering {
                let value1 = <Self as #redb::Value>::from_bytes(data1);
                let value2 = <Self as #redb::Value>::from_bytes(data2);
                ::std::cmp::Ord::cmp(&value1, &value2)
            }
        }
    })
}

/// Derives `redb::Value` for a struct whose fields all implement `Value`.
///
/// The generated implementation refers to the redb crate as `::redb`. When it should be
/// generated for a crate under another name -- a renamed dependency
/// (`my_redb = { package = "redb" }`), or one of several versions of redb -- name that crate
/// with `#[redb(crate = "my_redb")]`.
#[proc_macro_derive(Value, attributes(redb))]
pub fn derive_value(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match generate_value_impl(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn generate_value_impl(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let Data::Struct(data_struct) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "Value can only be derived for structs",
        ));
    };

    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let self_type = generate_self_type(name, generics)?;
    let redb = redb_crate_path_for(input)?;

    let type_name_impl = generate_type_name(name, &data_struct.fields, &redb);
    let as_bytes_impl = generate_as_bytes(&data_struct.fields, &redb);
    let from_bytes_impl = generate_from_bytes(name, &data_struct.fields, &redb);
    let fixed_width_impl = generate_fixed_width(&data_struct.fields, &redb);

    Ok(quote! {
        impl #impl_generics #redb::Value for #name #ty_generics #where_clause {
            type SelfType<'a> = #self_type
            where
                Self: 'a;
            type AsBytes<'a> = ::std::vec::Vec<::std::primitive::u8>
            where
                Self: 'a;

            fn fixed_width() -> ::std::option::Option<::std::primitive::usize> {
                #fixed_width_impl
            }

            fn from_bytes<'a>(data: &'a [::std::primitive::u8]) -> Self::SelfType<'a>
            where
                Self: 'a,
            {
                #from_bytes_impl
            }

            fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
            where
                Self: 'b,
            {
                #as_bytes_impl
            }

            fn type_name() -> #redb::TypeName {
                #type_name_impl
            }
        }
    })
}

fn generate_self_type(
    name: &syn::Ident,
    generics: &syn::Generics,
) -> syn::Result<proc_macro2::TokenStream> {
    if generics.params.is_empty() {
        Ok(quote! { #name })
    } else {
        let mut params = vec![];
        for param in &generics.params {
            match param {
                GenericParam::Lifetime(_) => params.push(quote! { 'a }),
                GenericParam::Type(type_param) => {
                    return Err(syn::Error::new_spanned(
                        type_param,
                        "Value derivation is not implemented for structs with type parameters",
                    ));
                }
                GenericParam::Const(const_param) => {
                    return Err(syn::Error::new_spanned(
                        const_param,
                        "Value derivation is not implemented for structs with const parameters",
                    ));
                }
            }
        }

        Ok(quote! { #name<#(#params),*> })
    }
}

fn generate_type_name(
    struct_name: &Ident,
    fields: &Fields,
    redb: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    match fields {
        Fields::Named(fields_named) => {
            let field_strings: Vec<_> = fields_named
                .named
                .iter()
                .map(|field| {
                    let field_name = field.ident.as_ref().unwrap();
                    let field_type = &field.ty;
                    quote! {
                        ::std::format!(
                            "{}: {}",
                            ::std::stringify!(#field_name),
                            <#field_type as #redb::Value>::type_name().name(),
                        )
                    }
                })
                .collect();

            if field_strings.is_empty() {
                quote! {
                    #redb::TypeName::new(&::std::format!("{} {{}}",
                        ::std::stringify!(#struct_name),
                    ))
                }
            } else {
                quote! {
                    #redb::TypeName::new(&::std::format!("{} {{{}}}",
                        ::std::stringify!(#struct_name),
                        [#(#field_strings),*].join(", ")
                    ))
                }
            }
        }
        Fields::Unnamed(fields_unnamed) => {
            let field_strings: Vec<_> = fields_unnamed
                .unnamed
                .iter()
                .map(|field| {
                    let field_type = &field.ty;
                    quote! {
                        <#field_type as #redb::Value>::type_name().name()
                    }
                })
                .collect();

            if field_strings.is_empty() {
                quote! {
                    #redb::TypeName::new(&::std::format!("{}()",
                        ::std::stringify!(#struct_name),
                    ))
                }
            } else {
                quote! {
                    #redb::TypeName::new(&::std::format!("{}({})",
                        ::std::stringify!(#struct_name),
                        [#(#field_strings),*].join(", ")
                    ))
                }
            }
        }
        Fields::Unit => {
            quote! {
                #redb::TypeName::new(::std::stringify!(#struct_name))
            }
        }
    }
}

fn get_field_types(fields: &Fields) -> Vec<syn::Type> {
    match fields {
        Fields::Named(fields_named) => fields_named
            .named
            .iter()
            .map(|field| &field.ty)
            .cloned()
            .collect(),
        Fields::Unnamed(fields_unnamed) => fields_unnamed
            .unnamed
            .iter()
            .map(|field| &field.ty)
            .cloned()
            .collect(),
        Fields::Unit => vec![],
    }
}

fn generate_fixed_width(
    fields: &Fields,
    redb: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let field_types = get_field_types(fields);
    quote! {
        let mut total_width = 0usize;
        #(
            total_width += <#field_types as #redb::Value>::fixed_width()?;
        )*
        ::std::option::Option::Some(total_width)
    }
}

fn generate_as_bytes(fields: &Fields, redb: &proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    let field_types = get_field_types(fields);
    let field_accessors = match fields {
        Fields::Named(fields_named) => fields_named
            .named
            .iter()
            .map(|field| {
                let name = &field.ident;
                quote! { #name }
            })
            .collect(),
        Fields::Unnamed(_) => (0..field_types.len())
            .map(|i| {
                let index = syn::Index::from(i);
                quote! { #index }
            })
            .collect(),
        Fields::Unit => Vec::new(),
    };

    let num_fields = field_types.len();

    if num_fields == 0 {
        quote! { ::std::vec::Vec::new() }
    } else if num_fields == 1 {
        let field_accessor = &field_accessors[0];
        let field_type = &field_types[0];
        quote! {
            {
                let field_bytes = <#field_type as #redb::Value>::as_bytes(&value.#field_accessor);
                let bytes: &[::std::primitive::u8] = ::std::convert::AsRef::as_ref(&field_bytes);
                bytes.to_vec()
            }
        }
    } else {
        let field_types_except_last = &field_types[..num_fields - 1];
        let field_accessors_except_last = &field_accessors[..num_fields - 1];

        quote! {
            {
                let mut result = ::std::vec::Vec::new();

                #(
                    if <#field_types_except_last as #redb::Value>::fixed_width().is_none() {
                        let field_bytes = <#field_types_except_last as #redb::Value>::as_bytes(
                            &value.#field_accessors_except_last,
                        );
                        let bytes: &[::std::primitive::u8] =
                            ::std::convert::AsRef::as_ref(&field_bytes);
                        let len = bytes.len();
                        if len < 254 {
                            result.push(
                                <::std::primitive::u8 as ::std::convert::TryFrom<
                                    ::std::primitive::usize,
                                >>::try_from(len)
                                .unwrap(),
                            );
                        } else if let ::std::result::Result::Ok(u16_len) =
                            <::std::primitive::u16 as ::std::convert::TryFrom<
                                ::std::primitive::usize,
                            >>::try_from(len)
                        {
                            result.push(254u8);
                            result.extend_from_slice(&u16_len.to_le_bytes());
                        } else {
                            let u32_len = <::std::primitive::u32 as ::std::convert::TryFrom<
                                ::std::primitive::usize,
                            >>::try_from(len)
                            .unwrap();
                            result.push(255u8);
                            result.extend_from_slice(&u32_len.to_le_bytes());
                        }
                    }
                )*

                #(
                    {
                        let field_bytes = <#field_types as #redb::Value>::as_bytes(
                            &value.#field_accessors,
                        );
                        let bytes: &[::std::primitive::u8] =
                            ::std::convert::AsRef::as_ref(&field_bytes);
                        result.extend_from_slice(bytes);
                    }
                )*

                result
            }
        }
    }
}

fn generate_from_bytes(
    name: &Ident,
    fields: &Fields,
    redb: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let field_types = get_field_types(fields);
    let field_vars: Vec<_> = (0..field_types.len())
        .map(|i| quote::format_ident!("field_{}", i))
        .collect();
    let num_fields = field_types.len();

    let body = if num_fields == 0 {
        quote! {}
    } else if num_fields == 1 {
        let field_var = &field_vars[0];
        let field_type = &field_types[0];
        quote! {
            let #field_var = <#field_type as #redb::Value>::from_bytes(data);
        }
    } else {
        let field_types_except_last = &field_types[..num_fields - 1];
        let field_vars_except_last = &field_vars[..num_fields - 1];
        let last_field_var = field_vars.last();
        let last_field_type = field_types.last();

        quote! {
            let mut offset = 0usize;
            let mut var_lengths = ::std::vec::Vec::new();

            #(
                if <#field_types_except_last as #redb::Value>::fixed_width().is_none() {
                    let (len, bytes_read) = match data[offset] {
                        0u8..=253u8 => (data[offset] as ::std::primitive::usize, 1usize),
                        254u8 => (
                            ::std::primitive::u16::from_le_bytes([
                                data[offset + 1],
                                data[offset + 2],
                            ]) as ::std::primitive::usize,
                            3usize,
                        ),
                        255u8 => (
                            ::std::primitive::u32::from_le_bytes([
                                data[offset + 1],
                                data[offset + 2],
                                data[offset + 3],
                                data[offset + 4],
                            ]) as ::std::primitive::usize,
                            5usize,
                        ),
                    };
                    var_lengths.push(len);
                    offset += bytes_read;
                }
            )*

            let mut var_index = 0usize;
            #(
                let #field_vars_except_last = if let ::std::option::Option::Some(fixed_width) =
                    <#field_types_except_last as #redb::Value>::fixed_width()
                {
                    let field_data = &data[offset..offset + fixed_width];
                    offset += fixed_width;
                    <#field_types_except_last as #redb::Value>::from_bytes(field_data)
                } else {
                    let len = var_lengths[var_index];
                    let field_data = &data[offset..offset + len];
                    offset += len;
                    var_index += 1;
                    <#field_types_except_last as #redb::Value>::from_bytes(field_data)
                };
            )*

            let #last_field_var = if let ::std::option::Option::Some(fixed_width) =
                <#last_field_type as #redb::Value>::fixed_width()
            {
                let field_data = &data[offset..offset + fixed_width];
                <#last_field_type as #redb::Value>::from_bytes(field_data)
            } else {
                <#last_field_type as #redb::Value>::from_bytes(&data[offset..])
            };
        }
    };
    match fields {
        Fields::Named(fields_named) => {
            let field_names: Vec<_> = fields_named
                .named
                .iter()
                .map(|field| &field.ident)
                .collect();

            quote! {
                {
                    #body
                    #name {
                        #(#field_names: #field_vars),*
                    }
                }
            }
        }
        Fields::Unnamed(_) => {
            quote! {
                {
                    #body
                    #name(#(#field_vars),*)
                }
            }
        }
        Fields::Unit => {
            quote! { #name }
        }
    }
}
