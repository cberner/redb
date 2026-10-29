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
// is renamed (`my_redb = { package = "redb" }`). The named crate must be redb 4.5 or later.
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
    let path = path.unwrap_or_else(|| syn::parse_quote!(::redb));
    Ok(quote! { #path })
}

/// Derives `redb::Key` for a struct whose fields all implement `Key`.
///
/// The generated implementation refers to the redb crate as `::redb`. When the dependency is
/// renamed (`my_redb = { package = "redb" }`), name that crate with `#[redb(crate = "my_redb")]`.
/// The derive requires redb 4.5. It implements `try_compare`: both encodings are decoded with
/// `Value::try_from_bytes`, then ordered with `Ord`. `compare` panics through `try_compare`.
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
    let redb = &redb_crate_path_for(input)?;
    let compare_fns = quote! {
        fn compare(
            data1: &[::std::primitive::u8],
            data2: &[::std::primitive::u8],
        ) -> ::std::cmp::Ordering {
            <Self as #redb::Key>::try_compare(data1, data2)
                .unwrap_or_else(|err| ::std::panic!("{err}"))
        }

        fn try_compare(
            data1: &[::std::primitive::u8],
            data2: &[::std::primitive::u8],
        ) -> ::std::result::Result<::std::cmp::Ordering, #redb::ValueError> {
            let value1 = <Self as #redb::Value>::try_from_bytes(data1)?;
            let value2 = <Self as #redb::Value>::try_from_bytes(data2)?;
            ::std::result::Result::Ok(::std::cmp::Ord::cmp(&value1, &value2))
        }
    };

    Ok(quote! {
        impl #impl_generics #redb::Key for #name #ty_generics #where_clause {
            #compare_fns
        }
    })
}

/// Derives `redb::Value` for a struct whose fields all implement `Value`.
///
/// A struct with exactly one field is encoded exactly as that field, like `(T,)`, so it
/// forwards the field's `Value::NICHE`, and `Option` of it is encoded as `Option` of the field
/// is. Other structs declare no niche.
///
/// The generated implementation refers to the redb crate as `::redb`. When the dependency is
/// renamed (`my_redb = { package = "redb" }`), name that crate with `#[redb(crate = "my_redb")]`.
/// The derive requires redb 4.5. It implements `try_from_bytes`, and `from_bytes` panics through
/// it. Each field is decoded with that field's `try_from_bytes`. A unit struct accepts only an
/// empty slice. A multi-field struct rejects a length prefix that is not the shortest form, a
/// truncated encoding, and trailing bytes, as tuples do.
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
    let redb = &redb_crate_path_for(input)?;

    let type_name_impl = generate_type_name(name, &data_struct.fields, redb);
    let as_bytes_impl = generate_as_bytes(&data_struct.fields, redb);
    let fixed_width_impl = generate_fixed_width(&data_struct.fields, redb);
    let niche_impl = generate_niche(&data_struct.fields, redb);
    let try_from_bytes_body = generate_try_from_bytes_body(name, &data_struct.fields, redb);
    let from_bytes_fns = quote! {
        fn from_bytes<'__redb_a>(
            data: &'__redb_a [::std::primitive::u8],
        ) -> Self::SelfType<'__redb_a>
        where
            Self: '__redb_a,
        {
            <Self as #redb::Value>::try_from_bytes(data)
                .unwrap_or_else(|err| ::std::panic!("{err}"))
        }

        fn try_from_bytes<'__redb_a>(
            data: &'__redb_a [::std::primitive::u8],
        ) -> ::std::result::Result<Self::SelfType<'__redb_a>, #redb::ValueError>
        where
            Self: '__redb_a,
        {
            #try_from_bytes_body
        }
    };

    Ok(quote! {
        impl #impl_generics #redb::Value for #name #ty_generics #where_clause {
            type SelfType<'__redb_a> = #self_type
            where
                Self: '__redb_a;
            type AsBytes<'__redb_a> = ::std::vec::Vec<::std::primitive::u8>
            where
                Self: '__redb_a;

            #niche_impl

            fn fixed_width() -> ::std::option::Option<::std::primitive::usize> {
                #fixed_width_impl
            }

            #from_bytes_fns

            fn as_bytes<'__redb_a, '__redb_b: '__redb_a>(
                value: &'__redb_a Self::SelfType<'__redb_b>,
            ) -> Self::AsBytes<'__redb_a>
            where
                Self: '__redb_b,
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
                GenericParam::Lifetime(_) => params.push(quote! { '__redb_a }),
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

// Renders the name of one field's type for embedding in the struct's `TypeName`. A user-defined
// type may share its name string with a built-in type (e.g. a user type named "String"), and only
// the classification of its `TypeName` tells the two apart, so user-defined field types are
// tagged to keep the composed names distinct. Built-in field types render as the bare name,
// which keeps the `TypeName` of structs made only of built-in types unchanged.
//
// The classification is read by rebuilding the `TypeName` through `TypeName::new` -- the
// user-defined constructor -- and comparing: `TypeName` equality is exactly (classification,
// name), so the rebuilt name is equal iff the field's classification is user-defined. redb's
// `TypeName::is_user_defined()` would say the same, but it is crate-private. Recursing into the
// field's type syntax instead would misrender aliases (`type A = Option<UserType>`).
fn render_field_type_name(
    field_type: &syn::Type,
    redb: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    quote! {
        {
            let type_name = <#field_type as #redb::Value>::type_name();
            if type_name == #redb::TypeName::new(type_name.name()) {
                ::std::format!("{}#user", type_name.name())
            } else {
                ::std::borrow::ToOwned::to_owned(type_name.name())
            }
        }
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
                    let rendered_type = render_field_type_name(&field.ty, redb);
                    quote! {
                        ::std::format!("{}: {}", ::std::stringify!(#field_name), #rendered_type)
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
                .map(|field| render_field_type_name(&field.ty, redb))
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

// A struct with one field is encoded exactly as that field, like `(T,)`, so the field's niche
// is its niche too. Any other struct keeps the trait's default, by declaring nothing. The
// generated code refers to `Value::NICHE`, so a one-field struct can only be derived against a
// redb that has it.
fn generate_niche(fields: &Fields, redb: &proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    let field_types = get_field_types(fields);
    let [field_type] = field_types.as_slice() else {
        return quote! {};
    };
    quote! {
        const NICHE: ::std::option::Option<&'static [::std::primitive::u8]> =
            <#field_type as #redb::Value>::NICHE;
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

fn construct_ok(name: &Ident, fields: &Fields, field_vars: &[Ident]) -> proc_macro2::TokenStream {
    match fields {
        Fields::Named(fields_named) => {
            let field_names: Vec<_> = fields_named
                .named
                .iter()
                .map(|field| &field.ident)
                .collect();
            quote! {
                ::std::result::Result::Ok(#name {
                    #(#field_names: #field_vars),*
                })
            }
        }
        Fields::Unnamed(_) => {
            quote! {
                ::std::result::Result::Ok(#name(#(#field_vars),*))
            }
        }
        Fields::Unit => {
            quote! { ::std::result::Result::Ok(#name) }
        }
    }
}

// Fallible decoder. `from_bytes` panics through it. Field encodings are delegated to each
// field's `try_from_bytes`, including a length prefix that is not the shortest form.
fn generate_try_from_bytes_body(
    name: &Ident,
    fields: &Fields,
    redb: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let field_types = get_field_types(fields);
    let field_vars: Vec<_> = (0..field_types.len())
        .map(|i| quote::format_ident!("field_{}", i))
        .collect();
    let constructed = construct_ok(name, fields, &field_vars);

    if field_types.is_empty() {
        return quote! {
            fn decode_error(message: &'static str) -> #redb::ValueError {
                ::std::boxed::Box::new(::std::io::Error::new(
                    ::std::io::ErrorKind::InvalidData,
                    message,
                ))
            }
            if data.is_empty() {
                #constructed
            } else {
                ::std::result::Result::Err(decode_error("invalid unit struct encoding"))
            }
        };
    }

    if let [field_type] = field_types.as_slice() {
        let field_var = &field_vars[0];
        return quote! {
            let #field_var = <#field_type as #redb::Value>::try_from_bytes(data)?;
            #constructed
        };
    }

    let last = field_types.len() - 1;
    let field_types_except_last = &field_types[..last];
    let field_vars_except_last = &field_vars[..last];
    let last_field_var = &field_vars[last];
    let last_field_type = &field_types[last];

    quote! {
        fn decode_error(message: &'static str) -> #redb::ValueError {
            ::std::boxed::Box::new(::std::io::Error::new(
                ::std::io::ErrorKind::InvalidData,
                message,
            ))
        }
        fn read_len(
            data: &[::std::primitive::u8],
            offset: ::std::primitive::usize,
        ) -> ::std::result::Result<
            (::std::primitive::usize, ::std::primitive::usize),
            #redb::ValueError,
        > {
            let ::std::option::Option::Some(rest) = data.get(offset..) else {
                return ::std::result::Result::Err(decode_error("encoded value is too short"));
            };
            let ::std::option::Option::Some(tag) = rest.first() else {
                return ::std::result::Result::Err(decode_error("encoded value is too short"));
            };
            match *tag {
                0u8..=253u8 => ::std::result::Result::Ok((
                    <::std::primitive::usize as ::std::convert::From<::std::primitive::u8>>::from(
                        *tag,
                    ),
                    1usize,
                )),
                254u8 => {
                    let ::std::option::Option::Some(bytes) = rest.get(1..3) else {
                        return ::std::result::Result::Err(decode_error("encoded value is too short"));
                    };
                    let ::std::result::Result::Ok(bytes) =
                        <[::std::primitive::u8; 2] as ::std::convert::TryFrom<&[::std::primitive::u8]>>::try_from(
                            bytes,
                        )
                    else {
                        return ::std::result::Result::Err(decode_error("encoded value is too short"));
                    };
                    let len = <::std::primitive::usize as ::std::convert::From<
                        ::std::primitive::u16,
                    >>::from(::std::primitive::u16::from_le_bytes(bytes));
                    if len < 254 {
                        return ::std::result::Result::Err(decode_error("non-minimal length encoding"));
                    }
                    ::std::result::Result::Ok((len, 3usize))
                }
                255u8 => {
                    let ::std::option::Option::Some(bytes) = rest.get(1..5) else {
                        return ::std::result::Result::Err(decode_error("encoded value is too short"));
                    };
                    let ::std::result::Result::Ok(bytes) =
                        <[::std::primitive::u8; 4] as ::std::convert::TryFrom<&[::std::primitive::u8]>>::try_from(
                            bytes,
                        )
                    else {
                        return ::std::result::Result::Err(decode_error("encoded value is too short"));
                    };
                    let len_u32 = ::std::primitive::u32::from_le_bytes(bytes);
                    let ::std::result::Result::Ok(len) =
                        <::std::primitive::usize as ::std::convert::TryFrom<::std::primitive::u32>>::try_from(
                            len_u32,
                        )
                    else {
                        return ::std::result::Result::Err(decode_error("encoded value is too short"));
                    };
                    if <::std::primitive::u16 as ::std::convert::TryFrom<::std::primitive::usize>>::try_from(
                        len,
                    )
                    .is_ok()
                    {
                        return ::std::result::Result::Err(decode_error(
                            "non-minimal length encoding",
                        ));
                    }
                    ::std::result::Result::Ok((len, 5usize))
                }
            }
        }

        let mut offset = 0usize;
        let mut var_lengths = ::std::vec::Vec::new();
        #(
            if <#field_types_except_last as #redb::Value>::fixed_width().is_none() {
                let (len, bytes_read) = read_len(data, offset)?;
                let ::std::option::Option::Some(next) = offset.checked_add(bytes_read) else {
                    return ::std::result::Result::Err(decode_error("encoded value is too short"));
                };
                if next > data.len() {
                    return ::std::result::Result::Err(decode_error("encoded value is too short"));
                }
                var_lengths.push(len);
                offset = next;
            }
        )*

        let mut var_index = 0usize;
        #(
            let #field_vars_except_last = if let ::std::option::Option::Some(fixed_width) =
                <#field_types_except_last as #redb::Value>::fixed_width()
            {
                let ::std::option::Option::Some(end) = offset.checked_add(fixed_width) else {
                    return ::std::result::Result::Err(decode_error("encoded value is too short"));
                };
                if end > data.len() {
                    return ::std::result::Result::Err(decode_error("encoded value is too short"));
                }
                let ::std::option::Option::Some(field_data) = data.get(offset..end) else {
                    return ::std::result::Result::Err(decode_error("encoded value is too short"));
                };
                offset = end;
                <#field_types_except_last as #redb::Value>::try_from_bytes(field_data)?
            } else {
                let ::std::option::Option::Some(len) = var_lengths.get(var_index).copied() else {
                    return ::std::result::Result::Err(decode_error("encoded value is too short"));
                };
                var_index += 1;
                let ::std::option::Option::Some(end) = offset.checked_add(len) else {
                    return ::std::result::Result::Err(decode_error("encoded value is too short"));
                };
                if end > data.len() {
                    return ::std::result::Result::Err(decode_error("encoded value is too short"));
                }
                let ::std::option::Option::Some(field_data) = data.get(offset..end) else {
                    return ::std::result::Result::Err(decode_error("encoded value is too short"));
                };
                offset = end;
                <#field_types_except_last as #redb::Value>::try_from_bytes(field_data)?
            };
        )*

        let #last_field_var = if let ::std::option::Option::Some(fixed_width) =
            <#last_field_type as #redb::Value>::fixed_width()
        {
            let ::std::option::Option::Some(end) = offset.checked_add(fixed_width) else {
                return ::std::result::Result::Err(decode_error("encoded value is too short"));
            };
            if end > data.len() {
                return ::std::result::Result::Err(decode_error("encoded value is too short"));
            }
            if end != data.len() {
                return ::std::result::Result::Err(decode_error("encoded value has trailing bytes"));
            }
            let ::std::option::Option::Some(field_data) = data.get(offset..end) else {
                return ::std::result::Result::Err(decode_error("encoded value is too short"));
            };
            <#last_field_type as #redb::Value>::try_from_bytes(field_data)?
        } else {
            let ::std::option::Option::Some(field_data) = data.get(offset..) else {
                return ::std::result::Result::Err(decode_error("encoded value is too short"));
            };
            <#last_field_type as #redb::Value>::try_from_bytes(field_data)?
        };

        #constructed
    }
}
