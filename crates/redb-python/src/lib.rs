#![deny(clippy::all, clippy::pedantic, clippy::disallowed_methods)]
#![allow(
    clippy::if_not_else,
    clippy::iter_not_returning_iterator,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    clippy::redundant_closure_for_method_calls,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::unnecessary_wraps,
    clippy::unreadable_literal
)]

//! Python bindings for redb.
//!
//! Tables declare their key and value types from the closed set in [`types::Type`]. Every
//! table operation dispatches once on that pair and then runs redb's statically typed API, so
//! the type names, orderings and encodings match a Rust program's exactly.

mod database;
mod error;
mod multimap;
mod table;
mod transaction;
mod types;

use pyo3::prelude::*;

#[pymodule(name = "redb")]
fn redb_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    types::register(m)?;
    error::register(m)?;
    database::register(m)?;
    transaction::register(m)?;
    table::register(m)?;
    multimap::register(m)?;
    Ok(())
}
