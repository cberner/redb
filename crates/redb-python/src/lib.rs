#![deny(clippy::all, clippy::pedantic, clippy::disallowed_methods)]
// TODO: revisit this list and see if we can enable some
#![allow(
    clippy::default_trait_access,
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

mod python;
pub use crate::python::redb;
