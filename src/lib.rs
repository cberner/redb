#![allow(clippy::drop_non_drop)]

pub use db::{Builder, Database, MultimapTableDefinition, TableDefinition, WriteStrategy};
pub use error::Error;
pub use multimap_table::{
    MultimapRangeIter, MultimapTable, MultimapValueIter, ReadOnlyMultimapTable,
    ReadableMultimapTable,
};
pub use table::{RangeIter, ReadOnlyTable, ReadableTable, Table};
pub use transactions::{DatabaseStats, Durability, ReadTransaction, WriteTransaction};
pub use tree_store::{AccessGuard, Savepoint};

type Result<T = (), E = Error> = std::result::Result<T, E>;

#[cfg(feature = "python")]
pub use crate::python::redb;

mod db;
mod error;
mod multimap_table;
#[cfg(feature = "python")]
mod python;
mod table;
mod transactions;
mod tree_store;
mod types;
