pub use db::{
    Database, DatabaseBuilder, Durability, MultimapTableDefinition, ReadTransaction,
    TableDefinition, WriteTransaction,
};
pub use error::Error;
pub use multimap_table::{
    MultimapRangeIter, MultimapTable, MultimapValueIter, ReadOnlyMultimapTable,
    ReadableMultimapTable,
};
pub use table::{RangeIter, ReadOnlyTable, ReadableTable, Table};
pub use tree_store::{AccessGuard, DatabaseStats};

type Result<T = (), E = Error> = std::result::Result<T, E>;

#[cfg(feature = "python")]
pub use crate::python::redb;

mod db;
mod error;
mod multimap_table;
#[cfg(feature = "python")]
mod python;
mod table;
mod tree_store;
mod types;
