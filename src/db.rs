use crate::table::Table;
use crate::Error;
use std::path::Path;

pub struct Database {}

impl Database {
    ///
    /// # Safety
    ///
    /// The file referenced by `path` must only be concurrently modified by compatible versions
    /// of redb
    pub unsafe fn open(_path: &Path) -> Result<Database, Error> {
        Ok(Database {})
    }

    pub fn open_table(&self, _name: &str) -> Result<Table, Error> {
        Table::new()
    }
}
