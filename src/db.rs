use crate::tree_store::{get_db_size, DatabaseStats, Storage, FREED_TABLE};
use crate::Error;
use crate::{ReadTransaction, Result, WriteTransaction};
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::path::Path;
use std::{io, panic};

// TODO: add trait bounds once const_fn_trait_bound is stable
pub struct TableDefinition<'a, K: ?Sized, V: ?Sized> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: ?Sized, V: ?Sized> TableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        // assert_ne! isn't stable, so we have to do it manually
        let name_bytes = name.as_bytes();
        let freed_name_bytes = FREED_TABLE.as_bytes();
        if name_bytes.len() == freed_name_bytes.len() {
            let mut equal = true;
            let mut i = 0;
            while i < name.len() {
                if name_bytes[i] != freed_name_bytes[i] {
                    equal = false;
                }
                i += 1;
            }
            if equal {
                panic!("Table name may not be equal to the internal free page table");
            }
        }
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        self.name
    }
}

impl<'a, K: ?Sized, V: ?Sized> Clone for TableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: ?Sized, V: ?Sized> Copy for TableDefinition<'a, K, V> {}

pub struct MultimapTableDefinition<'a, K: ?Sized, V: ?Sized> {
    name: &'a str,
    _key_type: PhantomData<K>,
    _value_type: PhantomData<V>,
}

impl<'a, K: ?Sized, V: ?Sized> MultimapTableDefinition<'a, K, V> {
    pub const fn new(name: &'a str) -> Self {
        assert!(!name.is_empty());
        // assert_ne! isn't stable, so we have to do it manually
        let name_bytes = name.as_bytes();
        let freed_name_bytes = FREED_TABLE.as_bytes();
        if name_bytes.len() == freed_name_bytes.len() {
            let mut equal = true;
            let mut i = 0;
            while i < name.len() {
                if name_bytes[i] != freed_name_bytes[i] {
                    equal = false;
                }
                i += 1;
            }
            if equal {
                panic!("Table name may not be equal to the internal free page table");
            }
        }
        Self {
            name,
            _key_type: PhantomData,
            _value_type: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        self.name
    }
}

impl<'a, K: ?Sized, V: ?Sized> Clone for MultimapTableDefinition<'a, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: ?Sized, V: ?Sized> Copy for MultimapTableDefinition<'a, K, V> {}

pub struct Database {
    storage: Storage,
}

impl Database {
    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    ///
    /// `db_size`: the maximum size in bytes of the database.
    ///
    /// # Safety
    ///
    /// The file referenced by `path` must not be concurrently modified by any other process
    pub unsafe fn create(path: impl AsRef<Path>, db_size: usize) -> Result<Database> {
        let file = if path.as_ref().exists() && File::open(path.as_ref())?.metadata()?.len() > 0 {
            let existing_size = get_db_size(path.as_ref())?;
            if existing_size != db_size {
                return Err(Error::DbSizeMismatch {
                    path: path.as_ref().to_string_lossy().to_string(),
                    size: existing_size,
                    requested_size: db_size,
                });
            }
            OpenOptions::new().read(true).write(true).open(path)?
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;

            file
        };

        let storage = Storage::new(file, db_size, None, true)?;
        Ok(Database { storage })
    }

    /// Opens an existing redb database.
    ///
    /// # Safety
    ///
    /// The file referenced by `path` must not be concurrently modified by any other process
    pub unsafe fn open(path: impl AsRef<Path>) -> Result<Database> {
        if File::open(path.as_ref())?.metadata()?.len() > 0 {
            let existing_size = get_db_size(path.as_ref())?;
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            let storage = Storage::new(file, existing_size, None, true)?;
            Ok(Database { storage })
        } else {
            Err(Error::Io(io::Error::from(ErrorKind::InvalidData)))
        }
    }

    pub fn builder() -> DatabaseBuilder {
        DatabaseBuilder::new()
    }

    pub fn begin_write(&self) -> Result<WriteTransaction> {
        WriteTransaction::new(&self.storage)
    }

    pub fn begin_read(&self) -> Result<ReadTransaction> {
        Ok(ReadTransaction::new(&self.storage))
    }

    pub fn stats(&self) -> Result<DatabaseStats> {
        self.storage.storage_stats()
    }

    pub fn print_debug(&self) {
        self.storage.print_debug()
    }
}

pub struct DatabaseBuilder {
    page_size: Option<usize>,
    dynamic_growth: bool,
}

impl DatabaseBuilder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            page_size: None,
            dynamic_growth: true,
        }
    }

    pub fn set_page_size(&mut self, size: usize) -> &mut Self {
        assert!(size.is_power_of_two());
        self.page_size = Some(size);
        self
    }

    /// Whether to grow the database file dynamically.
    /// When set to true, the database file will start at a small size and grow as insertions are made
    /// When set to false, the database file will be statically sized
    pub fn set_dynamic_growth(&mut self, enabled: bool) -> &mut Self {
        self.dynamic_growth = enabled;
        self
    }

    /// Opens the specified file as a redb database.
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    ///
    /// `db_size`: the maximum size in bytes of the database.
    ///
    /// # Safety
    ///
    /// The file referenced by `path` must only be concurrently modified by compatible versions
    /// of redb
    pub unsafe fn create(&self, path: impl AsRef<Path>, db_size: usize) -> Result<Database> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let storage = Storage::new(file, db_size, self.page_size, self.dynamic_growth)?;
        Ok(Database { storage })
    }
}
