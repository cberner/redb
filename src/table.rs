use crate::error::Error;
use crate::storage::Storage;
use crate::transactions::WriteTransaction;
use crate::ReadOnlyTransaction;

pub struct Table<'mmap> {
    storage: &'mmap Storage,
}

impl<'mmap> Table<'mmap> {
    pub(in crate) fn new(storage: &'mmap Storage) -> Result<Table<'mmap>, Error> {
        Ok(Table { storage })
    }

    pub fn begin_write(&'_ mut self) -> Result<WriteTransaction<'mmap>, Error> {
        Ok(WriteTransaction::new(self.storage))
    }

    pub fn read_transaction(&'_ self) -> Result<ReadOnlyTransaction<'mmap>, Error> {
        Ok(ReadOnlyTransaction::new(self.storage))
    }
}
