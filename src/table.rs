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

    pub fn begin_write(&mut self) -> Result<WriteTransaction, Error> {
        Ok(WriteTransaction::new(self.storage))
    }

    pub fn read_transaction(&self) -> Result<ReadOnlyTransaction, Error> {
        Ok(ReadOnlyTransaction::new(self.storage))
    }
}
