use crate::error::Error;
use crate::transactions::WriteTransaction;
use crate::ReadOnlyTransaction;
use std::cell::RefCell;
use std::collections::HashMap;

pub struct Table {
    data: RefCell<HashMap<Vec<u8>, Vec<u8>>>,
}

impl Table {
    pub(in crate) fn new() -> Result<Table, Error> {
        Ok(Table {
            data: RefCell::new(HashMap::new()),
        })
    }

    pub fn begin_write(&mut self) -> Result<WriteTransaction, Error> {
        Ok(WriteTransaction::new(&self.data))
    }

    pub fn read_transaction(&self) -> Result<ReadOnlyTransaction, Error> {
        Ok(ReadOnlyTransaction::new(self.data.borrow().clone()))
    }
}
