use crate::error::Error;
use std::cell::RefCell;
use std::collections::HashMap;

pub struct WriteTransaction<'a> {
    table: &'a RefCell<HashMap<Vec<u8>, Vec<u8>>>,
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl<'a> WriteTransaction<'a> {
    pub(in crate) fn new(table: &'a RefCell<HashMap<Vec<u8>, Vec<u8>>>) -> WriteTransaction<'a> {
        let data = table.borrow().clone();
        WriteTransaction { table, data }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    pub fn commit(self) -> Result<(), Error> {
        self.table.replace(self.data);
        Ok(())
    }
}

pub struct ReadOnlyTransaction {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl ReadOnlyTransaction {
    pub(in crate) fn new(data: HashMap<Vec<u8>, Vec<u8>>) -> ReadOnlyTransaction {
        ReadOnlyTransaction { data }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, Error> {
        Ok(self.data.get(key).map(|x| x.as_slice()))
    }
}
