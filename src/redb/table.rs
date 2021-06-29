use std::rc::Rc;

use super::scan_operator::{ScanOperator1, ScanOperator2, ScanOperator3};

use super::query::{Query1, Query2, Query3};

pub struct Table1<T> {
    pub column1: Rc<Vec<T>>,
}

impl<T: 'static> Table1<T> {
    fn table_scan(&self) -> ScanOperator1<T> {
        ScanOperator1 {
            column1: self.column1.clone(),
        }
    }

    pub fn query(&self) -> Query1<T> {
        Query1 {
            operator: Box::new(self.table_scan()),
        }
    }
}

pub struct Table2<T, U> {
    pub column1: Rc<Vec<T>>,
    pub column2: Rc<Vec<U>>,
}

impl<T: 'static, U: 'static> Table2<T, U>
where
    T: Copy,
    U: Copy,
{
    fn table_scan(&self) -> ScanOperator2<T, U> {
        ScanOperator2 {
            column1: self.column1.clone(),
            column2: self.column2.clone(),
        }
    }

    pub fn query(&self) -> Query2<T, U, T, U> {
        Query2 {
            operator: Box::new(self.table_scan()),
            projection: |x: &T, y: &U| (*x, *y),
        }
    }
}

pub struct Table3<T, U, V> {
    pub column1: Rc<Vec<T>>,
    pub column2: Rc<Vec<U>>,
    pub column3: Rc<Vec<V>>,
}

impl<T: 'static, U: 'static, V: 'static> Table3<T, U, V>
where
    T: Copy,
    U: Copy,
    V: Copy,
{
    fn table_scan(&self) -> ScanOperator3<T, U, V> {
        ScanOperator3 {
            column1: self.column1.clone(),
            column2: self.column2.clone(),
            column3: self.column3.clone(),
        }
    }

    pub fn query(&self) -> Query3<T, U, V, T, U, V> {
        Query3 {
            operator: Box::new(self.table_scan()),
            projection: |x, y, z| (*x, *y, *z),
        }
    }
}
