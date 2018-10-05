use std::rc::Rc;

use super::operator::{Operator1, Operator2, Operator3};

pub struct ScanOperator1<T> {
    pub column1: Rc<Vec<T>>
}

impl<T> Operator1<T> for ScanOperator1<T> {
    fn execute(&self) -> (Rc<Vec<T>>,) {
        (self.column1.clone(),)
    }
}

pub struct ScanOperator2<T, U> {
    pub column1: Rc<Vec<T>>,
    pub column2: Rc<Vec<U>>
}

impl<T, U> Operator2<T, U> for ScanOperator2<T, U> {
    fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>) {
        (self.column1.clone(), self.column2.clone())
    }
}

pub struct ScanOperator3<T, U, V> {
    pub column1: Rc<Vec<T>>,
    pub column2: Rc<Vec<U>>,
    pub column3: Rc<Vec<V>>
}

impl<T, U, V> Operator3<T, U, V> for ScanOperator3<T, U, V> {
    fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>, Rc<Vec<V>>) {
        (self.column1.clone(), self.column2.clone(), self.column3.clone())
    }
}
