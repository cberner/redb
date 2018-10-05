extern crate chrono;

use std::rc::Rc;

use chrono::NaiveDateTime;

use super::operator::{Operator1, Operator2, Operator3, Operator4};

use super::group_by_operator::BooleanNotNullGroupByCountOperator;
use super::group_by_operator::UInt8YearGroupByCountOperator;
use super::group_by_operator::UInt8GroupByF32AverageOperator;
use super::group_by_operator::UInt8YearAndDistanceGroupByCountOperator;


pub struct Query1<T> {
    pub operator: Box<Operator1<T>>
}

pub struct Query2<T, U> {
    pub operator: Box<Operator2<T, U>>
}

pub struct Query3<T, U, V> {
    pub operator: Box<Operator3<T, U, V>>
}

pub struct Query4<T, U, V, W> {
    pub operator: Box<Operator4<T, U, V, W>>
}

impl<T, U> Query2<T, U> {
    pub fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>) {
        self.operator.execute()
    }
}

impl<T, U, V> Query3<T, U, V> {
    pub fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>, Rc<Vec<V>>) {
        self.operator.execute()
    }
}

impl<T, U, V, W> Query4<T, U, V, W> {
    pub fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>, Rc<Vec<V>>, Rc<Vec<W>>) {
        self.operator.execute()
    }
}

impl Query1<i64> {
    pub fn count_group_by(&self) -> Query2<bool, i64> {
        let group_by = Box::new(BooleanNotNullGroupByCountOperator {
            group_by_bits: self.operator.execute().0.clone()
        });
        Query2 {
            operator: group_by
        }
    }
}


impl Query2<u8, f32> {
    pub fn group_by_avg(&self) -> Query2<u8, f32> {
        let columns = self.operator.execute();
        let group_by = Box::new(UInt8GroupByF32AverageOperator {
            group_by: columns.0,
            avg: columns.1
        });
        Query2 {
            operator: group_by
        }
    }
}

impl Query2<u8, NaiveDateTime> {
    pub fn count_group_by_extract_year(&self) -> Query3<u8, i64, i64> {
        let columns = self.operator.execute();
        let group_by = Box::new(UInt8YearGroupByCountOperator {
            uint8_column: columns.0,
            timestamp_column: columns.1
        });
        Query3 {
            operator: group_by
        }
    }
}

impl Query3<u8, NaiveDateTime, f32> {
    pub fn count_group_by_extract_year_and_distance(&self) -> Query4<u8, i64, i64, i64> {
        let columns = self.operator.execute();
        let group_by = Box::new(UInt8YearAndDistanceGroupByCountOperator {
            uint8_column: columns.0,
            timestamp_column: columns.1,
            distance_column: columns.2
        });
        Query4 {
            operator: group_by
        }
    }
}

