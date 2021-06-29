extern crate chrono;

use std::rc::Rc;

use chrono::NaiveDateTime;

use super::operator::{Operator1, Operator2, Operator3, Operator4};

use super::group_by_operator::BooleanNotNullGroupByCountOperator;
use super::group_by_operator::UInt8GroupByF32AverageOperator;
use super::group_by_operator::UInt8UInt8GroupByCountOperator;
use super::group_by_operator::UInt8UInt8UInt8GroupByCountOperator;
use super::scan_operator::{ScanOperator3, ScanOperator4};

#[derive(PartialEq)]
pub enum AggregationOperation {
    GroupBy,
    Count,
    Average,
}

pub struct Query1<T> {
    pub operator: Box<Operator1<T>>,
}

pub struct Query2<A, B, T, U> {
    pub operator: Box<Operator2<A, B>>,
    pub projection: fn(&A, &B) -> (T, U),
}

pub struct Query3<A, B, C, T, U, V> {
    pub operator: Box<Operator3<A, B, C>>,
    pub projection: fn(&A, &B, &C) -> (T, U, V),
}

pub struct Query4<A, B, C, D, T, U, V, W> {
    pub operator: Box<Operator4<A, B, C, D>>,
    pub projection: fn(&A, &B, &C, &D) -> (T, U, V, W),
}

impl<A, B, T, U> Query2<A, B, T, U> {
    pub fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>) {
        let columns = self.operator.execute();
        let mut output0: Vec<T> = vec![];
        let mut output1: Vec<U> = vec![];

        for i in 0..columns.0.len() {
            let (o0, o1) = (self.projection)(&columns.0[i], &columns.1[i]);
            output0.push(o0);
            output1.push(o1);
        }
        (Rc::new(output0), Rc::new(output1))
    }
}

impl<T, U> Query2<T, U, T, U> {
    pub fn project<X, Y>(self, projection: fn(&T, &U) -> (X, Y)) -> Query2<T, U, X, Y> {
        Query2 {
            operator: self.operator,
            projection: projection,
        }
    }
}

impl<T, U, V> Query3<T, U, V, T, U, V> {
    pub fn project<X, Y, Z>(
        self,
        projection: fn(&T, &U, &V) -> (X, Y, Z),
    ) -> Query3<T, U, V, X, Y, Z> {
        Query3 {
            operator: self.operator,
            projection: projection,
        }
    }
}

impl<T, U, V, W> Query4<T, U, V, W, T, U, V, W> {
    pub fn project<X, Y, Z, A>(
        self,
        projection: fn(&T, &U, &V, &W) -> (X, Y, Z, A),
    ) -> Query4<T, U, V, W, X, Y, Z, A> {
        Query4 {
            operator: self.operator,
            projection: projection,
        }
    }
}

impl<A, B, C, T, U, V> Query3<A, B, C, T, U, V> {
    #[inline]
    pub fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>, Rc<Vec<V>>) {
        let columns = self.operator.execute();
        let mut output0: Vec<T> = vec![];
        let mut output1: Vec<U> = vec![];
        let mut output2: Vec<V> = vec![];

        for i in 0..columns.0.len() {
            let (o0, o1, o2) = (self.projection)(&columns.0[i], &columns.1[i], &columns.2[i]);
            output0.push(o0);
            output1.push(o1);
            output2.push(o2);
        }
        (Rc::new(output0), Rc::new(output1), Rc::new(output2))
    }
}

impl<A, B, C, D, T, U, V, W> Query4<A, B, C, D, T, U, V, W> {
    pub fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>, Rc<Vec<V>>, Rc<Vec<W>>) {
        let columns = self.operator.execute();
        let mut output0: Vec<T> = vec![];
        let mut output1: Vec<U> = vec![];
        let mut output2: Vec<V> = vec![];
        let mut output3: Vec<W> = vec![];

        for i in 0..columns.0.len() {
            let (o0, o1, o2, o3) =
                (self.projection)(&columns.0[i], &columns.1[i], &columns.2[i], &columns.3[i]);
            output0.push(o0);
            output1.push(o1);
            output2.push(o2);
            output3.push(o3);
        }
        (
            Rc::new(output0),
            Rc::new(output1),
            Rc::new(output2),
            Rc::new(output3),
        )
    }
}

impl Query1<i64> {
    pub fn aggregate(
        &self,
        op1: AggregationOperation,
        op2: AggregationOperation,
    ) -> Query2<bool, i64, bool, i64> {
        assert!(op1 == AggregationOperation::GroupBy);
        assert!(op2 == AggregationOperation::Count);
        let group_by = Box::new(BooleanNotNullGroupByCountOperator {
            group_by_bits: self.operator.execute().0.clone(),
        });
        Query2 {
            operator: group_by,
            projection: |&x, &y| (x, y),
        }
    }
}

impl Query2<u8, f32, u8, f32> {
    pub fn aggregate(
        &self,
        op1: AggregationOperation,
        op2: AggregationOperation,
    ) -> Query2<u8, f32, u8, f32> {
        assert!(op1 == AggregationOperation::GroupBy);
        assert!(op2 == AggregationOperation::Average);
        let columns = self.operator.execute();
        let group_by = Box::new(UInt8GroupByF32AverageOperator {
            group_by: columns.0,
            avg: columns.1,
        });
        Query2 {
            operator: group_by,
            projection: |&x, &y| (x, y),
        }
    }
}

impl Query2<u8, NaiveDateTime, u8, u8> {
    #[inline]
    pub fn aggregate(
        self,
        op0: AggregationOperation,
        op1: AggregationOperation,
        op2: AggregationOperation,
    ) -> Query3<u8, u8, i64, u8, u8, i64> {
        assert!(op0 == AggregationOperation::GroupBy);
        assert!(op1 == AggregationOperation::GroupBy);
        assert!(op2 == AggregationOperation::Count);
        let columns = self.operator.execute();
        let group_by = UInt8UInt8GroupByCountOperator {
            uint8_column: columns.0,
            generic_column: columns.1,
            projection: self.projection,
        };
        // XXX: Materialize results to help compiler with inlining
        let columns_prime = group_by.execute();
        let scan = Box::new(ScanOperator3 {
            column1: columns_prime.0,
            column2: columns_prime.1,
            column3: columns_prime.2,
        });
        Query3 {
            operator: scan,
            projection: |c0: &u8, c1: &u8, c2: &i64| (*c0, *c1, *c2),
        }
    }
}

impl Query3<u8, NaiveDateTime, f32, u8, u8, u8> {
    #[inline]
    pub fn aggregate(
        &self,
        op0: AggregationOperation,
        op1: AggregationOperation,
        op2: AggregationOperation,
        op3: AggregationOperation,
    ) -> Query4<u8, u8, u8, i64, u8, u8, u8, i64> {
        assert!(op0 == AggregationOperation::GroupBy);
        assert!(op1 == AggregationOperation::GroupBy);
        assert!(op2 == AggregationOperation::GroupBy);
        assert!(op3 == AggregationOperation::Count);
        let columns = self.operator.execute();
        let group_by = UInt8UInt8UInt8GroupByCountOperator {
            uint8_column: columns.0,
            column1: columns.1,
            column2: columns.2,
            projection: self.projection,
        };
        // XXX: Materialize results to help compiler with inlining
        let columns_prime = group_by.execute();
        let scan = Box::new(ScanOperator4 {
            column1: columns_prime.0,
            column2: columns_prime.1,
            column3: columns_prime.2,
            column4: columns_prime.3,
        });
        Query4 {
            operator: scan,
            projection: |&a, &b, &c, &d| (a, b, c, d),
        }
    }
}
