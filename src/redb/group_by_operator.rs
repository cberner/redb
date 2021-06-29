extern crate chrono;

use std::rc::Rc;

use super::operator::{Operator2, Operator3, Operator4};

const MAX_PASSENGERS: usize = 10;

pub struct BooleanNotNullGroupByCountOperator {
    pub group_by_bits: Rc<Vec<i64>>,
}

impl Operator2<bool, i64> for BooleanNotNullGroupByCountOperator {
    fn execute(&self) -> (Rc<Vec<bool>>, Rc<Vec<i64>>) {
        let ones = self
            .group_by_bits
            .iter()
            .map(|x| x.count_ones())
            .fold(0, |sum, x| sum + x);
        let output = vec![
            (ones as i64),
            (self.group_by_bits.len() * 64) as i64 - ones as i64,
        ];
        (Rc::new(vec![true, false]), Rc::new(output))
    }
}

pub struct UInt8GroupByF32AverageOperator {
    pub group_by: Rc<Vec<u8>>,
    pub avg: Rc<Vec<f32>>,
}

impl Operator2<u8, f32> for UInt8GroupByF32AverageOperator {
    fn execute(&self) -> (Rc<Vec<u8>>, Rc<Vec<f32>>) {
        let mut sums: [f32; 256] = [0.0; 256];
        let mut counts: [u32; 256] = [0; 256];

        assert!(self.group_by.len() == self.avg.len());

        for i in 0..self.group_by.len() {
            // TODO: compiler doesn't seem to be able to elide these bounds checks,
            // so use get_unchecked() as the bounds checking incurs a ~30% performance penalty
            unsafe {
                let group = *self.group_by.get_unchecked(i) as usize;
                sums[group] += self.avg.get_unchecked(i);
                counts[group] += 1;
            }
        }

        let mut groups: Vec<u8> = vec![];
        let mut avgs: Vec<f32> = vec![];
        for i in 0..256 {
            if counts[i] > 0 {
                groups.push(i as u8);
                avgs.push(sums[i] / counts[i] as f32);
            }
        }
        (Rc::new(groups), Rc::new(avgs))
    }
}

pub struct UInt8UInt8GroupByCountOperator<T> {
    pub uint8_column: Rc<Vec<u8>>,
    pub generic_column: Rc<Vec<T>>,
    pub projection: fn(&u8, &T) -> (u8, u8),
}

impl<T> Operator3<u8, u8, i64> for UInt8UInt8GroupByCountOperator<T> {
    #[inline]
    fn execute(&self) -> (Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<i64>>) {
        const GROUPS: usize = 256 * 256;
        let mut counts: [u32; GROUPS] = [0; GROUPS];

        assert!(self.uint8_column.len() == self.generic_column.len());
        for i in 0..self.uint8_column.len() {
            unsafe {
                let (c0, c1) = (self.projection)(
                    self.uint8_column.get_unchecked(i),
                    self.generic_column.get_unchecked(i),
                );
                counts[(256 * (c1 as usize) + c0 as usize)] += 1;
            }
        }

        let mut c0_values: Vec<u8> = vec![];
        let mut c1_values: Vec<u8> = vec![];
        let mut count_values: Vec<i64> = vec![];
        for i in 0..GROUPS {
            if counts[i] > 0 {
                c0_values.push((i % 256) as u8);
                c1_values.push((i / 256) as u8);
                count_values.push(counts[i] as i64);
            }
        }
        (
            Rc::new(c0_values),
            Rc::new(c1_values),
            Rc::new(count_values),
        )
    }
}

pub struct UInt8UInt8UInt8GroupByCountOperator<T, U> {
    pub uint8_column: Rc<Vec<u8>>,
    pub column1: Rc<Vec<T>>,
    pub column2: Rc<Vec<U>>,
    pub projection: fn(&u8, &T, &U) -> (u8, u8, u8),
}

impl<T, U> Operator4<u8, u8, u8, i64> for UInt8UInt8UInt8GroupByCountOperator<T, U> {
    #[inline]
    fn execute(&self) -> (Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<u8>>, Rc<Vec<i64>>) {
        const GROUPS: usize = MAX_PASSENGERS * 100 * 100;
        let mut counts: [u32; GROUPS] = [0; GROUPS];

        assert!(self.uint8_column.len() == self.column1.len());
        assert!(self.uint8_column.len() == self.column2.len());
        for i in 0..self.uint8_column.len() {
            unsafe {
                let (c0, c1, c2) = (self.projection)(
                    self.uint8_column.get_unchecked(i),
                    self.column1.get_unchecked(i),
                    self.column2.get_unchecked(i),
                );
                counts[MAX_PASSENGERS * 100 * c1 as usize + 100 * c0 as usize + c2 as usize] += 1;
            }
        }

        let mut u8_values: Vec<u8> = vec![];
        let mut year_values: Vec<u8> = vec![];
        let mut distance_values: Vec<u8> = vec![];
        let mut count_values: Vec<i64> = vec![];
        for i in 0..GROUPS {
            if counts[i] > 0 {
                let distance = i % 100;
                let u8_value = (i % MAX_PASSENGERS * 100) / 100;
                let year = i / (MAX_PASSENGERS * 100);
                u8_values.push(u8_value as u8);
                year_values.push(year as u8);
                distance_values.push(distance as u8);
                count_values.push(counts[i] as i64);
            }
        }
        (
            Rc::new(u8_values),
            Rc::new(year_values),
            Rc::new(distance_values),
            Rc::new(count_values),
        )
    }
}
