extern crate chrono;

use std::rc::Rc;

use chrono::{Datelike, NaiveDateTime};

use super::operator::{Operator2, Operator3, Operator4};

const MAX_PASSENGERS: usize = 10;

pub struct BooleanNotNullGroupByCountOperator {
    pub group_by_bits: Rc<Vec<i64>>
}

impl Operator2<bool, i64> for BooleanNotNullGroupByCountOperator {
    fn execute(&self) -> (Rc<Vec<bool>>, Rc<Vec<i64>>) {
        let ones = self.group_by_bits.iter()
            .map(|x| x.count_ones())
            .fold(0, |sum, x| sum + x);
        let output = vec![(ones as i64), (self.group_by_bits.len() * 64) as i64 - ones as i64];
        (Rc::new(vec![true, false]), Rc::new(output))
    }
}

pub struct UInt8GroupByF32AverageOperator {
    pub group_by: Rc<Vec<u8>>,
    pub avg: Rc<Vec<f32>>
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

pub struct UInt8YearGroupByCountOperator {
    pub uint8_column: Rc<Vec<u8>>,
    pub timestamp_column: Rc<Vec<NaiveDateTime>>
}

impl Operator3<u8, i64, i64> for UInt8YearGroupByCountOperator {
    fn execute(&self) -> (Rc<Vec<u8>>, Rc<Vec<i64>>, Rc<Vec<i64>>) {
        const GROUPS: usize = 256 * 256;
        let mut counts: [u32; GROUPS] = [0; GROUPS];

        assert!(self.uint8_column.len() == self.timestamp_column.len());
        for i in 0..self.uint8_column.len() {
            unsafe {
                let c1 = *self.uint8_column.get_unchecked(i) as usize;
                let year = (self.timestamp_column.get_unchecked(i).date().year() - 1970) as usize;
                counts[(256 * year + c1) as usize] += 1;
            }
        }

        let mut u8_values: Vec<u8> = vec![];
        let mut year_values: Vec<i64> = vec![];
        let mut count_values: Vec<i64> = vec![];
        for i in 0..GROUPS {
            if counts[i] > 0 {
                let u8_value = i % 256;
                let year = i / 256 + 1970;
                u8_values.push(u8_value as u8);
                year_values.push(year as i64);
                count_values.push(counts[i] as i64);
            }
        }
        (Rc::new(u8_values), Rc::new(year_values), Rc::new(count_values))
    }
}

pub struct UInt8YearAndDistanceGroupByCountOperator {
    pub uint8_column: Rc<Vec<u8>>,
    pub timestamp_column: Rc<Vec<NaiveDateTime>>,
    pub distance_column: Rc<Vec<f32>>
}

impl Operator4<u8, i64, i64, i64> for UInt8YearAndDistanceGroupByCountOperator {
    fn execute(&self) -> (Rc<Vec<u8>>, Rc<Vec<i64>>, Rc<Vec<i64>>, Rc<Vec<i64>>) {
        const GROUPS: usize = MAX_PASSENGERS * 100 * 100;
        let mut counts: [u32; GROUPS] = [0; GROUPS];

        assert!(self.uint8_column.len() == self.timestamp_column.len());
        assert!(self.uint8_column.len() == self.distance_column.len());
        for i in 0..self.uint8_column.len() {
            unsafe {
                let c1 = *self.uint8_column.get_unchecked(i) as usize;
                let year = (self.timestamp_column.get_unchecked(i).date().year() - 1970) as usize;
                let distance = *self.distance_column.get_unchecked(i) as usize;
                counts[(MAX_PASSENGERS * 100 * year + 100 * c1 + distance) as usize] += 1;
            }
        }

        let mut u8_values: Vec<u8> = vec![];
        let mut year_values: Vec<i64> = vec![];
        let mut distance_values: Vec<i64> = vec![];
        let mut count_values: Vec<i64> = vec![];
        for i in 0..GROUPS {
            if counts[i] > 0 {
                let distance = i % 100;
                let u8_value = (i % MAX_PASSENGERS * 100) / 100;
                let year = i / (MAX_PASSENGERS * 100) + 1970;
                u8_values.push(u8_value as u8);
                year_values.push(year as i64);
                distance_values.push(distance as i64);
                count_values.push(counts[i] as i64);
            }
        }
        (Rc::new(u8_values), Rc::new(year_values), Rc::new(distance_values), Rc::new(count_values))
    }
}
