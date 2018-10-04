extern crate rand;
extern crate chrono;

use std::time::SystemTime;
use std::rc::Rc;

use rand::Rng;

use chrono::{NaiveDate, Datelike, NaiveDateTime};

const ELEMENTS: usize = 1000*1000*1000;
const MAX_PASSENGERS: usize = 10;

struct Table1<T> {
    column1: Rc<Vec<T>>
}

impl<T: 'static> Table1<T> {
    fn table_scan(&self) -> ScanOperator1<T> {
        ScanOperator1 {
            column1: self.column1.clone()
        }
    }

    fn query(&self) -> Query1<T> {
        Query1 {
            operator: Box::new(self.table_scan())
        }
    }
}

struct Table2<T, U> {
    column1: Rc<Vec<T>>,
    column2: Rc<Vec<U>>
}

impl<T: 'static, U: 'static> Table2<T, U> {
    fn table_scan(&self) -> ScanOperator2<T, U> {
        ScanOperator2 {
            column1: self.column1.clone(),
            column2: self.column2.clone()
        }
    }

    fn query(&self) -> Query2<T, U> {
        Query2 {
            operator: Box::new(self.table_scan())
        }
    }
}

struct Table3<T, U, V> {
    column1: Rc<Vec<T>>,
    column2: Rc<Vec<U>>,
    column3: Rc<Vec<V>>
}

impl<T: 'static, U: 'static, V: 'static> Table3<T, U, V> {
    fn table_scan(&self) -> ScanOperator3<T, U, V> {
        ScanOperator3 {
            column1: self.column1.clone(),
            column2: self.column2.clone(),
            column3: self.column3.clone()
        }
    }

    fn query(&self) -> Query3<T, U, V> {
        Query3 {
            operator: Box::new(self.table_scan())
        }
    }
}

trait Operator1<T> {
    fn execute(&self) -> (Rc<Vec<T>>,);
}

trait Operator2<T, U> {
    fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>);
}

trait Operator3<T, U, V> {
    fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>, Rc<Vec<V>>);
}

trait Operator4<T, U, V, W> {
    fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>, Rc<Vec<V>>, Rc<Vec<W>>);
}

struct ScanOperator1<T> {
    column1: Rc<Vec<T>>
}

impl<T> Operator1<T> for ScanOperator1<T> {
    fn execute(&self) -> (Rc<Vec<T>>,) {
        (self.column1.clone(),)
    }
}

struct ScanOperator2<T, U> {
    column1: Rc<Vec<T>>,
    column2: Rc<Vec<U>>
}

impl<T, U> Operator2<T, U> for ScanOperator2<T, U> {
    fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>) {
        (self.column1.clone(), self.column2.clone())
    }
}

struct ScanOperator3<T, U, V> {
    column1: Rc<Vec<T>>,
    column2: Rc<Vec<U>>,
    column3: Rc<Vec<V>>
}

impl<T, U, V> Operator3<T, U, V> for ScanOperator3<T, U, V> {
    fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>, Rc<Vec<V>>) {
        (self.column1.clone(), self.column2.clone(), self.column3.clone())
    }
}

struct BooleanNotNullGroupByCountOperator {
    group_by_bits: Rc<Vec<i64>>
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

struct UInt8GroupByF32AverageOperator {
    group_by: Rc<Vec<u8>>,
    avg: Rc<Vec<f32>>
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

struct UInt8YearGroupByCountOperator {
    uint8_column: Rc<Vec<u8>>,
    timestamp_column: Rc<Vec<NaiveDateTime>>
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

struct UInt8YearAndDistanceGroupByCountOperator {
    uint8_column: Rc<Vec<u8>>,
    timestamp_column: Rc<Vec<NaiveDateTime>>,
    distance_column: Rc<Vec<f32>>
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

struct Query1<T> {
    operator: Box<Operator1<T>>
}

impl Query1<i64> {
    fn count_group_by(&self) -> Query2<bool, i64> {
        let group_by = Box::new(BooleanNotNullGroupByCountOperator {
            group_by_bits: self.operator.execute().0.clone()
        });
        Query2 {
            operator: group_by
        }
    }
}

impl<T> Query1<T> {
    fn execute(&self) -> (Rc<Vec<T>>,) {
        self.operator.execute()
    }
}

struct Query2<T, U> {
    operator: Box<Operator2<T, U>>
}

impl<T, U> Query2<T, U> {
    fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>) {
        self.operator.execute()
    }
}

struct Query3<T, U, V> {
    operator: Box<Operator3<T, U, V>>
}

impl<T, U, V> Query3<T, U, V> {
    fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>, Rc<Vec<V>>) {
        self.operator.execute()
    }
}

impl Query3<u8, NaiveDateTime, f32> {
    fn count_group_by_extract_year_and_distance(&self) -> Query4<u8, i64, i64, i64> {
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

struct Query4<T, U, V, W> {
    operator: Box<Operator4<T, U, V, W>>
}

impl<T, U, V, W> Query4<T, U, V, W> {
    fn execute(&self) -> (Rc<Vec<T>>, Rc<Vec<U>>, Rc<Vec<V>>, Rc<Vec<W>>) {
        self.operator.execute()
    }
}

impl Query2<u8, f32> {
    fn group_by_avg(&self) -> Query2<u8, f32> {
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
    fn count_group_by_extract_year(&self) -> Query3<u8, i64, i64> {
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

fn query1() {
    let elements = ELEMENTS / 64;
    let mut data: Vec<i64> = vec![0; elements];
    for i in 0..elements {
        data[i] = rand::thread_rng().gen();
    }

    let start = SystemTime::now();

    let table = Table1 {
        column1: Rc::new(data)
    };

    let q1 = table.query();
    let q2 = q1.count_group_by();
    let op_output = q2.execute();
    let result = op_output.1;

    let end = SystemTime::now();
    let duration = end.duration_since(start)
        .expect("Time went backwards");
    println!("Query 1 result: {:?}", result);
    println!("Query 1 duration: {:.1}ms", duration.as_secs() as f32 * 1000.0 +
        (duration.subsec_nanos() as f32 / 1000.0 / 1000.0));
}

fn query2() {
    let mut num_passengers: Vec<u8> = vec![0; ELEMENTS];
    let mut total_fare: Vec<f32> = vec![0.0; ELEMENTS];
    for i in 0..ELEMENTS {
        num_passengers[i] = rand::thread_rng().gen_range(0, 10);
        total_fare[i] = rand::thread_rng().gen_range(1.0, 100.0);
    }

    let start = SystemTime::now();

    let table = Table2 {
        column1: Rc::new(num_passengers),
        column2: Rc::new(total_fare)
    };

    let q1 = table.query();
    let q2 = q1.group_by_avg();
    let op_output = q2.execute();
    let result = op_output.1;


    let end = SystemTime::now();
    let duration = end.duration_since(start)
        .expect("Time went backwards");
    println!("Query 2 result:");
    for i in 0..MAX_PASSENGERS {
        println!("{}: {}", i, result[i]);
    }
    println!("Query 2 duration: {:?}ms", duration.as_secs() * 1000 + (duration.subsec_nanos() / 1000 / 1000) as u64);
}

fn query3() {
    let mut num_passengers: Vec<u8> = vec![0; ELEMENTS];
    let mut pickup_timestamp: Vec<NaiveDateTime> = vec![NaiveDate::from_ymd(2001, 1, 1).and_hms(1, 1, 1); ELEMENTS];
    for i in 0..ELEMENTS {
        num_passengers[i] = rand::thread_rng().gen_range(0, 10);
        pickup_timestamp[i] = NaiveDateTime::from_timestamp(rand::thread_rng().gen_range(1, (2018 - 1970)*365*24*60*60), 0);
    }

    let start = SystemTime::now();

    let table = Table2 {
        column1: Rc::new(num_passengers),
        column2: Rc::new(pickup_timestamp)
    };

    let q1 = table.query();
    let q2 = q1.count_group_by_extract_year();
    let op_output = q2.execute();

    let end = SystemTime::now();
    let duration = end.duration_since(start)
        .expect("Time went backwards");
    println!("Query 3 (first 10) results:");
    for i in 0..10 {
        let passengers = op_output.0[i];
        let year = op_output.1[i];
        let count = op_output.2[i];
        println!("{}, {}: {}", passengers, year, count);
    }
    println!("Query 3 duration: {:?}ms", duration.as_secs() * 1000 + (duration.subsec_nanos() / 1000 / 1000) as u64);
}

fn query4() {
    let mut num_passengers: Vec<u8> = vec![0; ELEMENTS];
    let mut pickup_timestamp: Vec<NaiveDateTime> = vec![NaiveDate::from_ymd(2001, 1, 1).and_hms(1, 1, 1); ELEMENTS];
    let mut trip_distance: Vec<f32> = vec![0.0; ELEMENTS];
    for i in 0..ELEMENTS {
        num_passengers[i] = rand::thread_rng().gen_range(0, 10);
        pickup_timestamp[i] = NaiveDateTime::from_timestamp(rand::thread_rng().gen_range(1, (2018 - 1970)*365*24*60*60), 0);
        trip_distance[i] = rand::thread_rng().gen_range(0.1, 100.0);
    }

    let start = SystemTime::now();

    let table = Table3 {
        column1: Rc::new(num_passengers),
        column2: Rc::new(pickup_timestamp),
        column3:  Rc::new(trip_distance)
    };

    let q1 = table.query();
    let q2 = q1.count_group_by_extract_year_and_distance();
    let op_output = q2.execute();

    let end = SystemTime::now();
    let duration = end.duration_since(start)
        .expect("Time went backwards");
    println!("Query 4 (first 10) result:");
    for i in 0..10 {
        let passengers = op_output.0[i];
        let year = op_output.1[i];
        let distance = op_output.2[i];
        let count = op_output.3[i];
        println!("{}, {}, {}: {}", passengers, year, distance, count);
    }
    println!("Query 4 duration: {:?}ms", duration.as_secs() * 1000 + (duration.subsec_nanos() / 1000 / 1000) as u64);
}

fn main() {
    query1();
    query2();
    query3();
    query4();
}
