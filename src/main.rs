extern crate rand;
extern crate chrono;

use std::time::SystemTime;
use std::rc::Rc;

use rand::Rng;

use chrono::{NaiveDate, Datelike, NaiveDateTime};

const ELEMENTS: usize = 1000*1000*1000;
const MAX_PASSENGERS: usize = 10;

trait Column {
    fn i64_data(&self) -> &Vec<i64> {
        panic!();
    }

    fn u8_data(&self) -> &Vec<u8> {
        panic!();
    }

    fn f32_data(&self) -> &Vec<f32> {
        panic!();
    }
}

struct BooleanNotNullColumn {
    data: Vec<i64>
}

impl Column for BooleanNotNullColumn {
    fn i64_data(&self) -> &Vec<i64> {
        &self.data
    }
}

struct Float32Column {
    data: Vec<f32>
}

impl Column for Float32Column {
    fn f32_data(&self) -> &Vec<f32> {
        &self.data
    }
}

struct UInt8Column {
    data: Vec<u8>
}

impl Column for UInt8Column {
    fn u8_data(&self) -> &Vec<u8> {
        &self.data
    }
}

struct Int64Column {
    data: Vec<i64>
}

impl Column for Int64Column {
    fn i64_data(&self) -> &Vec<i64> {
        &self.data
    }
}

struct Table {
    columns: Vec<Rc<Column>>
}

impl Table {
    fn table_scan(&self) -> ScanOperator {
        ScanOperator {
            columns: self.columns.clone()
        }
    }
}
impl Table {
    fn query(&self) -> Query {
        Query {
            operator: Box::new(self.table_scan())
        }
    }
}

trait Operator {
    fn execute(&self) -> Vec<Rc<Column>>;
}

struct ScanOperator {
    columns: Vec<Rc<Column>>
}

impl Operator for ScanOperator {
    fn execute(&self) -> Vec<Rc<Column>> {
        self.columns.clone()
    }
}

struct BooleanNotNullGroupByCountOperator {
    group_by: Vec<Rc<Column>>
}

impl Operator for BooleanNotNullGroupByCountOperator {
    fn execute(&self) -> Vec<Rc<Column>> {
        assert!(self.group_by.len() == 1);
        let data = &self.group_by[0].i64_data();
        let ones = data.iter()
            .map(|x| x.count_ones())
            .fold(0, |sum, x| sum + x);
        let column_data = vec![(ones as i64), (data.len() * 64) as i64 - ones as i64];
        let output = Int64Column {
            data: column_data
        };
        vec![Rc::new(output)]
    }
}

struct UInt8GroupByF32AverageOperator {
    group_by: Vec<Rc<Column>>,
    avg: Vec<Rc<Column>>
}

impl Operator for UInt8GroupByF32AverageOperator {
    fn execute(&self) -> Vec<Rc<Column>> {
        assert!(self.group_by.len() == 1);
        assert!(self.avg.len() == 1);

        let mut sums: [f32; 256] = [0.0; 256];
        let mut counts: [u32; 256] = [0; 256];

        let group_column = &self.group_by[0].u8_data();
        let avg_column = &self.avg[0].f32_data();

        for i in 0..group_column.len() {
            sums[group_column[i] as usize] += avg_column[i];
            counts[group_column[i] as usize] += 1;
        }

        let mut groups: Vec<u8> = vec![];
        let mut avgs: Vec<f32> = vec![];
        for i in 0..256 {
            if counts[i] > 0 {
                groups.push(i as u8);
                avgs.push(sums[i] / counts[i] as f32);
            }
        }
        let groups_output = UInt8Column {
            data: groups
        };
        let avgs_output = Float32Column {
            data: avgs
        };
        vec![Rc::new(groups_output), Rc::new(avgs_output)]
    }
}

struct Query {
    operator: Box<Operator>
}

impl Query {
    fn count_group_by(&self, columns: &[usize]) -> Query {
        assert!(columns.len() == 1);
        let group_by = Box::new(BooleanNotNullGroupByCountOperator {
            group_by: self.operator.execute().clone()
        });
        Query {
            operator: group_by
        }
    }

    fn group_by_avg(&self, group_by_columns: &[usize], avg_columns: &[usize]) -> Query {
        assert!(group_by_columns.len() == 1);
        assert!(avg_columns.len() == 1);
        let mut columns = self.operator.execute().clone();
        let group_by = Box::new(UInt8GroupByF32AverageOperator {
            group_by: vec![columns.remove(group_by_columns[0])],
            avg: vec![columns.remove(avg_columns[0] - 1)]
        });
        Query {
            operator: group_by
        }
    }

    fn execute(&self) -> Vec<Rc<Column>> {
        self.operator.execute()
    }
}

fn query1() {
    let elements = ELEMENTS / 64;
    let mut data: Vec<i64> = vec![0; elements];
    for i in 0..elements {
        data[i] = rand::thread_rng().gen();
    }

    let start = SystemTime::now();

    let column = BooleanNotNullColumn {
        data: data
    };

    let table = Table {
        columns: vec![Rc::new(column)]
    };

    let q1 = table.query();
    let q2 = q1.count_group_by(&[0 as usize]);
    let op_output = q2.execute();
    let result = op_output[0].i64_data();

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

    let passengers_column = UInt8Column {
        data: num_passengers
    };

    let fare_column = Float32Column {
        data: total_fare
    };

    let table = Table {
        columns: vec![Rc::new(passengers_column), Rc::new(fare_column)]
    };

    let q1 = table.query();
    let q2 = q1.group_by_avg(&[0 as usize], &[1 as usize]);
    let op_output = q2.execute();
    let result = op_output[1].f32_data();


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

    let mut counts: [u32; MAX_PASSENGERS * (2018 - 1970)] = [0; MAX_PASSENGERS * (2018 - 1970)];

    let start = SystemTime::now();

    for i in 0..ELEMENTS {
        let passengers = num_passengers[i] as usize;
        let year = (pickup_timestamp[i].date().year() - 1970) as usize;
        counts[(MAX_PASSENGERS * year + passengers) as usize] += 1;
    }

    let end = SystemTime::now();
    let duration = end.duration_since(start)
        .expect("Time went backwards");
    println!("Query 3 (first 10) results:");
    for i in 0..10 {
        let passengers = i % MAX_PASSENGERS;
        let year = i / MAX_PASSENGERS + 1970;
        println!("{}, {}: {}", passengers, year, counts[i]);
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

    let mut counts: [u32; MAX_PASSENGERS * (2018 - 1970) * 100] = [0; MAX_PASSENGERS * (2018 - 1970) * 100];

    let start = SystemTime::now();

    for i in 0..ELEMENTS {
        let passengers = num_passengers[i] as usize;
        let year = (pickup_timestamp[i].date().year() - 1970) as usize;
        let distance = trip_distance[i] as usize;
        counts[(MAX_PASSENGERS * 100 * year + 100 * passengers + distance) as usize] += 1;
    }

    let end = SystemTime::now();
    let duration = end.duration_since(start)
        .expect("Time went backwards");
    println!("Query 4 (first 10) result:");
    for i in 0..10 {
        let distance = i % 100;
        let passengers = (i % MAX_PASSENGERS * 100) / 100;
        let year = i / (MAX_PASSENGERS * 100) + 1970;
        println!("{}, {}, {}: {}", passengers, year, distance, counts[i]);
    }
    println!("Query 4 duration: {:?}ms", duration.as_secs() * 1000 + (duration.subsec_nanos() / 1000 / 1000) as u64);
}

fn main() {
    query1();
    query2();
    query3();
    query4();
}
