extern crate chrono;
extern crate rand;

use std::rc::Rc;
use std::time::SystemTime;

use rand::Rng;

use chrono::{Datelike, NaiveDate, NaiveDateTime};

mod redb;

use redb::AggregationOperation::Average;
use redb::AggregationOperation::Count;
use redb::AggregationOperation::GroupBy;
use redb::{Table1, Table2, Table3};

const ELEMENTS: usize = 1000 * 1000 * 1000;
const MAX_PASSENGERS: usize = 10;

fn query1() {
    let elements = ELEMENTS / 64;
    let mut data: Vec<i64> = vec![0; elements];
    for i in 0..elements {
        data[i] = rand::thread_rng().gen();
    }

    let start = SystemTime::now();

    let table = Table1 {
        column1: Rc::new(data),
    };

    let q1 = table.query();
    let q2 = q1.aggregate(GroupBy, Count);
    let op_output = q2.execute();
    let result = op_output.1;

    let end = SystemTime::now();
    let duration = end.duration_since(start).expect("Time went backwards");
    println!("Query 1 result: {:?}", result);
    println!(
        "Query 1 duration: {:.1}ms",
        duration.as_secs() as f32 * 1000.0 + (duration.subsec_nanos() as f32 / 1000.0 / 1000.0)
    );
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
        column2: Rc::new(total_fare),
    };

    let q1 = table.query();
    let q2 = q1.aggregate(GroupBy, Average);
    let op_output = q2.execute();
    let result = op_output.1;

    let end = SystemTime::now();
    let duration = end.duration_since(start).expect("Time went backwards");
    println!("Query 2 result:");
    for i in 0..MAX_PASSENGERS {
        println!("{}: {}", i, result[i]);
    }
    println!(
        "Query 2 duration: {:?}ms",
        duration.as_secs() * 1000 + (duration.subsec_nanos() / 1000 / 1000) as u64
    );
}

#[inline(always)]
fn inline_project3(x: &u8, y: &NaiveDateTime) -> (u8, u8) {
    (*x, (y.date().year() - 1970) as u8)
}

fn query3() {
    let mut num_passengers: Vec<u8> = vec![0; ELEMENTS];
    let mut pickup_timestamp: Vec<NaiveDateTime> =
        vec![NaiveDate::from_ymd(2001, 1, 1).and_hms(1, 1, 1); ELEMENTS];
    for i in 0..ELEMENTS {
        num_passengers[i] = rand::thread_rng().gen_range(0, 10);
        pickup_timestamp[i] = NaiveDateTime::from_timestamp(
            rand::thread_rng().gen_range(1, (2018 - 1970) * 365 * 24 * 60 * 60),
            0,
        );
    }

    let start = SystemTime::now();

    let table = Table2 {
        column1: Rc::new(num_passengers),
        column2: Rc::new(pickup_timestamp),
    };

    let q1 = table.query();
    let q2 = q1.project(inline_project3);
    let q3 = q2.aggregate(GroupBy, GroupBy, Count);
    let q4 = q3.project(|&p, &y, &c| (p, y as i64 + 1970, c));
    let op_output = q4.execute();

    let end = SystemTime::now();
    let duration = end.duration_since(start).expect("Time went backwards");
    println!("Query 3 (first 10) results:");
    for i in 0..10 {
        let passengers = op_output.0[i];
        let year = op_output.1[i];
        let count = op_output.2[i];
        println!("{}, {}: {}", passengers, year, count);
    }
    println!(
        "Query 3 duration: {:?}ms",
        duration.as_secs() * 1000 + (duration.subsec_nanos() / 1000 / 1000) as u64
    );
}

#[inline(always)]
fn inline_project4(x: &u8, y: &NaiveDateTime, d: &f32) -> (u8, u8, u8) {
    (*x, (y.date().year() - 1970) as u8, *d as u8)
}

fn query4() {
    let mut num_passengers: Vec<u8> = vec![0; ELEMENTS];
    let mut pickup_timestamp: Vec<NaiveDateTime> =
        vec![NaiveDate::from_ymd(2001, 1, 1).and_hms(1, 1, 1); ELEMENTS];
    let mut trip_distance: Vec<f32> = vec![0.0; ELEMENTS];
    for i in 0..ELEMENTS {
        num_passengers[i] = rand::thread_rng().gen_range(0, 10);
        pickup_timestamp[i] = NaiveDateTime::from_timestamp(
            rand::thread_rng().gen_range(1, (2018 - 1970) * 365 * 24 * 60 * 60),
            0,
        );
        trip_distance[i] = rand::thread_rng().gen_range(0.1, 100.0);
    }

    let start = SystemTime::now();

    let table = Table3 {
        column1: Rc::new(num_passengers),
        column2: Rc::new(pickup_timestamp),
        column3: Rc::new(trip_distance),
    };

    let q1 = table.query();
    let q2 = q1.project(inline_project4);
    let q3 = q2.aggregate(GroupBy, GroupBy, GroupBy, Count);
    let q4 = q3.project(|&p, &y, &d, &c| (p, y as i64 + 1970, d, c));
    let op_output = q4.execute();

    let end = SystemTime::now();
    let duration = end.duration_since(start).expect("Time went backwards");
    println!("Query 4 (first 10) result:");
    for i in 0..10 {
        let passengers = op_output.0[i];
        let year = op_output.1[i];
        let distance = op_output.2[i];
        let count = op_output.3[i];
        println!("{}, {}, {}: {}", passengers, year, distance, count);
    }
    println!(
        "Query 4 duration: {:?}ms",
        duration.as_secs() * 1000 + (duration.subsec_nanos() / 1000 / 1000) as u64
    );
}

fn main() {
    query1();
    query2();
    query3();
    query4();
}
