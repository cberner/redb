extern crate rand;

use std::time::SystemTime;

use rand::Rng;

const ELEMENTS: usize = 1000*1000*1000;
const MAX_PASSENGERS: usize = 10;

fn query1() {
    let elements = ELEMENTS / 64;
    let mut data: Vec<i64> = vec![0; elements];
    for i in 0..elements {
        data[i] = rand::thread_rng().gen();
    }

    let start = SystemTime::now();

    let ones = data.iter()
        .map(|x| x.count_ones())
        .fold(0, |sum, x| sum + x);

    let end = SystemTime::now();
    let duration = end.duration_since(start)
        .expect("Time went backwards");
    println!("Query 1 result: {}", ones);
    println!("Query 1 duration: {:?}", duration);
}

fn query2() {
    let mut num_passengers: Vec<u8> = vec![0; ELEMENTS];
    let mut total_fare: Vec<f32> = vec![0.0; ELEMENTS];
    for i in 0..ELEMENTS {
        num_passengers[i] = rand::thread_rng().gen_range(0, 10);
        total_fare[i] = rand::thread_rng().gen_range(1.0, 100.0);
    }

    let mut sums: [f32; MAX_PASSENGERS] = [0.0; MAX_PASSENGERS];
    let mut counts: [u32; MAX_PASSENGERS] = [0; MAX_PASSENGERS];

    let start = SystemTime::now();

    for i in 0..ELEMENTS {
        sums[num_passengers[i] as usize] += total_fare[i];
        counts[num_passengers[i] as usize] += 1;
    }

    let end = SystemTime::now();
    let duration = end.duration_since(start)
        .expect("Time went backwards");
    println!("Query 2 result:");
    for i in 0..MAX_PASSENGERS {
        println!("{}: {}", i, sums[i] / counts[i] as f32);
    }
    println!("Query 2 duration: {:?}", duration);
}

fn main() {
    query1();
    query2();
}
