extern crate rand;

use std::time::SystemTime;

use rand::Rng;

const ELEMENTS: usize = 1000*1000*1000/64;

fn main() {
    let mut data: Vec<i64> = vec![0; ELEMENTS];
    for i in 0..ELEMENTS {
        data[i] = rand::thread_rng().gen();
    }

    let start = SystemTime::now();

    let ones = data.iter()
        .map(|x| x.count_ones())
        .fold(0, |sum, x| sum + x);

    let end = SystemTime::now();
    let duration = end.duration_since(start)
        .expect("Time went backwards");
    println!("Result: {}", ones);
    println!("Duration: {:?}", duration);
}
