// Built without std, so that the derived implementations are checked to need none
#![no_std]

use redb_derive::{Key, Value};

#[derive(Debug, Key, Value, PartialEq, Eq, PartialOrd, Ord)]
pub struct Point {
    pub x: u64,
    pub y: u64,
}

#[derive(Debug, Value)]
pub struct Named<'a> {
    pub name: &'a str,
    pub id: u32,
}
