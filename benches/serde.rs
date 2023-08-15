use std::{env::current_dir, time::Instant};

use rand::{rngs::StdRng, Rng, SeedableRng};
use redb::{Database, RedbValue, TableDefinition};
use serde_derive::{Deserialize, Serialize};
use tempfile::NamedTempFile;

const TABLE: TableDefinition<u64, Dynamic> = TableDefinition::new("my_data");

#[derive(Debug, Serialize, Deserialize)]
struct DynamicData {
    int_part: u64,
    part1: Vec<u8>,
    part2: Vec<u8>,
}

#[derive(Debug)]
struct Dynamic;

impl RedbValue for Dynamic {
    type SelfType<'a> = DynamicData;

    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        bincode::deserialize(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        bincode::serialize(value).unwrap()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("Row")
    }
}

const ELEMENTS: u64 = 10_000;

fn gen_data(count: u64) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(0);
    let mut random_data = vec![];
    for _ in 0..count {
        random_data.push(rng.gen());
    }
    random_data
}

fn main() {
    let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
    let db = Database::create(tmpfile).unwrap();

    let mut all_bytes = 0;
    let mut test_data = Vec::new();
    for key in 0..ELEMENTS {
        let part1_data = key.to_le_bytes();
        let part2_data = gen_data(key);
        let dyn_data = DynamicData {
            int_part: key,
            part1: part1_data.to_vec(),
            part2: part2_data.to_vec(),
        };
        all_bytes += Dynamic::as_bytes(&dyn_data).len();
        test_data.push(dyn_data);
    }

    {
        let write_txn = db.begin_write().unwrap();
        let mut table = write_txn.open_table(TABLE).unwrap();

        let start = Instant::now();
        {
            for dyn_data in test_data {
                table.insert(dyn_data.int_part, &dyn_data).unwrap();
            }
        }
        drop(table);
        write_txn.commit().unwrap();

        let end = Instant::now();
        let duration = end - start;
        println!(
            "Bulk loaded {} Dynamic elements ({} bytes) in {}ms",
            ELEMENTS,
            all_bytes,
            duration.as_millis()
        );
    }
}
