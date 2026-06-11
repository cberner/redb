// Randomized stress test for extract_if/extract_from_if: interleaves front and
// back iteration over large trees of mixed dirty and committed pages, checking
// every yielded entry, the surviving table contents, and guard validity
// against a reference model.
use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::collections::BTreeMap;

const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("stress");

fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state >> 33
}

#[test]
fn extract_if_randomized_stress() {
    let tmpfile = tempfile::NamedTempFile::new().unwrap();
    let db = Database::create(tmpfile.path()).unwrap();

    for seed in 0..6u64 {
        let mut rng = seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1);
        let committed = 20_000 + (lcg(&mut rng) % 20_000);
        let uncommitted = 10_000 + (lcg(&mut rng) % 10_000);
        let modulus = [1, 2, 3, 7, 16, 64][(lcg(&mut rng) % 6) as usize];
        let range_start = lcg(&mut rng) % 20_000;
        let range_end = range_start + 10_000 + lcg(&mut rng) % 40_000;
        let take_limit = if lcg(&mut rng) % 3 == 0 {
            Some(lcg(&mut rng) % 2_000)
        } else {
            None
        };

        // Mix of committed and uncommitted pages, variable value sizes.
        let mut reference: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            // Reset table from any previous seed.
            table.retain(|_, _| false).unwrap();
            for _ in 0..committed {
                let key = lcg(&mut rng) % 60_000;
                let value = vec![(key % 251) as u8; (key % 90) as usize + 1];
                table.insert(key, value.as_slice()).unwrap();
                reference.insert(key, value);
            }
        }
        write_txn.commit().unwrap();

        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(TABLE).unwrap();
            for _ in 0..uncommitted {
                let key = lcg(&mut rng) % 60_000;
                let value = vec![(key % 13) as u8; (key % 120) as usize + 1];
                table.insert(key, value.as_slice()).unwrap();
                reference.insert(key, value);
            }

            // Matching keys within the range, by the same pure predicate.
            let mut matching: Vec<u64> = reference
                .range(range_start..range_end)
                .map(|(key, _)| *key)
                .filter(|key| key % modulus == 0)
                .collect();

            let mut iter = table
                .extract_from_if(range_start..range_end, |key, _| key % modulus == 0)
                .unwrap();
            let mut front_index = 0usize;
            let mut back_index = matching.len();
            let mut yielded: Vec<(u64, Vec<u8>)> = vec![];
            let mut guards = vec![];
            let mut steps = 0u64;
            loop {
                if let Some(limit) = take_limit
                    && steps >= limit
                {
                    break;
                }
                steps += 1;
                let from_front = lcg(&mut rng) % 2 == 0;
                let entry = if from_front {
                    iter.next()
                } else {
                    iter.next_back()
                };
                match entry {
                    Some(entry) => {
                        let (key_guard, value_guard) = entry.unwrap();
                        let expected_key = if from_front {
                            assert!(front_index < back_index, "yield past convergence");
                            front_index += 1;
                            matching[front_index - 1]
                        } else {
                            assert!(front_index < back_index, "yield past convergence");
                            back_index -= 1;
                            matching[back_index]
                        };
                        assert_eq!(key_guard.value(), expected_key, "seed {seed}");
                        assert_eq!(
                            value_guard.value(),
                            reference[&expected_key].as_slice(),
                            "seed {seed}"
                        );
                        yielded.push((expected_key, reference[&expected_key].clone()));
                        guards.push((key_guard, value_guard));
                    }
                    None => {
                        assert_eq!(front_index, back_index, "early exhaustion, seed {seed}");
                        break;
                    }
                }
            }
            drop(iter);

            // Guards must remain readable after the iterator is gone.
            for ((expected_key, expected_value), (key_guard, value_guard)) in
                yielded.iter().zip(&guards)
            {
                assert_eq!(key_guard.value(), *expected_key);
                assert_eq!(value_guard.value(), expected_value.as_slice());
            }
            drop(guards);

            // Only yielded entries were removed.
            for (key, _) in &yielded {
                assert!(table.get(key).unwrap().is_none(), "seed {seed} key {key}");
                reference.remove(key);
            }
            assert_eq!(table.len().unwrap(), reference.len() as u64, "seed {seed}");
            for entry in table.iter().unwrap() {
                let (key_guard, value_guard) = entry.unwrap();
                assert_eq!(
                    value_guard.value(),
                    reference[&key_guard.value()].as_slice(),
                    "seed {seed}"
                );
            }
            matching.clear();
        }
        write_txn.commit().unwrap();
    }
}
