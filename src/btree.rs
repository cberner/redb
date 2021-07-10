use crate::btree::Node::{Internal, Leaf};
use crate::page_manager::{Page, PageManager};
use std::cell::Cell;
use std::cmp::Ordering;
use std::convert::TryInto;

const LEAF: u8 = 1;
const INTERNAL: u8 = 2;

fn write_vec(value: &[u8], output: &mut [u8], mut index: usize) -> usize {
    output[index..(index + 8)].copy_from_slice(&(value.len() as u64).to_be_bytes());
    index += 8;
    output[index..(index + value.len())].copy_from_slice(value);
    index += value.len();
    index
}

// Returns the (offset, len) of the value for the queried key, if present
pub(in crate) fn lookup_in_raw<'a>(
    page: Page<'a>,
    query: &[u8],
    manager: &'a PageManager,
) -> Option<(Page<'a>, usize, usize)> {
    let node_mem = page.memory();
    let mut index = 0;
    match node_mem[index] {
        LEAF => {
            index += 1;
            let key_len =
                u64::from_be_bytes(node_mem[index..(index + 8)].try_into().unwrap()) as usize;
            index += 8;
            match query.cmp(&node_mem[index..(index + key_len)]) {
                Ordering::Less => None,
                Ordering::Equal => {
                    index += key_len;
                    let value_len =
                        u64::from_be_bytes(node_mem[index..(index + 8)].try_into().unwrap())
                            as usize;
                    index += 8;
                    Some((page, index, value_len))
                }
                Ordering::Greater => {
                    index += key_len;
                    let value_len =
                        u64::from_be_bytes(node_mem[index..(index + 8)].try_into().unwrap())
                            as usize;
                    index += 8 + value_len;
                    let second_key_len =
                        u64::from_be_bytes(node_mem[index..(index + 8)].try_into().unwrap())
                            as usize;
                    index += 8;
                    if query == &node_mem[index..(index + second_key_len)] {
                        index += second_key_len;
                        let value_len =
                            u64::from_be_bytes(node_mem[index..(index + 8)].try_into().unwrap())
                                as usize;
                        index += 8;
                        Some((page, index, value_len))
                    } else {
                        None
                    }
                }
            }
        }
        INTERNAL => {
            index += 1;
            let key_len =
                u64::from_be_bytes(node_mem[index..(index + 8)].try_into().unwrap()) as usize;
            index += 8;
            let key = &node_mem[index..(index + key_len)];
            index += key_len;
            let left_page = u64::from_be_bytes(node_mem[index..(index + 8)].try_into().unwrap());
            index += 8;
            let right_page = u64::from_be_bytes(node_mem[index..(index + 8)].try_into().unwrap());
            if query <= key {
                lookup_in_raw(manager.get_page(left_page), query, manager)
            } else {
                lookup_in_raw(manager.get_page(right_page), query, manager)
            }
        }
        _ => unreachable!(),
    }
}

#[derive(Eq, PartialEq, Debug)]
pub(in crate) enum Node {
    Leaf((Vec<u8>, Vec<u8>), Option<(Vec<u8>, Vec<u8>)>),
    Internal(Box<Node>, Vec<u8>, Box<Node>),
}

impl Node {
    // Returns the page number that the node was written to
    pub(in crate) fn to_bytes(&self, page_manager: &PageManager) -> u64 {
        match self {
            Node::Leaf(left_val, right_val) => {
                let mut page = page_manager.allocate();
                let mut index = 0;
                let output = page.memory_mut();
                output[index] = LEAF;
                index += 1;
                index = write_vec(&left_val.0, output, index);
                index = write_vec(&left_val.1, output, index);
                if let Some(right) = right_val {
                    index = write_vec(&right.0, output, index);
                    write_vec(&right.1, output, index);
                } else {
                    // empty right val stored as a single 0 length key
                    write_vec(&[], output, index);
                }

                page.get_page_number()
            }
            Node::Internal(left, key, right) => {
                let left_page = left.to_bytes(page_manager);
                let right_page = right.to_bytes(page_manager);
                let mut page = page_manager.allocate();
                let mut index = 0;
                let output = page.memory_mut();
                output[index] = INTERNAL;
                index += 1;
                index = write_vec(key, output, index);
                output[index..(index + 8)].copy_from_slice(&left_page.to_be_bytes());
                index += 8;
                output[index..(index + 8)].copy_from_slice(&right_page.to_be_bytes());

                page.get_page_number()
            }
        }
    }

    fn get_max_key(&self) -> Vec<u8> {
        match self {
            Node::Leaf((left_key, _), right_val) => {
                if let Some((right_key, _)) = right_val {
                    right_key.to_vec()
                } else {
                    left_key.to_vec()
                }
            }
            Node::Internal(_left, _key, right) => right.get_max_key(),
        }
    }
}

pub(in crate) struct BtreeBuilder {
    pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl BtreeBuilder {
    pub(in crate) fn new() -> BtreeBuilder {
        BtreeBuilder { pairs: vec![] }
    }

    pub(in crate) fn add(&mut self, key: &[u8], value: &[u8]) {
        self.pairs.push((key.to_vec(), value.to_vec()));
    }

    pub(in crate) fn build(mut self) -> Node {
        assert!(!self.pairs.is_empty());
        self.pairs.sort();
        let mut leaves = vec![];
        for group in self.pairs.chunks(2) {
            let leaf = if group.len() == 1 {
                Leaf((group[0].0.to_vec(), group[0].1.to_vec()), None)
            } else {
                assert_eq!(group.len(), 2);
                if group[0].0 == group[1].0 {
                    todo!("support overwriting existing keys");
                }
                Leaf(
                    (group[0].0.to_vec(), group[0].1.to_vec()),
                    Some((group[1].0.to_vec(), group[1].1.to_vec())),
                )
            };
            leaves.push(leaf);
        }

        let mut bottom = leaves;
        let maybe_previous_node: Cell<Option<Node>> = Cell::new(None);
        while bottom.len() > 1 {
            let mut internals = vec![];
            for node in bottom.drain(..) {
                if let Some(previous_node) = maybe_previous_node.take() {
                    let key = previous_node.get_max_key();
                    let internal = Internal(Box::new(previous_node), key, Box::new(node));
                    internals.push(internal)
                } else {
                    maybe_previous_node.set(Some(node));
                }
            }
            if let Some(previous_node) = maybe_previous_node.take() {
                internals.push(previous_node);
            }

            bottom = internals
        }

        bottom.pop().unwrap()
    }
}

#[cfg(test)]
mod test {
    use crate::btree::Node::{Internal, Leaf};
    use crate::btree::{BtreeBuilder, Node};

    fn gen_tree() -> Node {
        let left = Leaf(
            (b"hello".to_vec(), b"world".to_vec()),
            Some((b"hello2".to_vec(), b"world2".to_vec())),
        );
        let right = Leaf((b"hello3".to_vec(), b"world3".to_vec()), None);
        Internal(Box::new(left), b"hello2".to_vec(), Box::new(right))
    }

    #[test]
    fn builder() {
        let expected = gen_tree();
        let mut builder = BtreeBuilder::new();
        builder.add(b"hello2", b"world2");
        builder.add(b"hello3", b"world3");
        builder.add(b"hello", b"world");

        assert_eq!(expected, builder.build());
    }
}
