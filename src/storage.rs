use crate::Error;
use memmap2::MmapMut;
use std::cell::{Ref, RefCell};
use std::convert::TryInto;
use std::ops::Deref;

const MAGICNUMBER: [u8; 4] = [b'r', b'e', b'd', b'b'];
const ENTRIES_OFFSET: usize = MAGICNUMBER.len();
const DATA_OFFSET: usize = ENTRIES_OFFSET + 8;

pub(in crate) struct Storage {
    mmap: RefCell<MmapMut>,
}

impl Storage {
    pub(in crate) fn new(mmap: MmapMut) -> Storage {
        Storage {
            mmap: RefCell::new(mmap),
        }
    }

    pub(in crate) fn initialize(&self) -> Result<(), Error> {
        let mut mmap = self.mmap.borrow_mut();
        if mmap[0..MAGICNUMBER.len()] == MAGICNUMBER {
            return Ok(());
        }
        mmap[ENTRIES_OFFSET..(ENTRIES_OFFSET + 8)].copy_from_slice(&0u64.to_be_bytes());
        mmap.flush()?;
        // Write the magic number only after the data structure is initialized and written to disk
        // to ensure that it's crash safe
        mmap[0..MAGICNUMBER.len()].copy_from_slice(&MAGICNUMBER);
        mmap.flush()?;

        Ok(())
    }

    pub(in crate) fn append(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let mut mmap = self.mmap.borrow_mut();
        let mut entries = u64::from_be_bytes(
            mmap[ENTRIES_OFFSET..(ENTRIES_OFFSET + 8)]
                .try_into()
                .unwrap(),
        ) as usize;

        let mut index = DATA_OFFSET;
        let mut entry = 0;
        while entry < entries {
            let key_len = u64::from_be_bytes(mmap[index..(index + 8)].try_into().unwrap()) as usize;
            index += 8;
            if key == &mmap[index..(index + key_len)] {
                todo!("support overwriting existing keys");
            }
            index += key_len;
            let value_len =
                u64::from_be_bytes(mmap[index..(index + 8)].try_into().unwrap()) as usize;
            index += 8 + value_len;
            entry += 1;
        }

        // Does not exist, append the new key & value
        mmap[index..(index + 8)].copy_from_slice(&(key.len() as u64).to_be_bytes());
        index += 8;
        mmap[index..(index + key.len())].copy_from_slice(key);
        index += key.len();
        mmap[index..(index + 8)].copy_from_slice(&(value.len() as u64).to_be_bytes());
        index += 8;
        mmap[index..(index + value.len())].copy_from_slice(value);

        entries += 1;
        mmap[ENTRIES_OFFSET..(ENTRIES_OFFSET + 8)].copy_from_slice(&entries.to_be_bytes());
        Ok(())
    }

    pub(in crate) fn fsync(&self) -> Result<(), Error> {
        self.mmap.borrow().flush()?;
        Ok(())
    }

    pub(in crate) fn get(&self, key: &[u8]) -> Result<Option<AccessGuard>, Error> {
        let mmap = self.mmap.borrow();

        let entries = u64::from_be_bytes(
            mmap[ENTRIES_OFFSET..(ENTRIES_OFFSET + 8)]
                .try_into()
                .unwrap(),
        ) as usize;

        let mut index = DATA_OFFSET;
        let mut entry = 0;
        while entry < entries {
            let key_len = u64::from_be_bytes(mmap[index..(index + 8)].try_into().unwrap()) as usize;
            index += 8;
            let value_len = u64::from_be_bytes(
                mmap[(index + key_len)..(index + key_len + 8)]
                    .try_into()
                    .unwrap(),
            ) as usize;
            if key == &mmap[index..(index + key_len)] {
                index += key_len + 8;
                let guard = AccessGuard {
                    mmap_ref: mmap,
                    offset: index,
                    len: value_len,
                };
                return Ok(Some(guard));
            }
            index += key_len + 8 + value_len;
            entry += 1;
        }

        Ok(None)
    }
}

pub struct AccessGuard<'mmap> {
    mmap_ref: Ref<'mmap, MmapMut>,
    offset: usize,
    len: usize,
}

impl<'mmap> Deref for AccessGuard<'mmap> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.mmap_ref[self.offset..(self.offset + self.len)]
    }
}
