use std::convert::TryInto;

pub(in crate) struct PageAllocator {
    next_free_page: u64,
}

impl PageAllocator {
    pub(in crate) const fn state_size() -> usize {
        8
    }

    pub(in crate) fn initialize(output: &mut [u8]) -> Self {
        output[0..8].copy_from_slice(&1u64.to_be_bytes());
        PageAllocator {
            // Page 0 is always reserved for the database metadata
            next_free_page: 1,
        }
    }

    pub(in crate) fn restore(state: &[u8]) -> Self {
        PageAllocator {
            next_free_page: u64::from_be_bytes(state[0..8].try_into().unwrap()),
        }
    }

    pub(in crate) fn allocate(&mut self) -> u64 {
        self.next_free_page += 1;
        self.next_free_page - 1
    }

    pub(in crate) fn store_state(&self, output: &mut [u8]) {
        output.copy_from_slice(&self.next_free_page.to_be_bytes());
    }
}
