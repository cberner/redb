mod buddy_allocator;
mod grouped_bitmap;
mod mmap;
mod page_allocator;
mod page_manager;
mod utils;

pub(crate) use page_manager::{get_db_size, PageNumber};

pub(in crate::tree_store) use page_manager::{Page, PageImpl, PageMut, TransactionalMemory};
