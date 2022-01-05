mod page_allocator;
mod page_manager;
mod utils;

pub(in crate) use page_manager::{get_db_size, PageNumber};

pub(in crate::tree_store) use page_manager::{Page, PageImpl, PageMut, TransactionalMemory};
