#[cfg(unix)]
pub(crate) fn get_page_size() -> usize {
    use libc::{sysconf, _SC_PAGESIZE};

    unsafe { sysconf(_SC_PAGESIZE).try_into().unwrap() }
}

#[cfg(not(unix))]
pub(crate) fn get_page_size() -> usize {
    4096
}

#[cfg(unix)]
pub(crate) fn is_page_aligned(size: usize) -> bool {
    let os_page_size = get_page_size();

    size % os_page_size == 0
}

#[cfg(not(unix))]
pub(crate) fn is_page_aligned(_: usize) -> bool {
    false
}
