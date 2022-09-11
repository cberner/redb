use tempfile::NamedTempFile;

use rand::Rng;
use std::env::current_dir;
use std::os::unix::io::AsRawFd;
use std::time::SystemTime;
use std::{ptr, slice};

fn human_readable_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{}B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{}KiB", bytes / 1024)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{}MiB", bytes / 1024 / 1024)
    } else if bytes < 1024 * 1024 * 1024 * 1024 {
        format!("{}GiB", bytes / 1024 / 1024 / 1024)
    } else {
        format!("{}TiB", bytes / 1024 / 1024 / 1024 / 1024)
    }
}

fn mmap_bench(mmap: &mut [u8], page_size: usize, data: &[u8], print: bool) {
    let pages = data.len() / page_size;
    let spacing = mmap.len() / pages;

    let start = SystemTime::now();
    for i in 0..pages {
        let mmap_start = i * spacing;
        let data_start = i * page_size;
        mmap[mmap_start..(mmap_start + page_size)]
            .copy_from_slice(&data[data_start..(data_start + page_size)]);
    }

    let result = unsafe {
        libc::msync(
            mmap.as_mut_ptr() as *mut libc::c_void,
            mmap.len() as libc::size_t,
            libc::MS_SYNC,
        )
    };
    assert_eq!(result, 0);

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    let throughput = data.len() * 1000 / duration.as_millis() as usize;
    if print {
        println!(
            "page_size: {}. Wrote {} bytes in {}ms ({}/s)",
            human_readable_bytes(page_size),
            human_readable_bytes(data.len()),
            duration.as_millis(),
            human_readable_bytes(throughput),
        );
    }
}

fn main() {
    const DATA_SIZE: usize = 128 * 1024 * 1024;
    const MAX_PAGE: usize = 128 * 1024;
    const MIN_PAGE: usize = 512;

    let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
    let file = tmpfile.into_file();

    let len = MAX_PAGE / MIN_PAGE * DATA_SIZE;
    file.set_len(len as u64).unwrap();

    let mmap_raw = unsafe {
        libc::mmap(
            ptr::null_mut(),
            len as libc::size_t,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    assert_ne!(mmap_raw, libc::MAP_FAILED);
    let mmap = unsafe { slice::from_raw_parts_mut(mmap_raw as *mut u8, len) };

    // Warm-up
    let mut data = vec![0u8; DATA_SIZE];
    for x in data.iter_mut() {
        *x = rand::thread_rng().gen();
    }
    let mut page_size = MIN_PAGE;
    while page_size <= MAX_PAGE {
        mmap_bench(mmap, page_size, &data, false);
        page_size *= 2;
    }

    let mut page_size = MIN_PAGE;
    while page_size <= MAX_PAGE {
        // generate new data to be sure pages get dirtied
        for x in data.iter_mut() {
            *x = rand::thread_rng().gen();
        }
        mmap_bench(mmap, page_size, &data, true);
        page_size *= 2;
    }
}
