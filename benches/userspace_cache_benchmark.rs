#[cfg(target_os = "linux")]
use std::env::current_dir;
#[cfg(target_os = "linux")]
use tempfile::NamedTempFile;

#[cfg(target_os = "linux")]
mod unix {
    use rand::prelude::SliceRandom;
    use rand::Rng;
    use std::collections::BTreeMap;
    use std::fs::{File, OpenOptions};
    use std::io::{IoSlice, Seek, SeekFrom, Write};
    use std::ops::DerefMut;
    use std::os::unix::fs::FileExt;
    use std::path::Path;
    use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, MutexGuard, RwLock};
    use std::time::{Duration, SystemTime};
    use std::{io, thread};
    use std::{os::unix::io::AsRawFd, ptr, slice};

    const ITERATIONS: usize = 3;
    const VALUE_SIZE: usize = 2024;
    const ELEMENTS_SPACE: usize = 1_000_000;
    const ELEMENTS: usize = ELEMENTS_SPACE / 10;

    const USERSPACE_CACHE_BYTES: usize = 4 * VALUE_SIZE * ELEMENTS;

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

    fn print_load_time(name: &str, duration: Duration) {
        let throughput = ELEMENTS * VALUE_SIZE * 1000 / duration.as_millis() as usize;
        println!(
            "{}: Loaded {} items ({}) in {}ms ({}/s)",
            name,
            ELEMENTS,
            human_readable_bytes(ELEMENTS * VALUE_SIZE),
            duration.as_millis(),
            human_readable_bytes(throughput),
        );
    }

    fn gen_data(count: usize, value_size: usize) -> Vec<Vec<u8>> {
        let mut values = vec![];
        for _ in 0..count {
            let value: Vec<u8> = (0..value_size).map(|_| rand::thread_rng().gen()).collect();
            values.push(value);
        }
        values
    }

    fn gen_entry_indices() -> Vec<usize> {
        let mut page_numbers: Vec<usize> = (0..ELEMENTS_SPACE).collect();
        page_numbers.shuffle(&mut rand::thread_rng());
        page_numbers.drain(ELEMENTS..);
        page_numbers
    }

    #[repr(C, align(4096))]
    struct AlignedPage([u8; PagedCachedFile::page_size() as usize]);

    impl AlignedPage {
        fn new() -> Self {
            Self([0; 4096])
        }
    }

    struct WritablePage<'a> {
        page: u64,
        guard: MutexGuard<'a, BTreeMap<u64, AlignedPage>>,
    }

    impl<'a> WritablePage<'a> {
        fn mut_data(&mut self) -> &mut [u8] {
            &mut self
                .guard
                .entry(self.page)
                .or_insert_with(AlignedPage::new)
                .0
        }
    }

    struct ReadablePage {
        data: Arc<AlignedPage>,
    }

    impl ReadablePage {
        fn data(&self) -> &[u8] {
            &self.data.0
        }
    }

    struct PagedCachedFile {
        file: File,
        max_read_cache_bytes: usize,
        read_cache_bytes: AtomicUsize,
        _max_write_buffer_bytes: usize,
        _write_buffer_bytes: AtomicUsize,
        read_cache: Vec<RwLock<BTreeMap<u64, Arc<AlignedPage>>>>,
        write_buffer: Vec<Mutex<BTreeMap<u64, AlignedPage>>>,
    }

    impl PagedCachedFile {
        fn new(path: &Path, max_read_cache_bytes: usize, max_write_buffer_bytes: usize) -> Self {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)
                .unwrap();

            let mut read_cache = Vec::with_capacity(Self::lock_stripes());
            for _ in 0..Self::lock_stripes() {
                read_cache.push(RwLock::new(BTreeMap::new()));
            }

            let mut write_buffer = Vec::with_capacity(Self::lock_stripes());
            for _ in 0..Self::lock_stripes() {
                write_buffer.push(Mutex::new(BTreeMap::new()));
            }

            Self {
                file,
                max_read_cache_bytes,
                read_cache_bytes: AtomicUsize::new(0),
                _max_write_buffer_bytes: max_write_buffer_bytes,
                _write_buffer_bytes: AtomicUsize::new(0),
                read_cache,
                write_buffer,
            }
        }

        const fn lock_stripes() -> usize {
            131
        }

        const fn page_size() -> u64 {
            4096
        }

        const fn target_write_size() -> u64 {
            65536
        }

        fn read_page_direct(&self, page: u64) -> io::Result<AlignedPage> {
            let mut buffer = AlignedPage::new();
            let offset = page * Self::page_size();
            self.file.read_exact_at(&mut buffer.0, offset)?;
            Ok(buffer)
        }

        fn read_page(&self, page: u64) -> io::Result<ReadablePage> {
            let cache_slot = page as usize % self.read_cache.len();
            {
                let read_lock = self.read_cache[cache_slot].read().unwrap();
                if let Some(cached) = read_lock.get(&page) {
                    return Ok(ReadablePage {
                        data: cached.clone(),
                    });
                }
            }

            let buffer = Arc::new(self.read_page_direct(page)?);
            let cache_size = self
                .read_cache_bytes
                .fetch_add(buffer.0.len(), Ordering::AcqRel);
            let mut write_lock = self.read_cache[cache_slot].write().unwrap();
            write_lock.insert(page, buffer.clone());
            let mut removed = 0;
            if cache_size + buffer.0.len() > self.max_read_cache_bytes {
                while removed < buffer.0.len() {
                    let k = *write_lock.iter().next().unwrap().0;
                    let v = write_lock.remove(&k).unwrap();
                    removed += v.0.len();
                }
            }
            if removed > 0 {
                self.read_cache_bytes.fetch_sub(removed, Ordering::AcqRel);
            }

            Ok(ReadablePage { data: buffer })
        }

        fn write_page(&self, page: u64) -> WritablePage {
            WritablePage {
                page,
                guard: self.write_buffer[page as usize % Self::lock_stripes()]
                    .lock()
                    .unwrap(),
            }
        }

        fn flush2(&mut self) -> io::Result<()> {
            for stripe in 0..Self::lock_stripes() {
                let x: BTreeMap<u64, AlignedPage> = BTreeMap::new();
                let write_buffer =
                    std::mem::replace(self.write_buffer[stripe].lock().unwrap().deref_mut(), x);
                let mut batch: Vec<IoSlice> = vec![];
                let mut batch_start_offset = None;
                let mut batch_last_page = None;
                let mut iter = write_buffer.iter();
                loop {
                    let entry = iter.next();
                    if entry.is_none() {
                        // submit batch
                        self.file
                            .seek(SeekFrom::Start(batch_start_offset.unwrap()))?;
                        let written = self.file.write_vectored(&batch)?;
                        assert_eq!(written, batch.len() * Self::page_size() as usize);
                        break;
                    }
                    let (page, buffer) = entry.unwrap();
                    let offset = *page * Self::page_size();
                    if batch_start_offset.is_none() {
                        batch_start_offset = Some(offset);
                        batch_last_page = Some(page);
                        assert!(batch.is_empty());
                        batch.push(IoSlice::new(&buffer.0));
                    } else if *batch_last_page.unwrap() == page - 1
                        && batch.len() * (Self::page_size() as usize)
                            < Self::target_write_size() as usize
                    {
                        batch_last_page = Some(page);
                        batch.push(IoSlice::new(&buffer.0));
                    } else {
                        // submit batch
                        self.file
                            .seek(SeekFrom::Start(batch_start_offset.unwrap()))?;
                        let written = self.file.write_vectored(&batch)?;
                        assert_eq!(written, batch.len() * Self::page_size() as usize);

                        // Enqueue this entry
                        batch.clear();
                        batch_start_offset = Some(offset);
                        batch_last_page = Some(page);
                        batch.push(IoSlice::new(&buffer.0));
                    }
                }
            }

            self.file.sync_all()
        }
    }

    fn do_mmap_read(
        entry_indices: &[usize],
        pairs: Arc<Vec<Vec<u8>>>,
        mmap_raw: *mut libc::c_void,
        len: usize,
        threads: usize,
    ) -> Duration {
        let mut chunks: Vec<Vec<usize>> = vec![];
        for chunk in entry_indices.chunks_exact(ELEMENTS / threads) {
            chunks.push(chunk.to_vec());
        }

        let mut thread_joins = vec![];

        let start = SystemTime::now();
        for chunk in chunks {
            let len2 = len;
            let atomic_ptr = AtomicPtr::new(mmap_raw);
            let pairs2 = pairs.clone();
            let t = thread::spawn(move || {
                let mmap_raw2 = atomic_ptr.load(Ordering::SeqCst);
                let mmap = unsafe { slice::from_raw_parts_mut(mmap_raw2 as *mut u8, len2) };
                let pairs_len = pairs2.len();
                let mut checksum = 0u64;
                let mut expected_checksum = 0u64;
                for &i in &chunk {
                    let value = &pairs2[i % pairs_len];
                    let offset = i * value.len();
                    let buffer = &mmap[offset..(offset + value.len())];
                    checksum += buffer[0] as u64;
                    expected_checksum += value[0] as u64;
                }
                assert_eq!(checksum, expected_checksum);
            });
            thread_joins.push(t);
        }
        for t in thread_joins {
            t.join().unwrap();
        }
        let end = SystemTime::now();
        end.duration_since(start).unwrap()
    }

    #[inline(never)]
    fn warmup_mmap_read(
        entry_indices: &[usize],
        pairs: Arc<Vec<Vec<u8>>>,
        mmap_raw: *mut libc::c_void,
        len: usize,
        threads: usize,
    ) -> Duration {
        do_mmap_read(entry_indices, pairs, mmap_raw, len, threads)
    }

    pub fn mmap_bench(path: &Path, threads: usize) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        let len = 4 * 1024 * 1024 * 1024;
        file.set_len(len).unwrap();
        file.sync_all().unwrap();

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
        let result = unsafe { libc::madvise(mmap_raw, len as libc::size_t, libc::MADV_RANDOM) };
        assert_eq!(result, 0);

        let pairs = Arc::new(gen_data(1000, VALUE_SIZE));
        let pairs_len = pairs.len();

        let entry_indices = gen_entry_indices();
        let mut chunks: Vec<Vec<usize>> = vec![];
        for chunk in entry_indices.chunks_exact(ELEMENTS / threads) {
            chunks.push(chunk.to_vec());
        }

        let mut thread_joins = vec![];

        let start = SystemTime::now();
        for chunk in chunks {
            let len2 = len;
            let atomic_ptr = AtomicPtr::new(mmap_raw);
            let pairs2 = pairs.clone();
            let t = thread::spawn(move || {
                let mmap_raw2 = atomic_ptr.load(Ordering::SeqCst);
                let mmap =
                    unsafe { slice::from_raw_parts_mut(mmap_raw2 as *mut u8, len2 as usize) };
                for i in chunk {
                    let write_index = i * VALUE_SIZE;
                    let value = &pairs2[i % pairs_len];
                    mmap[write_index..(write_index + value.len())].copy_from_slice(value);
                }
            });
            thread_joins.push(t);
        }
        for t in thread_joins {
            t.join().unwrap();
        }

        let result = unsafe { libc::msync(mmap_raw, len as libc::size_t, libc::MS_SYNC) };
        assert_eq!(result, 0);
        file.sync_all().unwrap();

        // Drop the page cache
        let result = unsafe { libc::munmap(mmap_raw, len as libc::size_t) };
        assert_eq!(result, 0);
        let result = unsafe {
            libc::posix_fadvise64(
                file.as_raw_fd(),
                0,
                len as libc::off64_t,
                libc::POSIX_FADV_DONTNEED,
            )
        };
        assert_eq!(result, 0);
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
        let result = unsafe { libc::madvise(mmap_raw, len as libc::size_t, libc::MADV_RANDOM) };
        assert_eq!(result, 0);

        let end = SystemTime::now();
        let duration = end.duration_since(start).unwrap();
        print_load_time(&format!("mmap() threads={}", threads), duration);

        {
            let duration = warmup_mmap_read(
                &entry_indices,
                pairs.clone(),
                mmap_raw,
                len as usize,
                threads,
            );
            println!(
                "mmap() threads={}: Warmup random read {} items in {}ms",
                threads,
                ELEMENTS,
                duration.as_millis()
            );
            for _ in 0..ITERATIONS {
                let duration = do_mmap_read(
                    &entry_indices,
                    pairs.clone(),
                    mmap_raw,
                    len as usize,
                    threads,
                );
                println!(
                    "mmap() threads={}: Random read {} items in {}us",
                    threads,
                    ELEMENTS,
                    duration.as_micros()
                );
            }
        }
    }

    #[inline(never)]
    fn do_userspace_read(
        entry_indices: &[usize],
        pairs: Arc<Vec<Vec<u8>>>,
        file: Arc<PagedCachedFile>,
        threads: usize,
        direct: bool,
    ) -> Duration {
        let mut chunks: Vec<Vec<usize>> = vec![];
        for chunk in entry_indices.chunks_exact(ELEMENTS / threads) {
            chunks.push(chunk.to_vec());
        }

        let mut thread_joins = vec![];

        let start = SystemTime::now();
        for chunk in chunks {
            let pairs2 = pairs.clone();
            let file2 = file.clone();
            let t = thread::spawn(move || {
                let mut checksum = 0u64;
                let mut expected_checksum = 0u64;
                let pairs_len = pairs2.len();
                for &i in &chunk {
                    let value = &pairs2[i % pairs_len];
                    let page_num = i / 2;
                    let offset = VALUE_SIZE * (i % 2);
                    if direct {
                        let page = file2.read_page_direct(page_num as u64).unwrap();
                        checksum += page.0[offset] as u64;
                    } else {
                        let page = file2.read_page(page_num as u64).unwrap();
                        checksum += page.data()[offset] as u64;
                    }
                    expected_checksum += value[0] as u64;
                }
                assert_eq!(checksum, expected_checksum);
            });
            thread_joins.push(t);
        }
        for t in thread_joins {
            t.join().unwrap();
        }
        let end = SystemTime::now();
        end.duration_since(start).unwrap()
    }

    #[inline(never)]
    fn warmup_userspace_read(
        entry_indices: &[usize],
        pairs: Arc<Vec<Vec<u8>>>,
        file: Arc<PagedCachedFile>,
        threads: usize,
        direct: bool,
    ) -> Duration {
        do_userspace_read(entry_indices, pairs, file, threads, direct)
    }

    #[inline(never)]
    fn real_userspace_read(
        entry_indices: &[usize],
        pairs: Arc<Vec<Vec<u8>>>,
        file: Arc<PagedCachedFile>,
        threads: usize,
        direct: bool,
    ) -> Duration {
        do_userspace_read(entry_indices, pairs, file, threads, direct)
    }

    pub fn userspace_page_cache(path: &Path, threads: usize) {
        let mut file = Arc::new(PagedCachedFile::new(
            path,
            USERSPACE_CACHE_BYTES / 2,
            USERSPACE_CACHE_BYTES / 2,
        ));

        // We assume two values fit into a single page
        assert!(2 * VALUE_SIZE <= PagedCachedFile::page_size() as usize);

        let pairs = Arc::new(gen_data(1000, VALUE_SIZE));
        let pairs_len = pairs.len();

        let entry_indices = gen_entry_indices();
        let mut chunks: Vec<Vec<usize>> = vec![];
        for chunk in entry_indices.chunks_exact(ELEMENTS / threads) {
            chunks.push(chunk.to_vec());
        }

        let mut thread_joins = vec![];

        let start = SystemTime::now();
        for chunk in chunks {
            let pairs2 = pairs.clone();
            let file2 = file.clone();
            let t = thread::spawn(move || {
                for i in chunk {
                    let value = &pairs2[i % pairs_len];
                    let page_num = i / 2;
                    let offset = VALUE_SIZE * (i % 2);
                    let mut page = file2.write_page(page_num as u64);
                    let data = page.mut_data();
                    data[offset..(offset + value.len())].copy_from_slice(value);
                }
            });
            thread_joins.push(t);
        }
        for t in thread_joins {
            t.join().unwrap();
        }
        let end = SystemTime::now();
        let duration = end.duration_since(start).unwrap();
        println!(
            "userspace cached threads={}: Writes without flush in {}ms",
            threads,
            duration.as_millis()
        );
        Arc::get_mut(&mut file).unwrap().flush2().unwrap();

        let len = file.file.metadata().unwrap().len();
        let result = unsafe {
            libc::posix_fadvise64(
                file.file.as_raw_fd(),
                0,
                len as libc::off64_t,
                libc::POSIX_FADV_DONTNEED,
            )
        };
        assert_eq!(result, 0);

        let end = SystemTime::now();
        let duration = end.duration_since(start).unwrap();
        print_load_time(&format!("userspace cached threads={}", threads), duration);

        {
            let duration =
                warmup_userspace_read(&entry_indices, pairs.clone(), file.clone(), threads, true);
            println!(
                "userspace cached threads={}: Warmup (direct=true) random read {} items in {}ms",
                threads,
                ELEMENTS,
                duration.as_millis()
            );
            let duration =
                warmup_userspace_read(&entry_indices, pairs.clone(), file.clone(), threads, false);
            println!(
                "userspace cached threads={}: Warmup (direct=false) random read {} items in {}ms",
                threads,
                ELEMENTS,
                duration.as_millis()
            );
            for _ in 0..ITERATIONS {
                let duration =
                    real_userspace_read(&entry_indices, pairs.clone(), file.clone(), threads, true);
                println!(
                    "userspace cached threads={}: Random (direct=true) read {} items in {}us",
                    threads,
                    ELEMENTS,
                    duration.as_micros()
                );
            }
            for _ in 0..ITERATIONS {
                let duration = real_userspace_read(
                    &entry_indices,
                    pairs.clone(),
                    file.clone(),
                    threads,
                    false,
                );
                println!(
                    "userspace cached threads={}: Random (direct=false) read {} items in {}us",
                    threads,
                    ELEMENTS,
                    duration.as_micros()
                );
            }
        }
    }
}

fn main() {
    #[cfg(target_os = "linux")]
    {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        unix::mmap_bench(tmpfile.path(), 1);
    }
    #[cfg(target_os = "linux")]
    {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        unix::mmap_bench(tmpfile.path(), 8);
    }
    #[cfg(target_os = "linux")]
    {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        unix::userspace_page_cache(tmpfile.path(), 1);
    }
    #[cfg(target_os = "linux")]
    {
        let tmpfile: NamedTempFile = NamedTempFile::new_in(current_dir().unwrap()).unwrap();
        unix::userspace_page_cache(tmpfile.path(), 8);
    }
}
