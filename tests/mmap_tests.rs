#[cfg(unix)]
#[test]
// Test that reloading an mmap'ed region is thread-safe and atomic
fn reload_mmap() {
    use std::os::unix::io::AsRawFd;
    use std::sync::mpsc::channel;
    use std::{ptr, slice, thread};
    use tempfile::NamedTempFile;

    struct SendPtr(*mut libc::c_void);
    unsafe impl Send for SendPtr {}

    let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
    let (file, path) = tmpfile.into_parts();
    let valid_len = 10 * 1024 * 1024usize;
    file.set_len(valid_len as u64).unwrap();

    let fd = file.as_raw_fd();
    let mem = unsafe {
        libc::mmap(
            ptr::null_mut(),
            1024 * 1024 * 1024,
            libc::PROT_READ,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };

    let wrapper = SendPtr(mem);
    let (sender, receiver) = channel();
    let t = thread::spawn(move || {
        let moved = wrapper;
        let mem_ref = unsafe { slice::from_raw_parts(moved.0 as *const u8, valid_len) };
        let mut result = 0;
        for i in 0..10_000_000usize {
            result += mem_ref[i % valid_len];
        }
        sender.send(result).unwrap();
        result
    });

    let wrapper = SendPtr(mem);
    let t2 = thread::spawn(move || {
        let moved = wrapper;
        let mut iterations = 0;
        while receiver.try_recv().is_err() {
            for i in 0..100 {
                if i % 2 == 0 {
                    file.set_len(valid_len as u64).unwrap();
                } else {
                    file.set_len(2 * valid_len as u64).unwrap();
                }
                let new_mem = unsafe {
                    libc::mmap(
                        moved.0,
                        1024 * 1024 * 1024,
                        libc::PROT_READ,
                        libc::MAP_SHARED | libc::MAP_FIXED,
                        fd,
                        0,
                    )
                };
                assert_eq!(new_mem, moved.0);
                iterations += 1;
            }
        }
        iterations
    });

    let remaps = t2.join().unwrap();
    assert!(remaps > 1000);

    let read_result = t.join().unwrap();
    assert_eq!(read_result, 0);

    drop(path);
}
