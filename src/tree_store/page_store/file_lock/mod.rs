#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub(super) use unix::FileLock;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(super) use windows::FileLock;
