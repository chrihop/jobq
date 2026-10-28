use anyhow::{ensure, Context, Result};
use std::{
    fs::{File, OpenOptions},
    os::{
        fd::AsRawFd,
        unix::fs::{OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub use macos::*;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::*;
pub struct ProcessUsage {
    pub token: u64,
    pub rss: u64,
    pub footprint: u64,
    pub live: bool,
}

/// Identity facts any user may read about any process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicProcess {
    /// Start in microseconds: Unix time on macOS, time since boot on Linux.
    pub start_us: u64,
    /// Real uid; unchanged by exec of setuid binaries.
    pub uid: u32,
    pub status: u32,
}

/// Suspend-inclusive elapsed time: CLOCK_MONOTONIC on macOS, CLOCK_BOOTTIME
/// on Linux. Independent of wall-clock adjustments.
#[derive(Clone, Copy, Debug)]
pub struct Stopwatch(std::time::Duration);
impl Stopwatch {
    fn now() -> std::time::Duration {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: ts is writable and these clocks are supported by our target kernels.
        #[cfg(target_os = "macos")]
        let clock = libc::CLOCK_MONOTONIC;
        #[cfg(target_os = "linux")]
        let clock = libc::CLOCK_BOOTTIME;
        unsafe { libc::clock_gettime(clock, &mut ts) };
        std::time::Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
    }
    pub fn start() -> Self {
        Self(Self::now())
    }
    pub fn elapsed(&self) -> std::time::Duration {
        Self::now().saturating_sub(self.0)
    }
}

pub fn nonblocking(fd: i32) -> Result<()> {
    // SAFETY: fcntl operates on a caller-owned open descriptor.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    ensure!(
        flags >= 0 && unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } >= 0,
        "set nonblocking: {}",
        std::io::Error::last_os_error()
    );
    Ok(())
}
pub fn private_dir(path: &Path) -> Result<()> {
    let existed = path.exists();
    std::fs::create_dir_all(path)?;
    let meta = std::fs::symlink_metadata(path)?;
    use std::os::unix::fs::MetadataExt;
    // SAFETY: geteuid has no arguments or memory effects.
    ensure!(
        meta.is_dir() && meta.uid() == unsafe { libc::geteuid() },
        "directory must be owned by current user: {}",
        path.display()
    );
    if !existed {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    ensure!(
        std::fs::metadata(path)?.permissions().mode() & 0o077 == 0,
        "directory must have owner-only permissions: {}",
        path.display()
    );
    Ok(())
}
pub fn private_file(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?)
}
pub fn atomic_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    use std::io::Write;
    let temp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
    let mut f = private_file(&temp)?;
    f.write_all(&serde_json::to_vec(value)?)?;
    f.sync_all()?;
    std::fs::rename(&temp, path)?;
    File::open(path.parent().context("file has no parent")?)?.sync_all()?;
    Ok(())
}
pub fn runtime_dir() -> PathBuf {
    PathBuf::from(format!("/tmp/jobq-{}", unsafe { libc::geteuid() }))
}
pub fn socket_path() -> PathBuf {
    runtime_dir().join("socket")
}
pub fn state_dir() -> PathBuf {
    let home = PathBuf::from(std::env::var_os("HOME").unwrap_or_default());
    #[cfg(target_os = "macos")]
    {
        home.join("Library/Application Support/jobq")
    }
    #[cfg(target_os = "linux")]
    {
        std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .filter(|p| p.is_absolute())
            .unwrap_or_else(|| home.join(".local/state"))
            .join("jobq")
    }
}
pub fn lock(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    // SAFETY: the descriptor stays alive in the returned File for the entire lock lifetime.
    ensure!(
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
        "another jobq daemon is already running for this account"
    );
    Ok(file)
}
