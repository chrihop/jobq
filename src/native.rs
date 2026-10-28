use anyhow::{ensure, Context, Result};
use std::{
    fs::{File, OpenOptions},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd},
        unix::fs::{OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};

pub fn exit_notification(pid: i32) -> Result<OwnedFd> {
    let fd = unsafe { libc::kqueue() };
    ensure!(
        fd >= 0,
        "create process exit notifier: {}",
        std::io::Error::last_os_error()
    );
    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
    ensure!(
        unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } >= 0,
        "protect notifier descriptor"
    );
    let event = libc::kevent {
        ident: pid as usize,
        filter: libc::EVFILT_PROC,
        flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_ONESHOT,
        fflags: libc::NOTE_EXIT,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    ensure!(
        unsafe { libc::kevent(fd, &event, 1, std::ptr::null_mut(), 0, std::ptr::null()) } == 0,
        "watch command exit: {}",
        std::io::Error::last_os_error()
    );
    Ok(owned)
}

#[repr(C)]
#[derive(Default)]
struct Usage {
    uuid: [u8; 16],
    user: u64,
    system: u64,
    idle: u64,
    interrupt: u64,
    pageins: u64,
    wired: u64,
    resident: u64,
    footprint: u64,
    start: u64,
    exit: u64,
}
#[link(name = "proc")]
unsafe extern "C" {
    fn proc_pid_rusage(pid: i32, flavor: i32, buffer: *mut Usage) -> i32;
    fn proc_listpgrppids(group: i32, buffer: *mut i32, bytes: i32) -> i32;
}
pub fn physical_memory() -> Result<u64> {
    let mut value = 0u64;
    let mut size = std::mem::size_of::<u64>();
    // SAFETY: value/size point to correctly sized writable storage; name is NUL terminated.
    let rc = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&mut value as *mut u64).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    ensure!(
        rc == 0,
        "read physical RAM: {}",
        std::io::Error::last_os_error()
    );
    Ok(value)
}

/// Read-only scheduling diagnostics. Requested QoS is not effective QoS;
/// resource-accounting samples from the actual workload establish the latter.
pub fn scheduling_diagnostics() -> serde_json::Value {
    fn sysctl_bytes(name: &str) -> Option<Vec<u8>> {
        let name = std::ffi::CString::new(name).ok()?;
        let mut size = 0;
        unsafe {
            if libc::sysctlbyname(
                name.as_ptr(),
                std::ptr::null_mut(),
                &mut size,
                std::ptr::null_mut(),
                0,
            ) != 0
                || size > 4096
            {
                return None;
            }
            let mut data = vec![0u8; size];
            if libc::sysctlbyname(
                name.as_ptr(),
                data.as_mut_ptr().cast(),
                &mut size,
                std::ptr::null_mut(),
                0,
            ) != 0
            {
                return None;
            }
            data.truncate(size);
            Some(data)
        }
    }
    fn number(name: &str) -> Option<u32> {
        Some(u32::from_ne_bytes(sysctl_bytes(name)?.try_into().ok()?))
    }
    fn string(name: &str) -> Option<String> {
        let data = sysctl_bytes(name)?;
        Some(String::from_utf8_lossy(&data).trim_end_matches('\0').into())
    }
    unsafe extern "C" {
        fn pthread_get_qos_class_np(
            thread: libc::pthread_t,
            class: *mut u32,
            priority: *mut i32,
        ) -> i32;
    }
    let mut qos = 0;
    let mut priority = 0;
    let rc = unsafe { pthread_get_qos_class_np(libc::pthread_self(), &mut qos, &mut priority) };
    let levels: Vec<_> = (0..number("hw.nperflevels").unwrap_or(0).min(16))
        .map(|n| {
            serde_json::json!({"level":n,"name":string(&format!("hw.perflevel{n}.name")),
            "logical_cpus":number(&format!("hw.perflevel{n}.logicalcpu"))})
        })
        .collect();
    serde_json::json!({"cpu":string("machdep.cpu.brand_string"),"performance_levels":levels,
        "daemon_requested_qos":if rc == 0 { Some(qos) } else { None },
        "daemon_requested_qos_name":match qos { 0x21=>"user_interactive",0x19=>"user_initiated",0x15=>"default",0x11=>"utility",0x09=>"background",_=>"unspecified" },
        "launch_service":std::env::var("XPC_SERVICE_NAME").ok(),
        "note":"Requested QoS is not proof of effective worker QoS or core placement. Use the scheduling probe for workload accounting."})
}
fn usage(pid: i32) -> std::io::Result<Usage> {
    let mut value = Usage::default();
    // SAFETY: Usage matches the SDK's rusage_info_v0; flavor 0 writes exactly that layout.
    if unsafe { proc_pid_rusage(pid, 0, &mut value) } == 0 {
        Ok(value)
    } else {
        Err(std::io::Error::last_os_error())
    }
}
pub struct ProcessUsage {
    pub token: u64,
    pub rss: u64,
    pub footprint: u64,
    pub live: bool,
}

/// Public KERN_PROC_PID starts with extern_proc. Only its SDK-defined prefix
/// is needed; the opaque pointer values are never dereferenced. Unlike task
/// inspection, this BSD status remains available for exited setuid helpers
/// and for processes owned by other users.
#[repr(C)]
struct ProcessPrefix {
    started: libc::timeval,
    vmspace: *mut libc::c_void,
    sigacts: *mut libc::c_void,
    flags: libc::c_int,
    status: libc::c_char,
    pid: libc::pid_t,
}
/// SDK layout (arm64/x86_64): sizeof(struct kinfo_proc) and
/// offsetof(kp_eproc.e_pcred.p_ruid). Checked against the live kernel in tests.
const KINFO_PROC_SIZE: usize = 648;
const KINFO_PROC_RUID: usize = 392;

/// Identity facts any user may read about any process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublicProcess {
    /// Wall-clock start (microseconds since the epoch), fixed at fork.
    pub start_us: u64,
    /// Real uid; unchanged by exec of setuid binaries.
    pub uid: u32,
    pub status: u32,
}

pub fn public_process(pid: i32) -> std::io::Result<PublicProcess> {
    let mut mib = [libc::CTL_KERN, libc::KERN_PROC, libc::KERN_PROC_PID, pid];
    let mut data = [0u8; 4096];
    let mut size = data.len();
    if unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len() as _,
            data.as_mut_ptr().cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    } < 0
    {
        return Err(std::io::Error::last_os_error());
    }
    if size == 0 {
        return Err(std::io::Error::from_raw_os_error(libc::ESRCH));
    }
    if size < KINFO_PROC_SIZE {
        return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
    }
    let prefix = unsafe { std::ptr::read_unaligned(data.as_ptr().cast::<ProcessPrefix>()) };
    if prefix.pid != pid {
        return Err(std::io::Error::from_raw_os_error(libc::EINVAL));
    }
    let uid = unsafe { std::ptr::read_unaligned(data.as_ptr().add(KINFO_PROC_RUID).cast::<u32>()) };
    Ok(PublicProcess {
        start_us: (prefix.started.tv_sec.max(0) as u64)
            .saturating_mul(1_000_000)
            .saturating_add(prefix.started.tv_usec.max(0) as u64),
        uid,
        status: prefix.status as u32,
    })
}

/// Fork records the wall start and the rusage start abstime together. Mach
/// absolute time stops during sleep while wall time does not, so a process
/// with this start token began no later than the returned wall time (barring
/// a backwards wall-clock step).
pub fn latest_start_us(token: u64) -> Option<u64> {
    #[repr(C)]
    struct Timebase {
        numer: u32,
        denom: u32,
    }
    unsafe extern "C" {
        fn mach_timebase_info(info: *mut Timebase) -> i32;
        fn mach_absolute_time() -> u64;
    }
    let mut base = Timebase { numer: 0, denom: 0 };
    // SAFETY: base is writable storage with the SDK's mach_timebase_info layout.
    if unsafe { mach_timebase_info(&mut base) } != 0 || base.denom == 0 {
        return None;
    }
    let ticks = unsafe { mach_absolute_time() }.checked_sub(token)?;
    let since_us = (ticks as u128 * base.numer as u128 / base.denom as u128 / 1000) as u64;
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_micros() as u64;
    now_us.checked_sub(since_us)
}

pub fn process_status(pid: i32) -> std::io::Result<u32> {
    bsd_info(pid).map(|info| info.pbi_status)
}

/// Elapsed time for deadlines and durations. CLOCK_MONOTONIC keeps counting
/// through system sleep on macOS; std's Instant (CLOCK_UPTIME_RAW) does not.
#[derive(Clone, Copy, Debug)]
pub struct Stopwatch(std::time::Duration);
impl Stopwatch {
    fn now() -> std::time::Duration {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: ts is valid writable storage; CLOCK_MONOTONIC always exists on macOS.
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
        std::time::Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
    }
    pub fn start() -> Self {
        Self(Self::now())
    }
    pub fn elapsed(&self) -> std::time::Duration {
        Self::now().saturating_sub(self.0)
    }
}

pub fn process_usage(pid: i32) -> std::io::Result<ProcessUsage> {
    // macOS setuid helpers (notably /bin/ps) can deny libproc queries during
    // their short lifetime. Retry a bounded window; never turn a sustained
    // denial into a zero-memory sample or a fabricated dead process.
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
    loop {
        let error = match usage(pid) {
            Ok(value) => match bsd_info(pid) {
                Ok(info) => {
                    return Ok(ProcessUsage {
                        token: value.start,
                        rss: value.resident,
                        footprint: value.footprint,
                        live: value.exit == 0 && info.pbi_status != libc::SZOMB,
                    })
                }
                Err(error) => error,
            },
            Err(error) if error.raw_os_error() == Some(libc::EPERM) => match bsd_info(pid) {
                Ok(info) if info.pbi_status == libc::SZOMB => {
                    return Ok(ProcessUsage {
                        token: 0,
                        rss: 0,
                        footprint: 0,
                        live: false,
                    })
                }
                Err(gone) if gone.raw_os_error() == Some(libc::ESRCH) => return Err(gone),
                _ => match public_process(pid) {
                    Ok(public) if public.status == libc::SZOMB => {
                        return Ok(ProcessUsage {
                            token: 0,
                            rss: 0,
                            footprint: 0,
                            live: false,
                        })
                    }
                    Err(gone) if gone.raw_os_error() == Some(libc::ESRCH) => return Err(gone),
                    _ => error,
                },
            },
            Err(error) => error,
        };
        if error.raw_os_error() != Some(libc::EPERM) || std::time::Instant::now() >= deadline {
            return Err(error);
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

pub fn child_pids(parent: i32) -> Result<Vec<i32>> {
    let mut capacity = 64usize;
    loop {
        let mut pids = vec![0i32; capacity];
        unsafe { *libc::__error() = 0 };
        let n = unsafe {
            libc::proc_listchildpids(parent, pids.as_mut_ptr().cast(), (capacity * 4) as i32)
        };
        let error = std::io::Error::last_os_error();
        ensure!(
            n >= 0 && (n != 0 || matches!(error.raw_os_error(), Some(0) | Some(libc::ESRCH))),
            "enumerate children of {parent}: {error}"
        );
        if (n as usize) < capacity {
            pids.truncate(n as usize);
            pids.retain(|pid| *pid > 1);
            return Ok(pids);
        }
        capacity *= 2;
        ensure!(capacity <= 1_048_576, "child inventory overflow");
    }
}

fn bsd_info(pid: i32) -> std::io::Result<libc::proc_bsdinfo> {
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of_val(&info) as i32;
    let n = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            (&mut info as *mut libc::proc_bsdinfo).cast(),
            size,
        )
    };
    if n == size {
        Ok(info)
    } else {
        Err(std::io::Error::last_os_error())
    }
}

pub fn parent_pid(pid: i32) -> std::io::Result<i32> {
    bsd_info(pid).map(|info| info.pbi_ppid as i32)
}

pub fn token(pid: i32) -> Result<u64> {
    Ok(usage(pid)?.start)
}
pub fn same_process(pid: i32, token: u64) -> bool {
    usage(pid).is_ok_and(|v| v.start == token)
}
pub fn is_live(pid: i32) -> bool {
    usage(pid).is_ok_and(|v| v.exit == 0)
}
pub fn group_has_live(group: i32, exclude: Option<i32>) -> Result<bool> {
    for pid in group_pids(group)? {
        if Some(pid) == exclude {
            continue;
        }
        match usage(pid) {
            Ok(v) if v.exit == 0 => return Ok(true),
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => {}
            Err(e) => return Err(e).context("check group cleanup"),
        }
    }
    Ok(false)
}
pub fn group_pids(group: i32) -> Result<Vec<i32>> {
    let mut capacity = 64usize;
    loop {
        let mut pids = vec![0i32; capacity];
        // libproc can return zero on syscall failure, not only for an empty group.
        // Reset this thread's errno so those failures cannot masquerade as zero usage.
        unsafe {
            *libc::__error() = 0;
        }
        // SAFETY: buffer has capacity * sizeof(pid_t) writable bytes.
        let n = unsafe { proc_listpgrppids(group, pids.as_mut_ptr(), (capacity * 4) as i32) };
        let error = std::io::Error::last_os_error();
        if n == 0 && !matches!(error.raw_os_error(), Some(0) | Some(libc::ESRCH)) {
            return Err(error).context("enumerate process group");
        }
        ensure!(
            n >= 0,
            "enumerate process group: {}",
            std::io::Error::last_os_error()
        );
        if (n as usize) < capacity {
            pids.truncate(n as usize);
            pids.retain(|p| *p > 0);
            return Ok(pids);
        }
        capacity *= 2;
        ensure!(capacity <= 1_048_576, "process group enumeration overflow");
    }
}
pub fn group_sample(group: i32) -> Result<(u64, u64, usize)> {
    let mut rss = 0u64;
    let mut footprint = 0u64;
    let mut count = 0;
    for pid in group_pids(group)? {
        match usage(pid) {
            Ok(v) => {
                rss = rss.saturating_add(v.resident);
                footprint = footprint.saturating_add(v.footprint);
                count += 1;
            }
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => {}
            Err(e) => return Err(e).context("read group resource usage"),
        }
    }
    Ok((rss, footprint, count))
}
pub fn kill_group(group: i32) -> Result<()> {
    ensure!(group > 1, "refusing invalid process group");
    // SAFETY: negative PID addresses only this isolated process group.
    let rc = unsafe { libc::kill(-group, libc::SIGKILL) };
    if rc < 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(());
        }
        // Darwin returns EPERM for an existing group containing only zombies.
        // Do not confuse that case with a permission failure against live work.
        if error.raw_os_error() == Some(libc::EPERM) {
            let mut live = false;
            for pid in group_pids(group)? {
                match usage(pid) {
                    Ok(v) => live |= v.exit == 0,
                    Err(e) if e.raw_os_error() == Some(libc::ESRCH) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            if !live {
                return Ok(());
            }
        }
        return Err(error.into());
    }
    Ok(())
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
    PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
        .join("Library/Application Support/jobq")
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
