//! Linux 5.3+ backend: procfs identities/RSS, pidfd signalling and exit events.
use super::{ProcessUsage, PublicProcess};
use anyhow::{ensure, Context, Result};
use std::{
    fs, io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::{
        atomic::{AtomicI32, Ordering},
        OnceLock,
    },
};

pub const PROCESS_ZOMBIE: u32 = 4;
pub const PROCESS_STOPPED: u32 = 3;
pub const PROCESS_RUNNING: u32 = 1;
pub const MEMORY_METRIC: &str = "rss";
static SUBREAPER_PID: AtomicI32 = AtomicI32::new(0);

pub fn adopts_orphans() -> bool {
    // Fork does not inherit the kernel's subreaper setting. Bind the cached
    // capability to this process, not a boolean copied into forked children.
    SUBREAPER_PID.load(Ordering::Relaxed) == std::process::id() as i32
}

fn proc_error(e: io::Error) -> io::Error {
    match e.raw_os_error() {
        Some(libc::ENOENT) => io::Error::from_raw_os_error(libc::ESRCH),
        Some(libc::EACCES) => io::Error::from_raw_os_error(libc::EPERM),
        _ => e,
    }
}
fn read(path: impl AsRef<std::path::Path>) -> io::Result<String> {
    // Process names may contain arbitrary bytes. Only ASCII numeric fields are
    // interpreted; replacing invalid name bytes must not break monitoring.
    fs::read(path)
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .map_err(proc_error)
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid procfs record")
}
#[derive(Debug)]
struct Stat {
    pid: i32,
    parent: i32,
    group: i32,
    start: u64,
    state: u32,
    rss_pages: u64,
}
fn parse_stat(raw: &str) -> io::Result<Stat> {
    // comm can contain spaces, parentheses and newlines. Fields start after
    // the final ')', not after a whitespace token or the first closing bracket.
    let (head, tail) = raw.rsplit_once(')').ok_or_else(invalid)?;
    let pid = head
        .split_once('(')
        .ok_or_else(invalid)?
        .0
        .trim()
        .parse()
        .map_err(|_| invalid())?;
    let f: Vec<_> = tail.split_whitespace().collect();
    if f.len() < 22 {
        return Err(invalid());
    }
    let state = match f[0] {
        "Z" | "X" | "x" => PROCESS_ZOMBIE,
        "T" | "t" => PROCESS_STOPPED,
        _ => PROCESS_RUNNING,
    };
    Ok(Stat {
        pid,
        parent: f[1].parse().map_err(|_| invalid())?,
        group: f[2].parse().map_err(|_| invalid())?,
        start: f[19].parse().map_err(|_| invalid())?,
        state,
        rss_pages: f[21].parse::<i64>().map_err(|_| invalid())?.max(0) as u64,
    })
}
fn stat(pid: i32) -> io::Result<Stat> {
    if pid <= 0 {
        return Err(io::Error::from_raw_os_error(libc::ESRCH));
    }
    let value = parse_stat(&read(format!("/proc/{pid}/stat"))?)?;
    if value.pid != pid {
        return Err(invalid());
    }
    Ok(value)
}
fn tick_rate() -> io::Result<u64> {
    let n = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if n <= 0 {
        Err(invalid())
    } else {
        Ok(n as u64)
    }
}
fn start_us(ticks: u64) -> io::Result<u64> {
    Ok((u128::from(ticks) * 1_000_000 / u128::from(tick_rate()?)) as u64)
}
fn boot_seed() -> io::Result<u64> {
    static SEED: OnceLock<u64> = OnceLock::new();
    if let Some(seed) = SEED.get() {
        return Ok(*seed);
    }
    let id = read("/proc/sys/kernel/random/boot_id")?;
    if id.trim().len() != 36 {
        return Err(invalid());
    }
    let hash = id.trim().bytes().fold(0xcbf29ce484222325u64, |a, b| {
        (a ^ u64::from(b)).wrapping_mul(0x100000001b3)
    });
    Ok(*SEED.get_or_init(|| hash))
}
fn identity(ticks: u64) -> io::Result<u64> {
    // Invertible within one boot; reboot changes the seed. Tokens are opaque,
    // not chronological. Do not compare tokens to infer process ancestry.
    let token = boot_seed()? ^ ticks;
    if token == 0 {
        Err(invalid())
    } else {
        Ok(token)
    }
}
pub fn process_usage(pid: i32) -> io::Result<ProcessUsage> {
    let value = stat(pid)?;
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page <= 0 {
        return Err(invalid());
    }
    let rss = value.rss_pages.saturating_mul(page as u64);
    Ok(ProcessUsage {
        token: identity(value.start)?,
        rss,
        footprint: rss,
        live: value.state != PROCESS_ZOMBIE,
    })
}
pub fn public_process(pid: i32) -> io::Result<PublicProcess> {
    let before = stat(pid)?;
    let text = read(format!("/proc/{pid}/status"))?;
    let uid = text
        .lines()
        .find_map(|l| l.strip_prefix("Uid:"))
        .and_then(|l| l.split_whitespace().next())
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    if stat(pid)?.start != before.start {
        return Err(io::Error::from_raw_os_error(libc::ESRCH));
    }
    Ok(PublicProcess {
        start_us: start_us(before.start)?,
        uid,
        status: before.state,
    })
}
pub fn latest_start_us(token: u64) -> Option<u64> {
    start_us(token ^ boot_seed().ok()?).ok()
}
pub fn process_status(pid: i32) -> io::Result<u32> {
    Ok(stat(pid)?.state)
}
pub fn parent_pid(pid: i32) -> io::Result<i32> {
    Ok(stat(pid)?.parent)
}
pub fn token(pid: i32) -> Result<u64> {
    Ok(process_usage(pid)?.token)
}
pub fn same_process(pid: i32, token: u64) -> bool {
    process_usage(pid).is_ok_and(|v| v.token == token)
}
pub fn is_live(pid: i32) -> bool {
    process_usage(pid).is_ok_and(|v| v.live)
}

pub fn exit_notification(pid: i32) -> Result<OwnedFd> {
    pidfd(pid).context("open process exit notifier (Linux 5.3+ with pidfd support required)")
}
fn pidfd(pid: i32) -> io::Result<OwnedFd> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0u32) };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd as i32) })
    }
}
pub fn signal_identity(pid: i32, token: u64, signal: i32) -> io::Result<()> {
    let handle = pidfd(pid)?;
    if process_usage(pid)?.token != token {
        return Ok(());
    }
    let rc = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            handle.as_raw_fd(),
            signal,
            std::ptr::null::<libc::siginfo_t>(),
            0u32,
        )
    };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
pub fn physical_memory() -> Result<u64> {
    let raw = read("/proc/meminfo")?;
    let kib = raw
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))
        .and_then(|l| l.split_whitespace().next())
        .context("missing MemTotal")?
        .parse::<u64>()?;
    kib.checked_mul(1024).context("MemTotal overflow")
}
pub fn scheduling_diagnostics() -> serde_json::Value {
    serde_json::json!({"platform":"linux","memory_metric":MEMORY_METRIC,
        "cpu_count":std::thread::available_parallelism().map(|n|n.get()).ok(),
        "note":"Memory enforcement samples summed process-tree RSS from procfs; shared pages may be counted more than once. CPU scheduling is controlled by Linux. Host RAM is not a cgroup memory limit."})
}
pub fn max_rss_bytes(value: libc::c_long) -> u64 {
    (value.max(0) as u64).saturating_mul(1024)
}
pub fn peer_uid(fd: i32) -> Result<u32> {
    let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
    let mut size = std::mem::size_of_val(&cred) as libc::socklen_t;
    ensure!(
        unsafe {
            libc::getsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut cred as *mut libc::ucred).cast(),
                &mut size,
            )
        } == 0
            && size as usize == std::mem::size_of_val(&cred),
        "read peer credentials"
    );
    Ok(cred.uid)
}
pub fn child_pids(parent: i32) -> Result<Vec<i32>> {
    let tasks = match fs::read_dir(format!("/proc/{parent}/task")) {
        Ok(d) => d,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut children = std::collections::BTreeSet::new();
    for task in tasks {
        let path = task?.path().join("children");
        match read(&path) {
            Ok(raw) => {
                for child in raw.split_whitespace() {
                    children.insert(child.parse::<i32>()?);
                }
            }
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => {}
            Err(e) => return Err(e).context("read thread children"),
        }
    }
    Ok(children.into_iter().filter(|p| *p > 1).collect())
}
pub fn group_pids(group: i32) -> Result<Vec<i32>> {
    let mut pids = Vec::new();
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<i32>() else {
            continue;
        };
        match stat(pid) {
            Ok(s) if s.group == group => pids.push(pid),
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => {}
            Err(e) => return Err(e).context("enumerate process groups"),
        }
    }
    Ok(pids)
}
pub fn group_has_live(group: i32, exclude: Option<i32>) -> Result<bool> {
    for pid in group_pids(group)? {
        if Some(pid) == exclude {
            continue;
        }
        match process_usage(pid) {
            Ok(v) if v.live => return Ok(true),
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(false)
}
pub fn group_sample(group: i32) -> Result<(u64, u64, usize)> {
    let (mut rss, mut count) = (0u64, 0);
    for pid in group_pids(group)? {
        match process_usage(pid) {
            Ok(v) if v.live => {
                rss = rss.saturating_add(v.rss);
                count += 1
            }
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok((rss, rss, count))
}
pub fn kill_group(group: i32) -> Result<()> {
    ensure!(group > 1, "refusing invalid process group");
    if unsafe { libc::kill(-group, libc::SIGKILL) } < 0 {
        let e = io::Error::last_os_error();
        if e.raw_os_error() != Some(libc::ESRCH) {
            return Err(e.into());
        }
    }
    Ok(())
}
pub fn initialize_supervisor() -> Result<()> {
    ensure!(
        unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } == 0,
        "enable descendant adoption: {}",
        io::Error::last_os_error()
    );
    SUBREAPER_PID.store(std::process::id() as i32, Ordering::Relaxed);
    Ok(())
}
pub fn adopted_pids() -> Result<Vec<i32>> {
    let mut enabled: libc::c_int = 0;
    ensure!(
        unsafe { libc::prctl(libc::PR_GET_CHILD_SUBREAPER, &mut enabled, 0, 0, 0) } == 0,
        "read descendant adoption"
    );
    if enabled != 0 {
        child_pids(std::process::id() as i32)
    } else {
        Ok(Vec::new())
    }
}
pub fn reap_adopted() {
    // Called only after wait4 collected the command's exit status/accounting.
    loop {
        let rc = unsafe { libc::waitpid(-1, std::ptr::null_mut(), libc::WNOHANG) };
        if rc > 0 {
            continue;
        }
        if rc < 0 && io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        break;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn proc_stat_names_and_accounting() {
        let mut fields = vec!["0"; 22];
        fields[0] = "S";
        fields[1] = "12";
        fields[2] = "34";
        fields[19] = "987";
        fields[21] = "42";
        let s = parse_stat(&format!("123 (a ) odd\nname) {}", fields.join(" "))).unwrap();
        assert_eq!(
            (s.pid, s.parent, s.group, s.start, s.rss_pages),
            (123, 12, 34, 987, 42)
        );
        assert!(parse_stat("bad").is_err());
        assert_eq!(max_rss_bytes(12), 12288);
    }
    #[test]
    fn live_identity_peer_and_pidfd() {
        let pid = std::process::id() as i32;
        let v = process_usage(pid).unwrap();
        assert!(v.live && v.rss > 0);
        assert_eq!(public_process(pid).unwrap().uid, unsafe { libc::getuid() });
        assert_eq!(
            latest_start_us(v.token),
            Some(public_process(pid).unwrap().start_us)
        );
        assert!(exit_notification(pid).is_ok());
        assert!(signal_identity(pid, v.token ^ 1, libc::SIGKILL).is_ok());
        let (a, _) = std::os::unix::net::UnixStream::pair().unwrap();
        assert_eq!(peer_uid(a.as_raw_fd()).unwrap(), unsafe { libc::geteuid() });
    }
}
