use crate::{
    model::*,
    native,
    process_tree::{self, Host, Native, ProcessTree},
};
use anyhow::{Context as _, Result};
use std::{
    collections::VecDeque,
    fs::File,
    io::{Read, Write},
    os::{fd::AsRawFd, unix::process::CommandExt},
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

struct Output {
    file: File,
    bytes: u64,
    cap: u64,
    truncated: bool,
    eof: bool,
}
impl Output {
    fn drain(&mut self, input: &mut impl Read) -> Result<()> {
        let mut buf = [0u8; 16384];
        // Bound work per iteration so an output flood cannot postpone deadlines.
        for _ in 0..16 {
            match input.read(&mut buf) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(n) => {
                    let keep = (self.cap.saturating_sub(self.bytes) as usize).min(n);
                    if keep > 0 {
                        self.file.write_all(&buf[..keep])?;
                        self.bytes += keep as u64;
                    }
                    self.truncated |= keep < n;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

pub fn run(dir: &Path) -> Result<()> {
    let spec: RunSpec = serde_json::from_slice(&std::fs::read(dir.join("spec.json"))?)?;
    // Parent persists our identity before opening the launch gate. EOF before G means no launch.
    let mut gate = [0u8; 1];
    let n = std::io::stdin().read(&mut gate)?;
    if n == 0 || gate[0] != b'G' {
        native::atomic_json(
            &dir.join("result.json"),
            &Outcome::interrupted("daemon disappeared before launch"),
        )?;
        return Ok(());
    }
    let result = execute(dir, &spec);
    let outcome = match result {
        Ok(o) => o,
        Err(e) => {
            let mut o = Outcome::interrupted(&format!("supervisor failure: {e:#}"));
            o.state = "monitoring_failed".into();
            o
        }
    };
    native::atomic_json(&dir.join("result.json"), &outcome)?;
    Ok(())
}

/// Bound on post-failure cleanup. A tree that cannot be confirmed gone in this
/// window is reported as unresolved instead of holding the job open forever.
const CLEANUP_LIMIT: Duration = Duration::from_secs(10);
/// Bound on waiting for the root to exit after a direct SIGKILL.
const REAP_LIMIT: Duration = Duration::from_secs(2);

/// Retry `attempt` until it confirms cleanup or `limit` elapses; on timeout
/// return the last reason cleanup was not confirmed.
fn settle(limit: Duration, mut attempt: impl FnMut() -> Result<bool>) -> Result<(), String> {
    let deadline = Instant::now() + limit;
    loop {
        let reason = match attempt() {
            Ok(true) => return Ok(()),
            Ok(false) => "live tracked processes remain".to_string(),
            Err(e) => format!("{e:#}"),
        };
        if Instant::now() >= deadline {
            return Err(reason);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Root {
    Running,
    Exited,
    Reaped,
}
/// WNOWAIT leaves an exited root unreaped, so its PID and PGID stay pinned.
fn root_state(pid: i32) -> Root {
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    let rc = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as u32,
            &mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if rc < 0 {
        if std::io::Error::last_os_error().raw_os_error() == Some(libc::ECHILD) {
            Root::Reaped
        } else {
            Root::Running
        }
    } else if unsafe { info.si_pid() } != 0 {
        Root::Exited
    } else {
        Root::Running
    }
}

struct GroupGuard<H: Host = Native> {
    pid: i32,
    tree: Option<ProcessTree<H>>,
    armed: bool,
}
struct Reaped {
    status: libc::c_int,
    usage: libc::rusage,
}
impl<H: Host> GroupGuard<H> {
    /// Kill and confirm the tree within `limit`. Never hangs: an unresolved
    /// identity yields Err naming it rather than an endless retry.
    fn cleanup(&mut self, limit: Duration) -> Result<(), String> {
        let (pid, tree) = (self.pid, &mut self.tree);
        settle(limit, || {
            let root = root_state(pid);
            let clean = match tree.as_mut() {
                Some(tree) => {
                    if root != Root::Running {
                        tree.mark_root_exited();
                    }
                    tree.kill()?;
                    !tree.has_live()?
                }
                None if root == Root::Reaped => true,
                None => {
                    native::kill_group(pid)?;
                    !native::group_has_live(pid, Some(pid))?
                }
            };
            Ok(clean && root != Root::Running)
        })
    }

    /// Reap the root. Until now it is our unreaped child, so its PID cannot
    /// have been reused and a direct SIGKILL is safe.
    fn reap(&mut self) -> Result<Reaped, String> {
        self.armed = false;
        if root_state(self.pid) == Root::Reaped {
            return Err("command process was already reaped".into());
        }
        if root_state(self.pid) == Root::Running {
            unsafe { libc::kill(self.pid, libc::SIGKILL) };
        }
        let deadline = Instant::now() + REAP_LIMIT;
        loop {
            let mut status = 0;
            let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
            let rc = unsafe { libc::wait4(self.pid, &mut status, libc::WNOHANG, &mut usage) };
            if rc == self.pid {
                return Ok(Reaped { status, usage });
            }
            if rc < 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                return Err(format!(
                    "reap command process {}: {}",
                    self.pid,
                    std::io::Error::last_os_error()
                ));
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "command process {} did not exit after SIGKILL; left to launchd",
                    self.pid
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl<H: Host> Drop for GroupGuard<H> {
    fn drop(&mut self) {
        // Unwinding path only; normal and error paths settle explicitly.
        if self.armed {
            let _ = self.cleanup(CLEANUP_LIMIT);
            let _ = self.reap();
        }
    }
}

/// Terminal outcome for a supervisor failure after launch. The message says
/// whether cleanup was confirmed and names unresolved identities if not.
fn failure_outcome(
    error: &anyhow::Error,
    cleanup: Result<(), String>,
    reaped: Result<Reaped, String>,
    elapsed: Duration,
    stats: Stats,
) -> Outcome {
    let mut message = format!("supervisor failure: {error:#}; ");
    match cleanup {
        Ok(()) => message.push_str("process tree cleanup confirmed"),
        Err(reason) => message.push_str(&format!(
            "process tree cleanup unresolved after {}s: {reason}",
            CLEANUP_LIMIT.as_secs()
        )),
    }
    let mut o = Outcome::interrupted("");
    match reaped {
        Ok(r) => {
            o.exit_code = libc::WIFEXITED(r.status).then(|| libc::WEXITSTATUS(r.status));
            o.signal = libc::WIFSIGNALED(r.status).then(|| libc::WTERMSIG(r.status));
            o.command_user_cpu_seconds = cpu_seconds(r.usage.ru_utime);
            o.command_system_cpu_seconds = cpu_seconds(r.usage.ru_stime);
            o.command_max_rss_bytes = r.usage.ru_maxrss as u64;
        }
        Err(reason) => message.push_str(&format!("; {reason}")),
    }
    o.state = "monitoring_failed".into();
    o.message = Some(message);
    o.elapsed_ms = elapsed.as_millis() as u64;
    o.stats = stats;
    o
}
fn cpu_seconds(t: libc::timeval) -> f64 {
    t.tv_sec as f64 + t.tv_usec as f64 / 1e6
}

/// Gated exec keeps a stable process-group identity before arbitrary user code runs.
/// If the supervisor dies before opening the gate, EOF makes this launcher exit.
pub fn exec_job(dir: &Path) -> Result<()> {
    let spec: RunSpec = serde_json::from_slice(&std::fs::read(dir.join("spec.json"))?)?;
    let mut gate = [0u8; 1];
    if std::io::stdin().read(&mut gate)? == 0 || gate[0] != b'G' {
        return Ok(());
    }
    let mut command = match &spec.context.command {
        CommandSpec::Exec { argv } => {
            let mut c = Command::new(&argv[0]);
            c.args(&argv[1..]);
            c
        }
        CommandSpec::Shell { command, shell } => {
            let mut c = Command::new(shell);
            c.arg("-c").arg(command);
            c
        }
    };
    unsafe {
        libc::umask(spec.context.umask.unwrap_or(0o022) as libc::mode_t);
    }
    // The supervisor started us with exactly the job environment; inheriting it
    // lets the daemon redact spec.json without racing this read.
    let error = command
        .current_dir(&spec.context.cwd)
        .stdin(Stdio::null())
        .exec();
    native::atomic_json(&dir.join("exec_error.json"), &error.to_string())?;
    Err(error.into())
}

fn execute(dir: &Path, spec: &RunSpec) -> Result<Outcome> {
    let mut out = Output {
        file: native::private_file(&dir.join("stdout"))?,
        bytes: 0,
        cap: spec.log_cap_bytes,
        truncated: false,
        eof: false,
    };
    let mut err = Output {
        file: native::private_file(&dir.join("stderr"))?,
        bytes: 0,
        cap: spec.log_cap_bytes,
        truncated: false,
        eof: false,
    };
    let mut command = Command::new(std::env::current_exe()?);
    command.arg("__exec").arg(dir);
    command
        .env_clear()
        .envs(&spec.context.env)
        .current_dir(&spec.context.cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = match command.spawn() {
        Ok(c) => c,
        Err(e) => {
            let mut o = Outcome::interrupted(&e.to_string());
            o.state = "spawn_failed".into();
            return Ok(o);
        }
    };
    let pid = child.id() as i32;
    let mut guard = GroupGuard {
        pid,
        tree: None,
        armed: true,
    };
    let mut clock = None;
    let mut stats = Stats::default();
    let monitored = monitor(
        dir,
        spec,
        &mut child,
        &mut guard,
        [&mut out, &mut err],
        &mut clock,
        &mut stats,
    );
    let elapsed = clock.map_or(Duration::ZERO, |c: native::Stopwatch| c.elapsed());
    let mut reason = match monitored {
        Ok(reason) => reason,
        Err(e) => {
            let cleanup = guard.cleanup(CLEANUP_LIMIT);
            let reaped = guard.reap();
            let mut o = failure_outcome(&e, cleanup, reaped, elapsed, stats);
            o.stdout_truncated = out.truncated;
            o.stderr_truncated = err.truncated;
            let _ = out.file.sync_all();
            let _ = err.file.sync_all();
            return Ok(o);
        }
    };
    // Cleanup is confirmed; reaping releases the pinned PID and PGID.
    let Reaped { status, usage } = guard.reap().map_err(anyhow::Error::msg)?;
    let exit_code = libc::WIFEXITED(status).then(|| libc::WEXITSTATUS(status));
    let signal = libc::WIFSIGNALED(status).then(|| libc::WTERMSIG(status));
    if reason.is_none() && dir.join("exec_error.json").exists() {
        reason = Some((
            "spawn_failed".into(),
            serde_json::from_slice::<String>(&std::fs::read(dir.join("exec_error.json"))?)?,
        ));
    }
    let (state, message) = match reason {
        Some((s, m)) => (s, Some(m)),
        None => (
            if exit_code == Some(0) {
                "succeeded"
            } else {
                "failed"
            }
            .into(),
            None,
        ),
    };
    out.file.sync_all()?;
    err.file.sync_all()?;
    Ok(Outcome {
        state,
        exit_code,
        signal,
        message,
        finished_at_ms: wall_ms(),
        elapsed_ms: elapsed.as_millis() as u64,
        command_user_cpu_seconds: cpu_seconds(usage.ru_utime),
        command_system_cpu_seconds: cpu_seconds(usage.ru_stime),
        command_max_rss_bytes: usage.ru_maxrss as u64,
        stdout_truncated: out.truncated,
        stderr_truncated: err.truncated,
        stats,
    })
}

/// Run the command to completion. Returns the abnormal-termination reason, if
/// any, once the whole tree is confirmed gone; the root is left unreaped.
fn monitor(
    dir: &Path,
    spec: &RunSpec,
    child: &mut std::process::Child,
    guard: &mut GroupGuard,
    [out, err]: [&mut Output; 2],
    clock_slot: &mut Option<native::Stopwatch>,
    stats: &mut Stats,
) -> Result<Option<(String, String)>> {
    let pid = guard.pid;
    let exit_notification = native::exit_notification(pid)?;
    let token = native::token(pid).context("obtain command identity")?;
    let root = process_tree::enroll(&Native, pid, token);
    guard.tree = Some(ProcessTree::new(dir, root)?);
    native::atomic_json(
        &dir.join("started.json"),
        &serde_json::json!({"pid":pid,"token":token,"process_tree":true}),
    )?;
    let mut stdout = child.stdout.take().context("missing stdout")?;
    let mut stderr = child.stderr.take().context("missing stderr")?;
    for fd in [0, 1, stdout.as_raw_fd(), stderr.as_raw_fd()] {
        native::nonblocking(fd)?;
    }
    // Deadlines count real elapsed time, including system sleep.
    let clock = *clock_slot.insert(native::Stopwatch::start());
    let started = wall_ms();
    child
        .stdin
        .take()
        .context("missing launch gate")?
        .write_all(b"G")?;
    let mut events = VecDeque::from([serde_json::to_vec(&RunnerEvent::Started {
        at_ms: started,
        pid,
        token,
    })?]);
    let mut reason: Option<(String, String)> = None;
    let mut root_exited = false;
    let mut next_sample = Duration::ZERO;
    let mut next_report = Duration::ZERO;
    loop {
        let elapsed = clock.elapsed();
        // Read cancellation/death before checking normal completion, giving a received cancellation
        // deterministic priority over an as-yet-unobserved exit.
        let mut control = [0u8; 32];
        match std::io::stdin().read(&mut control) {
            Ok(0) => {
                reason
                    .get_or_insert(("interrupted".into(), "daemon control channel closed".into()));
            }
            Ok(n) => {
                if control[..n].contains(&b'C') {
                    reason.get_or_insert(("cancelled".into(), "withdrawn by client".into()));
                } else if control[..n].contains(&b'S') {
                    reason.get_or_insert(("interrupted".into(), "server shutdown".into()));
                }
            }
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) => {}
            Err(e) => {
                reason.get_or_insert(("monitoring_failed".into(), e.to_string()));
            }
        }
        if elapsed >= Duration::from_millis(spec.timeout_ms) {
            reason.get_or_insert(("timed_out".into(), "queue wall-clock limit exceeded".into()));
        }
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // WNOWAIT pins the group leader's PID until group cleanup has completed.
        let rc = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as u32,
                &mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if rc < 0 {
            reason.get_or_insert((
                "monitoring_failed".into(),
                std::io::Error::last_os_error().to_string(),
            ));
        } else {
            root_exited = unsafe { info.si_pid() } != 0;
        }
        if root_exited {
            guard.tree.as_mut().unwrap().mark_root_exited();
        }
        if elapsed >= next_sample && !root_exited {
            match guard.tree.as_mut().unwrap().sample() {
                Ok((rss, footprint, count)) => {
                    stats.sampled_at_ms = wall_ms();
                    stats.rss_bytes = rss;
                    stats.footprint_bytes = footprint;
                    stats.sampled_process_count = count;
                    stats.peak_sampled_rss_bytes = stats.peak_sampled_rss_bytes.max(rss);
                    stats.peak_sampled_footprint_bytes =
                        stats.peak_sampled_footprint_bytes.max(footprint);
                    if footprint > spec.memory_bytes {
                        reason.get_or_insert((
                            "memory_exceeded".into(),
                            "sampled process tree physical footprint exceeded cap".into(),
                        ));
                    }
                }
                Err(e) => {
                    reason.get_or_insert(("monitoring_failed".into(), format!("{e:#}")));
                }
            }
            next_sample = elapsed + Duration::from_millis(spec.monitor_ms);
        }
        if reason.is_some() || root_exited {
            guard.tree.as_mut().unwrap().kill()?;
        }
        for (output, input) in [
            (&mut *out, &mut stdout as &mut dyn Read),
            (&mut *err, &mut stderr as &mut dyn Read),
        ] {
            if !output.eof {
                if let Err(e) = output.drain(&mut { input }) {
                    reason.get_or_insert((
                        "monitoring_failed".into(),
                        format!("log write/read failed: {e:#}"),
                    ));
                    guard.tree.as_mut().unwrap().kill()?;
                }
            }
        }
        if elapsed >= next_report {
            if events.len() < 2 {
                events.push_back(serde_json::to_vec(&RunnerEvent::Sample {
                    stats: stats.clone(),
                    stdout_truncated: out.truncated,
                    stderr_truncated: err.truncated,
                })?);
            }
            next_report = elapsed + Duration::from_millis(500);
        }
        while let Some(event) = events.front() {
            let mut line = event.clone();
            line.push(b'\n');
            // Events are far below PIPE_BUF: a nonblocking write is atomic, or returns EAGAIN.
            let n = unsafe { libc::write(1, line.as_ptr().cast(), line.len()) };
            if n == line.len() as isize {
                events.pop_front();
            } else {
                break;
            }
        }
        if root_exited && (reason.is_some() || (out.eof && err.eof)) {
            // A group can contain orphaned zombies, which consume no memory/CPU. Count live
            // members by checking kill delivery and rusage exit tokens, rather than waiting for init.
            let alive = guard.tree.as_mut().unwrap().has_live()?;
            if !alive {
                break;
            }
        }
        let wait = if reason.is_some() || root_exited {
            10
        } else {
            let until_sample = next_sample.saturating_sub(clock.elapsed()).as_millis() as u64;
            let until_deadline = spec
                .timeout_ms
                .saturating_sub(clock.elapsed().as_millis() as u64);
            until_sample.min(until_deadline).clamp(1, 100) as i32
        };
        let mut fds = [
            libc::pollfd {
                fd: 0,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: if out.eof { -1 } else { stdout.as_raw_fd() },
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: if err.eof { -1 } else { stderr.as_raw_fd() },
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: if root_exited {
                    -1
                } else {
                    exit_notification.as_raw_fd()
                },
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as _, wait) };
    }
    Ok(reason)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process_tree::fake::{FakeHost, Proc};
    use std::os::unix::process::ExitStatusExt;

    fn sleeper() -> (i32, u64) {
        let child = Command::new("/bin/sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id() as i32;
        // The guard owns reaping from here on.
        std::mem::forget(child);
        (pid, native::token(pid).unwrap())
    }

    #[test]
    fn settle_is_bounded_and_reports_last_reason() {
        let started = Instant::now();
        let reason = settle(Duration::from_millis(50), || {
            anyhow::bail!("read tracked process pid 2789: Operation not permitted")
        })
        .unwrap_err();
        assert!(reason.contains("pid 2789"), "{reason}");
        assert!(started.elapsed() < Duration::from_secs(2));
        let mut attempts = 0;
        settle(Duration::from_secs(5), || {
            attempts += 1;
            Ok(attempts == 3)
        })
        .unwrap();
        assert_eq!(attempts, 3);
    }

    #[test]
    fn real_tree_cleanup_confirms_and_reaps_root() {
        let dir = tempfile::tempdir().unwrap();
        let (pid, token) = sleeper();
        let root = process_tree::enroll(&Native, pid, token);
        let mut guard = GroupGuard {
            pid,
            tree: Some(ProcessTree::new(dir.path(), root).unwrap()),
            armed: true,
        };
        guard.cleanup(Duration::from_secs(5)).unwrap();
        let reaped = guard.reap().unwrap();
        assert!(libc::WIFSIGNALED(reaped.status));
        assert_eq!(libc::WTERMSIG(reaped.status), libc::SIGKILL);
    }

    /// Job 6faba084: a tracked PID that cannot be identified used to make
    /// GroupGuard::drop retry forever with the root unreaped and no result.
    #[test]
    fn unresolved_member_ends_in_monitoring_failed_and_root_is_reaped() {
        let dir = tempfile::tempdir().unwrap();
        let (pid, token) = sleeper();
        let host = FakeHost::default();
        host.insert(
            pid,
            Proc::Ours {
                token,
                start_us: 1,
                parent: 1,
                group: pid,
            },
        );
        host.insert(
            2789,
            Proc::Ours {
                token: token + 1,
                start_us: 2,
                parent: pid,
                group: pid,
            },
        );
        let root = process_tree::enroll(&host, pid, token);
        let mut tree = ProcessTree::with_host(host.clone(), dir.path(), root).unwrap();
        tree.refresh().unwrap();
        // Same start time, different real uid: never positively identifiable.
        host.insert(
            2789,
            Proc::Foreign {
                start_us: 2,
                uid: 0,
            },
        );
        let mut guard = GroupGuard {
            pid,
            tree: Some(tree),
            armed: true,
        };
        let started = Instant::now();
        let cleanup = guard.cleanup(Duration::from_millis(200));
        let reaped = guard.reap();
        assert!(started.elapsed() < Duration::from_secs(10));
        let reason = cleanup.as_ref().unwrap_err();
        assert!(reason.contains("pid 2789"), "{reason}");
        // The fake never delivers signals, so the root was still running and
        // the guard killed and reaped it directly.
        let status = std::process::ExitStatus::from_raw(reaped.as_ref().unwrap().status);
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        assert!(!host.signals().iter().any(|(p, _)| *p == 2789));
        let outcome = failure_outcome(
            &anyhow::anyhow!("unresolved tracked processes"),
            cleanup,
            reaped,
            Duration::from_secs(3),
            Stats::default(),
        );
        assert_eq!(outcome.state, "monitoring_failed");
        assert_eq!(outcome.signal, Some(libc::SIGKILL));
        let message = outcome.message.unwrap();
        assert!(message.contains("cleanup unresolved"), "{message}");
        assert!(message.contains("pid 2789"), "{message}");
    }
}
