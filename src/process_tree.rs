//! Follow cooperating descendants across process-group/session changes. Retain
//! start identities after reparenting and on disk for supervisor-crash recovery.
//!
//! A retained PID may later be reused by an unrelated process, including one
//! owned by another user whose libproc usage is denied. Such a PID is only
//! treated as gone when its public start time proves it is a different
//! process; anything that cannot be positively identified is neither signalled
//! nor reported as cleaned up.
use crate::native::{self, ProcessUsage, PublicProcess};
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Version 2 adds the public start time and real uid to each identity.
/// Version-1 journals still load; their identities carry no origin.
const JOURNAL_VERSION: u32 = 2;
/// Fork-time wall/abstime capture skew plus clock-slew allowance used only for
/// version-1 identities, which lack a recorded wall start.
const LEGACY_START_SLACK_US: u64 = 2_000_000;
/// Bound on the freeze phase of one kill pass; a member that never reports
/// stopped (or cannot be identified) must not stall cleanup indefinitely.
const FREEZE_LIMIT: Duration = Duration::from_secs(2);

/// Public identity facts captured at enrollment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Origin {
    /// Wall-clock start (KERN_PROC_PID p_starttime) in microseconds.
    pub start_us: u64,
    /// Real uid at enrollment.
    pub uid: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Identity {
    pub pid: i32,
    /// proc_pid_rusage start abstime.
    pub token: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<Origin>,
}
impl Identity {
    pub fn new(pid: i32, token: u64) -> Self {
        Self {
            pid,
            token,
            origin: None,
        }
    }
    fn same_start(&self, other: &Identity) -> bool {
        self.pid == other.pid && self.token == other.token
    }
}
impl std::fmt::Display for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pid {} (start token {}", self.pid, self.token)?;
        if let Some(o) = self.origin {
            write!(f, ", started {}us, uid {}", o.start_us, o.uid)?;
        }
        f.write_str(")")
    }
}

/// Process queries and signals. `Native` is the real system; tests inject
/// fixed answers to exercise reuse and permission cases deterministically.
pub trait Host {
    /// libproc usage with its bounded EPERM retry (see native::process_usage).
    fn usage(&self, pid: i32) -> io::Result<ProcessUsage>;
    /// Unprivileged KERN_PROC_PID view.
    fn public(&self, pid: i32) -> io::Result<PublicProcess>;
    fn status(&self, pid: i32) -> io::Result<u32>;
    fn group_pids(&self, group: i32) -> Result<Vec<i32>>;
    fn child_pids(&self, parent: i32) -> Result<Vec<i32>>;
    fn parent_pid(&self, pid: i32) -> io::Result<i32>;
    fn signal(&self, pid: i32, signal: i32) -> io::Result<()>;
    /// Latest wall start (us) a process with this start token can have.
    fn latest_start_us(&self, token: u64) -> Option<u64>;
    fn uid(&self) -> u32;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Native;
impl Host for Native {
    fn usage(&self, pid: i32) -> io::Result<ProcessUsage> {
        native::process_usage(pid)
    }
    fn public(&self, pid: i32) -> io::Result<PublicProcess> {
        native::public_process(pid)
    }
    fn status(&self, pid: i32) -> io::Result<u32> {
        native::process_status(pid)
    }
    fn group_pids(&self, group: i32) -> Result<Vec<i32>> {
        native::group_pids(group)
    }
    fn child_pids(&self, parent: i32) -> Result<Vec<i32>> {
        native::child_pids(parent)
    }
    fn parent_pid(&self, pid: i32) -> io::Result<i32> {
        native::parent_pid(pid)
    }
    fn signal(&self, pid: i32, signal: i32) -> io::Result<()> {
        // SAFETY: kill has no memory effects; callers identify the target first.
        if unsafe { libc::kill(pid, signal) } < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    fn latest_start_us(&self, token: u64) -> Option<u64> {
        native::latest_start_us(token)
    }
    fn uid(&self) -> u32 {
        // SAFETY: getuid has no arguments or memory effects.
        unsafe { libc::getuid() }
    }
}

/// A positively identified, still-running tracked process.
pub enum Found {
    Readable(ProcessUsage),
    /// Usage is denied (for example after a setuid exec) but the public start
    /// time and uid match the enrolled identity.
    Unreadable {
        status: u32,
    },
}

/// Ok(None): the tracked process has exited or its PID now belongs to a
/// provably different process. Err: identity cannot be decided either way.
pub fn lookup(host: &impl Host, id: Identity) -> Result<Option<Found>> {
    let denied = match host.usage(id.pid) {
        Ok(v) if v.token == id.token && v.live => return Ok(Some(Found::Readable(v))),
        Ok(_) => return Ok(None),
        Err(e) if e.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
        Err(e) if e.raw_os_error() == Some(libc::EPERM) => e,
        Err(e) => return Err(e).with_context(|| format!("read tracked process {id}")),
    };
    // A sustained denial usually means the PID was reused by another user's
    // process. Decide from the public start time, never from the denial.
    let public = match host.public(id.pid) {
        Ok(p) => p,
        Err(e) if e.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
        Err(e) => {
            return Err(e)
                .with_context(|| format!("read tracked process {id}: {denied}; public lookup"))
        }
    };
    if public.status == libc::SZOMB {
        return Ok(None);
    }
    match id.origin {
        Some(o) if public.start_us != o.start_us => Ok(None),
        Some(o) if public.uid == o.uid => Ok(Some(Found::Unreadable {
            status: public.status,
        })),
        Some(o) => bail!(
            "read tracked process {id}: {denied}; occupant has the same start time but real uid {} instead of {}, cannot positively identify",
            public.uid,
            o.uid
        ),
        None => {
            // Version-1 identity: only the abstime token is known. A start
            // after the latest possible original start proves replacement; a
            // foreign uid keeps a backwards clock step from inverting that.
            let latest = host.latest_start_us(id.token);
            if public.uid != host.uid()
                && latest.is_some_and(|l| public.start_us > l.saturating_add(LEGACY_START_SLACK_US))
            {
                return Ok(None);
            }
            bail!(
                "read tracked process {id}: {denied}; occupant (uid {}, started {}us, latest original start {}) cannot be positively identified",
                public.uid,
                public.start_us,
                latest.map_or("unknown".into(), |l| format!("{l}us"))
            )
        }
    }
}

/// Capture the public origin, then confirm the PID still holds the same
/// process so the two reads describe one process.
fn origin(host: &impl Host, pid: i32, token: u64) -> Option<Origin> {
    let public = host.public(pid).ok()?;
    matches!(host.usage(pid), Ok(v) if v.token == token).then_some(Origin {
        start_us: public.start_us,
        uid: public.uid,
    })
}

/// Identity of a process this caller just observed with `token`.
pub fn enroll(host: &impl Host, pid: i32, token: u64) -> Identity {
    Identity {
        pid,
        token,
        origin: origin(host, pid, token),
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    version: u32,
    root: Identity,
    members: Vec<Identity>,
}

pub struct ProcessTree<H: Host = Native> {
    host: H,
    root: Identity,
    members: BTreeMap<i32, Identity>,
    journal: PathBuf,
    root_exited: bool,
}

impl ProcessTree<Native> {
    pub fn new(dir: &Path, root: Identity) -> Result<Self> {
        Self::with_host(Native, dir, root)
    }
    pub fn load(dir: &Path, root: Identity) -> Result<Self> {
        Self::load_with(Native, dir, root)
    }
}

impl<H: Host> ProcessTree<H> {
    pub fn with_host(host: H, dir: &Path, root: Identity) -> Result<Self> {
        ensure!(root.pid > 1 && root.token != 0, "invalid root identity");
        let tree = Self {
            host,
            root,
            members: BTreeMap::from([(root.pid, root)]),
            journal: dir.join("processes.json"),
            root_exited: false,
        };
        tree.persist()?;
        Ok(tree)
    }

    /// `root` is the started.json identity (pid and token); the journal may
    /// add its origin.
    pub fn load_with(host: H, dir: &Path, root: Identity) -> Result<Self> {
        let journal = dir.join("processes.json");
        let value: Snapshot = serde_json::from_slice(&std::fs::read(&journal)?)?;
        ensure!(
            matches!(value.version, 1 | JOURNAL_VERSION),
            "unsupported process inventory version {}",
            value.version
        );
        ensure!(
            value.root.same_start(&root),
            "process inventory identity mismatch"
        );
        ensure!(
            value.members.len() <= 1_048_576,
            "process inventory overflow"
        );
        let mut members = BTreeMap::new();
        for member in value.members {
            ensure!(
                member.pid > 1 && member.token != 0,
                "invalid process identity"
            );
            ensure!(
                members.insert(member.pid, member).is_none(),
                "duplicate process identity"
            );
        }
        ensure!(
            members
                .get(&root.pid)
                .is_some_and(|m| m.same_start(&value.root)),
            "process inventory lost root"
        );
        Ok(Self {
            host,
            root: value.root,
            members,
            journal,
            root_exited: false,
        })
    }

    /// Only the owning supervisor's WNOWAIT observation grants this fact. The
    /// root PID remains pinned until wait4; its protected zombie needs no query.
    pub fn mark_root_exited(&mut self) {
        self.root_exited = true;
    }

    fn identities(&self) -> Vec<Identity> {
        self.members
            .values()
            .filter(|m| !self.root_exited || m.pid != self.root.pid)
            .copied()
            .collect()
    }

    fn persist(&self) -> Result<()> {
        native::atomic_json(
            &self.journal,
            &Snapshot {
                version: JOURNAL_VERSION,
                root: self.root,
                members: self.members.values().copied().collect(),
            },
        )
    }

    fn enroll(&mut self, pid: i32, token: u64) -> bool {
        if self.members.get(&pid).is_some_and(|m| m.token == token) {
            return false;
        }
        let identity = enroll(&self.host, pid, token);
        self.members.insert(pid, identity);
        true
    }

    /// Census descendants of every retained, still-matching parent. Also retain
    /// the original group to cover already orphaned ordinary background children.
    pub fn refresh(&mut self) -> Result<()> {
        let mut changed = false;
        let root_matches =
            matches!(self.host.usage(self.root.pid), Ok(v) if v.token == self.root.token);
        if self.root_exited || root_matches {
            for pid in self.host.group_pids(self.root.pid)? {
                if self.root_exited && pid == self.root.pid {
                    continue;
                }
                match self.host.usage(pid) {
                    Ok(v) if v.live => changed |= self.enroll(pid, v.token),
                    Ok(_) => {}
                    Err(e) if e.raw_os_error() == Some(libc::ESRCH) => {}
                    Err(e) => {
                        return Err(e).with_context(|| format!("read original group member {pid}"))
                    }
                }
            }
        }
        let mut pending = VecDeque::from(self.identities());
        let mut visited = BTreeSet::new();
        // An exited, reused or unidentifiable parent cannot grant ownership of
        // children. kill/has_live report unidentifiable members themselves.
        let owns = |host: &H, parent| matches!(lookup(host, parent), Ok(Some(_)));
        while let Some(parent) = pending.pop_front() {
            if !visited.insert(parent) || !owns(&self.host, parent) {
                continue;
            }
            let children = self.host.child_pids(parent.pid)?;
            if !owns(&self.host, parent) {
                continue;
            }
            for pid in children {
                let value = match self.host.usage(pid) {
                    Ok(v) => v,
                    Err(e) if e.raw_os_error() == Some(libc::ESRCH) => continue,
                    Err(e) => {
                        return Err(e)
                            .with_context(|| format!("read child {pid} of {}", parent.pid))
                    }
                };
                if !value.live || value.token < parent.token {
                    continue;
                }
                match self.host.parent_pid(pid) {
                    Ok(ppid) if ppid == parent.pid => {}
                    Ok(_) => continue,
                    Err(e) if e.raw_os_error() == Some(libc::ESRCH) => continue,
                    Err(e) => return Err(e).with_context(|| format!("read parent of child {pid}")),
                }
                if !owns(&self.host, parent) {
                    break;
                }
                changed |= self.enroll(pid, value.token);
                pending.push_back(self.members[&pid]);
                ensure!(
                    self.members.len() <= 1_048_576,
                    "process inventory overflow"
                );
            }
        }
        if changed {
            self.persist()?;
        }
        Ok(())
    }

    pub fn sample(&mut self) -> Result<(u64, u64, usize)> {
        self.refresh()?;
        let (mut rss, mut footprint, mut count) = (0u64, 0u64, 0usize);
        for identity in self.identities() {
            match lookup(&self.host, identity)? {
                Some(Found::Readable(value)) => {
                    rss = rss.saturating_add(value.rss);
                    footprint = footprint.saturating_add(value.footprint);
                    count += 1;
                }
                // Never turn a sustained denial into a zero-memory sample.
                Some(Found::Unreadable { .. }) => {
                    bail!("tracked process {identity} is live but its memory cannot be read")
                }
                None => {}
            }
        }
        Ok((rss, footprint, count))
    }

    /// Never signal an identity that disappeared, was replaced by a reused
    /// PID, or cannot be positively identified.
    fn signal(&self, id: Identity, signal: i32) -> Result<()> {
        if lookup(&self.host, id)?.is_none() {
            return Ok(());
        }
        match self.host.signal(id.pid, signal) {
            Ok(()) => Ok(()),
            Err(e) if e.raw_os_error() == Some(libc::ESRCH) => Ok(()),
            Err(e)
                if e.raw_os_error() == Some(libc::EPERM)
                    && matches!(lookup(&self.host, id), Ok(None)) =>
            {
                Ok(())
            }
            Err(e) => Err(e).with_context(|| format!("signal {signal} to {id}")),
        }
    }

    fn stopped_or_gone(&self, id: Identity) -> Result<bool> {
        match lookup(&self.host, id)? {
            None => Ok(true),
            Some(Found::Unreadable { status }) => Ok(status == libc::SSTOP),
            Some(Found::Readable(_)) => match self.host.status(id.pid) {
                Ok(status) => Ok(matches!(status, libc::SSTOP | libc::SZOMB)),
                Err(e) if e.raw_os_error() == Some(libc::ESRCH) => Ok(true),
                Err(e) => Err(e).with_context(|| format!("read status of {id}")),
            },
        }
    }

    /// Freeze parents before killing, then repeat discovery to close the
    /// cooperative fork-during-cancellation window. Signal only recorded IDs,
    /// never a detached PGID whose leader may have exited and been reused.
    /// Every identifiable member is killed even when others are unresolved;
    /// the error then names each unresolved identity.
    pub fn kill(&mut self) -> Result<()> {
        let deadline = Instant::now() + FREEZE_LIMIT;
        loop {
            let mut settled = self.refresh().is_ok();
            let before = self.members.clone();
            for identity in self.identities() {
                // Unresolved members are reported by the final pass below.
                let _ = self.signal(identity, libc::SIGSTOP);
            }
            settled &= self.refresh().is_ok();
            for identity in self.identities() {
                settled &= self.stopped_or_gone(identity).unwrap_or(true);
            }
            if (settled && self.members == before) || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let refreshed = self.refresh();
        let mut unresolved = Vec::new();
        for identity in self.identities().into_iter().rev() {
            if let Err(e) = self.signal(identity, libc::SIGKILL) {
                unresolved.push(format!("{e:#}"));
            }
        }
        if let Err(e) = refreshed {
            unresolved.push(format!("process discovery: {e:#}"));
        }
        if unresolved.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(
                "unresolved tracked processes: {}",
                unresolved.join("; ")
            ))
        }
    }

    pub fn has_live(&mut self) -> Result<bool> {
        self.refresh()?;
        let mut live = false;
        let mut unresolved = Vec::new();
        for identity in self.identities() {
            match lookup(&self.host, identity) {
                Ok(found) => live |= found.is_some(),
                Err(e) => unresolved.push(format!("{e:#}")),
            }
        }
        ensure!(
            unresolved.is_empty(),
            "unresolved tracked processes: {}",
            unresolved.join("; ")
        );
        Ok(live)
    }
}

#[cfg(test)]
pub mod fake {
    use super::*;
    use std::{
        cell::RefCell,
        collections::{BTreeMap, BTreeSet},
        rc::Rc,
    };

    #[derive(Clone, Copy, Debug)]
    pub enum Proc {
        /// Our own, readable process.
        Ours {
            token: u64,
            start_us: u64,
            parent: i32,
            group: i32,
        },
        /// Another user's process: usage is denied with EPERM.
        Foreign { start_us: u64, uid: u32 },
    }

    #[derive(Default)]
    pub struct State {
        pub procs: BTreeMap<i32, Proc>,
        pub stopped: BTreeSet<i32>,
        pub signals: Vec<(i32, i32)>,
        pub latest_start_us: Option<u64>,
    }

    pub const UID: u32 = 501;

    #[derive(Clone, Default)]
    pub struct FakeHost(pub Rc<RefCell<State>>);
    impl FakeHost {
        pub fn insert(&self, pid: i32, proc: Proc) {
            self.0.borrow_mut().procs.insert(pid, proc);
        }
        pub fn signals(&self) -> Vec<(i32, i32)> {
            self.0.borrow().signals.clone()
        }
    }
    fn esrch() -> io::Error {
        io::Error::from_raw_os_error(libc::ESRCH)
    }
    impl Host for FakeHost {
        fn usage(&self, pid: i32) -> io::Result<ProcessUsage> {
            match self.0.borrow().procs.get(&pid) {
                Some(Proc::Ours { token, .. }) => Ok(ProcessUsage {
                    token: *token,
                    rss: 10,
                    footprint: 20,
                    live: true,
                }),
                Some(Proc::Foreign { .. }) => Err(io::Error::from_raw_os_error(libc::EPERM)),
                None => Err(esrch()),
            }
        }
        fn public(&self, pid: i32) -> io::Result<PublicProcess> {
            let state = self.0.borrow();
            let status = if state.stopped.contains(&pid) {
                libc::SSTOP
            } else {
                libc::SRUN
            };
            match state.procs.get(&pid) {
                Some(Proc::Ours { start_us, .. }) => Ok(PublicProcess {
                    start_us: *start_us,
                    uid: UID,
                    status,
                }),
                Some(Proc::Foreign { start_us, uid }) => Ok(PublicProcess {
                    start_us: *start_us,
                    uid: *uid,
                    status,
                }),
                None => Err(esrch()),
            }
        }
        fn status(&self, pid: i32) -> io::Result<u32> {
            self.public(pid).map(|p| p.status)
        }
        fn group_pids(&self, group: i32) -> Result<Vec<i32>> {
            Ok(self
                .0
                .borrow()
                .procs
                .iter()
                .filter(|(_, p)| matches!(p, Proc::Ours { group: g, .. } if *g == group))
                .map(|(pid, _)| *pid)
                .collect())
        }
        fn child_pids(&self, parent: i32) -> Result<Vec<i32>> {
            Ok(self
                .0
                .borrow()
                .procs
                .iter()
                .filter(|(_, p)| matches!(p, Proc::Ours { parent: q, .. } if *q == parent))
                .map(|(pid, _)| *pid)
                .collect())
        }
        fn parent_pid(&self, pid: i32) -> io::Result<i32> {
            match self.0.borrow().procs.get(&pid) {
                Some(Proc::Ours { parent, .. }) => Ok(*parent),
                Some(Proc::Foreign { .. }) => Ok(1),
                None => Err(esrch()),
            }
        }
        fn signal(&self, pid: i32, signal: i32) -> io::Result<()> {
            let mut state = self.0.borrow_mut();
            match state.procs.get(&pid) {
                Some(Proc::Ours { .. }) => {}
                Some(Proc::Foreign { .. }) => {
                    return Err(io::Error::from_raw_os_error(libc::EPERM))
                }
                None => return Err(esrch()),
            }
            state.signals.push((pid, signal));
            match signal {
                libc::SIGSTOP => {
                    state.stopped.insert(pid);
                }
                libc::SIGKILL => {
                    state.procs.remove(&pid);
                    state.stopped.remove(&pid);
                }
                _ => {}
            }
            Ok(())
        }
        fn latest_start_us(&self, _token: u64) -> Option<u64> {
            self.0.borrow().latest_start_us
        }
        fn uid(&self) -> u32 {
            UID
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::{FakeHost, Proc, UID};
    use super::*;

    const ROOT: i32 = 1000;

    fn ours(token: u64, parent: i32) -> Proc {
        Proc::Ours {
            token,
            start_us: token * 10,
            parent,
            group: ROOT,
        }
    }

    /// Root plus two enrolled descendants, as in job 6faba084.
    fn tree(host: &FakeHost, dir: &Path) -> ProcessTree<FakeHost> {
        host.insert(ROOT, ours(100, 1));
        host.insert(2789, ours(200, ROOT));
        host.insert(2790, ours(300, 2789));
        let root = enroll(host, ROOT, 100);
        let mut tree = ProcessTree::with_host(host.clone(), dir, root).unwrap();
        tree.refresh().unwrap();
        assert_eq!(tree.members.len(), 3);
        assert!(tree.members.values().all(|m| m.origin.is_some()));
        tree
    }

    #[test]
    fn reused_pid_owned_by_root_is_replaced_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let host = FakeHost::default();
        let mut tree = tree(&host, dir.path());
        // Both descendants exit; launchd starts root-owned daemons on their PIDs.
        host.insert(
            2789,
            Proc::Foreign {
                start_us: 9_000_000,
                uid: 0,
            },
        );
        host.insert(
            2790,
            Proc::Foreign {
                start_us: 9_500_000,
                uid: 0,
            },
        );
        let id = tree.members[&2789];
        assert!(lookup(&host, id).unwrap().is_none());
        tree.kill().unwrap();
        assert!(!tree.has_live().unwrap());
        // The root was killed; the foreign occupants were never signalled.
        let signals = host.signals();
        assert!(signals.contains(&(ROOT, libc::SIGKILL)));
        assert!(signals.iter().all(|(pid, _)| *pid == ROOT));
    }

    #[test]
    fn same_start_different_uid_is_unresolved_and_never_signalled() {
        let dir = tempfile::tempdir().unwrap();
        let host = FakeHost::default();
        let mut tree = tree(&host, dir.path());
        let id = tree.members[&2789];
        host.insert(
            2789,
            Proc::Foreign {
                start_us: id.origin.unwrap().start_us,
                uid: 0,
            },
        );
        let error = tree.kill().unwrap_err().to_string();
        assert!(error.contains("pid 2789"), "{error}");
        assert!(tree.has_live().is_err());
        // Identifiable members are still killed despite the unresolved one.
        let signals = host.signals();
        assert!(signals.contains(&(2790, libc::SIGKILL)));
        assert!(!signals.iter().any(|(pid, _)| *pid == 2789));
    }

    #[test]
    fn identified_but_unreadable_member_is_live_and_signalled() {
        let dir = tempfile::tempdir().unwrap();
        let host = FakeHost::default();
        let tree = tree(&host, dir.path());
        let id = tree.members[&2790];
        // Same process after a setuid exec: usage denied, real uid unchanged.
        host.insert(
            2790,
            Proc::Foreign {
                start_us: id.origin.unwrap().start_us,
                uid: UID,
            },
        );
        assert!(matches!(
            lookup(&host, id).unwrap(),
            Some(Found::Unreadable { .. })
        ));
    }

    #[test]
    fn version_one_journal_loads_and_uses_conservative_start_bound() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("processes.json"),
            r#"{"version":1,"root":{"pid":1000,"token":100},"members":[{"pid":1000,"token":100},{"pid":2789,"token":200}]}"#,
        )
        .unwrap();
        let host = FakeHost::default();
        let tree =
            ProcessTree::load_with(host.clone(), dir.path(), Identity::new(ROOT, 100)).unwrap();
        let id = tree.members[&2789];
        assert_eq!(id.origin, None);
        host.0.borrow_mut().latest_start_us = Some(5_000_000);
        // Foreign occupant started well after the original could have.
        host.insert(
            2789,
            Proc::Foreign {
                start_us: 8_000_000,
                uid: 0,
            },
        );
        assert!(lookup(&host, id).unwrap().is_none());
        // Within the slack window, or with an unknown bound, refuse to decide.
        host.insert(
            2789,
            Proc::Foreign {
                start_us: 6_000_000,
                uid: 0,
            },
        );
        assert!(lookup(&host, id).is_err());
        host.0.borrow_mut().latest_start_us = None;
        assert!(lookup(&host, id).is_err());
        // Our own uid is never inferred to be a replacement from the bound.
        host.0.borrow_mut().latest_start_us = Some(5_000_000);
        host.insert(
            2789,
            Proc::Foreign {
                start_us: 8_000_000,
                uid: UID,
            },
        );
        assert!(lookup(&host, id).is_err());
        // Persisting upgrades the journal; it reloads with the same identities.
        tree.persist().unwrap();
        let raw = std::fs::read_to_string(dir.path().join("processes.json")).unwrap();
        assert!(raw.contains(r#""version":2"#), "{raw}");
        ProcessTree::load_with(host, dir.path(), Identity::new(ROOT, 100)).unwrap();
    }

    #[test]
    fn version_two_journal_round_trips_origins() {
        let dir = tempfile::tempdir().unwrap();
        let host = FakeHost::default();
        let tree = tree(&host, dir.path());
        let loaded =
            ProcessTree::load_with(host.clone(), dir.path(), Identity::new(ROOT, 100)).unwrap();
        assert_eq!(loaded.members, tree.members);
        assert_eq!(loaded.root.origin, tree.root.origin);
        assert!(
            ProcessTree::load_with(host, dir.path(), Identity::new(ROOT, 101)).is_err(),
            "token mismatch must not load"
        );
    }

    #[test]
    fn native_public_view_matches_libproc_for_this_process() {
        let pid = std::process::id() as i32;
        let public = native::public_process(pid).unwrap();
        // SAFETY: getuid has no arguments or memory effects.
        assert_eq!(public.uid, unsafe { libc::getuid() });
        let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
        let size = std::mem::size_of_val(&info) as i32;
        assert_eq!(
            unsafe {
                libc::proc_pidinfo(
                    pid,
                    libc::PROC_PIDTBSDINFO,
                    0,
                    (&mut info as *mut libc::proc_bsdinfo).cast(),
                    size,
                )
            },
            size
        );
        assert_eq!(
            public.start_us,
            info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec
        );
        assert_eq!(public.uid, info.pbi_ruid);
        // The rusage-token bound is never earlier than the real start.
        let token = native::token(pid).unwrap();
        let latest = native::latest_start_us(token).unwrap();
        assert!(latest + LEGACY_START_SLACK_US >= public.start_us);
        // Launchd is another user's process; its public view is still readable.
        assert_eq!(native::public_process(1).unwrap().uid, 0);
        let id = enroll(&Native, pid, token);
        assert_eq!(id.origin.map(|o| o.start_us), Some(public.start_us));
    }
}
