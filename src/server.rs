use crate::{config::Config, model::*, native, scheduler, store::Store};
use anyhow::{ensure, Context as _, Result};
use base64::Engine;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::{
            fs::{FileTypeExt, PermissionsExt},
            net::{UnixListener, UnixStream},
        },
    },
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const REQUEST_LIMIT: usize = 4 * 1024 * 1024;
struct Runner {
    child: Child,
    control: ChildStdin,
    events: ChildStdout,
    buffer: Vec<u8>,
    log_truncated: [bool; 2],
}
struct Peer {
    socket: UnixStream,
    input: Vec<u8>,
    output: Vec<u8>,
    sent: usize,
    handled: bool,
    born: Instant,
}
pub struct Server {
    cfg: Config,
    config_path: Option<PathBuf>,
    state: PathBuf,
    store: Store,
    jobs: Vec<Job>,
    runners: HashMap<String, Runner>,
    maintenance: MaintenanceMode,
    scheduling: Value,
}
impl Server {
    fn maintenance_status(&self) -> Value {
        let active = self.jobs.iter().filter(|j| j.active()).count();
        let queued = self.jobs.iter().filter(|j| j.queued()).count();
        let ready = self.maintenance != MaintenanceMode::Running
            && active == 0
            && self.runners.is_empty()
            && (self.maintenance == MaintenanceMode::Paused || queued == 0);
        json!({"mode":self.maintenance,"phase":if self.maintenance == MaintenanceMode::Running {"running"} else if ready {"drained"} else {"draining"},
            "admission_open":self.maintenance == MaintenanceMode::Running,
            "dispatch_open":self.maintenance != MaintenanceMode::Paused,
            "active_jobs":active,"active_runners":self.runners.len(),"queued_jobs":queued,
            "ready_to_restart":ready})
    }
    fn estimates(&self) -> HashMap<String, scheduler::Estimate> {
        if self.maintenance == MaintenanceMode::Paused {
            HashMap::new()
        } else {
            scheduler::estimates(&self.jobs, &self.cfg, self.now())
        }
    }
    /// All *_at_ms values are Unix wall-clock milliseconds. A sleep-excluding
    /// monotonic offset would drift behind started_at_ms across system sleep.
    fn now(&self) -> u64 {
        wall_ms()
    }
    fn dir(&self, id: &str) -> PathBuf {
        self.state.join("jobs").join(id)
    }
    fn index(&self, id: &str) -> Result<usize> {
        self.jobs
            .iter()
            .position(|j| j.id == id)
            .context("job not found")
    }
    fn snapshot(&self, id: &str) -> Result<Value> {
        let j = &self.jobs[self.index(id)?];
        let estimates = if j.terminal() {
            HashMap::new()
        } else {
            self.estimates()
        };
        let mut v = serde_json::to_value(j)?;
        v["snapshot_at_ms"] = json!(self.now());
        v["maintenance"] = self.maintenance_status();
        v["memory_metric"] = json!(native::MEMORY_METRIC);
        v["elapsed_ms"] = json!(j
            .outcome
            .as_ref()
            .map(|o| o.elapsed_ms)
            .or_else(|| j.started_at_ms.map(|s| self.now().saturating_sub(s))));
        v["estimate"] = serde_json::to_value(estimates.get(id))?;
        v["queue"] = json!(if j.emergency {
            "emergency".into()
        } else {
            format!("{}ms", j.queue_limit_ms)
        });
        v["logs"] = json!({"stdout_available":self.dir(id).join("stdout").exists(),"stderr_available":self.dir(id).join("stderr").exists()});
        Ok(v)
    }
    fn handle(&mut self, request: Request) -> Result<Value> {
        match request {
            Request::Submit {
                token,
                mut context,
                timeout_ms,
                max_memory_bytes,
                emergency,
                env_overrides,
            } => {
                ensure!(
                    !token.is_empty() && token.len() <= 256,
                    "token must be 1..256 bytes"
                );
                if let Some(id) = self.store.token(&token)? {
                    return self.snapshot(&id);
                }
                if self.maintenance != MaintenanceMode::Running {
                    return Err(MaintenanceError.into());
                }
                ensure!(
                    self.jobs.iter().filter(|j| j.queued()).count() < self.cfg.max_pending,
                    "pending queue capacity exceeded"
                );
                let timeout = timeout_ms.unwrap_or(self.cfg.default_timeout_ms);
                let limit = self.cfg.route(timeout)?;
                let memory = max_memory_bytes.unwrap_or(self.cfg.default_memory_bytes);
                ensure!(
                    memory > 0 && memory <= self.cfg.memory_cap_bytes,
                    "job memory must be positive and fit total memory budget"
                );
                validate_context(&context)?;
                for (k, v) in &env_overrides {
                    crate::config::validate_env(k, v)?;
                }
                context.env.extend(self.cfg.env.clone());
                context.env.extend(env_overrides);
                let mut j = Job {
                    id: uuid::Uuid::new_v4().to_string(),
                    sequence: 0,
                    state: "queued".into(),
                    emergency,
                    requested_timeout_ms: timeout,
                    queue_limit_ms: limit,
                    max_memory_bytes: memory,
                    submitted_at_ms: self.now(),
                    started_at_ms: None,
                    cancellation_requested: false,
                    stats: Stats::default(),
                    outcome: None,
                    helper_pid: None,
                    helper_token: None,
                    reservation_quarantined: false,
                };
                self.store.insert(&mut j, &token, &context)?;
                let id = j.id.clone();
                self.jobs.push(j);
                self.snapshot(&id)
            }
            Request::Status { id } => self.snapshot(&id),
            Request::Jobs { all, cursor, limit } => {
                let paged = cursor.is_some() || limit.is_some();
                let limit = if paged {
                    limit.unwrap_or(25)
                } else {
                    usize::MAX
                };
                ensure!(!paged || (1..=100).contains(&limit), "limit must be 1..100");
                let after = match cursor {
                    Some(cursor) => {
                        ensure!(
                            !cursor.is_empty()
                                && cursor.len() <= 20
                                && cursor.bytes().all(|c| c.is_ascii_digit()),
                            "invalid job cursor"
                        );
                        cursor.parse::<u64>().context("invalid job cursor")?
                    }
                    None => 0,
                };
                let estimates = self.estimates();
                let mut selected: Vec<_> = self
                    .jobs
                    .iter()
                    .filter(|j| j.sequence > after && (all || !j.terminal()))
                    .take(limit.saturating_add(1))
                    .collect();
                let more = selected.len() > limit;
                selected.truncate(limit);
                let next_cursor = more.then(|| selected.last().unwrap().sequence.to_string());
                let rows: Vec<_> = selected
                    .into_iter()
                    .map(|j| json!({"job":j,"estimate":estimates.get(&j.id)}))
                    .collect();
                let mut result = json!({"snapshot_at_ms":self.now(),"jobs":rows,"maintenance":self.maintenance_status()});
                if paged {
                    result["next_cursor"] = json!(next_cursor);
                }
                Ok(result)
            }
            Request::Queues {} => {
                let estimates = self.estimates();
                let mut limits = self.cfg.queues_ms.clone();
                for j in self.jobs.iter().filter(|j| j.active() && !j.emergency) {
                    if !limits.contains(&j.queue_limit_ms) {
                        limits.push(j.queue_limit_ms);
                    }
                }
                limits.sort_unstable();
                let queues:Vec<_>=limits.iter().map(|q|json!({"limit_ms":q,"accepting":self.maintenance == MaintenanceMode::Running && self.cfg.queues_ms.contains(q),
                    "queued":self.jobs.iter().filter(|j|!j.emergency&&j.queued()&&j.queue_limit_ms==*q).count(),
                    "running":self.jobs.iter().filter(|j|!j.emergency&&j.active()&&j.queue_limit_ms==*q).count(),
                    "estimate": queue_estimate(&self.jobs,&estimates,|j|!j.emergency&&j.queue_limit_ms==*q)})).collect();
                let reserved: u64 = self
                    .jobs
                    .iter()
                    .filter(|j| j.holds_reservation())
                    .map(|j| j.max_memory_bytes)
                    .sum();
                let quarantined: Vec<_> = self
                    .jobs
                    .iter()
                    .filter(|j| j.reservation_quarantined)
                    .map(|j| &j.id)
                    .collect();
                Ok(
                    json!({"snapshot_at_ms":self.now(),"config":self.cfg,"ordinary":queues,"maintenance":self.maintenance_status(),"scheduling":self.scheduling,"memory_metric":native::MEMORY_METRIC,
                    "emergency":{"queued":self.jobs.iter().filter(|j|j.emergency&&j.queued()).count(),"running":self.jobs.iter().filter(|j|j.emergency&&j.active()).count(),"estimate":queue_estimate(&self.jobs,&estimates,|j|j.emergency)},
                    "active_runners":self.runners.len(),"reserved_memory_bytes":reserved,"quarantined_jobs":quarantined,
                    "free_admission_memory_bytes":self.cfg.memory_cap_bytes.saturating_sub(reserved)}),
                )
            }
            Request::Drain { all } => {
                let mode = if all {
                    MaintenanceMode::FinishAccepted
                } else {
                    MaintenanceMode::Paused
                };
                // Commit before acknowledging or changing dispatch/admission.
                self.store.set_maintenance(mode)?;
                self.maintenance = mode;
                Ok(self.maintenance_status())
            }
            Request::Resume {} => {
                self.store.set_maintenance(MaintenanceMode::Running)?;
                self.maintenance = MaintenanceMode::Running;
                Ok(self.maintenance_status())
            }
            Request::Maintenance {} => Ok(self.maintenance_status()),
            Request::Cancel { id } => {
                let i = self.index(&id)?;
                let mut j = self.jobs[i].clone();
                if j.queued() {
                    j.cancellation_requested = true;
                    let mut o = Outcome::interrupted("withdrawn before execution");
                    o.state = "cancelled".into();
                    j.state = o.state.clone();
                    j.outcome = Some(o);
                    self.store.save(&j)?;
                    self.jobs[i] = j;
                    self.redact(&id);
                } else if j.active() {
                    j.cancellation_requested = true;
                    j.state = "cancelling".into();
                    self.store.save(&j)?;
                    self.jobs[i] = j;
                    if let Some(r) = self.runners.get_mut(&id) {
                        let _ = r.control.write_all(b"C");
                    }
                }
                self.snapshot(&id)
            }
            Request::Reload {} => {
                let cfg = Config::load(self.config_path.as_deref())?;
                let mut jobs = self.jobs.clone();
                reconcile(&mut jobs, &cfg)?;
                self.store.save_many(&jobs)?;
                self.jobs = jobs;
                self.cfg = cfg;
                self.cfg.warn();
                Ok(json!({"reloaded":true,"config":self.cfg}))
            }
            Request::Logs {
                id,
                stream,
                offset,
                max_bytes,
            } => {
                let max_bytes = max_bytes.unwrap_or(65536);
                ensure!(
                    (1..=65536).contains(&max_bytes),
                    "max_bytes must be 1..65536"
                );
                let j = &self.jobs[self.index(&id)?];
                ensure!(
                    matches!(stream.as_str(), "stdout" | "stderr"),
                    "stream must be stdout or stderr"
                );
                let path = self.dir(&id).join(&stream);
                let mut buf = Vec::new();
                let mut available_bytes = 0;
                if path.exists() {
                    let mut f = File::open(path)?;
                    available_bytes = f.metadata()?.len();
                    f.seek(SeekFrom::Start(offset))?;
                    // Read from one size snapshot even while the supervisor appends.
                    f.take(max_bytes.min(available_bytes.saturating_sub(offset)))
                        .read_to_end(&mut buf)?;
                }
                let truncated = j.outcome.as_ref().map_or_else(
                    || {
                        self.runners
                            .get(&id)
                            .is_some_and(|r| r.log_truncated[usize::from(stream == "stderr")])
                    },
                    |o| {
                        if stream == "stdout" {
                            o.stdout_truncated
                        } else {
                            o.stderr_truncated
                        }
                    },
                );
                Ok(
                    json!({"id":id,"stream":stream,"offset":offset,"next_offset":offset.saturating_add(buf.len() as u64),
                    "available_bytes":available_bytes,"has_more":offset.saturating_add(buf.len() as u64)<available_bytes,
                    "data_base64":base64::engine::general_purpose::STANDARD.encode(&buf),"terminal":j.terminal(),"truncated":truncated}),
                )
            }
        }
    }
    fn dispatch(&mut self) -> Result<()> {
        if self.maintenance == MaintenanceMode::Paused {
            return Ok(());
        }
        while self.runners.len() < self.cfg.runners {
            let reserved: u64 = self
                .jobs
                .iter()
                .filter(|j| j.holds_reservation())
                .map(|j| j.max_memory_bytes)
                .sum();
            let Some(i) = scheduler::select(
                &self.jobs,
                &self.cfg,
                self.now(),
                self.cfg.memory_cap_bytes.saturating_sub(reserved),
            ) else {
                break;
            };
            let id = self.jobs[i].id.clone();
            let mut j = self.jobs[i].clone();
            j.state = "starting".into();
            self.store.save(&j)?;
            self.jobs[i] = j;
            if let Err(e) = self.launch(i) {
                let mut j = self.jobs[i].clone();
                let mut o = Outcome::interrupted(&format!("launch failed: {e:#}"));
                o.state = "spawn_failed".into();
                j.state = o.state.clone();
                j.outcome = Some(o);
                self.store.save(&j)?;
                self.jobs[i] = j;
                self.redact(&id);
                eprintln!("job {id}: launch failed: {e:#}");
            }
        }
        Ok(())
    }
    fn launch(&mut self, i: usize) -> Result<()> {
        let j = &self.jobs[i];
        let dir = self.dir(&j.id);
        native::private_dir(&dir)?;
        let spec = RunSpec {
            id: j.id.clone(),
            context: self.store.context(&j.id)?,
            timeout_ms: j.queue_limit_ms,
            memory_bytes: j.max_memory_bytes,
            monitor_ms: self.cfg.monitor_ms,
            log_cap_bytes: self.cfg.log_cap_bytes,
        };
        native::atomic_json(&dir.join("spec.json"), &spec)?;
        let mut child = Command::new(std::env::current_exe()?)
            .arg("__supervise")
            .arg(&dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let pid = child.id() as i32;
        let mut control = child.stdin.take().context("missing control pipe")?;
        let events = child.stdout.take().context("missing event pipe")?;
        let setup = (|| -> Result<()> {
            let token = native::token(pid)?;
            let mut j = self.jobs[i].clone();
            j.helper_pid = Some(pid);
            j.helper_token = Some(token);
            self.store.save(&j)?;
            self.jobs[i] = j;
            native::nonblocking(events.as_raw_fd())?;
            native::nonblocking(control.as_raw_fd())?;
            control.write_all(b"G")?;
            Ok(())
        })();
        if let Err(e) = setup {
            drop(control);
            let _ = child.wait();
            return Err(e);
        }
        self.runners.insert(
            self.jobs[i].id.clone(),
            Runner {
                child,
                control,
                events,
                buffer: Vec::new(),
                log_truncated: [false; 2],
            },
        );
        Ok(())
    }
    fn reap(&mut self) -> Result<()> {
        let ids: Vec<_> = self.runners.keys().cloned().collect();
        for id in ids {
            let i = self.index(&id)?;
            let r = self.runners.get_mut(&id).unwrap();
            let mut buf = [0u8; 4096];
            for _ in 0..16 {
                match r.events.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => r.buffer.extend_from_slice(&buf[..n]),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e.into()),
                }
            }
            ensure!(r.buffer.len() < 65536, "supervisor event buffer overflow");
            while let Some(end) = r.buffer.iter().position(|b| *b == b'\n') {
                let line: Vec<_> = r.buffer.drain(..=end).collect();
                let ev: RunnerEvent = serde_json::from_slice(&line)?;
                match ev {
                    RunnerEvent::Started { at_ms, .. } => {
                        let mut j = self.jobs[i].clone();
                        j.started_at_ms = Some(at_ms);
                        if !j.cancellation_requested {
                            j.state = "running".into();
                        }
                        self.store.save(&j)?;
                        self.jobs[i] = j;
                    }
                    RunnerEvent::Sample {
                        stats,
                        stdout_truncated,
                        stderr_truncated,
                    } => {
                        self.jobs[i].stats = stats;
                        r.log_truncated = [stdout_truncated, stderr_truncated];
                    }
                }
            }
            if let Some(status) = r.child.try_wait()? {
                let mut quarantine = false;
                let outcome = match read_result(&self.dir(&id)) {
                    Ok(Some(outcome)) => outcome,
                    missing => {
                        // An unexpected supervisor failure is not a license to release live
                        // work. An unconfirmed cleanup holds the reservation, not the daemon.
                        let mut message = match missing {
                            Err(e) => {
                                format!("supervisor exited with invalid result ({e:#}): {status}")
                            }
                            _ => format!("supervisor exited without durable result: {status}"),
                        };
                        if let Err(e) = cleanup_unfinished(&self.dir(&id), RECOVERY_CLEANUP_LIMIT) {
                            eprintln!(
                                "job {id}: cleanup unresolved; reservation quarantined: {e:#}"
                            );
                            message.push_str(&format!("; {}", quarantine_message(&e)));
                            quarantine = true;
                        }
                        let mut o = Outcome::interrupted(&message);
                        o.state = "monitoring_failed".into();
                        o.elapsed_ms = self.jobs[i]
                            .started_at_ms
                            .map_or(0, |t| self.now().saturating_sub(t));
                        o.stats = self.jobs[i].stats.clone();
                        o
                    }
                };
                let mut j = self.jobs[i].clone();
                j.reservation_quarantined = quarantine;
                j.state = outcome.state.clone();
                j.stats = outcome.stats.clone();
                j.outcome = Some(outcome);
                j.helper_pid = None;
                j.helper_token = None;
                self.store.save(&j)?;
                self.jobs[i] = j;
                self.runners.remove(&id);
                self.redact(&id);
            }
        }
        Ok(())
    }
    fn prune(&mut self) -> Result<()> {
        let cutoff = self.now().saturating_sub(self.cfg.retention_ms);
        let expired: Vec<_> = self
            .jobs
            .iter()
            .filter(|j| {
                // A quarantined job's process inventory is still needed.
                !j.reservation_quarantined
                    && j.outcome
                        .as_ref()
                        .is_some_and(|o| o.finished_at_ms < cutoff)
            })
            .map(|j| j.id.clone())
            .collect();
        for id in expired {
            let dir = self.dir(&id);
            if dir.exists() {
                std::fs::remove_dir_all(dir)?;
            }
            self.store.delete(&id)?;
            self.jobs.retain(|j| j.id != id);
        }
        Ok(())
    }
}

impl Server {
    /// Call whenever a job becomes terminal.
    fn redact(&self, id: &str) {
        redact_job(&self.store, &self.dir(id), id);
    }
    fn checkpoint(&self) {
        if let Err(e) = self.store.checkpoint() {
            eprintln!("database checkpoint failed: {e:#}");
        }
    }
    /// Retry cleanup of quarantined jobs once each (no waiting) and release a
    /// reservation only when the tree is confirmed gone.
    fn retry_quarantined(&mut self) -> Result<()> {
        for i in 0..self.jobs.len() {
            let j = &self.jobs[i];
            if !j.reservation_quarantined || self.runners.contains_key(&j.id) {
                continue;
            }
            if let (Some(pid), Some(token)) = (j.helper_pid, j.helper_token) {
                if native::same_process(pid, token) && native::is_live(pid) {
                    continue;
                }
            }
            if cleanup_unfinished(&self.dir(&j.id), Duration::ZERO).is_ok() {
                let mut j = j.clone();
                eprintln!("job {}: cleanup confirmed; reservation released", j.id);
                j.reservation_quarantined = false;
                j.helper_pid = None;
                j.helper_token = None;
                self.store.save(&j)?;
                self.jobs[i] = j;
            }
        }
        Ok(())
    }
}

fn queue_estimate(
    jobs: &[Job],
    estimates: &HashMap<String, scheduler::Estimate>,
    belongs: impl Fn(&Job) -> bool,
) -> Value {
    let queued_head = jobs
        .iter()
        .filter(|j| j.queued() && belongs(j))
        .min_by_key(|j| j.sequence);
    let last_historical = jobs
        .iter()
        .filter(|j| !j.terminal() && belongs(j))
        .filter_map(|j| estimates.get(&j.id).map(|e| e.historical.finish_at_ms))
        .max();
    let last_budget = jobs
        .iter()
        .filter(|j| !j.terminal() && belongs(j))
        .filter_map(|j| estimates.get(&j.id).map(|e| e.budget.finish_at_ms))
        .max();
    json!({"next_queued_job":queued_head.map(|j|&j.id),
        "next_start_historical_at_ms":queued_head.and_then(|j|estimates.get(&j.id).map(|e|e.historical.start_at_ms)),
        "last_finish_historical_at_ms":last_historical,"last_finish_budget_at_ms":last_budget,
        "assumptions":"No future submissions, cancellations, configuration changes, or cleanup delays; not a guarantee."})
}

pub fn reconcile(jobs: &mut [Job], cfg: &Config) -> Result<()> {
    for j in jobs.iter_mut().filter(|j| j.queued()) {
        ensure!(
            j.max_memory_bytes <= cfg.memory_cap_bytes,
            "queued job {} exceeds new memory budget",
            j.id
        );
        if !cfg.queues_ms.contains(&j.queue_limit_ms) {
            j.queue_limit_ms = cfg
                .route(j.requested_timeout_ms)
                .with_context(|| format!("cannot reassign queued job {}", j.id))?;
        }
    }
    Ok(())
}
fn validate_context(ctx: &Context) -> Result<()> {
    ensure!(ctx.umask.is_none_or(|m| m <= 0o777), "invalid umask");
    ensure!(
        Path::new(&ctx.cwd).is_absolute() && Path::new(&ctx.cwd).is_dir(),
        "cwd must be an existing absolute directory"
    );
    match &ctx.command {
        CommandSpec::Exec { argv } => ensure!(
            !argv.is_empty() && !argv[0].is_empty() && argv.iter().all(|a| !a.contains('\0')),
            "invalid argv"
        ),
        CommandSpec::Shell { command, shell } => ensure!(
            !command.is_empty()
                && !command.contains('\0')
                && Path::new(shell).is_absolute()
                && !shell.contains('\0'),
            "shell must be absolute; command must be nonempty and NUL-free"
        ),
    }
    for (k, v) in &ctx.env {
        crate::config::validate_env(k, v)?;
    }
    Ok(())
}
/// Startup and reap-time bound for confirming an orphaned tree is gone.
const RECOVERY_CLEANUP_LIMIT: Duration = Duration::from_secs(5);

fn quarantine_message(e: &anyhow::Error) -> String {
    format!("cleanup unresolved: {e:#}; memory reservation held until a later cleanup confirms the process tree is gone")
}
fn read_result(dir: &Path) -> Result<Option<Outcome>> {
    match std::fs::read(dir.join("result.json")) {
        Ok(raw) => Ok(Some(
            serde_json::from_slice(&raw).context("invalid supervisor result")?,
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).context("read supervisor result"),
    }
}
fn cleanup_unfinished(dir: &Path, limit: Duration) -> Result<()> {
    cleanup_unfinished_with(crate::process_tree::Native, dir, limit)
}
/// Kill and confirm an unsupervised tree. Retries until `limit`; an error
/// names what could not be confirmed and means the reservation must be held.
fn cleanup_unfinished_with<H: crate::process_tree::Host>(
    host: H,
    dir: &Path,
    limit: Duration,
) -> Result<()> {
    let path = dir.join("started.json");
    if path.exists() {
        let start: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        let pid = start["pid"].as_i64().context("missing command pid")? as i32;
        let token = start["token"]
            .as_u64()
            .context("missing command identity")?;
        if start["process_tree"] == true {
            let mut tree = crate::process_tree::ProcessTree::load_with(
                host,
                dir,
                crate::process_tree::Identity::new(pid, token),
            )
            .context("load process inventory")?;
            let deadline = Instant::now() + limit;
            loop {
                let error = match tree.kill().and_then(|()| tree.has_live()) {
                    Ok(false) => return Ok(()),
                    Ok(true) => anyhow::anyhow!("live tracked processes remain"),
                    Err(e) => e,
                };
                if Instant::now() >= deadline {
                    return Err(error.context(format!(
                        "unresolved process tree {pid}; refusing to release reservation until cleanup is confirmed"
                    )));
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        if native::same_process(pid, token) {
            native::kill_group(pid)?;
        }
        let deadline = Instant::now() + limit;
        while native::group_has_live(pid, None)? {
            ensure!(Instant::now()<deadline,"unresolved live group {pid}; refusing to release reservation until cleanup is confirmed");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    Ok(())
}
/// Replace a terminal job's environment values in the database and spec.json,
/// keeping the names. Nothing can still need them: the supervisor reads
/// spec.json before its launch gate, which only `launch` opens, and `__exec`
/// inherits its environment from the supervisor rather than rereading
/// spec.json. Failures are logged; privacy cleanup must not stop the daemon.
fn redact_job(store: &Store, dir: &Path, id: &str) {
    let result = store.redact_context(id).and_then(|()| {
        let path = dir.join("spec.json");
        let raw = match std::fs::read(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        let mut spec: RunSpec = serde_json::from_slice(&raw)?;
        if spec.context.redact_env() {
            native::atomic_json(&path, &spec)?;
        }
        Ok(())
    });
    if let Err(e) = result {
        eprintln!("job {id}: environment redaction failed: {e:#}");
    }
}
/// Startup sweep: also covers jobs that finished under a daemon without redaction.
fn redact_finished(store: &Store, state: &Path, jobs: &[Job]) {
    for j in jobs.iter().filter(|j| j.terminal()) {
        redact_job(store, &state.join("jobs").join(&j.id), &j.id);
    }
    if let Err(e) = store.checkpoint() {
        eprintln!("database checkpoint failed: {e:#}");
    }
}
/// Settle one job left active by a previous daemon.
fn recover_job<H: crate::process_tree::Host>(
    host: H,
    j: &Job,
    dir: &Path,
    limit: Duration,
) -> Result<Outcome> {
    if let (Some(pid), Some(token)) = (j.helper_pid, j.helper_token) {
        let deadline = Instant::now() + limit;
        while native::same_process(pid, token) && native::is_live(pid) {
            ensure!(
                Instant::now() < deadline,
                "previous supervisor {pid} still running; its cleanup is not confirmed"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    let result = read_result(dir);
    if let Ok(Some(outcome)) = result {
        return Ok(outcome);
    }
    cleanup_unfinished_with(host, dir, limit)?;
    Ok(Outcome::interrupted(&match result {
        Err(e) => format!("execution may have started before daemon interruption; not rerun; supervisor result unusable: {e:#}"),
        _ => "execution may have started before daemon interruption; not rerun".into(),
    }))
}
/// One job's unconfirmed cleanup must not stop the daemon: that job becomes
/// terminal with its reservation quarantined and startup continues.
fn recover<H: crate::process_tree::Host + Clone>(
    host: &H,
    store: &Store,
    jobs: &mut [Job],
    state: &Path,
    limit: Duration,
) -> Result<()> {
    for j in jobs.iter_mut().filter(|j| j.active()) {
        let dir = state.join("jobs").join(&j.id);
        let (outcome, helper) = match recover_job(host.clone(), j, &dir, limit) {
            Ok(outcome) => (outcome, (None, None)),
            Err(e) => {
                eprintln!(
                    "job {}: recovery cleanup unresolved; reservation quarantined: {e:#}",
                    j.id
                );
                j.reservation_quarantined = true;
                // Keep a live previous supervisor's identity so retries wait for it.
                (
                    Outcome::interrupted(&quarantine_message(&e)),
                    (j.helper_pid, j.helper_token),
                )
            }
        };
        j.state = outcome.state.clone();
        j.stats = outcome.stats.clone();
        j.outcome = Some(outcome);
        (j.helper_pid, j.helper_token) = helper;
        store.save(j)?;
    }
    Ok(())
}

pub fn run(
    config_path: Option<PathBuf>,
    state: PathBuf,
    socket: PathBuf,
    start_paused: bool,
) -> Result<()> {
    native::private_dir(&native::runtime_dir())?;
    let _lock = native::lock(&native::runtime_dir().join("daemon.lock"))?;
    native::private_dir(&state)?;
    let state = std::fs::canonicalize(state)?;
    native::private_dir(&state.join("jobs"))?;
    let parent = socket.parent().context("socket needs a parent")?;
    ensure!(
        parent.is_dir() && std::fs::metadata(parent)?.permissions().mode() & 0o077 == 0,
        "socket parent must be a private directory"
    );
    if socket.exists() {
        ensure!(
            std::fs::symlink_metadata(&socket)?.file_type().is_socket(),
            "refusing to replace non-socket path"
        );
        std::fs::remove_file(&socket)?;
    }
    let listener = UnixListener::bind(&socket)?;
    listener.set_nonblocking(true)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    struct SocketGuard(PathBuf);
    impl Drop for SocketGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _socket_guard = SocketGuard(socket.clone());
    let cfg = Config::load(config_path.as_deref())?;
    cfg.warn();
    let mut store = Store::open(&state.join("jobs.sqlite"))?;
    if start_paused {
        store.set_maintenance(MaintenanceMode::Paused)?;
    }
    let maintenance = store.maintenance()?;
    let mut jobs = store.jobs()?;
    recover(
        &crate::process_tree::Native,
        &store,
        &mut jobs,
        &state,
        RECOVERY_CLEANUP_LIMIT,
    )?;
    reconcile(&mut jobs, &cfg)?;
    store.save_many(&jobs)?;
    redact_finished(&store, &state, &jobs);
    let mut s = Server {
        cfg,
        config_path,
        state,
        store,
        jobs,
        runners: HashMap::new(),
        maintenance,
        scheduling: native::scheduling_diagnostics(),
    };
    let stop = Arc::new(AtomicBool::new(false));
    for sig in [libc::SIGINT, libc::SIGTERM] {
        signal_hook::flag::register(sig, stop.clone())?;
    }
    let mut peers = Vec::<Peer>::new();
    let mut next_prune = Instant::now();
    let mut stopping = false;
    eprintln!(
        "jobq ready: {} ({} runners, {} bytes budget)",
        socket.display(),
        s.cfg.runners,
        s.cfg.memory_cap_bytes
    );
    loop {
        if stop.load(Ordering::Relaxed) && !stopping {
            stopping = true;
            for r in s.runners.values_mut() {
                let _ = r.control.write_all(b"S");
            }
        }
        s.reap()?;
        if stopping {
            if s.runners.is_empty() {
                break;
            }
        } else {
            s.dispatch()?;
        }
        if Instant::now() >= next_prune {
            s.prune()?;
            s.retry_quarantined()?;
            s.checkpoint();
            next_prune = Instant::now() + Duration::from_secs(60);
        }
        if !stopping {
            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if peers.len() >= 128 {
                            drop(stream);
                            continue;
                        }
                        let uid = native::peer_uid(stream.as_raw_fd())?;
                        if uid != unsafe { libc::geteuid() } {
                            continue;
                        }
                        stream.set_nonblocking(true)?;
                        peers.push(Peer {
                            socket: stream,
                            input: Vec::new(),
                            output: Vec::new(),
                            sent: 0,
                            handled: false,
                            born: Instant::now(),
                        });
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e.into()),
                }
            }
        }
        let mut keep = Vec::new();
        for mut p in peers.drain(..) {
            if p.born.elapsed() > Duration::from_secs(10) {
                continue;
            }
            let mut closed = false;
            if !p.handled {
                let mut buf = [0u8; 16384];
                for _ in 0..16 {
                    match p.socket.read(&mut buf) {
                        Ok(0) => {
                            closed = true;
                            break;
                        }
                        Ok(n) => {
                            p.input.extend_from_slice(&buf[..n]);
                            if p.input.contains(&b'\n') || p.input.len() > REQUEST_LIMIT {
                                break;
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(_) => {
                            closed = true;
                            break;
                        }
                    }
                }
                let response = if p.input.len() > REQUEST_LIMIT {
                    Some(Response::error("too_large", "request exceeds 4MiB"))
                } else if let Some(end) = p.input.iter().position(|b| *b == b'\n') {
                    Some(match serde_json::from_slice::<Envelope>(&p.input[..end]) {
                        Ok(e) if e.version != 1 => {
                            Response::error("version_mismatch", "supported version is 1")
                        }
                        Ok(e) => match s.handle(e.request) {
                            Ok(v) => Response::success(v),
                            Err(e) => {
                                if e.chain().any(|e| e.is::<rusqlite::Error>()) {
                                    return Err(e);
                                }
                                let code = if e.is::<MaintenanceError>() {
                                    "maintenance"
                                } else {
                                    "request_failed"
                                };
                                Response::error(code, format!("{e:#}"))
                            }
                        },
                        Err(e) => Response::error("invalid_request", e.to_string()),
                    })
                } else {
                    None
                };
                if let Some(response) = response {
                    p.output = serde_json::to_vec(&response)?;
                    p.output.push(b'\n');
                    p.input.clear();
                    p.handled = true;
                }
            }
            if p.handled {
                match p.socket.write(&p.output[p.sent..]) {
                    Ok(0) => closed = true,
                    Ok(n) => p.sent += n,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => closed = true,
                }
                if p.sent == p.output.len() {
                    closed = true;
                }
            }
            if !closed {
                keep.push(p);
            }
        }
        peers = keep;
        if !stopping {
            s.dispatch()?;
        }
        // Sleep in the kernel. Active-job sampling/deadlines happen in supervisors.
        let mut fds = vec![libc::pollfd {
            fd: if stopping { -1 } else { listener.as_raw_fd() },
            events: libc::POLLIN,
            revents: 0,
        }];
        for p in &peers {
            fds.push(libc::pollfd {
                fd: p.socket.as_raw_fd(),
                events: if p.handled {
                    libc::POLLOUT
                } else {
                    libc::POLLIN
                },
                revents: 0,
            });
        }
        for r in s.runners.values() {
            fds.push(libc::pollfd {
                fd: r.events.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            });
        }
        unsafe {
            libc::poll(
                fds.as_mut_ptr(),
                fds.len() as _,
                if peers.is_empty() { 1000 } else { 20 },
            )
        };
    }
    Ok(())
}

#[cfg(test)]
mod maintenance_tests {
    use super::*;

    fn server(dir: &Path) -> Server {
        Server {
            cfg: Config::resolve(Default::default(), 2, 8 << 30).unwrap(),
            config_path: None,
            state: dir.to_path_buf(),
            store: Store::open(&dir.join("jobs.sqlite")).unwrap(),
            jobs: vec![],
            runners: HashMap::new(),
            maintenance: MaintenanceMode::Running,
            scheduling: json!({}),
        }
    }
    fn submit(dir: &Path, token: &str) -> Request {
        Request::Submit {
            token: token.into(),
            context: crate::model::Context {
                command: CommandSpec::Exec {
                    argv: vec!["/usr/bin/true".into()],
                },
                cwd: dir.to_str().unwrap().into(),
                env: Default::default(),
                umask: None,
            },
            timeout_ms: Some(1000),
            max_memory_bytes: Some(1024),
            emergency: false,
            env_overrides: Default::default(),
        }
    }
    #[test]
    fn drain_closes_admission_and_dispatch_but_preserves_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = server(tmp.path());
        let first = s.handle(submit(tmp.path(), "t")).unwrap();
        let m = s.handle(Request::Drain { all: false }).unwrap();
        assert_eq!(m["ready_to_restart"], true);
        assert_eq!(m["queued_jobs"], 1);
        assert_eq!(s.store.maintenance().unwrap(), MaintenanceMode::Paused);
        s.dispatch().unwrap();
        assert_eq!(s.jobs[0].state, "queued");
        assert!(s.runners.is_empty());
        assert_eq!(
            s.handle(submit(tmp.path(), "t")).unwrap()["id"],
            first["id"]
        );
        assert!(s
            .handle(submit(tmp.path(), "new"))
            .unwrap_err()
            .is::<MaintenanceError>());
        assert_eq!(s.store.jobs().unwrap().len(), 1);
        assert!(s.snapshot(first["id"].as_str().unwrap()).unwrap()["estimate"].is_null());
        s.handle(Request::Drain { all: true }).unwrap();
        assert_eq!(s.maintenance_status()["ready_to_restart"], false);
        s.jobs[0].state = "running".into();
        s.handle(Request::Drain { all: false }).unwrap();
        assert_eq!(s.maintenance_status()["ready_to_restart"], false);
        s.jobs[0].state = "succeeded".into();
        assert_eq!(s.maintenance_status()["ready_to_restart"], true);
    }

    fn submit_with_secret(dir: &Path, token: &str) -> Request {
        let Request::Submit {
            token,
            mut context,
            timeout_ms,
            max_memory_bytes,
            emergency,
            ..
        } = submit(dir, token)
        else {
            unreachable!()
        };
        context
            .env
            .insert("API_KEY".into(), "exported-secret".into());
        Request::Submit {
            token,
            context,
            timeout_ms,
            max_memory_bytes,
            emergency,
            env_overrides: [("OVERRIDE_KEY".into(), "override-secret".into())].into(),
        }
    }
    fn assert_redacted(ctx: &crate::model::Context) {
        assert!(ctx.env.contains_key("API_KEY") && ctx.env.contains_key("OVERRIDE_KEY"));
        assert!(
            ctx.env.values().all(|v| v == REDACTED_ENV_VALUE),
            "{:?}",
            ctx.env
        );
    }

    #[test]
    fn queued_env_is_kept_until_cancel_before_run_redacts_it() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = server(tmp.path());
        let id = s.handle(submit_with_secret(tmp.path(), "t")).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            s.store.context(&id).unwrap().env["API_KEY"],
            "exported-secret"
        );
        s.handle(Request::Cancel { id: id.clone() }).unwrap();
        assert_eq!(s.jobs[0].state, "cancelled");
        assert_redacted(&s.store.context(&id).unwrap());
    }

    #[test]
    fn launch_failure_redacts_env() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = server(tmp.path());
        native::private_dir(&tmp.path().join("jobs")).unwrap();
        let id = s.handle(submit_with_secret(tmp.path(), "t")).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        // A file where the job directory belongs fails launch before any spawn.
        std::fs::write(s.dir(&id), "not a directory").unwrap();
        s.dispatch().unwrap();
        assert_eq!(s.jobs[0].state, "spawn_failed");
        assert!(s.runners.is_empty());
        assert_redacted(&s.store.context(&id).unwrap());
    }

    /// Jobs finished under a daemon without redaction are scrubbed at startup;
    /// unfinished jobs keep the env their launch still needs.
    #[test]
    fn startup_sweep_redacts_only_terminal_jobs() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = server(tmp.path());
        native::private_dir(&tmp.path().join("jobs")).unwrap();
        let mut ids = vec![];
        for token in ["done", "running", "queued"] {
            let id = s.handle(submit_with_secret(tmp.path(), token)).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_string();
            let dir = s.dir(&id);
            native::private_dir(&dir).unwrap();
            let spec = RunSpec {
                id: id.clone(),
                context: s.store.context(&id).unwrap(),
                timeout_ms: 1000,
                memory_bytes: 1024,
                monitor_ms: 100,
                log_cap_bytes: 1024,
            };
            native::atomic_json(&dir.join("spec.json"), &spec).unwrap();
            ids.push(id);
        }
        s.jobs[0].state = "succeeded".into();
        s.jobs[0].outcome = Some(Outcome::interrupted("fixture"));
        s.jobs[1].state = "running".into();
        let spec = |id: &str| -> RunSpec {
            serde_json::from_slice(&std::fs::read(s.dir(id).join("spec.json")).unwrap()).unwrap()
        };

        redact_finished(&s.store, &s.state, &s.jobs);

        assert_redacted(&s.store.context(&ids[0]).unwrap());
        assert_redacted(&spec(&ids[0]).context);
        for id in &ids[1..] {
            assert_eq!(
                s.store.context(id).unwrap().env["API_KEY"],
                "exported-secret"
            );
            assert_eq!(spec(id).context.env["OVERRIDE_KEY"], "override-secret");
        }
        // A missing spec.json (never launched) is not an error.
        std::fs::remove_file(s.dir(&ids[0]).join("spec.json")).unwrap();
        redact_job(&s.store, &s.dir(&ids[0]), &ids[0]);
    }

    #[test]
    fn failed_persistence_cannot_resume_admission() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = server(tmp.path());
        s.handle(Request::Drain { all: false }).unwrap();
        rusqlite::Connection::open(tmp.path().join("jobs.sqlite"))
            .unwrap()
            .execute("DROP TABLE service_state", [])
            .unwrap();
        assert!(s.handle(Request::Resume {}).is_err());
        assert_eq!(s.maintenance, MaintenanceMode::Paused);
        assert!(s
            .handle(submit(tmp.path(), "new"))
            .unwrap_err()
            .is::<MaintenanceError>());
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::process_tree::fake::{FakeHost, Proc};

    fn job(store: &mut Store, dir: &Path, id: &str) -> Job {
        let mut j = Job {
            id: id.into(),
            sequence: 0,
            state: "running".into(),
            emergency: false,
            requested_timeout_ms: 1000,
            queue_limit_ms: 1000,
            max_memory_bytes: 1 << 30,
            submitted_at_ms: 0,
            started_at_ms: Some(0),
            cancellation_requested: false,
            stats: Default::default(),
            outcome: None,
            helper_pid: None,
            helper_token: None,
            reservation_quarantined: false,
        };
        let ctx = crate::model::Context {
            command: CommandSpec::Exec {
                argv: vec!["/usr/bin/true".into()],
            },
            cwd: dir.to_str().unwrap().into(),
            env: Default::default(),
            umask: None,
        };
        store.insert(&mut j, id, &ctx).unwrap();
        native::private_dir(&dir.join("jobs").join(id)).unwrap();
        j
    }
    fn started(dir: &Path, id: &str, journal: &str) {
        let job = dir.join("jobs").join(id);
        std::fs::write(
            job.join("started.json"),
            r#"{"pid":1000,"token":100,"process_tree":true}"#,
        )
        .unwrap();
        std::fs::write(job.join("processes.json"), journal).unwrap();
    }

    /// The 2026-10-20 crash loop: one job's tracked PIDs were reused by
    /// root-owned daemons and every daemon start exited 2.
    #[test]
    fn one_unresolved_job_is_quarantined_and_startup_continues() {
        let tmp = tempfile::tempdir().unwrap();
        native::private_dir(&tmp.path().join("state")).unwrap();
        let state = tmp.path().join("state");
        native::private_dir(&state.join("jobs")).unwrap();
        let mut store = Store::open(&state.join("jobs.sqlite")).unwrap();
        let mut jobs = vec![
            job(&mut store, &state, "stuck"),
            job(&mut store, &state, "replaced"),
            job(&mut store, &state, "never-started"),
        ];
        // Version-1 identities and no usable start bound: undecidable.
        started(
            &state,
            "stuck",
            r#"{"version":1,"root":{"pid":1000,"token":100},"members":[{"pid":1000,"token":100},{"pid":2789,"token":200}]}"#,
        );
        // Version-2 identity whose PID now holds a later-started root daemon.
        started(
            &state,
            "replaced",
            r#"{"version":2,"root":{"pid":1000,"token":100},"members":[{"pid":1000,"token":100},{"pid":2790,"token":300,"origin":{"start_us":3000,"uid":501}}]}"#,
        );
        let host = FakeHost::default();
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

        recover(&host, &store, &mut jobs, &state, Duration::ZERO).unwrap();

        let stuck = &jobs[0];
        assert_eq!(stuck.state, "interrupted");
        assert!(stuck.terminal() && stuck.holds_reservation());
        let message = stuck.outcome.as_ref().unwrap().message.clone().unwrap();
        assert!(message.contains("cleanup unresolved"), "{message}");
        assert!(message.contains("pid 2789"), "{message}");
        for j in &jobs[1..] {
            assert_eq!(j.state, "interrupted");
            assert!(!j.holds_reservation(), "{}", j.id);
        }
        assert!(
            host.signals().is_empty(),
            "foreign processes were signalled"
        );
        // Durable: a restarted daemon sees the same terminal, quarantined job.
        let reloaded = store.jobs().unwrap();
        assert!(reloaded[0].reservation_quarantined && reloaded[0].terminal());
        assert!(!reloaded[1].reservation_quarantined);
    }

    #[test]
    fn quarantine_releases_only_after_confirmed_cleanup_and_blocks_prune() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path().to_path_buf();
        native::private_dir(&state.join("jobs")).unwrap();
        let mut store = Store::open(&state.join("jobs.sqlite")).unwrap();
        let mut j = job(&mut store, &state, "q");
        j.state = "interrupted".into();
        let mut o = Outcome::interrupted("fixture");
        o.finished_at_ms = 0;
        j.outcome = Some(o);
        j.reservation_quarantined = true;
        store.save(&j).unwrap();
        let mut s = Server {
            cfg: Config::resolve(Default::default(), 2, 8 << 30).unwrap(),
            config_path: None,
            state: state.clone(),
            store,
            jobs: vec![j],
            runners: HashMap::new(),
            maintenance: MaintenanceMode::Running,
            scheduling: json!({}),
        };
        s.prune().unwrap();
        assert_eq!(s.jobs.len(), 1, "quarantined job must not be pruned");
        // No started.json: nothing ever ran, so cleanup is confirmed.
        s.retry_quarantined().unwrap();
        assert!(!s.jobs[0].reservation_quarantined);
        assert!(!s.store.jobs().unwrap()[0].reservation_quarantined);
    }
}
