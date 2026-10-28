use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceMode {
    #[default]
    Running,
    Paused,
    FinishAccepted,
}

#[derive(Debug)]
pub struct MaintenanceError;
impl std::fmt::Display for MaintenanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("admission closed for maintenance; this submission was not accepted; retry the same token after resume")
    }
}
impl std::error::Error for MaintenanceError {}

pub fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommandSpec {
    Exec { argv: Vec<String> },
    Shell { command: String, shell: String },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Context {
    pub command: CommandSpec,
    pub cwd: String,
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub umask: Option<u32>,
}
/// Stands in for an environment value removed from a finished job's records.
pub const REDACTED_ENV_VALUE: &str = "<redacted>";
impl Context {
    /// Replace every environment value, keeping the names. Returns whether
    /// anything changed, so already-redacted records are not rewritten.
    pub fn redact_env(&mut self) -> bool {
        let mut changed = false;
        for value in self.env.values_mut() {
            if value != REDACTED_ENV_VALUE {
                *value = REDACTED_ENV_VALUE.into();
                changed = true;
            }
        }
        changed
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Stats {
    pub sampled_at_ms: u64,
    pub rss_bytes: u64,
    pub footprint_bytes: u64,
    pub peak_sampled_rss_bytes: u64,
    pub peak_sampled_footprint_bytes: u64,
    pub sampled_process_count: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Outcome {
    pub state: String,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub message: Option<String>,
    pub finished_at_ms: u64,
    pub elapsed_ms: u64,
    pub command_user_cpu_seconds: f64,
    pub command_system_cpu_seconds: f64,
    pub command_max_rss_bytes: u64,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub stats: Stats,
}
impl Outcome {
    pub fn interrupted(message: &str) -> Self {
        Self {
            state: "interrupted".into(),
            exit_code: None,
            signal: None,
            message: Some(message.into()),
            finished_at_ms: wall_ms(),
            elapsed_ms: 0,
            command_user_cpu_seconds: 0.0,
            command_system_cpu_seconds: 0.0,
            command_max_rss_bytes: 0,
            stdout_truncated: false,
            stderr_truncated: false,
            stats: Stats::default(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub sequence: u64,
    pub state: String,
    pub emergency: bool,
    pub requested_timeout_ms: u64,
    pub queue_limit_ms: u64,
    pub max_memory_bytes: u64,
    pub submitted_at_ms: u64,
    pub started_at_ms: Option<u64>,
    pub cancellation_requested: bool,
    pub stats: Stats,
    pub outcome: Option<Outcome>,
    /// Terminal job whose process tree could not be confirmed gone. Its memory
    /// stays reserved until a later cleanup attempt confirms it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub reservation_quarantined: bool,
    #[serde(skip)]
    pub helper_pid: Option<i32>,
    #[serde(skip)]
    pub helper_token: Option<u64>,
}
impl Job {
    pub fn queued(&self) -> bool {
        self.state == "queued"
    }
    pub fn active(&self) -> bool {
        matches!(self.state.as_str(), "starting" | "running" | "cancelling")
    }
    pub fn terminal(&self) -> bool {
        !self.queued() && !self.active()
    }
    /// Counts against the memory budget: running work, or a terminal job
    /// whose processes may still be alive.
    pub fn holds_reservation(&self) -> bool {
        self.active() || self.reservation_quarantined
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSpec {
    pub id: String,
    pub context: Context,
    pub timeout_ms: u64,
    pub memory_bytes: u64,
    pub monitor_ms: u64,
    pub log_cap_bytes: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RunnerEvent {
    Started {
        at_ms: u64,
        pid: i32,
        token: u64,
    },
    Sample {
        stats: Stats,
        stdout_truncated: bool,
        stderr_truncated: bool,
    },
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Submit {
        token: String,
        context: Context,
        timeout_ms: Option<u64>,
        max_memory_bytes: Option<u64>,
        #[serde(default)]
        emergency: bool,
        #[serde(default)]
        env_overrides: BTreeMap<String, String>,
    },
    Status {
        id: String,
    },
    Jobs {
        #[serde(default)]
        all: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
    },
    Queues {},
    /// Hold waiting jobs by default; optionally finish all accepted work.
    Drain {
        #[serde(default)]
        all: bool,
    },
    Resume {},
    Maintenance {},
    Cancel {
        id: String,
    },
    Reload {},
    Logs {
        id: String,
        stream: String,
        #[serde(default)]
        offset: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_bytes: Option<u64>,
    },
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub version: u32,
    #[serde(flatten)]
    pub request: Request,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wire_roundtrip_and_reject_unknown_operation_fields() {
        let wire = r#"{"version":1,"op":"queues"}"#;
        let e: Envelope = serde_json::from_str(wire).unwrap();
        assert_eq!(serde_json::to_string(&e).unwrap(), wire);
        assert!(
            serde_json::from_str::<Envelope>(r#"{"version":1,"op":"queues","surprise":true}"#)
                .is_err()
        );
        assert!(serde_json::from_str::<Envelope>(
            r#"{"version":1,"op":"status","id":"x","surprise":true}"#
        )
        .is_err());
    }
    #[test]
    fn env_redaction_keeps_names_and_is_idempotent() {
        let mut ctx = Context {
            command: CommandSpec::Exec {
                argv: vec!["/usr/bin/true".into()],
            },
            cwd: "/".into(),
            env: BTreeMap::from([
                ("API_KEY".into(), "secret".into()),
                ("PATH".into(), "/usr/bin".into()),
            ]),
            umask: None,
        };
        assert!(ctx.redact_env());
        assert_eq!(
            ctx.env.keys().collect::<Vec<_>>(),
            ["API_KEY", "PATH"].iter().collect::<Vec<_>>()
        );
        assert!(ctx.env.values().all(|v| v == REDACTED_ENV_VALUE));
        assert!(!ctx.redact_env());
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub version: u32,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiError>,
}
impl Response {
    pub fn success(value: serde_json::Value) -> Self {
        Self {
            version: 1,
            ok: true,
            data: Some(value),
            error: None,
        }
    }
    pub fn error(code: &str, message: impl Into<String>) -> Self {
        Self {
            version: 1,
            ok: false,
            data: None,
            error: Some(ApiError {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}
