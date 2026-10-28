use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};

pub fn duration(s: &str) -> Result<u64> {
    let d = humantime::parse_duration(s).context("invalid duration")?;
    let ms = u64::try_from(d.as_millis())?;
    ensure!(
        ms > 0 && ms <= 31_536_000_000,
        "duration must be 1ms..365days"
    );
    Ok(ms)
}

pub fn bytes(s: &str) -> Result<u64> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let n: u64 = s[..split].parse().context("memory size needs an integer")?;
    let unit = s[split..].trim().to_ascii_lowercase();
    let scale = match unit.as_str() {
        "" | "b" => 1,
        "kb" => 1000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        "kib" => 1024,
        "mib" => 1_048_576,
        "gib" => 1_073_741_824,
        "tib" => 1_099_511_627_776,
        _ => bail!("size unit must be B, KB, MB, GB, KiB, MiB, GiB, or TiB"),
    };
    let result = n.checked_mul(scale).context("size overflow")?;
    ensure!(result > 0 && result <= i64::MAX as u64, "size out of range");
    Ok(result)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FileConfig {
    pub queues: Vec<String>,
    pub default_timeout: String,
    pub default_max_memory: String,
    pub runners: Option<usize>,
    pub memory_cap: Option<String>,
    pub aging_after: String,
    pub monitor_interval: String,
    pub log_cap: String,
    pub retention: String,
    pub max_pending: usize,
    pub performance_while_running: bool,
    pub env: BTreeMap<String, String>,
}
impl Default for FileConfig {
    fn default() -> Self {
        Self {
            queues: ["1s", "5s", "10s", "20s", "60s", "300s", "600s"]
                .map(String::from)
                .to_vec(),
            default_timeout: "10s".into(),
            default_max_memory: "4GiB".into(),
            runners: None,
            memory_cap: None,
            aging_after: "30s".into(),
            monitor_interval: "100ms".into(),
            log_cap: "16MiB".into(),
            retention: "7days".into(),
            max_pending: 10000,
            performance_while_running: false,
            env: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Config {
    pub queues_ms: Vec<u64>,
    pub default_timeout_ms: u64,
    pub default_memory_bytes: u64,
    pub runners: usize,
    pub memory_cap_bytes: u64,
    pub aging_ms: u64,
    pub monitor_ms: u64,
    pub log_cap_bytes: u64,
    pub retention_ms: u64,
    pub max_pending: usize,
    pub performance_while_running: bool,
    #[serde(skip_serializing)]
    pub env: BTreeMap<String, String>,
    pub cpu_count: usize,
    pub physical_memory_bytes: u64,
}
impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let raw: FileConfig = match path {
            Some(p) => toml::from_str(
                &std::fs::read_to_string(p).with_context(|| format!("read {}", p.display()))?,
            )?,
            None => FileConfig::default(),
        };
        let cpu_count = std::thread::available_parallelism()?.get();
        let physical_memory_bytes = crate::native::physical_memory()?;
        Self::resolve(raw, cpu_count, physical_memory_bytes)
    }
    pub fn resolve(raw: FileConfig, cpu_count: usize, physical_memory_bytes: u64) -> Result<Self> {
        let mut queues_ms = raw
            .queues
            .iter()
            .map(|s| duration(s))
            .collect::<Result<Vec<_>>>()?;
        ensure!(!queues_ms.is_empty(), "at least one queue is required");
        ensure!(queues_ms.len() <= 128, "at most 128 queues");
        queues_ms.sort_unstable();
        ensure!(
            !queues_ms.windows(2).any(|w| w[0] == w[1]),
            "queue limits must differ"
        );
        let default_timeout_ms = duration(&raw.default_timeout)?;
        ensure!(
            default_timeout_ms <= *queues_ms.last().unwrap(),
            "default timeout exceeds largest queue"
        );
        let runners = raw.runners.unwrap_or(cpu_count);
        ensure!(runners > 0, "runners must be positive");
        ensure!(raw.max_pending > 0, "max_pending must be positive");
        let memory_cap_bytes = raw
            .memory_cap
            .as_deref()
            .map(bytes)
            .transpose()?
            .unwrap_or(physical_memory_bytes / 4 * 3);
        let monitor_ms = duration(&raw.monitor_interval)?;
        ensure!(monitor_ms <= 1000, "monitor_interval must be <= 1s");
        for (k, v) in &raw.env {
            validate_env(k, v)?;
        }
        Ok(Self {
            queues_ms,
            default_timeout_ms,
            default_memory_bytes: bytes(&raw.default_max_memory)?,
            runners,
            memory_cap_bytes,
            aging_ms: duration(&raw.aging_after)?,
            monitor_ms,
            log_cap_bytes: bytes(&raw.log_cap)?,
            retention_ms: duration(&raw.retention)?,
            max_pending: raw.max_pending,
            performance_while_running: raw.performance_while_running,
            env: raw.env,
            cpu_count,
            physical_memory_bytes,
        })
    }
    pub fn route(&self, requested: u64) -> Result<u64> {
        ensure!(requested > 0, "timeout must be positive");
        self.queues_ms
            .iter()
            .copied()
            .find(|q| *q >= requested)
            .context("timeout exceeds largest queue")
    }
    pub fn warn(&self) {
        if self.runners > self.cpu_count {
            eprintln!(
                "warning: {} runners exceed {} logical CPUs",
                self.runners, self.cpu_count
            );
        }
        if self.memory_cap_bytes > self.physical_memory_bytes {
            eprintln!(
                "warning: memory cap exceeds physical RAM; host memory exhaustion remains possible"
            );
        }
        if self.default_memory_bytes > self.memory_cap_bytes {
            eprintln!(
                "warning: default job memory exceeds admission budget; submit with a smaller cap"
            );
        }
    }
}
pub fn validate_env(k: &str, v: &str) -> Result<()> {
    ensure!(
        !k.is_empty() && !k.contains(['=', '\0']) && !v.contains('\0'),
        "invalid environment entry"
    );
    Ok(())
}
