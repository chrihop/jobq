//! Deterministic policy shared by CLI and MCP set waits.
use anyhow::{ensure, Result};
use rmcp::schemars::{self, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WaitMode {
    Any,
    All,
}

pub fn validate(ids: &[String], block: bool, wait_ms: Option<u64>) -> Result<Option<u64>> {
    ensure!(
        (1..=100).contains(&ids.len()),
        "ids must contain 1..100 unique job IDs"
    );
    let mut seen = HashSet::new();
    for id in ids {
        ensure!(
            !id.trim().is_empty() && id.len() <= 256,
            "job IDs must contain 1..256 bytes and not be blank"
        );
        ensure!(seen.insert(id), "duplicate job ID: {id}");
    }
    ensure!(
        !block || wait_ms.is_none(),
        "block=true cannot be combined with wait_ms"
    );
    if block {
        Ok(None)
    } else {
        let ms = wait_ms.unwrap_or(10000);
        ensure!((1..=30000).contains(&ms), "wait_ms must be 1..30000");
        Ok(Some(ms))
    }
}

pub fn terminal(job: &Value) -> Result<bool> {
    match job["state"].as_str() {
        Some("queued" | "starting" | "running" | "cancelling") => Ok(false),
        Some(
            "succeeded" | "failed" | "timed_out" | "memory_exceeded" | "cancelled" | "spawn_failed"
            | "monitoring_failed" | "interrupted",
        ) => Ok(true),
        _ => anyhow::bail!("missing or unknown job state"),
    }
}

pub fn summarize(ids: &[String], jobs: &[Value], mode: WaitMode) -> Result<Value> {
    ensure!(ids.len() == jobs.len(), "incomplete set snapshot");
    let mut completed = Vec::new();
    let mut pending = Vec::new();
    for (id, job) in ids.iter().zip(jobs) {
        ensure!(
            job["id"].as_str() == Some(id),
            "mismatched job ID in status response"
        );
        if terminal(job)? {
            completed.push(id);
        } else {
            pending.push(id);
        }
    }
    let satisfied = match mode {
        WaitMode::Any => !completed.is_empty(),
        WaitMode::All => pending.is_empty(),
    };
    Ok(
        json!({"mode":mode,"condition_met":satisfied,"wait_expired":!satisfied,
        "completed_ids":completed,"pending_ids":pending,"jobs":jobs,
        "observed_at_ms":crate::model::wall_ms(),
        "snapshot_note":"Per-job observations, not an atomic daemon snapshot. Terminal results are retained for this wait; see each job's snapshot_at_ms."}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validation_and_terminal_policy() {
        assert!(validate(&[], true, None).is_err());
        assert!(validate(&vec!["x".into(); 101], true, None).is_err());
        assert!(validate(&["x".into(), "x".into()], true, None).is_err());
        assert!(validate(&[" ".into()], true, None).is_err());
        assert!(validate(&["x".into()], true, Some(1)).is_err());
        for ms in [0, 30001] {
            assert!(validate(&["x".into()], false, Some(ms)).is_err());
        }
        assert_eq!(validate(&["x".into()], false, None).unwrap(), Some(10000));
        let ids = vec!["a".into(), "b".into()];
        let jobs = vec![
            json!({"id":"a","state":"failed"}),
            json!({"id":"b","state":"cancelling"}),
        ];
        let any = summarize(&ids, &jobs, WaitMode::Any).unwrap();
        assert_eq!(any["condition_met"], true);
        assert_eq!(any["pending_ids"], json!(["b"]));
        assert_eq!(
            summarize(&ids, &jobs, WaitMode::All).unwrap()["wait_expired"],
            true
        );
        assert!(terminal(&json!({"state":"unknown"})).is_err());
        assert!(summarize(&ids, &[jobs[1].clone(), jobs[0].clone()], WaitMode::Any).is_err());
    }
}
