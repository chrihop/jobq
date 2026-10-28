use crate::{config::Config, model::Job};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, VecDeque};

fn lane(j: &Job) -> (bool, u64) {
    if j.emergency {
        (true, 0)
    } else {
        (false, j.queue_limit_ms)
    }
}

/// All selection, including ETA simulation, uses this one policy.
pub fn select(jobs: &[Job], cfg: &Config, now: u64, free_memory: u64) -> Option<usize> {
    let mut heads: BTreeMap<(bool, u64), usize> = BTreeMap::new();
    for (i, j) in jobs.iter().enumerate().filter(|(_, j)| j.queued()) {
        let key = lane(j);
        heads
            .entry(key)
            .and_modify(|old| {
                if jobs[*old].sequence > j.sequence {
                    *old = i;
                }
            })
            .or_insert(i);
    }
    choose_head(
        jobs,
        &heads.values().copied().collect::<Vec<_>>(),
        cfg,
        now,
        free_memory,
    )
}

fn choose_head(
    jobs: &[Job],
    heads: &[usize],
    cfg: &Config,
    now: u64,
    free_memory: u64,
) -> Option<usize> {
    let aged = heads
        .iter()
        .copied()
        .filter(|i| now.saturating_sub(jobs[*i].submitted_at_ms) >= cfg.aging_ms)
        .min_by_key(|i| jobs[*i].sequence);
    if let Some(i) = aged {
        return (jobs[i].max_memory_bytes <= free_memory).then_some(i);
    }
    if let Some(i) = heads.iter().copied().find(|i| jobs[*i].emergency) {
        return (jobs[i].max_memory_bytes <= free_memory).then_some(i);
    }
    heads
        .iter()
        .copied()
        .filter(|i| jobs[*i].max_memory_bytes <= free_memory)
        .min_by_key(|i| jobs[*i].queue_limit_ms)
}

#[derive(Debug, Clone, Serialize)]
pub struct Projection {
    pub start_at_ms: u64,
    pub finish_at_ms: u64,
}
#[derive(Debug, Serialize)]
pub struct Estimate {
    pub snapshot_at_ms: u64,
    pub historical: Projection,
    pub budget: Projection,
    pub sample_count: usize,
    pub confidence: &'static str,
    pub assumptions: &'static str,
}
fn history(jobs: &[Job]) -> HashMap<u64, Vec<u64>> {
    let mut h: HashMap<u64, Vec<u64>> = HashMap::new();
    let mut finished: Vec<_> = jobs
        .iter()
        .filter_map(|j| j.outcome.as_ref().map(|o| (j, o)))
        .filter(|(_, o)| matches!(o.state.as_str(), "succeeded" | "timed_out"))
        .collect();
    finished.sort_by_key(|(_, o)| std::cmp::Reverse(o.finished_at_ms));
    for (j, o) in finished {
        let v = h.entry(j.queue_limit_ms).or_default();
        if v.len() < 100 {
            v.push(o.elapsed_ms.max(1).min(j.queue_limit_ms));
        }
    }
    h
}
fn remaining(job: &Job, now: u64, samples: &HashMap<u64, Vec<u64>>, historical: bool) -> u64 {
    let elapsed = job
        .started_at_ms
        .map(|t| now.saturating_sub(t))
        .unwrap_or(0);
    let budget = job.queue_limit_ms.saturating_sub(elapsed).max(1);
    if !historical {
        return budget;
    }
    let mut values: Vec<_> = samples
        .get(&job.queue_limit_ms)
        .into_iter()
        .flatten()
        .copied()
        .filter(|d| *d > elapsed)
        .collect();
    if values.is_empty() {
        return budget;
    }
    values.sort_unstable();
    values[values.len() / 2]
        .saturating_sub(elapsed)
        .max(1)
        .min(budget)
}
fn simulate(
    jobs: &[Job],
    cfg: &Config,
    now: u64,
    h: &HashMap<u64, Vec<u64>>,
    historical: bool,
) -> HashMap<String, Projection> {
    // Quarantined (terminal) jobs keep their reservation for the whole projection.
    let quarantined: u64 = jobs
        .iter()
        .filter(|j| j.reservation_quarantined)
        .map(|j| j.max_memory_bytes)
        .sum();
    let capacity = cfg.memory_cap_bytes.saturating_sub(quarantined);
    let mut jobs: Vec<_> = jobs.iter().filter(|j| !j.terminal()).cloned().collect();
    let mut result = HashMap::new();
    let mut active = Vec::<(usize, u64)>::new();
    let mut pending: BTreeMap<(bool, u64), VecDeque<usize>> = BTreeMap::new();
    let mut ordered: Vec<_> = jobs
        .iter()
        .enumerate()
        .filter(|(_, j)| j.queued())
        .map(|(i, _)| i)
        .collect();
    ordered.sort_by_key(|i| jobs[*i].sequence);
    for i in ordered {
        pending.entry(lane(&jobs[i])).or_default().push_back(i);
    }
    let mut clock = now;
    for (i, j) in jobs.iter().enumerate().filter(|(_, j)| j.active()) {
        let end = now.saturating_add(remaining(j, now, h, historical));
        active.push((i, end));
        result.insert(
            j.id.clone(),
            Projection {
                start_at_ms: j.started_at_ms.unwrap_or(now),
                finish_at_ms: end,
            },
        );
    }
    loop {
        let reserved: u64 = active.iter().map(|(i, _)| jobs[*i].max_memory_bytes).sum();
        if active.len() < cfg.runners {
            let heads: Vec<_> = pending
                .values()
                .filter_map(|v| v.front().copied())
                .collect();
            if let Some(i) =
                choose_head(&jobs, &heads, cfg, clock, capacity.saturating_sub(reserved))
            {
                let end = clock.saturating_add(remaining(&jobs[i], clock, h, historical));
                pending.get_mut(&lane(&jobs[i])).unwrap().pop_front();
                jobs[i].state = "running".into();
                result.insert(
                    jobs[i].id.clone(),
                    Projection {
                        start_at_ms: clock,
                        finish_at_ms: end,
                    },
                );
                active.push((i, end));
                continue;
            }
        }
        let Some(next) = active.iter().map(|(_, end)| *end).min() else {
            break;
        };
        clock = next;
        active.retain(|(i, end)| {
            if *end <= clock {
                jobs[*i].state = "succeeded".into();
                false
            } else {
                true
            }
        });
    }
    result
}
pub fn estimates(jobs: &[Job], cfg: &Config, now: u64) -> HashMap<String, Estimate> {
    let h = history(jobs);
    let historical = simulate(jobs, cfg, now, &h, true);
    let budget = simulate(jobs, cfg, now, &h, false);
    jobs.iter().filter_map(|j| {
        let count=h.get(&j.queue_limit_ms).map_or(0,Vec::len);
        Some((j.id.clone(),Estimate{snapshot_at_ms:now,historical:historical.get(&j.id)?.clone(),budget:budget.get(&j.id)?.clone(),sample_count:count,
            confidence:if count<5 {"low"}else{"queue_history"},assumptions:"No future submissions, cancellations, configuration changes, or cleanup delays. Predictions are not guarantees."}))
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::FileConfig, model::Stats};
    fn config() -> Config {
        Config::resolve(FileConfig::default(), 2, 16 << 30).unwrap()
    }
    fn job(seq: u64, q: u64, mem: u64, emergency: bool) -> Job {
        Job {
            id: seq.to_string(),
            sequence: seq,
            state: "queued".into(),
            emergency,
            requested_timeout_ms: q,
            queue_limit_ms: q,
            max_memory_bytes: mem,
            submitted_at_ms: 0,
            started_at_ms: None,
            cancellation_requested: false,
            stats: Stats::default(),
            outcome: None,
            helper_pid: None,
            helper_token: None,
            reservation_quarantined: false,
        }
    }
    #[test]
    fn routing_and_default() {
        let c = config();
        assert_eq!(c.default_timeout_ms, 10000);
        for (t, q) in [(500, 1000), (1000, 1000), (2000, 5000), (22000, 60000)] {
            assert_eq!(c.route(t).unwrap(), q);
        }
        assert!(c.route(600001).is_err());
    }
    #[test]
    fn fifo_cannot_bypass_memory_blocked_head() {
        let c = config();
        let j = vec![
            job(1, 1000, 8, false),
            job(2, 1000, 1, false),
            job(3, 5000, 2, false),
        ];
        assert_eq!(select(&j, &c, 0, 3), Some(2));
        assert_eq!(select(&j, &c, 30000, 3), None);
    }
    #[test]
    fn emergency_and_continuous_traffic_aging() {
        let c = config();
        let j = vec![
            job(1, 60000, 4, false),
            job(2, 1000, 4, true),
            job(3, 1000, 4, false),
        ];
        assert_eq!(select(&j, &c, 0, 4), Some(1));
        assert_eq!(select(&j, &c, 30000, 4), Some(0));
    }
    #[test]
    fn budget_simulation_respects_memory_and_slots() {
        let mut c = config();
        c.memory_cap_bytes = 8;
        let j = vec![
            job(1, 1000, 8, false),
            job(2, 5000, 4, false),
            job(3, 5000, 4, false),
        ];
        let e = estimates(&j, &c, 0);
        assert_eq!(e["1"].budget.finish_at_ms, 1000);
        assert_eq!(e["2"].budget.start_at_ms, 1000);
        assert_eq!(e["3"].budget.finish_at_ms, 6000);
        assert_eq!(e["3"].confidence, "low");
    }
    #[test]
    fn quarantined_reservation_reduces_projected_capacity() {
        let mut c = config();
        c.memory_cap_bytes = 8;
        let mut quarantined = job(1, 1000, 4, false);
        quarantined.state = "interrupted".into();
        quarantined.reservation_quarantined = true;
        let j = vec![quarantined, job(2, 1000, 8, false), job(3, 5000, 4, false)];
        let e = estimates(&j, &c, 0);
        assert!(
            !e.contains_key("2"),
            "8 bytes cannot fit beside 4 quarantined"
        );
        assert_eq!(e["3"].budget.start_at_ms, 0);
    }
    #[test]
    fn running_estimates_condition_on_elapsed_and_exclude_cancelled_history() {
        let mut c = config();
        c.runners = 1;
        let mut jobs = Vec::new();
        for (i, elapsed) in [100, 200, 300, 400, 500, 600].into_iter().enumerate() {
            let mut j = job(i as u64, 1000, 1, false);
            let mut o = crate::model::Outcome::interrupted("fixture");
            o.state = "succeeded".into();
            o.elapsed_ms = elapsed;
            j.state = o.state.clone();
            j.outcome = Some(o);
            jobs.push(j);
        }
        let mut cancelled = job(7, 1000, 1, false);
        let mut o = crate::model::Outcome::interrupted("fixture");
        o.state = "cancelled".into();
        o.elapsed_ms = 999;
        cancelled.state = o.state.clone();
        cancelled.outcome = Some(o);
        jobs.push(cancelled);
        let mut running = job(8, 1000, 1, false);
        running.state = "running".into();
        running.started_at_ms = Some(0);
        jobs.push(running);
        let e = estimates(&jobs, &c, 250);
        assert_eq!(e["8"].historical.finish_at_ms, 500);
        assert_eq!(e["8"].budget.finish_at_ms, 1000);
        assert_eq!(e["8"].sample_count, 6);
    }
    #[test]
    fn reassignment_merges_fifo_and_preserves_running_limits() {
        let c = config();
        let mut running = job(1, 500, 1, false);
        running.state = "running".into();
        let mut jobs = vec![running, job(3, 5000, 1, false), job(2, 2000, 1, false)];
        crate::server::reconcile(&mut jobs, &c).unwrap();
        assert_eq!(jobs[0].queue_limit_ms, 500);
        assert_eq!(jobs[2].queue_limit_ms, 5000);
        assert_eq!(select(&jobs, &c, 0, 10), Some(2));
        let mut too_long = vec![job(4, 700000, 1, false)];
        assert!(crate::server::reconcile(&mut too_long, &c).is_err());
    }
}
