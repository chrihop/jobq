use crate::model::{Context, Job, MaintenanceMode};
use anyhow::{ensure, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::{os::unix::fs::PermissionsExt, path::Path};

pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if path.exists() {
            ensure!(
                !std::fs::symlink_metadata(path)?.file_type().is_symlink(),
                "database must not be a symlink"
            );
        }
        let conn = Connection::open(path)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; PRAGMA busy_timeout=5000; PRAGMA secure_delete=ON;
          CREATE TABLE IF NOT EXISTS metadata(version INTEGER NOT NULL);
          INSERT INTO metadata SELECT 1 WHERE NOT EXISTS(SELECT 1 FROM metadata);
          CREATE TABLE IF NOT EXISTS jobs(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,token TEXT NOT NULL UNIQUE,job TEXT NOT NULL,context TEXT NOT NULL,helper_pid INTEGER,helper_token INTEGER);")?;
        let version: i64 = conn.query_row("SELECT version FROM metadata", [], |r| r.get(0))?;
        ensure!(
            version == 1,
            "unsupported database schema version {version}"
        );
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS service_state(key TEXT PRIMARY KEY, value TEXT NOT NULL);",
        )?;
        Ok(Self { conn })
    }
    pub fn maintenance(&self) -> Result<MaintenanceMode> {
        let raw: Option<String> = self
            .conn
            .query_row(
                "SELECT value FROM service_state WHERE key='maintenance'",
                [],
                |r| r.get(0),
            )
            .optional()?;
        Ok(match raw {
            Some(raw) => serde_json::from_str(&raw)?,
            None => MaintenanceMode::Running,
        })
    }
    pub fn set_maintenance(&self, mode: MaintenanceMode) -> Result<()> {
        self.conn.execute(
            "INSERT INTO service_state(key,value) VALUES('maintenance',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [serde_json::to_string(&mode)?],
        )?;
        Ok(())
    }
    pub fn jobs(&self) -> Result<Vec<Job>> {
        let mut s = self
            .conn
            .prepare("SELECT job,helper_pid,helper_token FROM jobs ORDER BY seq")?;
        let rows = s.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<i32>>(1)?,
                r.get::<_, Option<u64>>(2)?,
            ))
        })?;
        rows.map(|r| {
            let (raw, pid, token) = r?;
            let mut j: Job = serde_json::from_str(&raw)?;
            j.helper_pid = pid;
            j.helper_token = token;
            Ok(j)
        })
        .collect()
    }
    pub fn token(&self, token: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row("SELECT id FROM jobs WHERE token=?1", [token], |r| r.get(0))
            .optional()?)
    }
    pub fn insert(&mut self, job: &mut Job, token: &str, ctx: &Context) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO jobs(id,token,job,context) VALUES(?1,?2,'',?3)",
            params![job.id, token, serde_json::to_string(ctx)?],
        )?;
        job.sequence = tx.last_insert_rowid() as u64;
        tx.execute(
            "UPDATE jobs SET job=?1 WHERE id=?2",
            params![serde_json::to_string(job)?, job.id],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn save(&self, j: &Job) -> Result<()> {
        self.conn.execute(
            "UPDATE jobs SET job=?1,helper_pid=?2,helper_token=?3 WHERE id=?4",
            params![
                serde_json::to_string(j)?,
                j.helper_pid,
                j.helper_token,
                j.id
            ],
        )?;
        Ok(())
    }
    pub fn save_many(&mut self, jobs: &[Job]) -> Result<()> {
        let tx = self.conn.transaction()?;
        for j in jobs {
            tx.execute(
                "UPDATE jobs SET job=?1 WHERE id=?2",
                params![serde_json::to_string(j)?, j.id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn context(&self, id: &str) -> Result<Context> {
        let raw: String =
            self.conn
                .query_row("SELECT context FROM jobs WHERE id=?1", [id], |r| r.get(0))?;
        Ok(serde_json::from_str(&raw)?)
    }
    /// Overwrite a job's stored environment values. `secure_delete` zeroes the
    /// replaced row; `checkpoint` drops the copies still held in the WAL.
    pub fn redact_context(&self, id: &str) -> Result<()> {
        let mut ctx = self.context(id)?;
        if ctx.redact_env() {
            self.conn.execute(
                "UPDATE jobs SET context=?1 WHERE id=?2",
                params![serde_json::to_string(&ctx)?, id],
            )?;
        }
        Ok(())
    }
    /// Copy the WAL into the database and truncate it to zero bytes.
    pub fn checkpoint(&self) -> Result<()> {
        self.conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
        Ok(())
    }
    pub fn delete(&self, id: &str) -> Result<()> {
        self.conn.execute("DELETE FROM jobs WHERE id=?1", [id])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{CommandSpec, Stats};
    fn job(id: &str) -> Job {
        Job {
            id: id.into(),
            sequence: 0,
            state: "queued".into(),
            emergency: false,
            requested_timeout_ms: 1000,
            queue_limit_ms: 1000,
            max_memory_bytes: 1024,
            submitted_at_ms: 10,
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
    fn durable_context_identity_and_transaction_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jobs.sqlite");
        let ctx = Context {
            command: CommandSpec::Exec {
                argv: vec!["/usr/bin/true".into()],
            },
            cwd: dir.path().to_str().unwrap().into(),
            env: std::collections::BTreeMap::from([("EXPORTED".into(), "original".into())]),
            umask: Some(0o022),
        };
        let mut store = Store::open(&path).unwrap();
        let mut first = job("first");
        store.insert(&mut first, "token", &ctx).unwrap();
        assert!(first.sequence > 0);
        first.helper_pid = Some(123);
        first.helper_token = Some(456);
        store.save(&first).unwrap();
        assert!(store.insert(&mut job("second"), "token", &ctx).is_err());
        assert_eq!(store.jobs().unwrap().len(), 1);
        drop(store);
        let store = Store::open(&path).unwrap();
        assert_eq!(store.token("token").unwrap().as_deref(), Some("first"));
        assert_eq!(store.context("first").unwrap().env["EXPORTED"], "original");
        assert_eq!(store.jobs().unwrap()[0].helper_token, Some(456));
    }
    #[test]
    fn redaction_leaves_no_secret_bytes_in_database_or_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jobs.sqlite");
        // Large enough to spill into overflow pages, as a full exported env does.
        let secret = format!("sk-{}", "7".repeat(9000));
        let ctx = Context {
            command: CommandSpec::Exec {
                argv: vec!["/usr/bin/true".into()],
            },
            cwd: dir.path().to_str().unwrap().into(),
            env: std::collections::BTreeMap::from([
                ("API_KEY".into(), secret.clone()),
                ("SHORT".into(), "short-secret-value".into()),
            ]),
            umask: None,
        };
        let mut store = Store::open(&path).unwrap();
        store.insert(&mut job("j"), "token", &ctx).unwrap();
        store.checkpoint().unwrap();
        let mut other = job("other");
        store.insert(&mut other, "other-token", &ctx).unwrap();
        store.redact_context("j").unwrap();
        store.redact_context("other").unwrap();
        store.checkpoint().unwrap();
        let stored = store.context("j").unwrap();
        assert_eq!(stored.env["API_KEY"], crate::model::REDACTED_ENV_VALUE);
        assert_eq!(stored.env.len(), 2);
        for file in ["jobs.sqlite", "jobs.sqlite-wal"] {
            let raw = std::fs::read(dir.path().join(file)).unwrap_or_default();
            for needle in [&secret[..64], "short-secret-value"] {
                assert!(
                    !raw.windows(needle.len()).any(|w| w == needle.as_bytes()),
                    "{needle} left in {file}"
                );
            }
        }
    }
    #[test]
    fn maintenance_is_durable_and_invalid_state_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jobs.sqlite");
        let store = Store::open(&path).unwrap();
        assert_eq!(store.maintenance().unwrap(), MaintenanceMode::Running);
        store.set_maintenance(MaintenanceMode::Paused).unwrap();
        drop(store);
        let store = Store::open(&path).unwrap();
        assert_eq!(store.maintenance().unwrap(), MaintenanceMode::Paused);
        store
            .set_maintenance(MaintenanceMode::FinishAccepted)
            .unwrap();
        assert_eq!(
            store.maintenance().unwrap(),
            MaintenanceMode::FinishAccepted
        );
        store
            .conn
            .execute(
                "UPDATE service_state SET value='broken' WHERE key='maintenance'",
                [],
            )
            .unwrap();
        assert!(store.maintenance().is_err());
    }

    #[test]
    fn refuses_incompatible_schema_without_replacing_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jobs.sqlite");
        drop(Store::open(&path).unwrap());
        let conn = Connection::open(&path).unwrap();
        conn.execute("UPDATE metadata SET version=99", []).unwrap();
        drop(conn);
        assert!(Store::open(&path).is_err());
        let conn = Connection::open(&path).unwrap();
        assert_eq!(
            conn.query_row("SELECT version FROM metadata", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            99
        );
    }
}
