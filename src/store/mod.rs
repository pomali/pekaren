//! The SQLite store: the only shared state in the system.
//!
//! One file, `store.db`, in WAL mode, opened by every client and worker. See
//! `docs/adr/0001-sqlite-crate.md` for why rusqlite and what the pragmas buy,
//! and `schema.sql` for the tables.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};

use crate::error::{Error, Result};
use crate::id::JobId;
use crate::job::{Command, Job, JobKind, Resources, Wake};
use crate::profile::Profile;
use crate::queue::{Filter, JobStatus, State};

const SCHEMA: &str = include_str!("schema.sql");

/// Unix millis, the store's one time unit.
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) fn to_ms(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) fn from_ms(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms.max(0) as u64)
}

pub(crate) struct Store {
    conn: Connection,
    root: PathBuf,
}

impl Store {
    /// Open (creating if needed) the store rooted at `root`.
    pub(crate) fn open(root: &Path) -> Result<Store> {
        std::fs::create_dir_all(root).map_err(|e| Error::io(root, e))?;
        let db = root.join("store.db");
        let conn = Connection::open(&db)?;

        // WAL so a reader never blocks the one writer; NORMAL because a
        // process crash is the case we care about and WAL already survives
        // it. busy_timeout because "another process is writing" is the
        // expected state here, not an error.
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(Duration::from_secs(10))?;

        let store = Store {
            conn,
            root: root.to_path_buf(),
        };
        store.migrate(&db)?;
        Ok(store)
    }

    fn migrate(&self, db: &Path) -> Result<()> {
        let found: i32 = self
            .conn
            .pragma_query_value(None, "user_version", |r| r.get(0))?;
        if found > crate::SCHEMA_VERSION {
            return Err(Error::SchemaTooNew {
                path: db.to_path_buf(),
                found,
                expected: crate::SCHEMA_VERSION,
            });
        }
        if found == 0 {
            // A fresh store. Under WAL two processes can reach here at once,
            // so take the write lock first and re-check.
            let tx = self.write_tx()?;
            let found: i32 = tx.pragma_query_value(None, "user_version", |r| r.get(0))?;
            if found == 0 {
                tx.execute_batch(SCHEMA)?;
                tx.pragma_update(None, "user_version", crate::SCHEMA_VERSION)?;
            }
            tx.commit()?;
        }
        Ok(())
    }

    /// Every write takes the store's one write lock up front: BEGIN
    /// IMMEDIATE, never a deferred transaction that upgrades mid-way and
    /// can deadlock against another process.
    fn write_tx(&self) -> rusqlite::Result<rusqlite::Transaction<'_>> {
        rusqlite::Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Where a job's output lands once an attempt commits.
    pub(crate) fn log_dir(&self, id: JobId) -> PathBuf {
        self.root.join("logs").join(id.get().to_string())
    }

    /// Insert a job and its edges in one transaction. The job is `ready` when
    /// it has no unmet dependencies and `pending` otherwise; nothing else
    /// decides the runnable set.
    pub(crate) fn insert_job(&self, job: &Job) -> Result<JobId> {
        let tx = self.write_tx()?;

        for dep in &job.deps {
            let known: Option<i64> = tx
                .query_row("SELECT id FROM jobs WHERE id = ?1", [dep.get()], |r| {
                    r.get(0)
                })
                .optional()?;
            if known.is_none() {
                return Err(Error::UnknownDependency(*dep));
            }
        }

        let (kind, command) = match &job.kind {
            JobKind::Command(c) => ("command", Some(c)),
            JobKind::Barrier => ("barrier", None),
        };
        let state = if job.deps.is_empty() {
            State::Ready
        } else {
            State::Pending
        };

        tx.execute(
            "INSERT INTO jobs (
                 name, kind, shell, program, cwd,
                 cpus, gpus, mem_mb, est_secs, kill_after_secs,
                 eval_prompt, retries, idempotent, failure_policy,
                 state, submitted_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
            params![
                job.name,
                kind,
                command.map(|c| c.shell as i64).unwrap_or(0),
                command.map(|c| c.program.as_str()),
                command
                    .and_then(|c| c.cwd.as_ref())
                    .map(|p| p.to_string_lossy().into_owned()),
                job.resources.cpus,
                job.resources.gpus,
                job.resources.mem_mb as i64,
                job.resources.est.map(|d| d.as_secs() as i64),
                job.kill_after_duration().map(|d| d.as_secs() as i64),
                job.eval_prompt,
                job.retries,
                job.idempotent as i64,
                policy_str(job.policy),
                state.as_str(),
                now_ms(),
            ],
        )?;
        let id = JobId::new(tx.last_insert_rowid());

        if let Some(cmd) = command {
            for (pos, arg) in cmd.args.iter().enumerate() {
                tx.execute(
                    "INSERT INTO job_args (job_id, pos, arg) VALUES (?1, ?2, ?3)",
                    params![id.get(), pos as i64, arg],
                )?;
            }
            for (key, val) in &cmd.env {
                tx.execute(
                    "INSERT OR REPLACE INTO job_env (job_id, key, val) VALUES (?1, ?2, ?3)",
                    params![id.get(), key, val],
                )?;
            }
        }

        for dep in &job.deps {
            tx.execute(
                "INSERT OR IGNORE INTO deps (child_id, parent_id) VALUES (?1, ?2)",
                params![id.get(), dep.get()],
            )?;
        }

        match &job.wake {
            Wake::Nothing => {}
            Wake::Always(cmd) => insert_wake(&tx, id, "always", None, cmd)?,
            Wake::ByWarmth {
                warm,
                cold,
                warm_until,
            } => {
                let until = to_ms(*warm_until);
                insert_wake(&tx, id, "warm", Some(until), warm)?;
                insert_wake(&tx, id, "cold", Some(until), cold)?;
            }
        }

        tx.commit()?;
        Ok(id)
    }

    pub(crate) fn status(&self, id: JobId) -> Result<JobStatus> {
        let mut stmt = self.conn.prepare(SELECT_JOB)?;
        let status = stmt
            .query_row([id.get()], |row| self.read_status(row))
            .optional()?;
        status.ok_or(Error::NoSuchJob(id))
    }

    pub(crate) fn list(&self, filter: Filter) -> Result<Vec<JobStatus>> {
        let (sql, state) = match filter {
            Filter::All => (format!("{SELECT_ALL} ORDER BY id"), None),
            Filter::State(s) => (
                format!("{SELECT_ALL} WHERE state = ?1 ORDER BY id"),
                Some(s),
            ),
            Filter::Unsettled => (
                format!("{SELECT_ALL} WHERE state IN ('pending', 'ready', 'running') ORDER BY id"),
                None,
            ),
        };
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = match state {
            Some(s) => stmt
                .query_map([s.as_str()], |row| self.read_status(row))?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            None => stmt
                .query_map([], |row| self.read_status(row))?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        };
        Ok(rows)
    }

    fn read_status(&self, row: &Row<'_>) -> rusqlite::Result<JobStatus> {
        let id = JobId::new(row.get("id")?);
        let kind: String = row.get("kind")?;
        let est_secs: Option<i64> = row.get("est_secs")?;
        Ok(JobStatus {
            id,
            name: row.get("name")?,
            is_barrier: kind == "barrier",
            state: State::from_db(&row.get::<_, String>("state")?),
            stalled: row.get::<_, i64>("stalled")? != 0,
            attempt: row.get::<_, i64>("attempt")? as u32,
            exit_code: row.get::<_, Option<i64>>("exit_code")?.map(|c| c as i32),
            failure: row.get("failure")?,
            eval_prompt: row.get("eval_prompt")?,
            resources: Resources {
                cpus: row.get::<_, i64>("cpus")? as u32,
                gpus: row.get::<_, i64>("gpus")? as u32,
                mem_mb: row.get::<_, i64>("mem_mb")? as u64,
                est: est_secs.map(|s| Duration::from_secs(s as u64)),
            },
            submitted_at: from_ms(row.get("submitted_at")?),
            started_at: row.get::<_, Option<i64>>("started_at")?.map(from_ms),
            finished_at: row.get::<_, Option<i64>>("finished_at")?.map(from_ms),
            deps: self.deps_of(id)?,
            profile: self.profile_of(id)?,
        })
    }

    fn deps_of(&self, id: JobId) -> rusqlite::Result<Vec<JobId>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT parent_id FROM deps WHERE child_id = ?1 ORDER BY parent_id")?;
        let ids = stmt
            .query_map([id.get()], |r| r.get::<_, i64>(0).map(JobId::new))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(ids)
    }

    /// The profile of the latest finished attempt, if any.
    fn profile_of(&self, id: JobId) -> rusqlite::Result<Option<Profile>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT wall_secs, peak_rss_mb, avg_cores, gpu_util_avg, gpu_util_peak, idle_fraction
             FROM attempts
             WHERE job_id = ?1 AND finished_at IS NOT NULL
             ORDER BY n DESC LIMIT 1",
        )?;
        stmt.query_row([id.get()], |r| {
            Ok(Profile {
                wall: Duration::from_secs_f64(r.get::<_, Option<f64>>(0)?.unwrap_or(0.0)),
                peak_rss_mb: r.get::<_, Option<i64>>(1)?.unwrap_or(0) as u64,
                avg_cores: r.get::<_, Option<f64>>(2)?.unwrap_or(0.0),
                gpu_util_avg: r.get::<_, Option<f64>>(3)?,
                gpu_util_peak: r.get::<_, Option<f64>>(4)?,
                idle_fraction: r.get::<_, Option<f64>>(5)?.unwrap_or(0.0),
            })
        })
        .optional()
    }

    /// The command a job runs, reassembled from its rows.
    pub(crate) fn command_of(&self, id: JobId) -> Result<Option<Command>> {
        let row: Option<(String, Option<String>, i64, Option<String>)> = self
            .conn
            .query_row(
                "SELECT kind, program, shell, cwd FROM jobs WHERE id = ?1",
                [id.get()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        let Some((kind, program, shell, cwd)) = row else {
            return Err(Error::NoSuchJob(id));
        };
        if kind == "barrier" {
            return Ok(None);
        }
        let mut stmt = self
            .conn
            .prepare("SELECT arg FROM job_args WHERE job_id = ?1 ORDER BY pos")?;
        let args = stmt
            .query_map([id.get()], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut stmt = self
            .conn
            .prepare("SELECT key, val FROM job_env WHERE job_id = ?1 ORDER BY key")?;
        let env = stmt
            .query_map([id.get()], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(Some(Command {
            program: program.unwrap_or_default(),
            args,
            shell: shell != 0,
            cwd: cwd.map(PathBuf::from),
            env,
        }))
    }

    /// Ids whose dependencies have all settled successfully, oldest first.
    /// The runnable set has exactly one definition, and it lives in the view.
    pub(crate) fn runnable(&self) -> Result<Vec<JobId>> {
        let mut stmt = self.conn.prepare("SELECT id FROM runnable ORDER BY id")?;
        let ids = stmt
            .query_map([], |r| r.get::<_, i64>(0).map(JobId::new))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(ids)
    }
}

fn insert_wake(
    tx: &rusqlite::Transaction<'_>,
    job: JobId,
    trigger: &str,
    warm_until: Option<i64>,
    cmd: &Command,
) -> Result<()> {
    tx.execute(
        "INSERT INTO wakes (job_id, trigger, warm_until, shell, program, cwd)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            job.get(),
            trigger,
            warm_until,
            cmd.shell as i64,
            cmd.program,
            cmd.cwd.as_ref().map(|p| p.to_string_lossy().into_owned()),
        ],
    )?;
    let wake_id = tx.last_insert_rowid();
    for (pos, arg) in cmd.args.iter().enumerate() {
        tx.execute(
            "INSERT INTO wake_args (wake_id, pos, arg) VALUES (?1, ?2, ?3)",
            params![wake_id, pos as i64, arg],
        )?;
    }
    Ok(())
}

fn policy_str(p: crate::job::FailurePolicy) -> &'static str {
    match p {
        crate::job::FailurePolicy::FailFast => "fail_fast",
        crate::job::FailurePolicy::FailAtEnd => "fail_at_end",
    }
}

const SELECT_ALL: &str = "SELECT id, name, kind, state, stalled, attempt, exit_code, failure,
     eval_prompt, cpus, gpus, mem_mb, est_secs, submitted_at, started_at, finished_at
     FROM jobs";

const SELECT_JOB: &str = "SELECT id, name, kind, state, stalled, attempt, exit_code, failure,
     eval_prompt, cpus, gpus, mem_mb, est_secs, submitted_at, started_at, finished_at
     FROM jobs WHERE id = ?1";
