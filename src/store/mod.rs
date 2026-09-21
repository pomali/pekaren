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
use crate::job::{
    Command, InputKind, Job, JobKind, OnChange, Resources, RustSource, Wake, WatchedPath,
};
use crate::profile::Profile;
use crate::queue::{Filter, JobStatus, State};

const SCHEMA: &str = include_str!("schema.sql");

/// One entry per schema version above 1, applied in order to bring an older
/// store forward. `SCHEMA` itself is always the current shape, so a fresh
/// store never runs a migration.
const MIGRATIONS: &[&str] = &[
    // v1 -> v2: Rust jobs remember the .rs file they run.
    "ALTER TABLE jobs ADD COLUMN script_path TEXT;",
    // v2 -> v3: task jobs, and the paths a job depends on.
    "ALTER TABLE jobs ADD COLUMN task_name TEXT;
     CREATE TABLE task_args (
       job_id INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
       pos    INTEGER NOT NULL,
       arg    TEXT NOT NULL,
       PRIMARY KEY (job_id, pos)
     ) WITHOUT ROWID;
     CREATE TABLE job_inputs (
       job_id      INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
       pos         INTEGER NOT NULL,
       kind        TEXT NOT NULL CHECK (kind IN ('binary', 'script', 'param')),
       path        TEXT NOT NULL,
       is_dir      INTEGER NOT NULL DEFAULT 0,
       hash        TEXT,
       recorded_at INTEGER NOT NULL,
       on_change   TEXT NOT NULL CHECK (on_change IN ('fail', 'warn', 'ignore')),
       PRIMARY KEY (job_id, pos)
     ) WITHOUT ROWID;
     CREATE TABLE job_events (
       id      INTEGER PRIMARY KEY AUTOINCREMENT,
       job_id  INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
       at      INTEGER NOT NULL,
       level   TEXT NOT NULL CHECK (level IN ('info', 'warn', 'error')),
       message TEXT NOT NULL
     );
     CREATE INDEX job_events_by_job ON job_events (job_id, at);",
];

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
        if found == crate::SCHEMA_VERSION {
            return Ok(());
        }

        // Under WAL two processes can reach here at once, so take the write
        // lock first and re-read the version under it.
        let tx = self.write_tx()?;
        let found: i32 = tx.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if found == 0 {
            tx.execute_batch(SCHEMA)?;
        } else {
            for step in &MIGRATIONS[(found as usize - 1)..] {
                tx.execute_batch(step)?;
            }
        }
        if found != crate::SCHEMA_VERSION {
            tx.pragma_update(None, "user_version", crate::SCHEMA_VERSION)?;
        }
        tx.commit()?;
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

    /// Write inline Rust source into the store and hand back its path.
    ///
    /// Content-addressed, so the same source submitted twice is one file and
    /// a rolled-back transaction leaves nothing but a reusable script. The
    /// write is a temp file plus a rename, so a reader never sees half a
    /// script.
    fn write_script(&self, source: &str) -> Result<PathBuf> {
        use std::hash::{Hash, Hasher};

        let dir = self.root.join("scripts");
        std::fs::create_dir_all(&dir).map_err(|e| Error::io(&dir, e))?;

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        source.hash(&mut hasher);
        let path = dir.join(format!("{:016x}.rs", hasher.finish()));
        if path.exists() {
            return Ok(path);
        }

        let tmp = path.with_extension("rs.tmp");
        std::fs::write(&tmp, source).map_err(|e| Error::io(&tmp, e))?;
        std::fs::rename(&tmp, &path).map_err(|e| Error::io(&path, e))?;
        Ok(path)
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

        // Rust and task jobs are command jobs once the thing they run is
        // located on disk; what the store keeps besides the command is the
        // path it remembers, and the hash of what was there at submit time.
        let mut watched: Vec<WatchedPath> = Vec::new();
        let (kind, command, script_path, task_name) = match &job.kind {
            JobKind::Command(c) => ("command", Some(c.clone()), None, None),
            JobKind::Barrier => ("barrier", None, None, None),
            JobKind::Rust(script) => {
                let path = match &script.source {
                    RustSource::File(p) => p.clone(),
                    RustSource::Inline(_) => {
                        let text = script.render().expect("inline source renders");
                        self.write_script(&text)?
                    }
                };
                watched.push(WatchedPath {
                    path: path.clone(),
                    kind: InputKind::Script,
                    on_change: job.on_code_change,
                });
                (
                    "command",
                    Some(Command::rust_script(&path)),
                    Some(path),
                    None,
                )
            }
            JobKind::Task(task) => {
                // The job points at this binary. Its argv stays empty: the
                // task reads its arguments through JobCtx, so a program
                // with its own CLI is never handed flags it did not expect.
                let exe = std::env::current_exe()
                    .map_err(|e| Error::io(std::path::Path::new("<current exe>"), e))?;
                watched.push(WatchedPath {
                    path: exe.clone(),
                    kind: InputKind::Binary,
                    on_change: job.on_code_change,
                });
                let command =
                    Command::exec(exe.to_string_lossy().into_owned(), Vec::<String>::new())
                        .env(crate::task::TASK_ENV, task.name());
                ("command", Some(command), None, Some(task.name()))
            }
        };
        watched.extend(job.watched.iter().cloned());
        let command = command.as_ref();
        let state = if job.deps.is_empty() {
            State::Ready
        } else {
            State::Pending
        };

        tx.execute(
            "INSERT INTO jobs (
                 name, kind, shell, program, cwd, script_path, task_name,
                 cpus, gpus, mem_mb, est_secs, kill_after_secs,
                 eval_prompt, retries, idempotent, failure_policy,
                 state, submitted_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                       ?17, ?18)",
            params![
                job.name,
                kind,
                command.map(|c| c.shell as i64).unwrap_or(0),
                command.map(|c| c.program.as_str()),
                command
                    .and_then(|c| c.cwd.as_ref())
                    .map(|p| p.to_string_lossy().into_owned()),
                script_path
                    .as_ref()
                    .map(|p| p.to_string_lossy().into_owned()),
                task_name,
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

        for (pos, arg) in job.task_args.iter().enumerate() {
            tx.execute(
                "INSERT INTO task_args (job_id, pos, arg) VALUES (?1, ?2, ?3)",
                params![id.get(), pos as i64, arg],
            )?;
        }

        // Hash every path the job depends on, now, while the submitter
        // still knows what it meant. A path that is absent records a NULL
        // hash: appearing later is a change too.
        let at = now_ms();
        for (pos, input) in watched.iter().enumerate() {
            let meta = std::fs::metadata(&input.path).ok();
            let hash = match &meta {
                Some(_) => Some(crate::hash::path_hash(&input.path)?),
                None => None,
            };
            tx.execute(
                "INSERT INTO job_inputs
                     (job_id, pos, kind, path, is_dir, hash, recorded_at, on_change)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    id.get(),
                    pos as i64,
                    input.kind.as_str(),
                    input.path.to_string_lossy().into_owned(),
                    meta.map(|m| m.is_dir() as i64).unwrap_or(0),
                    hash,
                    at,
                    input.on_change.as_str(),
                ],
            )?;
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
            script: row
                .get::<_, Option<String>>("script_path")?
                .map(PathBuf::from),
            task: row.get("task_name")?,
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

    /// The arguments a task job was submitted with.
    pub(crate) fn task_args(&self, id: JobId) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT arg FROM task_args WHERE job_id = ?1 ORDER BY pos")?;
        let args = stmt
            .query_map([id.get()], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(args)
    }

    /// The paths a job declared, with the hash each had at submit time.
    pub(crate) fn inputs(&self, id: JobId) -> Result<Vec<RecordedInput>> {
        let mut stmt = self.conn.prepare(
            "SELECT kind, path, is_dir, hash, on_change
             FROM job_inputs WHERE job_id = ?1 ORDER BY pos",
        )?;
        let rows = stmt
            .query_map([id.get()], |r| {
                Ok(RecordedInput {
                    kind: InputKind::from_db(&r.get::<_, String>(0)?),
                    path: PathBuf::from(r.get::<_, String>(1)?),
                    is_dir: r.get::<_, i64>(2)? != 0,
                    hash: r.get::<_, Option<String>>(3)?,
                    on_change: OnChange::from_db(&r.get::<_, String>(4)?),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub(crate) fn record_event(&self, id: JobId, level: &str, message: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO job_events (job_id, at, level, message) VALUES (?1, ?2, ?3, ?4)",
            params![id.get(), now_ms(), level, message],
        )?;
        Ok(())
    }

    pub(crate) fn events(&self, id: JobId) -> Result<Vec<(i64, String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT at, level, message FROM job_events WHERE job_id = ?1 ORDER BY at, id",
        )?;
        let rows = stmt
            .query_map([id.get()], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
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
     eval_prompt, script_path, task_name, cpus, gpus, mem_mb, est_secs,
     submitted_at, started_at, finished_at
     FROM jobs";

const SELECT_JOB: &str = "SELECT id, name, kind, state, stalled, attempt, exit_code, failure,
     eval_prompt, script_path, task_name, cpus, gpus, mem_mb, est_secs,
     submitted_at, started_at, finished_at
     FROM jobs WHERE id = ?1";

/// One row of `job_inputs`, as the store holds it.
pub(crate) struct RecordedInput {
    pub kind: InputKind,
    pub path: PathBuf,
    pub is_dir: bool,
    pub hash: Option<String>,
    pub on_change: OnChange,
}
