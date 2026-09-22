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

/// A job a worker has taken, with everything it needs to run it.
pub(crate) struct Claim {
    pub id: JobId,
    pub attempt: u32,
    pub token: String,
    pub command: Command,
    pub scratch: PathBuf,
    pub gpus: Vec<u32>,
    pub kill_after: Option<Duration>,
    pub task: Option<String>,
}

impl Claim {
    /// How to describe this job in an event: the task it runs, or the
    /// command it is.
    pub(crate) fn describe(&self) -> String {
        match &self.task {
            Some(task) => format!("task {task}"),
            None => self.command.program.clone(),
        }
    }
}

impl Store {
    /// Take the oldest runnable job that fits what is free, in one
    /// transaction: read the pool, pick a slice, write the lease. Losing
    /// the race to another process means the update matches nothing, and
    /// the next call tries again.
    pub(crate) fn claim_next(
        &self,
        host: &str,
        capacity: &crate::worker::Capacity,
        lease: Duration,
    ) -> Result<Option<Claim>> {
        let tx = self.write_tx()?;
        let now = now_ms();

        // What this host has already lent out, counted from live leases so
        // it cannot drift from the jobs actually running.
        let (used_cpus, used_mem): (i64, i64) = tx.query_row(
            "SELECT COALESCE(SUM(cpus), 0), COALESCE(SUM(mem_mb), 0)
             FROM jobs
             WHERE state = 'running' AND lease_host = ?1 AND lease_expires_at > ?2",
            params![host, now],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let mut free_gpus: Vec<u32> = {
            let mut stmt = tx.prepare("SELECT device FROM gpu_claims WHERE host = ?1")?;
            let taken = stmt
                .query_map([host], |r| r.get::<_, i64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            capacity
                .gpus
                .iter()
                .copied()
                .filter(|d| !taken.contains(&(*d as i64)))
                .collect()
        };
        let free_cpus = (capacity.cpus as i64 - used_cpus).max(0);
        let free_mem = (capacity.mem_mb as i64 - used_mem).max(0);

        // Barriers are settled here rather than run: they have no command.
        /// The columns a claim needs off the runnable view.
        struct Candidate {
            id: i64,
            gpus: i64,
            kill_after_secs: Option<i64>,
            task: Option<String>,
        }

        let candidate: Option<Candidate> = tx
            .query_row(
                "SELECT id, gpus, kill_after_secs, task_name
                 FROM runnable
                 WHERE kind = 'command'
                   AND cpus <= ?1 AND mem_mb <= ?2 AND gpus <= ?3
                 ORDER BY id LIMIT 1",
                params![free_cpus, free_mem, free_gpus.len() as i64],
                |r| {
                    Ok(Candidate {
                        id: r.get(0)?,
                        gpus: r.get(1)?,
                        kill_after_secs: r.get(2)?,
                        task: r.get(3)?,
                    })
                },
            )
            .optional()?;
        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let id = JobId::new(candidate.id);

        let token = lease_token(host);
        let scratch = self.root.join("scratch").join(&token);
        let attempt: i64 = tx.query_row(
            "SELECT attempt + 1 FROM jobs WHERE id = ?1",
            [id.get()],
            |r| r.get(0),
        )?;

        let changed = tx.execute(
            "UPDATE jobs
             SET state = 'running', attempt = ?2, lease_token = ?3, lease_host = ?4,
                 lease_expires_at = ?5, scratch_dir = ?6,
                 started_at = COALESCE(started_at, ?7), stalled = 0
             WHERE id = ?1 AND state IN ('pending', 'ready')",
            params![
                id.get(),
                attempt,
                token,
                host,
                now + lease.as_millis() as i64,
                scratch.to_string_lossy().into_owned(),
                now,
            ],
        )?;
        if changed == 0 {
            // Someone else took it between the read and the write.
            return Ok(None);
        }

        free_gpus.truncate(candidate.gpus as usize);
        for device in &free_gpus {
            tx.execute(
                "INSERT INTO gpu_claims (host, device, job_id, lease_token)
                 VALUES (?1, ?2, ?3, ?4)",
                params![host, *device as i64, id.get(), token],
            )?;
        }

        tx.execute(
            "INSERT INTO attempts (job_id, n, host, lease_token, started_at, scratch_dir)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                id.get(),
                attempt,
                host,
                token,
                now,
                scratch.to_string_lossy().into_owned()
            ],
        )?;

        tx.commit()?;

        let command = self.command_of(id)?.expect("a command job has a command");
        Ok(Some(Claim {
            id,
            attempt: attempt as u32,
            token,
            command,
            scratch,
            gpus: free_gpus,
            kill_after: candidate
                .kill_after_secs
                .map(|s| Duration::from_secs(s as u64)),
            task: candidate.task,
        }))
    }

    /// Record the child we started, so a reclaimer can kill the right
    /// process later. PID alone is not enough: they get recycled.
    pub(crate) fn record_child(&self, claim: &Claim, pid: u32, pid_start: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE jobs SET lease_pid = ?2, lease_pid_start = ?3
             WHERE id = ?1 AND lease_token = ?4",
            params![claim.id.get(), pid as i64, pid_start, claim.token],
        )?;
        self.conn.execute(
            "UPDATE attempts SET pid = ?3, pid_start = ?4 WHERE job_id = ?1 AND n = ?2",
            params![claim.id.get(), claim.attempt as i64, pid as i64, pid_start],
        )?;
        Ok(())
    }

    /// Push the lease out. Returns false once the lease is no longer ours,
    /// which is the worker's signal to stop before doing anything
    /// expensive or destructive.
    pub(crate) fn renew(&self, claim: &Claim, lease: Duration) -> Result<bool> {
        let changed = self.conn.execute(
            "UPDATE jobs SET lease_expires_at = ?2 WHERE id = ?1 AND lease_token = ?3",
            params![
                claim.id.get(),
                now_ms() + lease.as_millis() as i64,
                claim.token
            ],
        )?;
        Ok(changed == 1)
    }

    /// Write the outcome, but only while the lease still holds our token.
    ///
    /// This is the exactly-once commit: if another process reclaimed the
    /// job, the token changed, nothing is written, and the caller throws
    /// its output away.
    pub(crate) fn commit(
        &self,
        claim: &Claim,
        outcome: Outcome,
        exit_code: Option<i32>,
        note: Option<&str>,
    ) -> Result<bool> {
        let tx = self.write_tx()?;
        let now = now_ms();

        // A failed job that may safely run again goes back to the pool
        // rather than to a human.
        let (state, attempt_outcome) = match outcome {
            Outcome::Done => ("done", "done"),
            Outcome::Failed => {
                let (retries, idempotent, attempt): (i64, i64, i64) = tx.query_row(
                    "SELECT retries, idempotent, attempt FROM jobs WHERE id = ?1",
                    [claim.id.get()],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?;
                if idempotent != 0 && attempt <= retries {
                    ("ready", "failed")
                } else {
                    ("failed", "failed")
                }
            }
            Outcome::Killed => ("failed", "killed"),
            Outcome::Cancelled => ("cancelled", "cancelled"),
        };

        let changed = tx.execute(
            "UPDATE jobs
             SET state = ?2,
                 exit_code = ?3,
                 failure = ?4,
                 finished_at = CASE WHEN ?2 = 'ready' THEN NULL ELSE ?5 END,
                 lease_token = NULL, lease_expires_at = NULL,
                 lease_pid = NULL, lease_pid_start = NULL
             WHERE id = ?1 AND lease_token = ?6",
            params![claim.id.get(), state, exit_code, note, now, claim.token],
        )?;
        if changed == 0 {
            tx.commit()?;
            return Ok(false);
        }

        tx.execute(
            "UPDATE attempts
             SET finished_at = ?3, exit_code = ?4, outcome = ?5,
                 wall_secs = (?3 - started_at) / 1000.0
             WHERE job_id = ?1 AND n = ?2",
            params![
                claim.id.get(),
                claim.attempt as i64,
                now,
                exit_code,
                attempt_outcome
            ],
        )?;
        tx.execute(
            "DELETE FROM gpu_claims WHERE lease_token = ?1",
            [&claim.token],
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// Reclaim leases that have rotted, and say whose children may still be
    /// alive so the caller can kill them.
    pub(crate) fn reclaim_expired(&self, host: &str) -> Result<Vec<Orphan>> {
        let tx = self.write_tx()?;
        let now = now_ms();

        let mut stmt = tx.prepare(
            "SELECT id, lease_token, lease_pid, lease_pid_start, idempotent, retries, attempt
             FROM jobs
             WHERE state = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < ?1
               AND (lease_host = ?2 OR lease_host = '' OR lease_host IS NULL)",
        )?;
        let rotted = stmt
            .query_map(params![now, host], |r| {
                Ok((
                    JobId::new(r.get(0)?),
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<i64>>(2)?,
                    r.get::<_, Option<i64>>(3)?,
                    r.get::<_, i64>(4)? != 0,
                    r.get::<_, i64>(5)?,
                    r.get::<_, i64>(6)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);

        let mut orphans = Vec::new();
        for (id, token, pid, pid_start, idempotent, retries, attempt) in rotted {
            // A job that cannot safely run twice does not go back in the
            // pool, however innocent the crash looked.
            let state = if idempotent && attempt <= retries + 1 {
                "ready"
            } else {
                "failed"
            };
            tx.execute(
                "UPDATE jobs
                 SET state = ?2, lease_token = NULL, lease_expires_at = NULL,
                     lease_pid = NULL, lease_pid_start = NULL,
                     failure = CASE WHEN ?2 = 'failed'
                         THEN 'lease expired and the job is not safe to re-run' END,
                     finished_at = CASE WHEN ?2 = 'failed' THEN ?3 END
                 WHERE id = ?1 AND lease_token = ?4",
                params![id.get(), state, now, token],
            )?;
            tx.execute(
                "UPDATE attempts SET finished_at = ?3, outcome = 'reclaimed'
                 WHERE job_id = ?1 AND lease_token = ?2 AND finished_at IS NULL",
                params![id.get(), token, now],
            )?;
            tx.execute("DELETE FROM gpu_claims WHERE lease_token = ?1", [&token])?;
            tx.execute(
                "INSERT INTO job_events (job_id, at, level, message) VALUES (?1, ?2, 'warn', ?3)",
                params![
                    id.get(),
                    now,
                    format!("lease expired; job moved to {state}")
                ],
            )?;
            orphans.push(Orphan {
                id,
                pid: pid.map(|p| p as u32),
                pid_start,
            });
        }

        tx.commit()?;
        Ok(orphans)
    }

    /// Settle every barrier whose dependencies have decided, applying its
    /// failure policy. Returns the ones that changed state.
    pub(crate) fn settle_barriers(&self) -> Result<Vec<(JobId, State)>> {
        let tx = self.write_tx()?;
        let now = now_ms();

        let mut stmt = tx.prepare(
            "SELECT b.id, b.failure_policy,
                    (SELECT COUNT(*) FROM deps d JOIN jobs p ON p.id = d.parent_id
                      WHERE d.child_id = b.id AND p.state IN ('failed', 'cancelled')),
                    (SELECT COUNT(*) FROM deps d JOIN jobs p ON p.id = d.parent_id
                      WHERE d.child_id = b.id
                        AND p.state NOT IN ('done', 'failed', 'cancelled'))
             FROM jobs b
             WHERE b.kind = 'barrier' AND b.state IN ('pending', 'ready')",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    JobId::new(r.get(0)?),
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);

        let mut settled = Vec::new();
        for (id, policy, failed, unsettled) in rows {
            let state = match (failed, unsettled, policy.as_str()) {
                // Fail-fast notifies the moment a dependency fails; the
                // rest of the subgraph keeps running but nobody waits on it.
                (f, _, "fail_fast") if f > 0 => State::Failed,
                (f, 0, _) if f > 0 => State::Failed,
                (0, 0, _) => State::Done,
                _ => continue,
            };
            tx.execute(
                "UPDATE jobs SET state = ?2, finished_at = ?3,
                     failure = CASE WHEN ?2 = 'failed'
                         THEN 'a dependency failed' END
                 WHERE id = ?1 AND state IN ('pending', 'ready')",
                params![id.get(), state.as_str(), now],
            )?;
            settled.push((id, state));
        }

        tx.commit()?;
        Ok(settled)
    }

    pub(crate) fn cancel(&self, id: JobId) -> Result<bool> {
        let changed = self.conn.execute(
            "UPDATE jobs SET state = 'cancelled', finished_at = ?2
             WHERE id = ?1 AND state IN ('pending', 'ready')",
            params![id.get(), now_ms()],
        )?;
        Ok(changed == 1)
    }

    pub(crate) fn record_samples(
        &self,
        claim: &Claim,
        samples: &[crate::profile::Sample],
    ) -> Result<()> {
        let tx = self.write_tx()?;
        for s in samples {
            tx.execute(
                "INSERT OR REPLACE INTO samples (job_id, attempt, at, rss_mb, cpu_cores, gpu_util)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    claim.id.get(),
                    claim.attempt as i64,
                    to_ms(s.at),
                    s.rss_mb as i64,
                    s.cpu_cores,
                    s.gpu_util
                ],
            )?;
        }
        if let Some(profile) = crate::profile::roll_up(samples) {
            tx.execute(
                "UPDATE attempts
                 SET peak_rss_mb = ?3, avg_cores = ?4, idle_fraction = ?5
                 WHERE job_id = ?1 AND n = ?2",
                params![
                    claim.id.get(),
                    claim.attempt as i64,
                    profile.peak_rss_mb as i64,
                    profile.avg_cores,
                    profile.idle_fraction
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
}

/// How an attempt ended, from the worker's point of view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Done,
    Failed,
    Killed,
    Cancelled,
}

/// A child that may still be running with nobody supervising it.
pub(crate) struct Orphan {
    pub id: JobId,
    pub pid: Option<u32>,
    pub pid_start: Option<i64>,
}

/// Unique enough that two workers never mint the same one: host, process,
/// the clock, and a counter for the same process claiming twice in a
/// millisecond.
fn lease_token(host: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!(
        "{host}-{}-{nanos}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
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
