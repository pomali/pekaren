use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::error::{Error, Result};
use crate::id::JobId;
use crate::job::{Job, Resources};
use crate::profile::Profile;
use crate::store::Store;

/// Where a job is. `stalled` is a flag, not a state: a stalled job is still
/// running until something kills it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// Waiting on a dependency.
    Pending,
    /// Dependencies met, waiting for capacity.
    Ready,
    /// Leased by a worker.
    Running,
    Done,
    Failed,
    Cancelled,
}

impl State {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            State::Pending => "pending",
            State::Ready => "ready",
            State::Running => "running",
            State::Done => "done",
            State::Failed => "failed",
            State::Cancelled => "cancelled",
        }
    }

    pub(crate) fn from_db(s: &str) -> State {
        match s {
            "pending" => State::Pending,
            "ready" => State::Ready,
            "running" => State::Running,
            "done" => State::Done,
            "failed" => State::Failed,
            _ => State::Cancelled,
        }
    }

    /// Whether this job will never change again.
    pub fn is_settled(self) -> bool {
        matches!(self, State::Done | State::Failed | State::Cancelled)
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Everything a reader — `pec status`, an evaluator, the submitting agent —
/// needs about one job.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct JobStatus {
    pub id: JobId,
    pub name: Option<String>,
    pub is_barrier: bool,
    pub state: State,
    pub stalled: bool,
    /// How many times this job has been started.
    pub attempt: u32,
    pub exit_code: Option<i32>,
    /// Why it failed, in one line, when it did.
    pub failure: Option<String>,
    /// The prompt written at submit time, for whoever judges the output.
    pub eval_prompt: Option<String>,
    /// The `.rs` file behind a Rust job — worth handing to an evaluator
    /// along with the output, since it *is* the job.
    pub script: Option<PathBuf>,
    pub resources: Resources,
    pub submitted_at: SystemTime,
    pub started_at: Option<SystemTime>,
    pub finished_at: Option<SystemTime>,
    pub deps: Vec<JobId>,
    /// What the latest finished attempt actually cost.
    pub profile: Option<Profile>,
}

/// Which jobs [`Queue::list`] returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Filter {
    All,
    State(State),
    /// Anything that has not settled: pending, ready or running.
    Unsettled,
}

/// Where a job's output went.
#[derive(Clone, Debug)]
pub struct Logs {
    pub stdout: PathBuf,
    pub stderr: PathBuf,
}

/// What a sweep of the store cleaned up.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReapReport {
    /// Leases that had rotted; their jobs are runnable again (or failed, if
    /// not idempotent).
    pub leases_reclaimed: Vec<JobId>,
    /// Children killed because their supervising worker was gone.
    pub orphans_killed: Vec<JobId>,
    /// Notification claims left unfulfilled by a worker that died
    /// mid-handoff, now spawned.
    pub wakes_recovered: Vec<JobId>,
}

/// How a process opens the store.
#[derive(Clone, Debug)]
pub struct QueueOptions {
    grace: Duration,
    work_on_submit: bool,
}

impl Default for QueueOptions {
    fn default() -> Self {
        QueueOptions {
            grace: Duration::from_secs(5),
            work_on_submit: true,
        }
    }
}

impl QueueOptions {
    /// How long [`Queue::submit`] stays attached before detaching. A job that
    /// dies inside the window fails trivially — bad path, missing binary —
    /// and that failure comes back as an error from `submit` itself, while
    /// the agent still holds the thread. Zero disables the window.
    pub fn grace(mut self, d: Duration) -> Self {
        self.grace = d;
        self
    }

    /// Whether submitting also runs work in this process. On by default:
    /// with no daemon, the submitter is usually the only process around to
    /// start the first job.
    pub fn work_on_submit(mut self, yes: bool) -> Self {
        self.work_on_submit = yes;
        self
    }

    pub fn open(self, path: impl AsRef<Path>) -> Result<Queue> {
        Queue::open_with(path, self)
    }

    /// Open the default store with these options.
    pub fn open_default(self) -> Result<Queue> {
        Queue::open_with(default_store_path(), self)
    }
}

/// A handle on a store directory.
///
/// Opening one is cheap and creates the directory if it is missing. Nothing
/// here blocks except [`Queue::wait`]: the graph is built by value, from
/// [`JobId`]s.
pub struct Queue {
    store: Store,
    opts: QueueOptions,
}

impl Queue {
    /// Open (creating if needed) the store at `path`. A leading `~/` is
    /// expanded, since this path is usually typed by an agent into a script.
    pub fn open(path: impl AsRef<Path>) -> Result<Queue> {
        Queue::open_with(path, QueueOptions::default())
    }

    /// Open the default store: `$PEKAREN_STORE` if it is set, else
    /// `~/.pekaren`.
    ///
    /// This is what a submitting script, a worker and `pec` all reach for
    /// when nobody said otherwise, so they land in the same place without
    /// agreeing on a path first. See [`default_store_path`].
    pub fn open_default() -> Result<Queue> {
        Queue::open(default_store_path())
    }

    /// Start from non-default options: `Queue::options().grace(...).open(p)`.
    pub fn options() -> QueueOptions {
        QueueOptions::default()
    }

    fn open_with(path: impl AsRef<Path>, opts: QueueOptions) -> Result<Queue> {
        let root = expand_tilde(path.as_ref());
        Ok(Queue {
            store: Store::open(&root)?,
            opts,
        })
    }

    pub fn path(&self) -> &Path {
        self.store.root()
    }

    /// Record a job and return its handle.
    ///
    /// Within the grace window this also watches the job, so a trivial
    /// failure comes back as [`Error::EarlyFailure`] rather than as a
    /// notification an hour later. Past the window, every outcome goes
    /// through the one completion path: one channel, two latencies.
    pub fn submit(&self, job: Job) -> Result<JobId> {
        let id = self.store.insert_job(&job)?;
        if self.opts.work_on_submit && !self.opts.grace.is_zero() {
            // Milestone 2: run the local worker for the grace window and
            // turn an early death into Error::EarlyFailure.
        }
        Ok(id)
    }

    /// Record a job without the grace window, even when the queue was opened
    /// with one.
    pub fn submit_detached(&self, job: Job) -> Result<JobId> {
        self.store.insert_job(&job)
    }

    pub fn status(&self, id: JobId) -> Result<JobStatus> {
        self.store.status(id)
    }

    pub fn statuses(&self, ids: &[JobId]) -> Result<Vec<JobStatus>> {
        ids.iter().map(|id| self.store.status(*id)).collect()
    }

    /// The command a job runs, as it was recorded. `None` for a barrier.
    pub fn command(&self, id: JobId) -> Result<Option<crate::job::Command>> {
        self.store.command_of(id)
    }

    pub fn list(&self, filter: Filter) -> Result<Vec<JobStatus>> {
        self.store.list(filter)
    }

    /// Ids whose dependencies have all settled successfully — what a worker
    /// would consider claiming next.
    pub fn runnable(&self) -> Result<Vec<JobId>> {
        self.store.runnable()
    }

    /// Block until every id has settled, or the timeout expires.
    ///
    /// This is the explicit wait: the only call in the crate that blocks on
    /// other people's work.
    pub fn wait(&self, _ids: &[JobId], _timeout: Option<Duration>) -> Result<Vec<JobStatus>> {
        Err(Error::NotImplemented("Queue::wait"))
    }

    /// Where this job's output went. Present once an attempt has committed.
    pub fn logs(&self, id: JobId) -> Result<Logs> {
        let dir = self.store.log_dir(id);
        let attempt = self.store.status(id)?.attempt.max(1);
        Ok(Logs {
            stdout: dir.join(format!("{attempt}.out")),
            stderr: dir.join(format!("{attempt}.err")),
        })
    }

    pub fn cancel(&self, _id: JobId) -> Result<()> {
        Err(Error::NotImplemented("Queue::cancel"))
    }

    /// Sweep the store: reclaim rotted leases, kill orphans, finish handoffs
    /// a dying worker left unfulfilled. Any process may call it, and workers
    /// call it as they start.
    pub fn reap(&self) -> Result<ReapReport> {
        Err(Error::NotImplemented("Queue::reap"))
    }
}

/// Where the default store lives: `$PEKAREN_STORE` when set, otherwise
/// `~/.pekaren`. Resolved on every call, so a script that sets the variable
/// before opening gets what it set.
///
/// One definition, because a store nobody can find is the same as no store:
/// every caller that has no path of its own asks here.
pub fn default_store_path() -> PathBuf {
    match std::env::var_os("PEKAREN_STORE") {
        Some(dir) if !dir.is_empty() => expand_tilde(Path::new(&dir)),
        _ => expand_tilde(Path::new("~/.pekaren")),
    }
}

fn expand_tilde(path: &Path) -> PathBuf {
    let Ok(rest) = path.strip_prefix("~") else {
        return path.to_path_buf();
    };
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(rest),
        None => path.to_path_buf(),
    }
}
