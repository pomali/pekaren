//! Jobs that are functions in the submitting binary.
//!
//! A job has to outlive the process that submitted it, so the store can only
//! hold a pointer to code that exists on disk — never the code itself. A
//! task is the narrowest useful pointer: *this binary, this named function*.
//! The function stays ordinary compiled Rust, checked by the compiler and
//! visible to every tool that reads the crate, and the store holds the
//! binary's path and the name.
//!
//! ```no_run
//! use pekaren::prelude::*;
//!
//! fn train(ctx: &JobCtx) -> TaskResult {
//!     println!("training with lr={}", ctx.arg(0).unwrap_or("1e-3"));
//!     Ok(())
//! }
//!
//! fn main() -> Result<()> {
//!     let mut tasks = Tasks::new();
//!     let train = tasks.add("train", train);
//!     tasks.bootstrap()?; // a worker running this binary stops here
//!
//!     let q = Queue::open_default()?;
//!     for lr in ["1e-3", "3e-4"] {
//!         q.submit(Job::task(train).arg(lr).gpus(1).est_minutes(40))?;
//!     }
//!     Ok(())
//! }
//! ```
//!
//! `train` there is the handle `add` returned, not the string `"train"`: a
//! task that was never registered cannot be submitted, and a renamed one
//! stops compiling rather than failing an hour later.

use std::process::ExitCode;

use crate::error::{Error, Result};
use crate::id::JobId;
use crate::job::Job;
use crate::queue::Queue;

/// What a task function returns. Any error type works, `anyhow::Error`
/// included, so a task body reads like any other fallible function.
pub type TaskResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// A function a job can be.
pub type TaskFn = fn(&JobCtx) -> TaskResult;

/// Names the child process reads to find out it is a job rather than a
/// submitting script.
pub const TASK_ENV: &str = "PEKAREN_TASK";
/// The job the child is running, as `JobId` renders it.
pub const JOB_ENV: &str = "PEKAREN_JOB";

/// A registered task. Copy, so it is passed around like a `JobId`.
///
/// The only way to get one is [`Tasks::add`], which is what makes
/// [`Job::task`] impossible to call with a name nothing answers to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Task {
    name: &'static str,
}

impl Task {
    pub fn name(self) -> &'static str {
        self.name
    }

    /// A job that runs this task. Same as [`Job::task`].
    pub fn job(self) -> Job {
        Job::task(self)
    }
}

/// What a task can see about the job it is running.
pub struct JobCtx {
    queue: Queue,
    job: JobId,
    args: Vec<String>,
}

impl JobCtx {
    pub fn job(&self) -> JobId {
        self.job
    }

    /// The arguments the submitter attached with [`Job::arg`].
    pub fn args(&self) -> &[String] {
        &self.args
    }

    pub fn arg(&self, index: usize) -> Option<&str> {
        self.args.get(index).map(String::as_str)
    }

    /// The store this job came out of. A task can submit more work, and the
    /// new jobs join the same graph.
    pub fn queue(&self) -> &Queue {
        &self.queue
    }
}

/// The tasks a binary knows how to run.
///
/// Build it at the top of `main`, register every task, then call
/// [`bootstrap`](Tasks::bootstrap). In a submitting run, `bootstrap`
/// returns and the rest of `main` runs as usual. In a run a worker started,
/// it dispatches the named task and exits the process, so nothing below it
/// executes.
#[derive(Default)]
pub struct Tasks {
    registered: Vec<(&'static str, TaskFn)>,
}

impl Tasks {
    pub fn new() -> Self {
        Tasks::default()
    }

    /// Register a function under a name and get the handle to submit it
    /// with. Registering the same name twice panics: it is a bug in the
    /// binary, and the alternative is jobs silently running the wrong code.
    pub fn add(&mut self, name: &'static str, f: TaskFn) -> Task {
        assert!(
            !self.registered.iter().any(|(n, _)| *n == name),
            "task {name:?} registered twice"
        );
        self.registered.push((name, f));
        Task { name }
    }

    /// Whether this process was started to run a task, and which one.
    pub fn assigned_task() -> Option<String> {
        std::env::var(TASK_ENV).ok().filter(|s| !s.is_empty())
    }

    /// Run the assigned task and exit, or return so the caller carries on
    /// submitting.
    ///
    /// Call it once, after the last [`add`](Tasks::add) and before anything
    /// a job should not repeat.
    pub fn bootstrap(&self) -> Result<()> {
        match self.run_assigned()? {
            None => Ok(()),
            Some(code) => {
                // The task is the whole of this process's job. Nothing below
                // the bootstrap call in main is meant for it.
                std::process::exit(match code {
                    ExitCode::SUCCESS => 0,
                    _ => 1,
                })
            }
        }
    }

    /// [`bootstrap`](Tasks::bootstrap) without the exit: `Ok(None)` when this
    /// process is not running a task, otherwise the code it would exit with.
    /// Useful in tests, and in a `main` that has its own teardown.
    pub fn run_assigned(&self) -> Result<Option<ExitCode>> {
        let Some(name) = Tasks::assigned_task() else {
            return Ok(None);
        };
        let Some((_, f)) = self.registered.iter().find(|(n, _)| *n == name) else {
            return Err(Error::UnknownTask(name));
        };

        let ctx = JobCtx::from_env()?;
        match f(&ctx) {
            Ok(()) => Ok(Some(ExitCode::SUCCESS)),
            Err(e) => {
                // The worker reads the exit code; a human reads this.
                eprintln!("task {name}: {e}");
                Ok(Some(ExitCode::FAILURE))
            }
        }
    }
}

impl JobCtx {
    /// Rebuild the context from what the worker put in the environment.
    fn from_env() -> Result<JobCtx> {
        let raw = std::env::var(JOB_ENV).map_err(|_| Error::NotAJobProcess(JOB_ENV))?;
        let job: JobId = raw.parse().map_err(|_| Error::NotAJobProcess(JOB_ENV))?;
        let queue = Queue::open_default()?;
        let args = queue.task_args(job)?;
        Ok(JobCtx { queue, job, args })
    }
}
