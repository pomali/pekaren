//! Pekáreň — a daemon-free job queue for long-running agent work.
//!
//! State lives in a directory on disk, backed by SQLite in WAL mode. There is
//! no broker and no background service: every process that opens the store is
//! a client and potentially a worker. See `docs/design.md` for the model and
//! `docs/adr/` for the decisions behind this API.
//!
//! ```no_run
//! use pekaren::prelude::*;
//!
//! # fn main() -> pekaren::Result<()> {
//! let q = Queue::open_default()?;
//! let runs: Vec<JobId> = [1e-3, 3e-4, 1e-4]
//!     .iter()
//!     .map(|lr| {
//!         q.submit(
//!             Job::cmd(format!("python train.py --lr {lr}"))
//!                 .gpus(1)
//!                 .est_minutes(40)
//!                 .eval_prompt("Loss should drop below 0.3"),
//!         )
//!     })
//!     .collect::<Result<_>>()?;
//! q.submit(
//!     Job::barrier()
//!         .after(&runs)
//!         .policy(FailAtEnd)
//!         .on_done("pec-evaluate --sweep lr"),
//! )?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Milestone
//!
//! This is milestone 1: the public interface, the store schema, and submit /
//! read paths. Execution (claim, lease renewal, commit, handoff, profiling)
//! is declared here and implemented next; those entry points return
//! [`Error::NotImplemented`] rather than pretending to work.

mod error;
mod id;
mod job;
mod profile;
mod queue;
mod store;
mod worker;

pub use error::{Error, Result};
pub use id::JobId;
pub use job::{Command, FailurePolicy, Job, JobKind, KillAfter, Resources, Wake};
pub use profile::{Profile, Sample};
pub use queue::{
    Filter, JobStatus, Logs, Queue, QueueOptions, ReapReport, State, default_store_path,
};
pub use worker::{Capacity, Worker, WorkerReport};

/// Everything a submitting script needs, including the bare
/// [`FailurePolicy`] variants so `.policy(FailAtEnd)` reads as written.
pub mod prelude {
    pub use crate::FailurePolicy::*;
    pub use crate::{
        Command, FailurePolicy, Job, JobId, JobStatus, Queue, Resources, Result, State, Wake,
        default_store_path,
    };
}

/// Schema version this build speaks. A store written by a newer build is
/// refused rather than migrated backwards.
pub const SCHEMA_VERSION: i32 = 1;
