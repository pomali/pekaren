use std::path::PathBuf;

use crate::id::JobId;

/// The one error type the crate returns.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("store {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// The store was written by a newer build. Migrating forwards is fine;
    /// migrating backwards is not, so this is a hard stop.
    #[error("store {path} speaks schema v{found}, this build speaks v{expected}: upgrade pekaren")]
    SchemaTooNew {
        path: PathBuf,
        found: i32,
        expected: i32,
    },

    #[error("no such job: {0}")]
    NoSuchJob(JobId),

    /// A submitted job named a dependency the store does not hold. Since
    /// handles are values and the graph is built by value, this is the only
    /// way to get a malformed DAG: a cycle is unreachable because a job can
    /// only depend on ids that already exist.
    #[error("job depends on {0}, which this store does not hold")]
    UnknownDependency(JobId),

    /// The job died inside the grace window, while the submitting agent still
    /// holds the thread. Almost always a bad path or a missing binary.
    #[error("job {id} failed within the grace window (exit {exit:?}): {stderr_tail}")]
    EarlyFailure {
        id: JobId,
        exit: Option<i32>,
        stderr_tail: String,
    },

    /// The declaration cannot be met by this host, so the job would wait
    /// forever. Caught at submit rather than at claim.
    #[error("job declares {want} {resource}, host has {have}")]
    Unsatisfiable {
        resource: &'static str,
        want: u64,
        have: u64,
    },

    #[error("timed out waiting for {0} job(s)")]
    WaitTimeout(usize),

    /// This process was started to run a task the binary does not
    /// register. Almost always a binary older or newer than the one that
    /// submitted the job.
    #[error("this binary does not register a task called {0:?}")]
    UnknownTask(String),

    /// A task asked for its context outside a job process.
    #[error("not running as a job: {0} is not set")]
    NotAJobProcess(&'static str),

    /// A path the job depends on is not what it was at submit time, and the
    /// job said that should stop it.
    #[error("job {id} depends on {path}, which changed since it was submitted")]
    InputChanged { id: JobId, path: std::path::PathBuf },

    /// Declared in milestone 1, implemented in milestone 2.
    #[error("not implemented yet: {0}")]
    NotImplemented(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub(crate) fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Error::Io {
            path: path.into(),
            source,
        }
    }
}
