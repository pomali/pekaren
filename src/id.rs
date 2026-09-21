use std::fmt;
use std::str::FromStr;

/// A handle to a submitted job.
///
/// Small and `Copy`: the DAG is built by passing these around, and nothing
/// blocks until the caller explicitly waits. It is the SQLite rowid, so it is
/// stable, monotonic within a store, and meaningless across stores.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct JobId(pub(crate) i64);

impl JobId {
    /// The underlying rowid, for printing or passing through a shell.
    pub fn get(self) -> i64 {
        self.0
    }

    pub(crate) fn new(raw: i64) -> Self {
        JobId(raw)
    }
}

impl fmt::Display for JobId {
    /// Rendered `j42`, so an id survives a round trip through argv and log
    /// lines without looking like any other integer.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "j{}", self.0)
    }
}

/// Accepts both `j42` and `42`.
impl FromStr for JobId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.strip_prefix('j').unwrap_or(s).parse().map(JobId)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_text() {
        let id = JobId::new(42);
        assert_eq!(id.to_string(), "j42");
        assert_eq!("j42".parse::<JobId>().unwrap(), id);
        assert_eq!("42".parse::<JobId>().unwrap(), id);
    }
}
