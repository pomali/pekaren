//! Who submitted the work, so the handoff can reach them.
//!
//! A job that finishes an hour later is worth nothing if nobody hears about
//! it. The cheapest listener is the session that submitted it, still open:
//! waking it costs no new context, because its context never went away.
//!
//! Claude Code puts a session's own identity in the environment of every
//! command it runs, so a submitting script knows who it is without being
//! told and without a hook:
//!
//! | Variable | What |
//! | --- | --- |
//! | `CLAUDE_CODE_SESSION_ID` | the session, for resuming it later |
//! | `CLAUDE_CODE_MESSAGING_SOCKET` | its inbox, while it is alive |
//!
//! Pekáreň records both at submit time and **never** records
//! `CLAUDE_CODE_MESSAGING_TOKEN`. A store full of session tokens would be a
//! liability out of all proportion to what it buys: on macOS and Linux the
//! auth line that token signs is optional anyway.

use std::path::{Path, PathBuf};

/// The session a job was submitted from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Submitter {
    /// The session id, which `claude --resume` takes.
    pub session: String,
    /// Its inbox socket. Present while the session was alive at submit
    /// time; whether it still is, is what [`is_live`](Submitter::is_live)
    /// answers.
    pub socket: Option<PathBuf>,
    pub cwd: Option<PathBuf>,
}

impl Submitter {
    /// Read the submitting session out of the environment, if this process
    /// is running inside one.
    pub fn from_env() -> Option<Submitter> {
        let session = std::env::var("CLAUDE_CODE_SESSION_ID")
            .ok()
            .filter(|s| !s.is_empty())?;
        Some(Submitter {
            session,
            socket: std::env::var_os("CLAUDE_CODE_MESSAGING_SOCKET")
                .filter(|s| !s.is_empty())
                .map(PathBuf::from),
            cwd: std::env::current_dir().ok(),
        })
    }

    /// Whether that session is still listening.
    ///
    /// Answered by connecting to the socket and hanging up, because the
    /// file outliving the session is exactly the case this has to catch: a
    /// stale path refuses the connection, a live one accepts it. Nothing is
    /// written, so nothing is delivered.
    pub fn is_live(&self) -> bool {
        self.socket.as_deref().is_some_and(socket_accepts)
    }
}

#[cfg(unix)]
fn socket_accepts(path: &Path) -> bool {
    use std::os::unix::net::UnixStream;
    match UnixStream::connect(path) {
        Ok(stream) => {
            drop(stream);
            true
        }
        Err(_) => false,
    }
}

#[cfg(not(unix))]
fn socket_accepts(path: &Path) -> bool {
    // Named pipes on Windows, once there is something to test it against.
    path.exists()
}
