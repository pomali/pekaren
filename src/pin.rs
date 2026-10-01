//! Commit pinning: run a job in a checkout of the commit it was submitted
//! at, rather than in a working tree that may have moved on by the time a
//! worker gets to it.
//!
//! This is the one place pekaren knows about git, and it is kept to this
//! module: resolving a [`Pin`] at submit time into a [`Pinned`] commit, and
//! preparing a checkout of it before the job runs. Everything else sees a
//! command job with three more columns.
//!
//! Checkouts live in *lanes* under `<store>/lanes`, one per repository,
//! reused from job to job so that a build in one is incremental. A lane is
//! claimed like a GPU: a pinned job is not claimed while another job pinned
//! to the same repository is running, so two jobs never share a checkout.
//! What a lane holds is disposable; delete it and the next job clones
//! again.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// Where git is, for reading the repository a job is pinned to:
/// `PEKAREN_GIT`, else `git`.
///
/// Under WSL a worktree on the Windows side needs `git.exe`: Linux git
/// cannot follow its `gitdir: C:/...` pointer, and over the 9p mount its
/// `status` re-hashes every file. The lanes themselves are always driven by
/// the `git` on `PATH`, since they are local to the worker.
pub const GIT_ENV: &str = "PEKAREN_GIT";

/// What to pin a job to. Resolved when the job is submitted, against the
/// directory its command runs in (or the submitter's, if it names none).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pin {
    rev: String,
    allow_dirty: bool,
}

impl Pin {
    /// The commit checked out now. Refuses a tree with uncommitted changes
    /// to tracked files, which the pinned job would silently not see;
    /// untracked files are not the commit's business and do not count.
    pub fn head() -> Pin {
        Pin {
            rev: "HEAD".into(),
            allow_dirty: false,
        }
    }

    /// A revision named explicitly: a sha, a tag, a branch as it is now.
    /// The working tree's state does not matter, since it was not asked
    /// for.
    pub fn rev(rev: impl Into<String>) -> Pin {
        Pin {
            rev: rev.into(),
            allow_dirty: true,
        }
    }

    /// Pin `HEAD` even though the tree has uncommitted changes, which the
    /// job will not see.
    pub fn allow_dirty(mut self) -> Pin {
        self.allow_dirty = true;
        self
    }
}

/// The commit a job was pinned to, as the store holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Pinned {
    /// The repository's object store, which the lane fetches from. For a
    /// linked worktree, that of the main repository.
    pub repo: PathBuf,
    /// The full commit id.
    pub commit: String,
    /// Where in the checkout the job runs: its directory relative to the
    /// top of the tree it was submitted from. Empty at the top.
    pub dir: PathBuf,
}

/// Turn a request into a commit, now, while the tree is the one the
/// submitter means.
pub(crate) fn resolve(pin: &Pin, tree: &Path) -> Result<Pinned> {
    let git = std::env::var(GIT_ENV).unwrap_or_else(|_| "git".into());
    let ask = |args: &[&str]| -> Result<String> {
        // Read-only against a tree someone may be editing: no optional
        // index refresh, so nothing is written to it.
        let out = std::process::Command::new(&git)
            .arg("--no-optional-locks")
            .args(args)
            .current_dir(tree)
            .output()
            .map_err(|e| Error::Pin {
                path: tree.to_path_buf(),
                reason: format!("{git}: {e}"),
            })?;
        if !out.status.success() {
            return Err(Error::Pin {
                path: tree.to_path_buf(),
                reason: format!(
                    "git {} failed: {}",
                    args.join(" "),
                    String::from_utf8_lossy(&out.stderr).trim()
                ),
            });
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    };

    let commit = ask(&["rev-parse", "--verify", &format!("{}^{{commit}}", pin.rev)])?;
    if !pin.allow_dirty {
        let changes = ask(&["status", "--porcelain", "--untracked-files=no"])?;
        if !changes.is_empty() {
            return Err(Error::Pin {
                path: tree.to_path_buf(),
                reason: format!(
                    "tracked files have uncommitted changes, which the job would not see; \
                     commit them or allow a dirty tree:\n{changes}"
                ),
            });
        }
    }
    let repo = ask(&["rev-parse", "--path-format=absolute", "--git-common-dir"])?;
    let dir = ask(&["rev-parse", "--show-prefix"])?;
    Ok(Pinned {
        repo: local_path(&repo),
        commit,
        dir: PathBuf::from(dir.trim_end_matches('/')),
    })
}

/// A path git reported, as this process can open it. A Windows git run
/// from WSL answers `C:/Users/...`; `wslpath` knows where that is mounted.
/// Anything else is taken as it came.
fn local_path(reported: &str) -> PathBuf {
    let bytes = reported.as_bytes();
    let drive = bytes.len() > 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':';
    if cfg!(unix) && drive {
        let out = std::process::Command::new("wslpath")
            .args(["-u", reported])
            .output();
        if let Ok(out) = out {
            if out.status.success() {
                return PathBuf::from(String::from_utf8_lossy(&out.stdout).trim());
            }
        }
    }
    PathBuf::from(reported)
}

/// The lane for a repository: named after it so a human can tell lanes
/// apart, and keyed by a hash of its path, stable across builds, so the
/// same repository always lands in the same lane.
pub(crate) fn lane_name(repo: &Path) -> String {
    // `engine/.git` is the engine; a bare `engine.git` is too.
    let named = if repo.file_name().is_some_and(|n| n == ".git") {
        repo.parent().unwrap_or(repo)
    } else {
        repo
    };
    let name: String = named
        .file_name()
        .map(|n| n.to_string_lossy().trim_end_matches(".git").to_string())
        .unwrap_or_default()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    // FNV-1a: DefaultHasher may change between Rust releases, and a lane
    // that moved is a fresh clone.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in repo.to_string_lossy().bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{name}-{hash:016x}")
}

/// The git commands that bring `lane` to the pinned commit, each to be run
/// in turn until one fails: clone on first use, fetch the commit unless
/// the lane has it, check it out over whatever the last job left, and
/// remove untracked files — but not ignored ones, so a build directory
/// survives from job to job.
///
/// `has_commit` is asked once the lane exists, so a commit fetched before
/// is not fetched again.
pub(crate) fn checkout_steps(
    pinned: &Pinned,
    lane: &Path,
    mut has_commit: impl FnMut(&mut std::process::Command) -> bool,
) -> Vec<std::process::Command> {
    let source = format!("file://{}", pinned.repo.display());
    let git = |args: &[&str]| {
        let mut cmd = std::process::Command::new("git");
        cmd.arg("-C").arg(lane).args(args);
        cmd
    };
    let mut steps = Vec::new();
    let cloned = lane.join(".git").exists();
    if !cloned {
        let mut clone = std::process::Command::new("git");
        clone
            .args(["clone", "--quiet", "--no-checkout", &source])
            .arg(lane);
        steps.push(clone);
    }
    let present = cloned
        && has_commit(&mut git(&[
            "cat-file",
            "-e",
            &format!("{}^{{commit}}", pinned.commit),
        ]));
    if !present {
        steps.push(git(&["fetch", "--quiet", &source, &pinned.commit]));
    }
    steps.push(git(&[
        "-c",
        "advice.detachedHead=false",
        "checkout",
        "--quiet",
        "--force",
        "--detach",
        &pinned.commit,
    ]));
    steps.push(git(&["clean", "--quiet", "-fd"]));
    steps
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lane_is_named_after_its_repository_and_stays_put() {
        let a = lane_name(Path::new("/home/me/src/engine/.git"));
        assert!(a.starts_with("engine-"), "{a}");
        assert_eq!(a, lane_name(Path::new("/home/me/src/engine/.git")));
        assert_ne!(a, lane_name(Path::new("/elsewhere/engine/.git")));
        assert!(lane_name(Path::new("/srv/tools.git")).starts_with("tools-"));
        assert!(lane_name(Path::new("/srv/we ird")).starts_with("we_ird-"));
    }

    #[test]
    fn only_a_drive_path_is_translated() {
        assert_eq!(local_path("/home/me/.git"), PathBuf::from("/home/me/.git"));
        assert_eq!(local_path("relative/.git"), PathBuf::from("relative/.git"));
    }
}
