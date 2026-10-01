//! Commit pinning: a job runs in a checkout of the commit it was submitted
//! at, whatever has happened to the working tree since. Real git, against
//! real repositories in a temp dir.

use std::path::{Path, PathBuf};
use std::time::Duration;

use pekaren::prelude::*;
use pekaren::{Capacity, Error, Worker};

fn queue(dir: &Path) -> Queue {
    Queue::options().work_on_submit(false).open(dir).unwrap()
}

fn worker(q: &Queue) -> Worker<'_> {
    Worker::new(q)
        .capacity(Capacity {
            cpus: 4,
            mem_mb: 4096,
            gpus: vec![],
        })
        .poll_interval(Duration::from_millis(10))
}

fn git(dir: &Path, args: &[&str]) -> String {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["-c", "user.name=test", "-c", "user.email=test@localhost"])
        .args(args)
        .output()
        .unwrap();
    assert!(out.status.success(), "git {args:?}: {out:?}");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// A repository with one commit: a tool that says which version it is,
/// and a subdirectory.
fn repo(dir: &Path) -> PathBuf {
    let repo = dir.join("repo");
    std::fs::create_dir_all(repo.join("sub")).unwrap();
    git(&repo, &["init", "-q"]);
    std::fs::write(repo.join("tool.sh"), "echo version 1\n").unwrap();
    std::fs::write(repo.join("sub/keep"), "").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-qm", "v1"]);
    repo
}

fn commit_v2(repo: &Path) {
    std::fs::write(repo.join("tool.sh"), "echo version 2\n").unwrap();
    git(repo, &["commit", "-qam", "v2"]);
}

fn stdout(q: &Queue, id: JobId) -> String {
    std::fs::read_to_string(q.logs(id).unwrap().stdout)
        .unwrap()
        .trim()
        .to_string()
}

fn in_repo(repo: &Path, line: &str) -> Job {
    Job::run(Command::line(line).cwd(repo))
}

#[test]
fn a_pinned_job_runs_the_commit_it_was_submitted_at() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(&dir.path().join("store"));
    let repo = repo(dir.path());
    let v1 = git(&repo, &["rev-parse", "HEAD"]);

    let pinned = q
        .submit(in_repo(&repo, "sh tool.sh").pin(Pin::head()))
        .unwrap();
    let unpinned = q.submit(in_repo(&repo, "sh tool.sh")).unwrap();
    // The tree moves on before anything runs.
    commit_v2(&repo);
    worker(&q).run_until_idle().unwrap();

    assert_eq!(stdout(&q, pinned), "version 1");
    assert_eq!(stdout(&q, unpinned), "version 2");
    let pin = q.status(pinned).unwrap().pin.expect("recorded");
    assert_eq!(pin.commit, v1);
    assert_eq!(pin.repo.file_name().unwrap(), ".git");
    assert_eq!(pin.dir, PathBuf::new());

    // A later job pinned to the same repository reuses the lane.
    let later = q
        .submit(in_repo(&repo, "sh tool.sh").pin(Pin::head()))
        .unwrap();
    worker(&q).run_until_idle().unwrap();
    assert_eq!(stdout(&q, later), "version 2");
    let lanes = std::fs::read_dir(dir.path().join("store/lanes")).unwrap();
    assert_eq!(lanes.count(), 1);
}

#[test]
fn a_pinned_job_runs_where_it_would_have_in_the_tree() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(&dir.path().join("store"));
    let repo = repo(dir.path());
    let head = git(&repo, &["rev-parse", "HEAD"]);

    let id = q
        .submit(in_repo(&repo.join("sub"), "pwd; echo $PEKAREN_PIN_COMMIT").pin(Pin::head()))
        .unwrap();
    worker(&q).run_until_idle().unwrap();

    let out = stdout(&q, id);
    let mut lines = out.lines();
    let cwd = PathBuf::from(lines.next().unwrap());
    assert_eq!(cwd.file_name().unwrap(), "sub");
    assert!(
        cwd.starts_with(dir.path().join("store/lanes")),
        "{}",
        cwd.display()
    );
    assert_eq!(lines.next(), Some(head.as_str()));
    assert_eq!(q.status(id).unwrap().pin.unwrap().dir, PathBuf::from("sub"));
}

#[test]
fn a_tree_with_uncommitted_changes_is_refused_unless_asked() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(&dir.path().join("store"));
    let repo = repo(dir.path());

    // Untracked files are not the commit's business.
    std::fs::write(repo.join("scratch.txt"), "notes").unwrap();
    q.submit(in_repo(&repo, "true").pin(Pin::head())).unwrap();

    // An edit to a tracked file is: the job would silently not see it.
    std::fs::write(repo.join("tool.sh"), "echo edited\n").unwrap();
    match q.submit(in_repo(&repo, "true").pin(Pin::head())) {
        Err(Error::Pin { reason, .. }) => assert!(reason.contains("tool.sh"), "{reason}"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    let dirty = q
        .submit(in_repo(&repo, "sh tool.sh").pin(Pin::head().allow_dirty()))
        .unwrap();
    // Naming a revision says which commit, so the tree does not matter.
    let named = q
        .submit(in_repo(&repo, "sh tool.sh").pin(Pin::rev("HEAD")))
        .unwrap();
    worker(&q).run_until_idle().unwrap();
    assert_eq!(stdout(&q, dirty), "version 1");
    assert_eq!(stdout(&q, named), "version 1");

    // Nothing to pin at all.
    assert!(matches!(
        q.submit(in_repo(&repo, "true").pin(Pin::rev("no-such-branch"))),
        Err(Error::Pin { .. })
    ));
    assert!(matches!(
        q.submit(in_repo(dir.path(), "true").pin(Pin::head())),
        Err(Error::Pin { .. })
    ));
}

#[test]
fn jobs_pinned_to_one_repository_take_turns_with_its_checkout() {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    let q = queue(&store);
    let repo = repo(dir.path());

    let first = q
        .submit(in_repo(&repo, "sleep 0.5").pin(Pin::head()))
        .unwrap();
    let second = q
        .submit(in_repo(&repo, "sleep 0.1").pin(Pin::head()))
        .unwrap();
    let other = q.submit(in_repo(&repo, "true")).unwrap();

    let runner = std::thread::spawn(move || {
        let q = queue(&store);
        worker(&q).run_one().unwrap()
    });
    while q.status(first).unwrap().state != State::Running {
        std::thread::sleep(Duration::from_millis(10));
    }

    // The lane is busy: a strict worker waits for the second pinned job,
    // and the default one takes the unpinned job past it.
    assert_eq!(worker(&q).strict_fifo(true).run_one().unwrap(), None);
    assert_eq!(worker(&q).run_one().unwrap(), Some(other));
    assert_eq!(q.status(second).unwrap().state, State::Ready);

    assert_eq!(runner.join().unwrap(), Some(first));
    assert_eq!(worker(&q).run_one().unwrap(), Some(second));
    let (a, b) = (q.status(first).unwrap(), q.status(second).unwrap());
    assert!(b.started_at.unwrap() >= a.finished_at.unwrap());
}

#[test]
fn a_checkout_that_cannot_be_made_fails_the_job() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(&dir.path().join("store"));
    let repo = repo(dir.path());

    let id = q
        .submit(in_repo(&repo, "echo ran").pin(Pin::head()))
        .unwrap();
    // The repository is gone by the time a worker gets to the job.
    std::fs::remove_dir_all(&repo).unwrap();
    worker(&q).run_until_idle().unwrap();

    let s = q.status(id).unwrap();
    assert_eq!(s.state, State::Failed);
    assert!(s.failure.unwrap().starts_with("checking out"));
    let logs = q.logs(id).unwrap();
    assert!(!logs.stdout.exists(), "the command must not have run");
    // What git said is where the job's stderr would be.
    assert!(!std::fs::read_to_string(logs.stderr).unwrap().is_empty());
}
