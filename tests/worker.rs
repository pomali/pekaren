//! Execution: claim, supervise, commit. These run real child processes
//! against a real store, because the interesting failures are exactly the
//! ones a mock would paper over.

use std::time::Duration;

use pekaren::prelude::*;
use pekaren::{Filter, KillAfter, Worker};

fn queue(dir: &std::path::Path) -> Queue {
    Queue::options().work_on_submit(false).open(dir).unwrap()
}

fn worker(q: &Queue) -> Worker<'_> {
    Worker::new(q)
        .capacity(pekaren::Capacity {
            cpus: 4,
            mem_mb: 4096,
            gpus: vec![0, 1],
        })
        .poll_interval(Duration::from_millis(10))
}

#[test]
fn runs_a_job_and_records_what_happened() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());

    let id = q.submit(Job::cmd("echo hello from the oven")).unwrap();
    assert_eq!(worker(&q).run_one().unwrap(), Some(id));

    let s = q.status(id).unwrap();
    assert_eq!(s.state, State::Done);
    assert_eq!(s.exit_code, Some(0));
    assert_eq!(s.attempt, 1);
    assert!(s.started_at.is_some() && s.finished_at.is_some());

    let logs = q.logs(id).unwrap();
    assert_eq!(
        std::fs::read_to_string(&logs.stdout).unwrap().trim(),
        "hello from the oven"
    );

    // Nothing left claimable, and nothing left leased.
    assert_eq!(worker(&q).run_one().unwrap(), None);
    assert!(q.runnable().unwrap().is_empty());
}

#[test]
fn a_failing_job_fails_and_a_retryable_one_runs_again() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());

    let once = q.submit(Job::cmd("echo nope >&2; exit 3")).unwrap();
    let twice = q
        .submit(Job::cmd("exit 1").retries(1).idempotent(true))
        .unwrap();

    worker(&q).run_until_idle().unwrap();

    let s = q.status(once).unwrap();
    assert_eq!(s.state, State::Failed);
    assert_eq!(s.exit_code, Some(3));
    let logs = q.logs(once).unwrap();
    assert_eq!(
        std::fs::read_to_string(&logs.stderr).unwrap().trim(),
        "nope"
    );

    // Ran twice — the first attempt put it back in the pool — then gave up.
    let s = q.status(twice).unwrap();
    assert_eq!(s.state, State::Failed);
    assert_eq!(s.attempt, 2);
}

#[test]
fn a_barrier_settles_when_its_dependencies_do() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());

    let a = q.submit(Job::cmd("true")).unwrap();
    let b = q.submit(Job::cmd("true")).unwrap();
    let done = q.submit(Job::barrier().after([a, b])).unwrap();

    let bad = q.submit(Job::cmd("false")).unwrap();
    let fast = q.submit(Job::barrier().after([bad])).unwrap();
    let patient = q
        .submit(Job::barrier().after([bad, a]).policy(FailAtEnd))
        .unwrap();

    worker(&q).run_until_idle().unwrap();

    assert_eq!(q.status(done).unwrap().state, State::Done);
    assert_eq!(q.status(fast).unwrap().state, State::Failed);
    assert_eq!(q.status(patient).unwrap().state, State::Failed);
    assert!(q.list(Filter::Unsettled).unwrap().is_empty());
}

#[test]
fn a_job_that_will_not_end_is_killed_at_its_cap() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());

    let id = q
        .submit(Job::cmd("sleep 30").kill_after(KillAfter::Fixed(Duration::from_millis(300))))
        .unwrap();
    worker(&q).run_one().unwrap();

    let s = q.status(id).unwrap();
    assert_eq!(s.state, State::Failed);
    assert!(
        q.events(id)
            .unwrap()
            .iter()
            .any(|e| e.message.contains("past its cap")),
        "{:?}",
        q.events(id).unwrap()
    );
}

#[test]
fn a_rotted_lease_is_reclaimed_and_the_job_runs_again() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());
    let id = q
        .submit(Job::cmd("echo second time").idempotent(true).retries(1))
        .unwrap();

    // A worker that took the job and died without renewing: leave the
    // lease behind, already expired.
    {
        let conn = rusqlite::Connection::open(dir.path().join("store.db")).unwrap();
        conn.execute(
            "UPDATE jobs SET state = 'running', attempt = 1, lease_token = 'dead-worker',
                 lease_host = '', lease_expires_at = 1
             WHERE id = ?1",
            [id.get()],
        )
        .unwrap();
    }
    assert_eq!(q.status(id).unwrap().state, State::Running);

    let report = q.reap().unwrap();
    assert_eq!(report.leases_reclaimed, vec![id]);
    assert_eq!(q.status(id).unwrap().state, State::Ready);

    worker(&q).run_until_idle().unwrap();
    assert_eq!(q.status(id).unwrap().state, State::Done);
}

#[test]
fn a_job_whose_code_changed_fails_without_running() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());
    let marker = dir.path().join("ran");
    let script = dir.path().join("thing.sh");
    std::fs::write(&script, "#!/bin/sh\ntrue\n").unwrap();

    let id = q
        .submit(Job::cmd(format!("touch {}", marker.display())).watch_as(&script, OnChange::Fail))
        .unwrap();
    std::fs::write(&script, "#!/bin/sh\nfalse\n").unwrap();

    worker(&q).run_until_idle().unwrap();

    assert_eq!(q.status(id).unwrap().state, State::Failed);
    assert!(!marker.exists(), "the job must not have run");
    assert!(
        q.events(id)
            .unwrap()
            .iter()
            .any(|e| e.message.contains("changed since submit"))
    );
}

#[test]
fn wait_returns_once_everything_has_settled() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());
    let a = q.submit(Job::cmd("sleep 0.1")).unwrap();
    let barrier = q.submit(Job::barrier().after([a])).unwrap();

    // Work happens in another thread, the way it does in another process.
    let path = dir.path().to_path_buf();
    let runner = std::thread::spawn(move || {
        let q = Queue::options().work_on_submit(false).open(&path).unwrap();
        worker(&q).run_until_idle().unwrap();
    });

    let settled = q
        .wait(&[a, barrier], Some(Duration::from_secs(30)))
        .unwrap();
    assert!(settled.iter().all(|s| s.state == State::Done));
    runner.join().unwrap();
}

#[test]
fn the_grace_window_returns_a_trivial_failure_to_the_submitter() {
    let dir = tempfile::tempdir().unwrap();
    let q = Queue::options()
        .grace(Duration::from_secs(10))
        .open(dir.path())
        .unwrap();

    let err = q
        .submit(Job::cmd("exec /no/such/binary --train"))
        .expect_err("dies inside the window");
    match err {
        pekaren::Error::EarlyFailure { stderr_tail, .. } => {
            assert!(stderr_tail.contains("not found"), "{stderr_tail}");
        }
        other => panic!("wrong error: {other}"),
    }

    // A job that survives the window comes back as a handle, not an error.
    let id = q.submit(Job::cmd("sleep 0.3")).unwrap();
    q.wait(&[id], Some(Duration::from_secs(30))).unwrap();
    assert_eq!(q.status(id).unwrap().state, State::Done);
}
