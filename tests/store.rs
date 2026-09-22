//! Milestone 1 is worth only as much as the store behind it, so these tests
//! go through the real SQLite file rather than a mock.

use std::time::Duration;

use pekaren::prelude::*;
use pekaren::{Error, Filter};

fn temp_queue() -> (tempfile::TempDir, Queue) {
    let dir = tempfile::tempdir().expect("tempdir");
    let q = Queue::options()
        .work_on_submit(false)
        .open(dir.path())
        .expect("open store");
    (dir, q)
}

#[test]
fn submits_a_sweep_and_reads_it_back() {
    let (_dir, q) = temp_queue();

    let runs: Vec<JobId> = [1e-3, 3e-4]
        .iter()
        .map(|lr| {
            q.submit(
                Job::cmd(format!("python train.py --lr {lr}"))
                    .gpus(1)
                    .est_minutes(40)
                    .eval_prompt("Loss should drop below 0.3"),
            )
        })
        .collect::<Result<_>>()
        .expect("submit runs");

    let barrier = q
        .submit(
            Job::barrier()
                .after(&runs)
                .policy(FailAtEnd)
                .on_done("pec-evaluate --sweep lr"),
        )
        .expect("submit barrier");

    // Leaves are claimable at once; the barrier waits on them.
    for id in &runs {
        let s = q.status(*id).unwrap();
        assert_eq!(s.state, State::Ready);
        assert_eq!(s.resources.gpus, 1);
        assert_eq!(s.resources.est, Some(Duration::from_secs(40 * 60)));
        assert_eq!(s.eval_prompt.as_deref(), Some("Loss should drop below 0.3"));
        assert!(!s.is_barrier);
    }

    let b = q.status(barrier).unwrap();
    assert_eq!(b.state, State::Pending);
    assert!(b.is_barrier);
    assert_eq!(b.deps, runs);

    assert_eq!(q.runnable().unwrap(), runs);
    assert_eq!(q.list(Filter::Unsettled).unwrap().len(), 3);
    assert_eq!(q.list(Filter::State(State::Pending)).unwrap().len(), 1);
}

#[test]
fn rejects_a_dependency_the_store_does_not_hold() {
    let (_dir, q) = temp_queue();
    let real = q.submit(Job::cmd("true")).unwrap();
    let ghost = "j9999".parse::<JobId>().unwrap();

    let err = q
        .submit(Job::barrier().after([real, ghost]))
        .expect_err("unknown dependency");
    assert!(matches!(err, Error::UnknownDependency(id) if id == ghost));

    // The failed submit left nothing behind.
    assert_eq!(q.list(Filter::All).unwrap().len(), 1);
}

#[test]
fn a_second_process_sees_the_same_store() {
    let dir = tempfile::tempdir().unwrap();
    let id = {
        let q = Queue::options()
            .work_on_submit(false)
            .open(dir.path())
            .unwrap();
        q.submit(Job::cmd("echo hi").name("greeting")).unwrap()
    };

    let other = Queue::open(dir.path()).unwrap();
    let s = other.status(id).unwrap();
    assert_eq!(s.name.as_deref(), Some("greeting"));
    assert_eq!(s.state, State::Ready);
}

#[test]
fn store_is_in_wal_mode() {
    let (dir, _q) = temp_queue();
    // -wal appears next to the db as soon as the first write lands.
    assert!(dir.path().join("store.db").exists());
    assert!(dir.path().join("store.db-wal").exists());
}

#[test]
fn waiting_on_work_nobody_runs_times_out() {
    let (_dir, q) = temp_queue();
    let id = q.submit(Job::cmd("true")).unwrap();
    // No worker in this test, so the job never settles.
    match q.wait(&[id], Some(Duration::from_millis(50))) {
        Err(Error::WaitTimeout(1)) => {}
        other => panic!("expected a timeout naming one job, got {other:?}"),
    }
    assert_eq!(q.status(id).unwrap().state, State::Ready);
}

#[test]
fn opens_a_store_written_by_an_older_schema() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("store.db");

    // Build a v1 store by hand: strip everything v2 and v3 added.
    {
        let q = Queue::options()
            .work_on_submit(false)
            .open(dir.path())
            .unwrap();
        q.submit(Job::cmd("echo old")).unwrap();
    }
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(
            "ALTER TABLE jobs DROP COLUMN script_path;
             ALTER TABLE jobs DROP COLUMN task_name;
             DROP TABLE task_args;
             DROP TABLE job_inputs;
             DROP TABLE job_events;
             PRAGMA user_version = 1;",
        )
        .unwrap();
    }

    // Reopening runs both migrations in order, old rows intact.
    let q = Queue::options()
        .work_on_submit(false)
        .open(dir.path())
        .unwrap();
    assert_eq!(q.list(Filter::All).unwrap().len(), 1);
    let id = q.submit(Job::rust("fn main() {}")).unwrap();
    assert!(q.status(id).unwrap().script.is_some(), "v2 column works");
    // v3: the script is a watched input, hashed at submit.
    assert!(q.check_inputs(id).unwrap().is_empty());
    assert!(q.events(id).unwrap().is_empty());

    let conn = rusqlite::Connection::open(&db).unwrap();
    let version: i32 = conn
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .unwrap();
    assert_eq!(version, pekaren::SCHEMA_VERSION);
}
