//! The handoff: when a node settles, its wake command is spawned once,
//! however many processes are watching the store.

use std::path::Path;
use std::time::{Duration, Instant, SystemTime};

use pekaren::prelude::*;
use pekaren::{Capacity, Worker};

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

/// A wake runs detached, so give it a moment to have written its line.
fn lines_eventually(path: &Path, want: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let lines: Vec<String> = std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect();
        if lines.len() >= want || Instant::now() >= deadline {
            return lines;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn a_settled_barrier_wakes_once_however_many_workers_saw_it() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());
    let woke = dir.path().join("woke");

    let runs: Vec<JobId> = (0..6)
        .map(|_| q.submit(Job::cmd("sleep 0.05")).unwrap())
        .collect();
    let barrier = q
        .submit(
            Job::barrier()
                .after(&runs)
                .eval_prompt("all six slept")
                .on_done(format!(
                    "echo \"$PEKAREN_JOB $PEKAREN_STATE [$PEKAREN_EVAL_PROMPT]\" >> {}",
                    woke.display()
                )),
        )
        .unwrap();

    // Three workers race through the same store, each in its own thread
    // with its own connection, the way separate processes would.
    let threads: Vec<_> = (0..3)
        .map(|_| {
            let path = dir.path().to_path_buf();
            std::thread::spawn(move || {
                let q = queue(&path);
                worker(&q).run_until_idle().unwrap();
                // Sweep again after the fact, as every later process does.
                q.reap().unwrap();
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    q.reap().unwrap();

    assert_eq!(q.status(barrier).unwrap().state, State::Done);
    let lines = lines_eventually(&woke, 1);
    // Anything more would have landed by now as well.
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        std::fs::read_to_string(&woke).unwrap().lines().count(),
        1,
        "{lines:?}"
    );
    assert_eq!(lines[0], format!("{barrier} done [all six slept]"));
    assert!(
        q.events(barrier)
            .unwrap()
            .iter()
            .any(|e| e.message.starts_with("wake spawned"))
    );
    assert!(
        q.logs(barrier)
            .unwrap()
            .stdout
            .with_file_name("wake.err")
            .exists()
    );
}

#[test]
fn a_failed_node_wakes_too_and_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());
    let woke = dir.path().join("woke");

    let bad = q.submit(Job::cmd("exit 1")).unwrap();
    let barrier = q
        .submit(
            Job::barrier()
                .after([bad])
                .on_done(format!("echo $PEKAREN_STATE >> {}", woke.display())),
        )
        .unwrap();
    worker(&q).run_until_idle().unwrap();

    assert_eq!(q.status(barrier).unwrap().state, State::Failed);
    assert_eq!(lines_eventually(&woke, 1), ["failed"]);
}

#[test]
fn warmth_is_judged_by_when_the_node_settled() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());
    let woke = dir.path().join("woke");
    let say = |what: &str| Command::line(format!("echo {what} >> {}", woke.display()));

    let a = q.submit(Job::cmd("true")).unwrap();
    let later = SystemTime::now() + Duration::from_secs(3600);
    let earlier = SystemTime::now() - Duration::from_secs(3600);
    q.submit(Job::barrier().after([a]).on_done(Wake::ByWarmth {
        warm: say("warm"),
        cold: say("cold-unexpected"),
        warm_until: later,
    }))
    .unwrap();
    q.submit(Job::barrier().after([a]).on_done(Wake::ByWarmth {
        warm: say("warm-unexpected"),
        cold: say("cold"),
        warm_until: earlier,
    }))
    .unwrap();
    worker(&q).run_until_idle().unwrap();
    q.reap().unwrap();

    let mut lines = lines_eventually(&woke, 2);
    lines.sort();
    assert_eq!(lines, ["cold", "warm"]);
}

#[test]
fn a_claim_nobody_fulfilled_is_recovered_by_the_next_sweep() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());
    let woke = dir.path().join("woke");

    let a = q.submit(Job::cmd("true")).unwrap();
    let barrier = q
        .submit(
            Job::barrier()
                .after([a])
                .on_done(format!("echo recovered >> {}", woke.display())),
        )
        .unwrap();

    // Settled without anyone handing off: cancelling settles, it does not
    // spawn. Then a process claims the wake and dies before spawning it.
    q.cancel(a).unwrap();
    assert_eq!(q.status(barrier).unwrap().state, State::Failed);
    {
        let conn = rusqlite::Connection::open(dir.path().join("store.db")).unwrap();
        conn.execute(
            "UPDATE wakes SET claimed_by = 'dead-worker', claimed_at = 1",
            [],
        )
        .unwrap();
    }

    assert_eq!(q.reap().unwrap().wakes_recovered, [barrier]);
    assert!(q.reap().unwrap().wakes_recovered.is_empty());
    assert_eq!(lines_eventually(&woke, 1), ["recovered"]);
    assert!(
        q.events(barrier)
            .unwrap()
            .iter()
            .any(|e| e.message.contains("recovered"))
    );
}

#[test]
fn a_wake_that_cannot_start_is_reported_once() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());

    let a = q.submit(Job::cmd("true")).unwrap();
    let barrier = q
        .submit(
            Job::barrier()
                .after([a])
                .on_done(Command::exec("/no/such/wake", Vec::<String>::new())),
        )
        .unwrap();
    worker(&q).run_until_idle().unwrap();
    q.reap().unwrap();
    q.reap().unwrap();

    let errors: Vec<_> = q
        .events(barrier)
        .unwrap()
        .into_iter()
        .filter(|e| e.message.contains("did not spawn"))
        .collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
}
