//! The `pec` binary, driven the way a shell or an agent drives it: a real
//! process against a real store, judged by what it prints and how it
//! exits.

use std::path::Path;
use std::process::Output;
use std::time::Duration;

use pekaren::prelude::*;
use pekaren::{Filter, Worker};

fn queue(dir: &Path) -> Queue {
    Queue::options().work_on_submit(false).open(dir).unwrap()
}

fn worker(q: &Queue) -> Worker<'_> {
    Worker::new(q)
        .capacity(pekaren::Capacity {
            cpus: 4,
            mem_mb: 4096,
            gpus: vec![0],
        })
        .poll_interval(Duration::from_millis(10))
}

fn pec(store: &Path, args: &[&str]) -> Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_pec"))
        .arg("--store")
        .arg(store)
        .args(args)
        .output()
        .expect("run pec")
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn json_lines(out: &Output) -> Vec<serde_json::Value> {
    stdout(out)
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("{e}: {l}")))
        .collect()
}

/// The id `pec submit` or `pec barrier` printed, once it has succeeded.
fn submitted(out: Output) -> JobId {
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    stdout(&out).trim().parse().expect("a job id")
}

#[test]
fn submit_records_everything_the_flags_say() {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    std::fs::write(dir.path().join("data.txt"), "v1").unwrap();

    let first = submitted(pec(&store, &["submit", "--name", "first", "--", "true"]));
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_pec"))
        .arg("--store")
        .arg(&store)
        .current_dir(dir.path())
        .env("PEC_TEST_SECRET", "passed along")
        .args(["submit", "--name", "train", "--cpus", "2", "--gpus", "1"])
        .args(["--mem", "512", "--est", "5m", "--cap", "90s"])
        .args(["--after", &first.to_string(), "--env", "LR=3e-4"])
        .args(["--env-pass", "PEC_TEST_SECRET", "--watch", "data.txt"])
        .args(["--eval", "Loss below 0.3", "--"])
        .args(["echo", "$LR", "$PEC_TEST_SECRET"])
        .output()
        .unwrap();
    let id = submitted(out);

    let q = queue(&store);
    let s = q.status(id).unwrap();
    assert_eq!(s.name.as_deref(), Some("train"));
    assert_eq!(s.deps, [first]);
    assert_eq!(s.eval_prompt.as_deref(), Some("Loss below 0.3"));
    assert_eq!(
        (s.resources.cpus, s.resources.gpus, s.resources.mem_mb),
        (2, 1, 512)
    );
    assert_eq!(s.resources.est, Some(Duration::from_secs(300)));

    // The words after -- are one shell line, run where pec was called.
    let cmd = q.command(id).unwrap().unwrap();
    assert!(cmd.shell);
    assert_eq!(cmd.program, "echo $LR $PEC_TEST_SECRET");
    assert_eq!(cmd.cwd, Some(std::path::absolute(dir.path()).unwrap()));
    assert!(cmd.env.contains(&("LR".into(), "3e-4".into())));
    assert!(
        cmd.env
            .contains(&("PEC_TEST_SECRET".into(), "passed along".into()))
    );

    // The relative watch was made absolute, so a worker standing anywhere
    // checks the same file.
    std::fs::write(dir.path().join("data.txt"), "v2").unwrap();
    worker(&q).run_until_idle().unwrap();
    assert_eq!(q.status(id).unwrap().state, State::Done);
    assert!(
        q.events(id)
            .unwrap()
            .iter()
            .any(|e| e.message.contains("data.txt changed since submit"))
    );

    let logs = pec(&store, &["logs", &id.to_string()]);
    assert_eq!(stdout(&logs), "3e-4 passed along\n");
    let path = stdout(&pec(&store, &["logs", "--err", "--path", &id.to_string()]));
    assert!(path.trim_end().ends_with("1.err"), "{path}");
}

#[test]
fn submit_can_skip_the_shell_refuse_changed_code_and_retry() {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    let tool = dir.path().join("tool.sh");
    std::fs::write(&tool, "echo one").unwrap();
    let tool = tool.to_str().unwrap();

    let exec = submitted(pec(
        &store,
        &["submit", "--exec", "--", "printf", "%s|", "a b", "$HOME"],
    ));
    let guarded = submitted(pec(
        &store,
        &["submit", "--watch-fail", tool, "--", "sh", tool],
    ));
    let retried = submitted(pec(&store, &["submit", "--retries", "1", "--", "exit 1"]));
    std::fs::write(tool, "echo two").unwrap();

    let q = queue(&store);
    worker(&q).run_until_idle().unwrap();

    // No shell: nothing split, nothing expanded.
    assert_eq!(
        stdout(&pec(&store, &["logs", &exec.to_string()])),
        "a b|$HOME|"
    );

    let s = q.status(guarded).unwrap();
    assert_eq!(s.state, State::Failed);
    assert!(s.failure.unwrap().contains("changed since submit"));
    assert_eq!(s.exit_code, None, "it must not have run");

    let s = q.status(retried).unwrap();
    assert_eq!((s.state, s.attempt), (State::Failed, 2));
}

#[test]
fn a_wake_from_the_shell_runs_where_it_was_submitted() {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    let here = dir.path().join("here");
    std::fs::create_dir(&here).unwrap();

    let a = submitted(pec(&store, &["submit", "--", "true"]));
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_pec"))
        .arg("--store")
        .arg(&store)
        .current_dir(&here)
        .args(["barrier", "--after", &a.to_string()])
        .args(["--on-done", "echo $PEKAREN_JOB $PEKAREN_STATE > woke"])
        .output()
        .unwrap();
    let barrier = submitted(out);

    let q = queue(&store);
    worker(&q).run_until_idle().unwrap();
    let woke = here.join("woke");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !woke.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        std::fs::read_to_string(&woke).unwrap().trim(),
        format!("{barrier} done")
    );
}

#[test]
fn barrier_cancel_and_the_mistakes_pec_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    let a = submitted(pec(&store, &["submit", "--", "true"]));
    let b = submitted(pec(
        &store,
        &["submit", "--after", &a.to_string(), "--", "true"],
    ));
    let barrier = submitted(pec(
        &store,
        &[
            "barrier",
            "--after",
            &format!("{a},{b}"),
            "--name",
            "both",
            "--fail-at-end",
            "--on-done",
            "echo woke",
        ],
    ));

    let q = queue(&store);
    let s = q.status(barrier).unwrap();
    assert!(s.is_barrier);
    assert_eq!(s.deps, [a, b]);
    assert_eq!(s.name.as_deref(), Some("both"));

    // Cancelling the first takes everything behind it along.
    let out = pec(&store, &["cancel", &a.to_string()]);
    assert_eq!(out.status.code(), Some(0));
    assert_eq!(stdout(&out).trim(), format!("{a} cancelled"));
    assert_eq!(q.status(b).unwrap().state, State::Cancelled);
    assert_eq!(q.status(barrier).unwrap().state, State::Failed);

    // A job that already ran cannot be.
    let ran = submitted(pec(&store, &["submit", "--", "true"]));
    worker(&q).run_until_idle().unwrap();
    assert_eq!(
        pec(&store, &["cancel", &ran.to_string()]).status.code(),
        Some(1)
    );

    // Every mistake is exit 2, and none of them leaves a job behind.
    let before = q.list(Filter::All).unwrap().len();
    for args in [
        &["submit", "true"][..],
        &["submit", "--"],
        &["submit", "--cpus", "many", "--", "true"],
        &["submit", "--env", "novalue", "--", "true"],
        &[
            "submit",
            "--env-pass",
            "PEC_TEST_SURELY_UNSET",
            "--",
            "true",
        ],
        &["submit", "--after", "j999", "--", "true"],
        &["submit", "--frobnicate", "--", "true"],
        &["submit", "--est", "--", "true"],
        &["barrier", "--name", "lonely"],
    ] {
        assert_eq!(pec(&store, args).status.code(), Some(2), "{args:?}");
    }
    assert_eq!(q.list(Filter::All).unwrap().len(), before);
}

#[test]
fn status_json_is_one_object_per_job_with_what_a_reader_needs() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());
    let ok = q
        .submit(
            Job::cmd("echo hi")
                .name("greet")
                .eval_prompt("Says \"hi\"\non one line"),
        )
        .unwrap();
    let bad = q.submit(Job::cmd("echo boom >&2; exit 3").cpus(2)).unwrap();
    let barrier = q.submit(Job::barrier().after([ok, bad])).unwrap();
    worker(&q).run_until_idle().unwrap();

    let out = pec(dir.path(), &["status", "--json"]);
    assert!(out.status.success(), "{out:?}");
    let all = json_lines(&out);
    assert_eq!(all.len(), 3);

    let j = &all[0];
    assert_eq!(j["id"], ok.to_string());
    assert_eq!(j["name"], "greet");
    assert_eq!(j["kind"], "job");
    assert_eq!(j["state"], "done");
    assert_eq!(j["exit_code"], 0);
    assert_eq!(j["eval_prompt"], "Says \"hi\"\non one line");
    assert_eq!(j["command"]["program"], "echo hi");
    assert!(j["started_at"].as_f64().unwrap() >= j["submitted_at"].as_f64().unwrap());
    let stdout_log = j["logs"]["stdout"].as_str().unwrap();
    assert_eq!(std::fs::read_to_string(stdout_log).unwrap().trim(), "hi");

    let j = &all[1];
    assert_eq!(j["state"], "failed");
    assert_eq!(j["exit_code"], 3);
    assert_eq!(j["failure"], "exited non-zero");
    assert_eq!(j["resources"]["cpus"], 2);
    assert!(j["name"].is_null());
    let stderr_log = j["logs"]["stderr"].as_str().unwrap();
    assert_eq!(std::fs::read_to_string(stderr_log).unwrap().trim(), "boom");

    let j = &all[2];
    assert_eq!(j["kind"], "barrier");
    assert_eq!(j["state"], "failed");
    assert_eq!(
        j["deps"],
        serde_json::json!([ok.to_string(), bad.to_string()])
    );
    assert!(j["command"].is_null() && j["logs"].is_null());

    // Named ids narrow it down, in the order given.
    let out = pec(
        dir.path(),
        &["status", "--json", &barrier.to_string(), &ok.to_string()],
    );
    let some = json_lines(&out);
    assert_eq!(some.len(), 2);
    assert_eq!(some[0]["id"], barrier.to_string());
    assert_eq!(some[1]["id"], ok.to_string());
}

#[test]
fn status_is_a_table_and_one_job_in_full() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());
    let ok = q.submit(Job::cmd("true").name("fine")).unwrap();
    let bad = q
        .submit(
            Job::cmd("exit 3")
                .name("broken")
                .cpus(2)
                .eval_prompt("Should not fail"),
        )
        .unwrap();
    worker(&q).run_until_idle().unwrap();

    let table = stdout(&pec(dir.path(), &["status"]));
    let lines: Vec<&str> = table.lines().collect();
    assert_eq!(lines.len(), 2, "{table}");
    assert!(lines[0].starts_with(&ok.to_string()) && lines[0].contains("done"));
    assert!(lines[1].contains("broken"), "{table}");
    assert!(lines[1].contains("(exit 3: exited non-zero)"), "{table}");

    let full = stdout(&pec(dir.path(), &["status", &bad.to_string()]));
    assert!(full.starts_with(&format!("{bad} broken: failed")), "{full}");
    assert!(full.contains("declared  2 cpus"), "{full}");
    assert!(full.contains("command   exit 3"), "{full}");
    assert!(full.contains("eval      Should not fail"), "{full}");
    assert!(full.contains("1.err"), "{full}");
}

#[test]
fn wait_says_how_it_went_in_its_exit_code() {
    let dir = tempfile::tempdir().unwrap();
    let q = queue(dir.path());
    let ok = q.submit(Job::cmd("true")).unwrap();
    let bad = q.submit(Job::cmd("exit 3")).unwrap();
    let stranded = q.submit(Job::cmd("true").after([bad])).unwrap();
    worker(&q).run_until_idle().unwrap();
    // Submitted after the worker left, so nothing will run it.
    let queued = q.submit(Job::cmd("true")).unwrap();

    let code = |args: &[&str]| pec(dir.path(), args).status.code();
    let (ok, bad, stranded, queued) = (
        ok.to_string(),
        bad.to_string(),
        stranded.to_string(),
        queued.to_string(),
    );

    assert_eq!(code(&["wait", &ok]), Some(0));
    assert_eq!(code(&["wait", &ok, &bad]), Some(1));
    // A job a failed dependency stranded settles too, as cancelled.
    assert_eq!(code(&["wait", &stranded]), Some(1));
    assert_eq!(code(&["wait", "--timeout", "0.2", &queued]), Some(124));

    // Whatever the outcome, every job gets its line, JSON if asked.
    let out = pec(
        dir.path(),
        &["wait", "--timeout", "0", "--json", &ok, &queued],
    );
    assert_eq!(out.status.code(), Some(124));
    let states: Vec<String> = stdout(&out)
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap()["state"].to_string())
        .collect();
    assert_eq!(states, ["\"done\"", "\"ready\""]);

    // pec failing is not a job failing.
    assert_eq!(code(&["wait", "j9999"]), Some(2));
    assert_eq!(code(&["wait"]), Some(2));
    assert_eq!(code(&["wait", "--timeout", "soon", &ok]), Some(2));
}
