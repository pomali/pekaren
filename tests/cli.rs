//! The `pec` binary, driven the way a shell or an agent drives it: a real
//! process against a real store, judged by what it prints and how it
//! exits.

use std::path::Path;
use std::process::Output;
use std::time::Duration;

use pekaren::Worker;
use pekaren::prelude::*;

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
