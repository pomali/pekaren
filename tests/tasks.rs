//! Tasks: a job that is a function in the submitting binary.
//!
//! One test binary, one test, because dispatch reads process environment —
//! and because the child here *is* this binary, re-executed the way a
//! worker will run it.

use std::path::PathBuf;

use pekaren::prelude::*;
use pekaren::{DriftKind, InputKind, Level};

/// The job body. An ordinary function: the compiler checks it, tools see
/// it, and nothing about it is a string.
fn write_marker(ctx: &JobCtx) -> TaskResult {
    let path = PathBuf::from(ctx.arg(0).ok_or("no marker path")?);
    std::fs::write(&path, format!("{} ran with {:?}", ctx.job(), ctx.args()))?;
    Ok(())
}

fn tasks() -> (Tasks, Task) {
    let mut tasks = Tasks::new();
    let marker = tasks.add("write-marker", write_marker);
    (tasks, marker)
}

#[test]
fn a_task_job_runs_the_function_in_this_binary() {
    // The child process re-execs this same test. With PEKAREN_TASK set,
    // bootstrap runs the task and exits before anything below it — which
    // is exactly what it does at the top of a real main.
    let (registry, marker) = tasks();
    registry.bootstrap().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    let marker_file = dir.path().join("marker.txt");

    // SAFETY: this test binary holds exactly one test.
    unsafe { std::env::set_var("PEKAREN_STORE", &store) }

    // In a submitting run, dispatch found nothing to do and we carried on.
    assert!(registry.run_assigned().unwrap().is_none());

    let q = Queue::options()
        .work_on_submit(false)
        .open_default()
        .unwrap();
    let dataset = dir.path().join("dataset");
    std::fs::create_dir(&dataset).unwrap();
    std::fs::write(dataset.join("a.csv"), "1,2,3\n").unwrap();

    let id = q
        .submit(
            Job::task(marker)
                .arg(marker_file.to_string_lossy())
                .arg("--lr=3e-4")
                .watch(&dataset)
                .name("marker"),
        )
        .unwrap();

    // The store points at this binary and the registered name.
    let status = q.status(id).unwrap();
    assert_eq!(status.task.as_deref(), Some("write-marker"));
    let cmd = q.command(id).unwrap().unwrap();
    assert_eq!(
        PathBuf::from(&cmd.program),
        std::env::current_exe().unwrap()
    );
    assert!(cmd.args.is_empty(), "the binary's own argv stays empty");
    assert_eq!(
        q.task_args(id).unwrap(),
        vec![
            marker_file.to_string_lossy().to_string(),
            "--lr=3e-4".into()
        ]
    );

    // Nothing has moved yet.
    assert!(q.check_inputs(id).unwrap().is_empty());

    // Run it the way a worker will: the recorded command, with the job in
    // the environment. The extra arguments are libtest's, because this
    // binary's main is the test harness rather than a main that calls
    // bootstrap.
    let out = std::process::Command::new(&cmd.program)
        .args(["--exact", "a_task_job_runs_the_function_in_this_binary"])
        .envs(cmd.env.iter().cloned())
        .env("PEKAREN_JOB", id.to_string())
        .env("PEKAREN_STORE", &store)
        .output()
        .expect("re-exec this binary");
    assert!(
        out.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let written = std::fs::read_to_string(&marker_file).expect("the task ran");
    assert!(written.starts_with(&id.to_string()), "{written}");
    assert!(written.contains("--lr=3e-4"), "{written}");

    // A changed input warns and is recorded; the job is still runnable.
    std::fs::write(dataset.join("b.csv"), "4,5,6\n").unwrap();
    let drift = q.check_inputs(id).unwrap();
    assert_eq!(drift.len(), 1);
    assert_eq!(drift[0].kind, InputKind::Param);
    assert_eq!(drift[0].detail, DriftKind::Changed);
    assert!(!drift[0].is_fatal());

    // A changed binary is fatal: different code, same evaluation prompt.
    let fake_exe = dir.path().join("rebuilt");
    std::fs::write(&fake_exe, b"not the binary you submitted").unwrap();
    let pretend = q
        .submit(Job::cmd("true").watch_as(&fake_exe, OnChange::Fail))
        .unwrap();
    std::fs::write(&fake_exe, b"rebuilt since").unwrap();
    let drift = q.check_inputs(pretend).unwrap();
    assert_eq!(drift.len(), 1);
    assert!(drift[0].is_fatal());

    // Both were written to the jobs' events, for whoever reads them later.
    let events = q.events(id).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].level, Level::Warn);
    assert!(events[0].message.contains("changed since submit"));
    assert_eq!(q.events(pretend).unwrap()[0].level, Level::Error);
}
