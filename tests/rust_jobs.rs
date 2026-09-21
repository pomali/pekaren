//! Rust source as a job. One test in this binary, because the runner is
//! resolved from process environment.

use std::os::unix::fs::PermissionsExt;

use pekaren::prelude::*;

#[test]
fn rust_source_becomes_a_runnable_script() {
    let dir = tempfile::tempdir().unwrap();

    // With nothing configured, a Rust job runs as a cargo single-file
    // script, which needs a nightly toolchain.
    let q = Queue::options()
        .work_on_submit(false)
        .open(dir.path())
        .unwrap();
    let default = q.submit(Job::rust("fn main() {}")).unwrap();
    let cmd = q.command(default).unwrap().unwrap();
    assert_eq!(cmd.program, "cargo");
    assert_eq!(cmd.args[..2], ["+nightly".to_string(), "-Zscript".into()]);
    assert!(!cmd.shell);

    // A runner that compiles and runs with stable rustc, so this test can
    // prove the script we wrote is real Rust rather than plausible text.
    let runner = dir.path().join("run-with-rustc.sh");
    std::fs::write(
        &runner,
        "#!/bin/sh\nset -e\nout=$(mktemp -d)/script\nrustc -O \"$1\" -o \"$out\"\nexec \"$out\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&runner, std::fs::Permissions::from_mode(0o755)).unwrap();

    // SAFETY: this test binary holds exactly one test.
    unsafe { std::env::set_var("PEKAREN_RUST_RUNNER", &runner) }

    let id = q
        .submit(
            Job::rust(
                r#"
                fn main() {
                    let total: u32 = (1..=10).sum();
                    println!("sum={total}");
                }
                "#,
            )
            .name("adds up")
            .eval_prompt("Should print sum=55"),
        )
        .unwrap();

    // The store holds the script and the command that runs it.
    let status = q.status(id).unwrap();
    let script = status.script.expect("a Rust job remembers its script");
    assert!(script.starts_with(dir.path().join("scripts")));
    assert!(std::fs::read_to_string(&script).unwrap().contains("sum="));

    let cmd = q.command(id).unwrap().unwrap();
    assert_eq!(cmd.program, runner.to_string_lossy());
    assert_eq!(cmd.args, vec![script.to_string_lossy().to_string()]);

    // Run it the way a worker will, and check the job actually works.
    let out = std::process::Command::new(&cmd.program)
        .args(&cmd.args)
        .output()
        .expect("run the script");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "sum=55");

    // The same source twice is the same file: submitting is cheap and a
    // rolled-back transaction leaves nothing to clean up.
    let again = q.submit(Job::rust("fn main() {}")).unwrap();
    assert_eq!(
        q.status(again).unwrap().script,
        q.status(default).unwrap().script
    );

    // A manifest is written as frontmatter, ahead of the source.
    let with_deps = q
        .submit(Job::rust("fn main() {}").rust_manifest("[dependencies]\nserde_json = \"1\""))
        .unwrap();
    let text = std::fs::read_to_string(q.status(with_deps).unwrap().script.unwrap()).unwrap();
    assert!(
        text.starts_with("---\n[dependencies]\nserde_json = \"1\"\n---\n"),
        "{text}"
    );
}
