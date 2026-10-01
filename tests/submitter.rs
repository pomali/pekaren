//! Waking whoever submitted the work. One test in this binary, because the
//! submitting session is read from process environment.

use std::io::{BufRead, BufReader};
use std::os::unix::net::UnixListener;
use std::time::Duration;

use pekaren::prelude::*;
use pekaren::{Wake, Worker};

#[test]
fn a_settled_node_wakes_the_session_that_submitted_it() {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    let socket = dir.path().join("inbox.sock");
    let relayed = dir.path().join("relayed.txt");

    // A stand-in for a Claude Code session: something listening on an
    // inbox socket, with its id in the environment.
    let listener = UnixListener::bind(&socket).unwrap();

    // SAFETY: this test binary holds exactly one test.
    unsafe {
        std::env::set_var("PEKAREN_STORE", &store);
        std::env::set_var("CLAUDE_CODE_SESSION_ID", "session-under-test");
        std::env::set_var("CLAUDE_CODE_MESSAGING_SOCKET", &socket);
        // The token is deliberately never recorded; set it to prove that.
        std::env::set_var("CLAUDE_CODE_MESSAGING_TOKEN", "secret-token");
    }

    // A notifier that does the posting, because the message line's format
    // is not ours to invent. It writes what it was handed to a file.
    let notifier = dir.path().join("notify.sh");
    std::fs::write(
        &notifier,
        format!(
            "#!/bin/sh\nprintf '%s\\n%s\\n' \"$PEKAREN_WAKE_SESSION\" \"$PEKAREN_WAKE_MESSAGE\" > {}\n",
            relayed.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(
        &notifier,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();

    let q = Queue::options()
        .work_on_submit(false)
        .open_default()
        .unwrap();

    let run = q
        .submit(
            Job::cmd("echo trained")
                .name("train")
                .est_minutes(1)
                .eval_prompt("Loss should drop below 0.3"),
        )
        .unwrap();
    let barrier = q
        .submit(
            Job::barrier()
                .after([run])
                .name("sweep")
                .eval_prompt("Pick the best run")
                .on_done(Wake::submitter("echo nobody-was-listening")),
        )
        .unwrap();

    // The submitter was recorded from the environment — identity only.
    let who = q.submitter(barrier).unwrap().expect("a submitter");
    assert_eq!(who.session, "session-under-test");
    assert_eq!(who.socket.as_deref(), Some(socket.as_path()));
    assert!(who.is_live(), "the listener is up");
    let dump = std::fs::read_to_string(store.join("store.db")).unwrap_or_default();
    assert!(
        !dump.contains("secret-token"),
        "the messaging token must never reach the store"
    );

    // The message a wake carries: the outcome, the prompt written at submit
    // time, and what each dependency did.
    unsafe { std::env::set_var("PEKAREN_WAKE_NOTIFIER", &notifier) }
    Worker::new(&q)
        .poll_interval(Duration::from_millis(10))
        .run_until_idle()
        .unwrap();

    assert_eq!(q.status(barrier).unwrap().state, State::Done);
    let message = q.wake_message(barrier).unwrap();
    assert!(
        message.starts_with(&format!("{barrier} barrier \"sweep\" done")),
        "{message}"
    );
    assert!(message.contains("eval: Pick the best run"), "{message}");
    assert!(
        message.contains(&format!("{run} train done exit 0")),
        "{message}"
    );

    // The notifier ran, and got the socket, the session and the message.
    let relayed_text = wait_for_file(&relayed);
    assert!(
        relayed_text.starts_with("session-under-test\n"),
        "{relayed_text}"
    );
    assert!(
        relayed_text.contains("barrier \"sweep\" done"),
        "{relayed_text}"
    );
    assert!(
        q.events(barrier).unwrap().iter().any(|e| e
            .message
            .contains("wake spawned via the submitting session's inbox")),
        "{:?}",
        q.events(barrier).unwrap()
    );

    // Nothing was written to the socket by pekaren itself: the wire format
    // of a message line is not published, so the notifier owns it.
    listener.set_nonblocking(true).unwrap();
    match listener.accept() {
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
        Ok((stream, _)) => {
            // Only the liveness probe, which hangs up without a line.
            let mut line = String::new();
            let _ = BufReader::new(stream).read_line(&mut line);
            assert!(line.is_empty(), "pekaren wrote {line:?} to the inbox");
        }
        Err(e) => panic!("unexpected accept error: {e}"),
    }

    // With the session gone, the wake resumes it instead. PEKAREN_CLAUDE
    // stands in for the real CLI and records its arguments.
    drop(listener);
    std::fs::remove_file(&socket).unwrap();
    assert!(!q.submitter(barrier).unwrap().unwrap().is_live());

    let resumed = dir.path().join("resumed.txt");
    let fake_claude = dir.path().join("claude.sh");
    std::fs::write(
        &fake_claude,
        format!("#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\n", resumed.display()),
    )
    .unwrap();
    std::fs::set_permissions(
        &fake_claude,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();
    unsafe {
        std::env::remove_var("PEKAREN_WAKE_NOTIFIER");
        std::env::set_var("PEKAREN_CLAUDE", &fake_claude);
    }

    let second = q
        .submit(
            Job::cmd("true")
                .name("after")
                .on_done(Wake::submitter("echo nobody-was-listening")),
        )
        .unwrap();
    Worker::new(&q)
        .poll_interval(Duration::from_millis(10))
        .run_until_idle()
        .unwrap();

    let resumed_text = wait_for_file(&resumed);
    let lines: Vec<&str> = resumed_text.lines().collect();
    assert_eq!(lines[0], "--resume");
    assert_eq!(lines[1], "session-under-test");
    assert_eq!(lines[2], "-p");
    assert!(
        lines[3].contains(&format!("{second} job \"after\" done")),
        "{resumed_text}"
    );
    assert!(
        q.events(second)
            .unwrap()
            .iter()
            .any(|e| e.message.contains("no longer listening")),
        "{:?}",
        q.events(second).unwrap()
    );
}

/// A wake is spawned and reaped by another thread, so its effect arrives
/// shortly after the worker returns.
fn wait_for_file(path: &std::path::Path) -> String {
    for _ in 0..200 {
        if let Ok(text) = std::fs::read_to_string(path) {
            if !text.is_empty() {
                return text;
            }
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("{} never appeared", path.display());
}
