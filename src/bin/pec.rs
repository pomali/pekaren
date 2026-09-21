//! `pec` — the oven. A thin window onto a store.
//!
//! The full CLI is deferred (see the design doc's open questions): the shape
//! so far is `submit`, `status`, `wait`, `logs`, `reap`, all on the same
//! store as the library. What exists today is the read side, enough to look
//! at a store a script created.

use std::process::ExitCode;

use pekaren::{Filter, JobId, Queue, Result};

const USAGE: &str = "\
pec — pekáreň's oven

usage:
    pec [--store <dir>] status [<job-id>]
    pec [--store <dir>] runnable

<dir> defaults to $PEKAREN_STORE, then ~/.pekaren.

deferred: submit, wait, logs, reap.
";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("pec: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let mut args = std::env::args().skip(1).peekable();
    let mut store = std::env::var("PEKAREN_STORE").unwrap_or_else(|_| "~/.pekaren".into());

    while args.peek().map(|a| a == "--store").unwrap_or(false) {
        args.next();
        if let Some(dir) = args.next() {
            store = dir;
        }
    }

    let command = args.next().unwrap_or_else(|| "help".into());
    match command.as_str() {
        "status" => {
            let q = Queue::open(&store)?;
            match args.next() {
                Some(id) => {
                    let id: JobId = id
                        .parse()
                        .map_err(|_| pekaren::Error::NotImplemented("job id must look like j42"))?;
                    print_status(&q.status(id)?);
                }
                None => {
                    for s in q.list(Filter::All)? {
                        print_status(&s);
                    }
                }
            }
        }
        "runnable" => {
            let q = Queue::open(&store)?;
            for id in q.runnable()? {
                println!("{id}");
            }
        }
        _ => print!("{USAGE}"),
    }
    Ok(())
}

fn print_status(s: &pekaren::JobStatus) {
    let what = if s.is_barrier { "barrier" } else { "job" };
    let name = s.name.clone().unwrap_or_default();
    println!(
        "{:<6} {:<8} {:<9} {}",
        s.id.to_string(),
        what,
        s.state,
        name
    );
}
