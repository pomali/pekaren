//! `pec` — the oven. A thin window onto a store.
//!
//! The full CLI is deferred (see the design doc's open questions): the shape
//! so far is `submit`, `status`, `wait`, `logs`, `reap`, all on the same
//! store as the library. What exists today is the read side, enough to look
//! at a store a script created.
//!
//! Every subcommand opens the default store — `$PEKAREN_STORE`, else
//! `~/.pekaren` — unless `--store` says otherwise.

use std::process::ExitCode;

use pekaren::{Filter, JobId, JobStatus, Queue, QueueOptions, Result, default_store_path};

const USAGE: &str = "\
pec — pekáreň's oven

usage:
    pec [--store <dir>] status [<job-id>]
    pec [--store <dir>] runnable
    pec [--store <dir>] check [<job-id>]
    pec [--store <dir>] where

the store defaults to $PEKAREN_STORE, else ~/.pekaren.

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
    let mut store: Option<String> = None;

    while args.peek().map(|a| a == "--store").unwrap_or(false) {
        args.next();
        store = args.next();
    }

    // Reading a store should never start work in this process, whatever the
    // library's default is for a submitting script.
    let open = || -> Result<Queue> {
        let opts = QueueOptions::default().work_on_submit(false);
        match &store {
            Some(dir) => opts.open(dir),
            None => opts.open_default(),
        }
    };

    let command = args.next().unwrap_or_else(|| "help".into());
    match command.as_str() {
        "status" => {
            let q = open()?;
            match args.next() {
                Some(id) => print_status(&q.status(parse_id(&id)?)?),
                None => {
                    for s in q.list(Filter::All)? {
                        print_status(&s);
                    }
                }
            }
        }
        // Re-hash what each job depends on and say what moved. A worker
        // does this before running anything; this is the same check by
        // hand, for a store you are about to trust.
        "check" => {
            let q = open()?;
            let ids = match args.next() {
                Some(id) => vec![parse_id(&id)?],
                None => q
                    .list(Filter::Unsettled)?
                    .into_iter()
                    .map(|s| s.id)
                    .collect(),
            };
            let mut fatal = 0;
            for id in ids {
                for drift in q.check_inputs(id)? {
                    let mark = if drift.is_fatal() { "FAIL" } else { "warn" };
                    println!("{id} {mark} {:?} {}", drift.detail, drift.path.display());
                    fatal += u32::from(drift.is_fatal());
                }
            }
            if fatal > 0 {
                return Err(pekaren::Error::NotImplemented(
                    "jobs above depend on code that changed; they fail when claimed",
                ));
            }
        }
        "runnable" => {
            let q = open()?;
            for id in q.runnable()? {
                println!("{id}");
            }
        }
        // Where the store is, without opening or creating anything.
        "where" => match &store {
            Some(dir) => println!("{dir}"),
            None => println!("{}", default_store_path().display()),
        },
        _ => print!("{USAGE}"),
    }
    Ok(())
}

fn parse_id(raw: &str) -> Result<JobId> {
    raw.parse()
        .map_err(|_| pekaren::Error::NotImplemented("job id must look like j42"))
}

fn print_status(s: &JobStatus) {
    let what = if s.is_barrier {
        "barrier"
    } else if s.task.is_some() {
        "task"
    } else if s.script.is_some() {
        "rust"
    } else {
        "job"
    };
    let name = s
        .name
        .clone()
        .or_else(|| s.task.clone())
        .unwrap_or_default();
    let flag = if s.stalled { " (stalled)" } else { "" };
    println!(
        "{:<6} {:<8} {:<9} {name}{flag}",
        s.id.to_string(),
        what,
        s.state
    );
}
