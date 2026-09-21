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

fn print_status(s: &JobStatus) {
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
