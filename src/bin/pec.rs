//! `pec` — the oven. A thin window onto a store.
//!
//! Everything a script can do through the library that a shell also needs:
//! submit a command or a barrier, read and wait on what happened, run a
//! worker. Thin on purpose — each subcommand is a few calls into `Queue` —
//! so the library stays the one place the behaviour lives.
//!
//! Every subcommand opens the default store — `$PEKAREN_STORE`, else
//! `~/.pekaren` — unless `--store` says otherwise.

use std::fmt::Write as _;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pekaren::{
    Command, FailurePolicy, Filter, Job, JobId, JobStatus, KillAfter, OnChange, Queue,
    QueueOptions, State, default_store_path,
};

const USAGE: &str = "\
pec — pekáreň's oven

usage:
    pec [--store <dir>] submit [<options>] -- <command>...
    pec [--store <dir>] barrier --after <ids> [--name <n>] [--eval <text>]
                                [--on-done <command>] [--fail-at-end]
    pec [--store <dir>] work [--once | --forever [--idle <duration>]]
                             [--cpus <n>] [--gpus <0,1|none>] [--fifo]
    pec [--store <dir>] status [--json] [<job-id>...]
    pec [--store <dir>] wait [--timeout <secs>] [--json] <job-id>...
    pec [--store <dir>] logs [--err] [--path] <job-id>
    pec [--store <dir>] cancel <job-id>...
    pec [--store <dir>] runnable
    pec [--store <dir>] check [<job-id>]
    pec [--store <dir>] reap
    pec [--store <dir>] where

the store defaults to $PEKAREN_STORE, else ~/.pekaren.

submit prints the new job's id. The words after -- are one shell line
(sh -c), run in the directory pec was called from; --exec runs them as a
program and its arguments instead, with no shell. Options:
    --name <n>          a label for status
    --cpus <n>          cores it needs (default 1)
    --gpus <n>          GPUs it needs; it sees them as CUDA_VISIBLE_DEVICES
    --mem <mb>          memory it needs, in MB
    --est <duration>    how long it should take; the cap defaults to 3x this
    --cap <duration>    kill it after this long, or `never`
    --after <ids>       run only once these are done: j1,j2 (repeatable)
    --cwd <dir>         run here instead
    --env <key=value>   set in its environment (repeatable)
    --env-pass <key>    copy this variable's value now (repeatable)
    --watch <path>      warn if this changed before it runs (repeatable)
    --watch-fail <path> fail instead of running if it changed (repeatable)
    --eval <text>       the prompt to judge its output by
    --retries <n>       re-run it up to n times on failure; implies
                        --idempotent
    --idempotent        safe to run again after a lost lease
    --on-done <command> a shell line to spawn once it settles
    --exec              no shell: the first word is the program

an --on-done line (on a job or a barrier) is spawned once, by whichever
worker sees the node settle, in the directory pec was called from, with
PEKAREN_JOB, PEKAREN_STATE and PEKAREN_EVAL_PROMPT set; its output goes to
logs/<n>/wake.out and wake.err in the store.

work runs jobs one at a time until nothing is claimable, or with --once
just one. With --forever it stays up, looking at the store every --idle
(default 1s) when there is nothing to do, until SIGTERM or SIGINT: then it
finishes the job it is running and exits; a second signal stops it at
once. Run several for concurrency: they share the host's CPUs and GPUs
through the store. --cpus and --gpus override what the host appears to
have (GPUs default to $PEKAREN_GPUS, else $CUDA_VISIBLE_DEVICES).
A worker takes the oldest job that fits what is free; with --fifo it takes
jobs strictly in order, waiting for the oldest one it could run rather
than let smaller ones past, so a big job is not starved. Give every worker
on a store the same --fifo.

status shows every job, one line each, or one job in full; --json prints
one JSON object per job instead. logs prints the latest attempt's stdout,
or its stderr with --err, or with --path only where it is. cancel stops
jobs that have not started, and everything that depends on them.

wait exits 0 when every job is done, 1 when any failed or was cancelled,
and 124 when the timeout came first (like timeout(1)). A duration is
seconds, or 90s, 5m, 1.5h. check exits 1 when a job depends on code that
changed. Exit 2 is pec itself failing: a bad argument, or a store it
cannot use.
";

/// Exit code for a wait the timeout ended, as timeout(1) has it.
const TIMED_OUT: u8 = 124;

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("pec: {e}");
            ExitCode::from(2)
        }
    }
}

/// What stops pec before it can answer. Both kinds exit 2, so that 1 can
/// always mean "a job did not succeed".
#[derive(Debug)]
enum CliError {
    Store(pekaren::Error),
    Usage(String),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliError::Store(e) => e.fmt(f),
            CliError::Usage(what) => write!(f, "{what} (pec help for usage)"),
        }
    }
}

impl From<pekaren::Error> for CliError {
    fn from(e: pekaren::Error) -> Self {
        CliError::Store(e)
    }
}

type Result<T> = std::result::Result<T, CliError>;

fn usage<T>(what: impl Into<String>) -> Result<T> {
    Err(CliError::Usage(what.into()))
}

fn run() -> Result<ExitCode> {
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
        Ok(match &store {
            Some(dir) => opts.open(dir)?,
            None => opts.open_default()?,
        })
    };

    let command = args.next().unwrap_or_else(|| "help".into());
    match command.as_str() {
        // Parsed before the store is opened, so a typo creates nothing.
        "submit" => {
            let job = parse_submit(args.collect())?;
            println!("{}", open()?.submit(job)?);
        }
        "barrier" => {
            let job = parse_barrier(args.collect())?;
            println!("{}", open()?.submit(job)?);
        }
        "logs" => {
            let (mut err, mut path_only, mut id) = (false, false, None);
            for arg in args {
                match arg.as_str() {
                    "--err" => err = true,
                    "--path" => path_only = true,
                    raw => id = Some(parse_id(raw)?),
                }
            }
            let Some(id) = id else {
                return usage("logs needs a job id");
            };
            let logs = open()?.logs(id)?;
            let path = if err { logs.stderr } else { logs.stdout };
            if path_only {
                println!("{}", path.display());
            } else {
                match std::fs::read(&path) {
                    Ok(bytes) => {
                        let _ = std::io::stdout().write_all(&bytes);
                    }
                    Err(_) => {
                        eprintln!("pec: {id} has no output at {}", path.display());
                        return Ok(ExitCode::FAILURE);
                    }
                }
            }
        }
        "cancel" => {
            let ids = args.map(|a| parse_id(&a)).collect::<Result<Vec<_>>>()?;
            if ids.is_empty() {
                return usage("cancel needs at least one job id");
            }
            let q = open()?;
            let mut code = ExitCode::SUCCESS;
            for id in ids {
                q.cancel(id)?;
                match q.status(id)?.state {
                    State::Cancelled => println!("{id} cancelled"),
                    state => {
                        eprintln!(
                            "pec: {id} is {state}; only a job that has not started can be cancelled"
                        );
                        code = ExitCode::FAILURE;
                    }
                }
            }
            return Ok(code);
        }
        "status" => {
            let q = open()?;
            let mut json = false;
            let mut ids = Vec::new();
            for arg in args {
                match arg.as_str() {
                    "--json" => json = true,
                    id => ids.push(parse_id(id)?),
                }
            }
            let statuses = if ids.is_empty() {
                q.list(Filter::All)?
            } else {
                q.statuses(&ids)?
            };
            for s in &statuses {
                if json {
                    println!("{}", status_json(&q, s)?);
                } else if ids.len() == 1 {
                    print_detail(&q, s)?;
                } else {
                    println!("{}", status_line(&q, s)?);
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
                eprintln!("pec: jobs above depend on code that changed; they fail when claimed");
                return Ok(ExitCode::FAILURE);
            }
        }
        // With no daemon, this is how work gets done when the script
        // that submitted it has gone: a worker anyone can start.
        "work" => {
            let (mut once, mut forever, mut fifo) = (false, false, false);
            let mut idle = Duration::from_secs(1);
            let mut capacity = pekaren::Capacity::detect()?;
            while let Some(flag) = args.next() {
                let mut value = || match args.next() {
                    Some(v) => Ok(v),
                    None => usage(format!("{flag} needs a value")),
                };
                match flag.as_str() {
                    "--once" => once = true,
                    "--forever" => forever = true,
                    "--fifo" => fifo = true,
                    "--idle" => idle = parse_duration(&value()?)?,
                    "--cpus" => capacity.cpus = number(&flag, &value()?)?,
                    "--gpus" => capacity.gpus = parse_devices(&value()?)?,
                    other => return usage(format!("work does not take {other}")),
                }
            }
            let q = open()?;
            let mut worker = pekaren::Worker::new(&q)
                .capacity(capacity.clone())
                .strict_fifo(fifo);
            let report = if once {
                worker.run_one()?;
                Default::default()
            } else if forever {
                stop_on_signals();
                eprintln!(
                    "pec work: pid {}, {} cpus, gpus {:?}, store {}; stop with kill {0}",
                    std::process::id(),
                    capacity.cpus,
                    capacity.gpus,
                    q.path().display(),
                );
                let report = worker.idle_interval(idle).run_while(&STOP)?;
                eprintln!(
                    "pec work: stopped; {} done, {} failed",
                    report.committed.len(),
                    report.failed.len()
                );
                report
            } else {
                worker.run_until_idle()?
            };
            for id in &report.committed {
                println!("{id} done");
            }
            for id in &report.failed {
                println!("{id} failed");
            }
        }
        // The exit code is the answer, so a shell or an agent can branch
        // on it without reading anything; the lines are for the reader.
        "wait" => {
            let mut json = false;
            let mut timeout = None;
            let mut ids = Vec::new();
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--json" => json = true,
                    "--timeout" => match args.next() {
                        Some(t) => timeout = Some(parse_duration(&t)?),
                        None => return usage("--timeout needs a value"),
                    },
                    id => ids.push(parse_id(id)?),
                }
            }
            if ids.is_empty() {
                return usage("wait needs at least one job id");
            }
            let q = open()?;
            let statuses = match q.wait(&ids, timeout) {
                Ok(statuses) => statuses,
                Err(pekaren::Error::WaitTimeout(_)) => q.statuses(&ids)?,
                Err(e) => return Err(e.into()),
            };
            for s in &statuses {
                if json {
                    println!("{}", status_json(&q, s)?);
                } else {
                    println!("{}", status_line(&q, s)?);
                }
            }
            return Ok(if !statuses.iter().all(|s| s.state.is_settled()) {
                ExitCode::from(TIMED_OUT)
            } else if statuses.iter().all(|s| s.state == State::Done) {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            });
        }
        "reap" => {
            let q = open()?;
            let report = q.reap()?;
            for id in &report.leases_reclaimed {
                println!("{id} lease reclaimed");
            }
            for id in &report.orphans_killed {
                println!("{id} orphan killed");
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
        "help" | "--help" | "-h" => print!("{USAGE}"),
        other => {
            eprint!("pec: no command {other:?}\n\n{USAGE}");
            return Ok(ExitCode::from(2));
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// `pec submit [<options>] -- <command>...`, as the job it describes.
fn parse_submit(words: Vec<String>) -> Result<Job> {
    let Some(split) = words.iter().position(|w| w == "--") else {
        return usage("submit needs `-- <command>` after its options");
    };
    let command = &words[split + 1..];
    if command.is_empty() {
        return usage("submit needs a command after --");
    }

    let mut exec = false;
    let mut cwd = None;
    let mut env = Vec::new();
    let mut name = None;
    let mut cpus = None;
    let mut gpus = None;
    let mut mem_mb = None;
    let mut est = None;
    let mut cap = None;
    let mut after = Vec::new();
    let mut watched = Vec::new();
    let mut eval = None;
    let mut retries = None;
    let mut idempotent = false;
    let mut on_done = None;

    let mut flags = words[..split].iter();
    while let Some(flag) = flags.next() {
        let mut value = || match flags.next() {
            Some(v) => Ok(v.as_str()),
            None => usage(format!("{flag} needs a value")),
        };
        match flag.as_str() {
            "--exec" => exec = true,
            "--idempotent" => idempotent = true,
            "--name" => name = Some(value()?.to_string()),
            "--cpus" => cpus = Some(number(flag, value()?)?),
            "--gpus" => gpus = Some(number(flag, value()?)?),
            "--mem" => mem_mb = Some(number(flag, value()?)?),
            "--est" => est = Some(parse_duration(value()?)?),
            "--cap" => {
                cap = Some(match value()? {
                    "never" => KillAfter::Never,
                    raw => KillAfter::Fixed(parse_duration(raw)?),
                })
            }
            "--after" => after.extend(parse_ids(value()?)?),
            "--cwd" => cwd = Some(absolute(value()?)?),
            "--env" => match value()?.split_once('=') {
                Some((key, val)) if !key.is_empty() => env.push((key.into(), val.into())),
                _ => return usage("--env takes key=value"),
            },
            // Read now, from the submitting shell: the worker that runs
            // the job an hour later has an environment of its own.
            "--env-pass" => {
                let key = value()?;
                match std::env::var(key) {
                    Ok(val) => env.push((key.to_string(), val)),
                    Err(_) => return usage(format!("--env-pass {key}: not set here")),
                }
            }
            // Absolute, because the path is hashed here and checked again
            // by a worker that may be standing somewhere else.
            "--watch" => watched.push((absolute(value()?)?, OnChange::Warn)),
            "--watch-fail" => watched.push((absolute(value()?)?, OnChange::Fail)),
            "--eval" => eval = Some(value()?.to_string()),
            "--retries" => retries = Some(number(flag, value()?)?),
            "--on-done" => on_done = Some(value()?.to_string()),
            other => return usage(format!("submit does not take {other}")),
        }
    }

    let mut cmd = if exec {
        Command::exec(&command[0], &command[1..])
    } else {
        Command::line(command.join(" "))
    };
    // A job submitted from a shell runs where it was submitted, as it
    // would have had it been typed there.
    cmd = cmd.cwd(match cwd {
        Some(dir) => dir,
        None => absolute(".")?,
    });
    for (key, val) in env {
        cmd = cmd.env(key, val);
    }

    let mut job = Job::run(cmd).after(after);
    if let Some(name) = name {
        job = job.name(name);
    }
    if let Some(n) = cpus {
        job = job.cpus(n);
    }
    if let Some(n) = gpus {
        job = job.gpus(n);
    }
    if let Some(mb) = mem_mb {
        job = job.mem_mb(mb);
    }
    if let Some(d) = est {
        job = job.est(d);
    }
    if let Some(cap) = cap {
        job = job.kill_after(cap);
    }
    for (path, on_change) in watched {
        job = job.watch_as(path, on_change);
    }
    if let Some(prompt) = eval {
        job = job.eval_prompt(prompt);
    }
    // Asking for a retry is saying a second run is safe.
    if let Some(n) = retries {
        job = job.retries(n).idempotent(true);
    }
    if idempotent {
        job = job.idempotent(true);
    }
    if let Some(line) = on_done {
        job = job.on_done(wake(&line)?);
    }
    Ok(job)
}

/// A wake command from a shell: a shell line, run where it was submitted,
/// like the job itself.
fn wake(line: &str) -> Result<Command> {
    Ok(Command::line(line).cwd(absolute(".")?))
}

/// `pec barrier --after <ids> [...]`, as the barrier it describes.
fn parse_barrier(words: Vec<String>) -> Result<Job> {
    let mut job = Job::barrier();
    let mut after = Vec::new();
    let mut flags = words.iter();
    while let Some(flag) = flags.next() {
        let mut value = || match flags.next() {
            Some(v) => Ok(v.as_str()),
            None => usage(format!("{flag} needs a value")),
        };
        match flag.as_str() {
            "--after" => after.extend(parse_ids(value()?)?),
            "--name" => job = job.name(value()?),
            "--eval" => job = job.eval_prompt(value()?),
            "--on-done" => job = job.on_done(wake(value()?)?),
            "--fail-at-end" => job = job.policy(FailurePolicy::FailAtEnd),
            other => return usage(format!("barrier does not take {other}")),
        }
    }
    // A barrier on nothing is done the moment it exists; almost certainly
    // not what was meant.
    if after.is_empty() {
        return usage("barrier needs --after <ids>");
    }
    Ok(job.after(after))
}

fn parse_id(raw: &str) -> Result<JobId> {
    match raw.parse() {
        Ok(id) => Ok(id),
        Err(_) => usage(format!("{raw:?} is not a job id; they look like j42")),
    }
}

/// Set by SIGTERM or SIGINT. `pec work --forever` reads it between jobs:
/// it claims nothing new, finishes and commits what it is running, then
/// exits.
static STOP: AtomicBool = AtomicBool::new(false);

/// Route SIGTERM and SIGINT to [`STOP`]. A second one takes the default
/// action and ends the worker at once; its running child, in a process
/// group of its own, is then left to the next worker, which reclaims the
/// lease once it rots and kills the child.
///
/// std has no signal API, and one flag is not worth a dependency: the
/// handler only stores to an atomic and resets its own disposition, both
/// async-signal-safe.
#[cfg(unix)]
fn stop_on_signals() {
    const SIGINT: i32 = 2;
    const SIGTERM: i32 = 15;
    const SIG_DFL: usize = 0;
    unsafe extern "C" {
        fn signal(signum: i32, handler: usize) -> usize;
    }
    extern "C" fn on_signal(signum: i32) {
        STOP.store(true, std::sync::atomic::Ordering::Relaxed);
        // SAFETY: signal(2) is async-signal-safe, and SIG_DFL is valid
        // for any signal.
        unsafe {
            signal(signum, SIG_DFL);
        }
    }
    let handler = on_signal as extern "C" fn(i32) as usize;
    // SAFETY: the handler has the signature signal(2) expects, lives for
    // the whole program, and does nothing that is not async-signal-safe.
    unsafe {
        signal(SIGINT, handler);
        signal(SIGTERM, handler);
    }
}

#[cfg(not(unix))]
fn stop_on_signals() {}

/// GPU device indices: `0,1`, or `none` (or nothing) for a worker that
/// should take no GPU job.
fn parse_devices(raw: &str) -> Result<Vec<u32>> {
    if raw.is_empty() || raw == "none" {
        return Ok(Vec::new());
    }
    raw.split(',').map(|d| number("--gpus", d.trim())).collect()
}

/// `j1,j2`, as a shell passes a list without quoting.
fn parse_ids(raw: &str) -> Result<Vec<JobId>> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(parse_id)
        .collect()
}

fn number<T: std::str::FromStr>(flag: &str, raw: &str) -> Result<T> {
    match raw.parse() {
        Ok(n) => Ok(n),
        Err(_) => usage(format!("{flag} takes a number, not {raw:?}")),
    }
}

fn absolute(path: &str) -> Result<PathBuf> {
    match std::path::absolute(path) {
        Ok(p) => Ok(p),
        Err(e) => usage(format!("{path}: {e}")),
    }
}

/// Seconds, or a number with a unit: `90`, `90s`, `5m`, `1.5h`.
fn parse_duration(raw: &str) -> Result<Duration> {
    let (number, scale) = match raw.char_indices().last() {
        Some((i, 's')) => (&raw[..i], 1.0),
        Some((i, 'm')) => (&raw[..i], 60.0),
        Some((i, 'h')) => (&raw[..i], 3600.0),
        _ => (raw, 1.0),
    };
    match number.parse::<f64>() {
        Ok(n) if n >= 0.0 && n.is_finite() => Ok(Duration::from_secs_f64(n * scale)),
        _ => usage(format!(
            "{raw:?} is not a duration; try 90, 90s, 5m or 1.5h"
        )),
    }
}

fn kind(s: &JobStatus) -> &'static str {
    if s.is_barrier {
        "barrier"
    } else if s.task.is_some() {
        "task"
    } else if s.script.is_some() {
        "rust"
    } else {
        "job"
    }
}

fn display_name(s: &JobStatus) -> String {
    s.name
        .clone()
        .or_else(|| s.task.clone())
        .unwrap_or_default()
}

/// How long the job has run, or ran; `None` before it started.
fn ran_for(s: &JobStatus) -> Option<Duration> {
    let start = s.started_at?;
    let end = s.finished_at.unwrap_or_else(SystemTime::now);
    Some(end.duration_since(start).unwrap_or_default())
}

/// Why it ended the way it did, in a few words: the exit code when it was
/// not zero, and the failure line.
fn outcome(s: &JobStatus) -> String {
    let mut parts = Vec::new();
    if let Some(code) = s.exit_code.filter(|c| *c != 0) {
        parts.push(format!("exit {code}"));
    }
    if let Some(why) = &s.failure {
        parts.push(why.clone());
    }
    parts.join(": ")
}

/// One job, one line: the table `pec status` prints.
fn status_line(q: &Queue, s: &JobStatus) -> Result<String> {
    let mut line = format!(
        "{:<6} {:<8} {:<9} {:>7}  {}",
        s.id.to_string(),
        kind(s),
        s.state,
        ran_for(s).map(human).unwrap_or_else(|| "-".into()),
        display_name(s),
    );
    if s.stalled {
        line.push_str(" (stalled)");
    }
    let why = outcome(s);
    if !why.is_empty() {
        let _ = write!(line, "  ({why})");
    }
    let events = q.events(s.id)?.len();
    if events > 0 {
        let _ = write!(
            line,
            "  [{events} event{}]",
            if events == 1 { "" } else { "s" }
        );
    }
    Ok(line.trim_end().to_string())
}

/// One job in full: everything an evaluator or a human reading the store
/// would want before opening the logs.
fn print_detail(q: &Queue, s: &JobStatus) -> Result<()> {
    let mut head = format!("{} {}", s.id, s.state);
    let name = display_name(s);
    if !name.is_empty() {
        head = format!("{} {name}: {}", s.id, s.state);
    }
    let why = outcome(s);
    if !why.is_empty() {
        let _ = write!(head, " ({why})");
    }
    if s.stalled {
        head.push_str(" (stalled)");
    }
    println!("{head}");

    let row = |label: &str, value: &str| println!("  {label:<9} {value}");
    row("kind", &format!("{}, attempt {}", kind(s), s.attempt));
    if let Some(cmd) = q.command(s.id)? {
        let line = if cmd.shell {
            cmd.program.clone()
        } else {
            std::iter::once(cmd.program.as_str())
                .chain(cmd.args.iter().map(String::as_str))
                .collect::<Vec<_>>()
                .join(" ")
        };
        row("command", &line);
        if let Some(dir) = &cmd.cwd {
            row("cwd", &dir.display().to_string());
        }
    }
    if let Some(task) = &s.task {
        row("task", task);
    }
    if let Some(script) = &s.script {
        row("script", &script.display().to_string());
    }
    if !s.deps.is_empty() {
        let deps = q
            .statuses(&s.deps)?
            .iter()
            .map(|d| format!("{} ({})", d.id, d.state))
            .collect::<Vec<_>>()
            .join(", ");
        row("after", &deps);
    }

    let r = &s.resources;
    let mut declared = format!("{} cpu{}", r.cpus, if r.cpus == 1 { "" } else { "s" });
    if r.gpus > 0 {
        let _ = write!(
            declared,
            ", {} gpu{}",
            r.gpus,
            if r.gpus == 1 { "" } else { "s" }
        );
    }
    if r.mem_mb > 0 {
        let _ = write!(declared, ", {} MB", r.mem_mb);
    }
    if let Some(est) = r.est {
        let _ = write!(declared, ", est {}", human(est));
    }
    row("declared", &declared);

    // The line the roadmap asks for: declared against actual.
    if let Some(ran) = ran_for(s) {
        let mut actual = human(ran);
        if s.state == State::Running {
            actual = format!("running for {actual}");
        }
        if let Some(p) = &s.profile {
            let _ = write!(
                actual,
                ", {:.1} cores avg, peak {} MB, idle {:.0}%",
                p.avg_cores,
                p.peak_rss_mb,
                p.idle_fraction * 100.0
            );
            if let Some(gpu) = p.gpu_util_avg {
                let _ = write!(actual, ", gpu {:.0}% avg", gpu * 100.0);
            }
        }
        row("ran", &actual);
    }
    if let Some(start) = s.started_at {
        row(
            "waited",
            &human(start.duration_since(s.submitted_at).unwrap_or_default()),
        );
    }

    if !s.is_barrier && s.attempt > 0 {
        let logs = q.logs(s.id)?;
        row("stdout", &logs.stdout.display().to_string());
        row("stderr", &logs.stderr.display().to_string());
    }
    if let Some(prompt) = &s.eval_prompt {
        row("eval", prompt);
    }
    for (i, e) in q.events(s.id)?.iter().enumerate() {
        let at = e.at.duration_since(s.submitted_at).unwrap_or_default();
        let label = if i == 0 { "events" } else { "" };
        row(label, &format!("+{} {} {}", human(at), e.level, e.message));
    }
    Ok(())
}

/// One job as one line of JSON: everything the table and the full view
/// show, for a script or an agent that should not parse either.
fn status_json(q: &Queue, s: &JobStatus) -> Result<String> {
    let r = &s.resources;
    let resources = Json::obj()
        .raw("cpus", r.cpus)
        .raw("gpus", r.gpus)
        .raw("mem_mb", r.mem_mb)
        .opt("est_secs", r.est.map(|d| d.as_secs()))
        .done();
    let command = match q.command(s.id)? {
        Some(c) => Json::obj()
            .str("program", &c.program)
            .raw("args", array(c.args.iter().map(|a| quote(a))))
            .raw("shell", c.shell)
            .opt_str("cwd", c.cwd.map(|p| p.display().to_string()).as_deref())
            .done(),
        None => "null".into(),
    };
    let logs = if s.is_barrier {
        "null".into()
    } else {
        let logs = q.logs(s.id)?;
        Json::obj()
            .str("stdout", &logs.stdout.display().to_string())
            .str("stderr", &logs.stderr.display().to_string())
            .done()
    };
    let profile = match &s.profile {
        Some(p) => Json::obj()
            .raw("wall_secs", p.wall.as_secs_f64())
            .raw("peak_rss_mb", p.peak_rss_mb)
            .raw("avg_cores", p.avg_cores)
            .raw("idle_fraction", p.idle_fraction)
            .opt("gpu_util_avg", p.gpu_util_avg)
            .opt("gpu_util_peak", p.gpu_util_peak)
            .done(),
        None => "null".into(),
    };
    let events = array(q.events(s.id)?.iter().map(|e| {
        Json::obj()
            .raw("at", unix(e.at))
            .str("level", &e.level.to_string())
            .str("message", &e.message)
            .done()
    }));

    Ok(Json::obj()
        .str("id", &s.id.to_string())
        .opt_str("name", s.name.as_deref())
        .str("kind", kind(s))
        .str("state", &s.state.to_string())
        .raw("stalled", s.stalled)
        .raw("attempt", s.attempt)
        .opt("exit_code", s.exit_code)
        .opt_str("failure", s.failure.as_deref())
        .raw("deps", array(s.deps.iter().map(|d| quote(&d.to_string()))))
        .raw("resources", resources)
        .raw("submitted_at", unix(s.submitted_at))
        .opt("started_at", s.started_at.map(unix))
        .opt("finished_at", s.finished_at.map(unix))
        .raw("command", command)
        .opt_str("task", s.task.as_deref())
        .opt_str(
            "script",
            s.script
                .as_ref()
                .map(|p| p.display().to_string())
                .as_deref(),
        )
        .raw("logs", logs)
        .opt_str("eval_prompt", s.eval_prompt.as_deref())
        .raw("profile", profile)
        .raw("events", events)
        .done())
}

/// Just enough JSON to print a status. `pec` carries no serializer, and a
/// status is strings, numbers, booleans, nulls and a little nesting.
struct Json(String);

impl Json {
    fn obj() -> Json {
        Json(String::from("{"))
    }

    /// A value that is already JSON: a number, a boolean, a nested object.
    fn raw(mut self, key: &str, value: impl std::fmt::Display) -> Json {
        if self.0.len() > 1 {
            self.0.push(',');
        }
        let _ = write!(self.0, "{}:{value}", quote(key));
        self
    }

    fn str(self, key: &str, value: &str) -> Json {
        self.raw(key, quote(value))
    }

    fn opt(self, key: &str, value: Option<impl std::fmt::Display>) -> Json {
        match value {
            Some(v) => self.raw(key, v),
            None => self.raw(key, "null"),
        }
    }

    fn opt_str(self, key: &str, value: Option<&str>) -> Json {
        self.opt(key, value.map(quote))
    }

    fn done(mut self) -> String {
        self.0.push('}');
        self.0
    }
}

fn array(items: impl Iterator<Item = String>) -> String {
    format!("[{}]", items.collect::<Vec<_>>().join(","))
}

fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Unix seconds, to the millisecond the store keeps.
fn unix(t: SystemTime) -> f64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as f64 / 1000.0)
        .unwrap_or(0.0)
}

/// A duration the way a person says it: 850ms, 12s, 3m05s, 1h02m.
fn human(d: Duration) -> String {
    let secs = d.as_secs();
    match secs {
        0 => format!("{}ms", d.as_millis()),
        1..60 => format!("{secs}s"),
        60..3600 => format!("{}m{:02}s", secs / 60, secs % 60),
        _ => format!("{}h{:02}m", secs / 3600, secs % 3600 / 60),
    }
}
