//! `pec` — the oven. A thin window onto a store.
//!
//! The full CLI is deferred (see the design doc's open questions): the shape
//! so far is `submit`, `status`, `wait`, `logs`, `reap`, all on the same
//! store as the library. What exists today is the read side, enough to look
//! at a store a script created.
//!
//! Every subcommand opens the default store — `$PEKAREN_STORE`, else
//! `~/.pekaren` — unless `--store` says otherwise.

use std::fmt::Write as _;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pekaren::{Filter, JobId, JobStatus, Queue, QueueOptions, State, default_store_path};

const USAGE: &str = "\
pec — pekáreň's oven

usage:
    pec [--store <dir>] work [--once]
    pec [--store <dir>] status [--json] [<job-id>...]
    pec [--store <dir>] runnable
    pec [--store <dir>] check [<job-id>]
    pec [--store <dir>] wait [--timeout <secs>] [--json] <job-id>...
    pec [--store <dir>] reap
    pec [--store <dir>] where

the store defaults to $PEKAREN_STORE, else ~/.pekaren.

status shows every job, one line each, or one job in full; --json prints
one JSON object per job instead.

wait exits 0 when every job is done, 1 when any failed or was cancelled,
and 124 when the timeout came first (like timeout(1)); a timeout takes
seconds, or 90s, 5m, 1.5h. check exits 1 when a job depends on code that
changed. Exit 2 is pec itself failing: a bad argument, or a store it
cannot use.

deferred: submit, logs.
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
            let q = open()?;
            let once = args.next().as_deref() == Some("--once");
            let mut worker = pekaren::Worker::new(&q);
            let report = if once {
                worker.run_one()?;
                Default::default()
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

fn parse_id(raw: &str) -> Result<JobId> {
    match raw.parse() {
        Ok(id) => Ok(id),
        Err(_) => usage(format!("{raw:?} is not a job id; they look like j42")),
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
