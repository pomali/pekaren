use std::borrow::Borrow;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::id::JobId;

/// A command to run. The library stores it and executes it; it never parses
/// or rewrites it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Command {
    /// The shell line when `shell`, otherwise the program to exec.
    pub program: String,
    /// Arguments, when exec'ing directly. Empty for a shell line.
    pub args: Vec<String>,
    /// Run through `sh -c` (unix) rather than exec'ing `program` directly.
    pub shell: bool,
    pub cwd: Option<PathBuf>,
    /// Extra environment, applied on top of the worker's own.
    pub env: Vec<(String, String)>,
}

impl Command {
    /// A shell line: `Command::line("python train.py --lr 3e-4")` runs through
    /// `sh -c`, so redirection, globbing and `&&` work as written. The caller
    /// owns quoting, exactly as in a terminal.
    pub fn line(line: impl Into<String>) -> Self {
        Command {
            program: line.into(),
            args: Vec::new(),
            shell: true,
            cwd: None,
            env: Vec::new(),
        }
    }

    /// Exec a program directly, with no shell between: nothing is split,
    /// quoted or expanded.
    pub fn exec<I, S>(program: impl Into<String>, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Command {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            shell: false,
            cwd: None,
            env: Vec::new(),
        }
    }

    pub fn cwd(mut self, dir: impl AsRef<Path>) -> Self {
        self.cwd = Some(dir.as_ref().to_path_buf());
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// How a single-file Rust script is run.
    ///
    /// `cargo -Zscript` by default, which needs a nightly toolchain;
    /// `PEKAREN_RUST_RUNNER` overrides it with a whitespace-separated
    /// command line the script path is appended to (`rust-script`, say, or
    /// a wrapper of your own). Resolved at submit time and stored, so the
    /// job runs the same way an hour later whatever the worker's
    /// environment looks like.
    pub fn rust_script(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_string_lossy().into_owned();
        match std::env::var("PEKAREN_RUST_RUNNER") {
            Ok(runner) if !runner.trim().is_empty() => {
                let mut words = runner.split_whitespace().map(str::to_owned);
                let program = words.next().expect("non-empty after trim check");
                let args: Vec<String> = words.chain(std::iter::once(path)).collect();
                Command::exec(program, args)
            }
            _ => Command::exec("cargo", ["+nightly", "-Zscript", &path]),
        }
    }
}

impl From<&str> for Command {
    fn from(line: &str) -> Self {
        Command::line(line)
    }
}

impl From<String> for Command {
    fn from(line: String) -> Self {
        Command::line(line)
    }
}

/// Rust source to run as a job, rather than a command line.
///
/// Rust is already the scripting surface for building the graph; this is the
/// same thing one level down, so a job can be a few lines of Rust without a
/// separate file, a `python -c`, or a shell quoting puzzle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RustScript {
    pub source: RustSource,
    /// An inline dependency manifest, written into the script's frontmatter.
    /// Ignored when the source already carries its own.
    pub manifest: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RustSource {
    /// Source text, written into the store at submit time.
    Inline(String),
    /// An existing `.rs` file, run where it lies.
    File(PathBuf),
}

impl RustScript {
    /// The text as it will be written: the source, with a frontmatter
    /// manifest in front of it when one was declared and the source has
    /// none of its own.
    pub fn render(&self) -> Option<String> {
        let RustSource::Inline(source) = &self.source else {
            return None;
        };
        let has_frontmatter = source.trim_start().starts_with("---");
        Some(match &self.manifest {
            Some(manifest) if !has_frontmatter => {
                format!("---\n{}\n---\n\n{source}", manifest.trim())
            }
            _ => source.clone(),
        })
    }
}

/// What a node is: work, or a place where work meets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobKind {
    Command(Command),
    /// Rust source, run as a single-file script.
    Rust(RustScript),
    /// A node with no command of its own that fires when its dependencies
    /// settle. Notification hangs off barriers, not leaves, so an expensive
    /// context is pinged once per subgraph rather than once per job.
    Barrier,
}

/// What a barrier does with a failed dependency.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum FailurePolicy {
    /// Mark the barrier failed and notify immediately. The default, because
    /// wasted compute is the safer surprise over a wasted hour.
    #[default]
    FailFast,
    /// Mark the barrier failed, let the remaining jobs run, notify once with
    /// the full picture. Suits sweeps where a missing point is tolerable.
    FailAtEnd,
}

/// What gets spawned when a node settles. The library stores the command and
/// runs it; it does not interpret it.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum Wake {
    /// Nothing is spawned. The outcome sits in the store until someone asks.
    #[default]
    Nothing,
    /// Always spawn this command.
    Always(Command),
    /// Spawn `warm` while the submitting session's prompt cache is still
    /// judged warm, `cold` after that.
    ///
    /// Cache-warm detection is an open question. Today the cutoff is a plain
    /// wall-clock deadline written at submit time, when the agent still knows
    /// what it is worth waking for.
    ByWarmth {
        warm: Command,
        cold: Command,
        warm_until: SystemTime,
    },
}

impl From<Command> for Wake {
    fn from(cmd: Command) -> Self {
        Wake::Always(cmd)
    }
}

impl From<&str> for Wake {
    fn from(line: &str) -> Self {
        Wake::Always(Command::line(line))
    }
}

impl From<String> for Wake {
    fn from(line: String) -> Self {
        Wake::Always(Command::line(line))
    }
}

/// What a job says it needs. Drives admission control, and doubles as the
/// baseline its actual run is compared against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resources {
    /// Counted, not named: 4 of 16 cores.
    pub cpus: u32,
    /// Counted at declaration, assigned by name at claim: the worker sets
    /// `CUDA_VISIBLE_DEVICES` to the devices it took.
    pub gpus: u32,
    /// Admission control only; the OOM killer is the backstop. 0 means the
    /// job made no claim.
    pub mem_mb: u64,
    /// Estimated wall-clock. Feeds the runaway cap and the
    /// declared-vs-actual line the evaluator reads.
    pub est: Option<Duration>,
}

impl Default for Resources {
    fn default() -> Self {
        Resources {
            cpus: 1,
            gpus: 0,
            mem_mb: 0,
            est: None,
        }
    }
}

/// When the supervising worker kills a job that will not end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KillAfter {
    /// A multiple of the declared estimate. Ignored when nothing was
    /// estimated.
    EstimateTimes(u32),
    Fixed(Duration),
    Never,
}

impl Default for KillAfter {
    fn default() -> Self {
        KillAfter::EstimateTimes(3)
    }
}

/// A job, described before it exists. Built by value; nothing touches the
/// store until [`Queue::submit`](crate::Queue::submit).
#[derive(Clone, Debug)]
pub struct Job {
    pub(crate) name: Option<String>,
    pub(crate) kind: JobKind,
    pub(crate) deps: Vec<JobId>,
    pub(crate) resources: Resources,
    pub(crate) eval_prompt: Option<String>,
    pub(crate) retries: u32,
    pub(crate) idempotent: bool,
    pub(crate) policy: FailurePolicy,
    pub(crate) wake: Wake,
    pub(crate) kill_after: KillAfter,
}

impl Job {
    /// A job that runs a shell line. See [`Command::line`] for what the shell
    /// does with it.
    pub fn cmd(line: impl Into<String>) -> Self {
        Job::new(JobKind::Command(Command::line(line)))
    }

    /// A job that execs a program directly, with no shell between.
    pub fn exec<I, S>(program: impl Into<String>, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Job::new(JobKind::Command(Command::exec(program, args)))
    }

    /// A job built from a [`Command`] you assembled yourself.
    pub fn run(command: Command) -> Self {
        Job::new(JobKind::Command(command))
    }

    /// A job that runs Rust source directly.
    ///
    /// The source is written into the store at submit time and run as a
    /// single-file script, so a job can be Rust without a file to manage or
    /// a project to scaffold. Add dependencies with
    /// [`rust_manifest`](Job::rust_manifest).
    ///
    /// ```no_run
    /// # use pekaren::prelude::*;
    /// # fn main() -> Result<()> {
    /// # let q = Queue::open_default()?;
    /// q.submit(
    ///     Job::rust(r#"fn main() { println!("hello from a job"); }"#)
    ///         .eval_prompt("Should print a greeting"),
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn rust(source: impl Into<String>) -> Self {
        Job::new(JobKind::Rust(RustScript {
            source: RustSource::Inline(source.into()),
            manifest: None,
        }))
    }

    /// A job that runs an existing `.rs` file as a single-file script,
    /// where it lies. Nothing is copied, so edits to the file before it is
    /// claimed are picked up.
    pub fn rust_file(path: impl AsRef<Path>) -> Self {
        Job::new(JobKind::Rust(RustScript {
            source: RustSource::File(path.as_ref().to_path_buf()),
            manifest: None,
        }))
    }

    /// The inline dependency manifest for a [`Job::rust`] source, as the
    /// body of a cargo script's frontmatter:
    ///
    /// ```no_run
    /// # use pekaren::prelude::*;
    /// Job::rust("fn main() { /* ... */ }").rust_manifest("[dependencies]\nserde_json = \"1\"");
    /// ```
    ///
    /// Ignored on any other kind of job, and on source that already carries
    /// its own frontmatter.
    pub fn rust_manifest(mut self, manifest: impl Into<String>) -> Self {
        if let JobKind::Rust(script) = &mut self.kind {
            script.manifest = Some(manifest.into());
        }
        self
    }

    /// A barrier: no command, fires when its dependencies settle.
    pub fn barrier() -> Self {
        Job::new(JobKind::Barrier)
    }

    fn new(kind: JobKind) -> Self {
        Job {
            name: None,
            kind,
            deps: Vec::new(),
            resources: Resources::default(),
            eval_prompt: None,
            retries: 0,
            idempotent: false,
            policy: FailurePolicy::default(),
            wake: Wake::default(),
            kill_after: KillAfter::default(),
        }
    }

    /// A label for humans and for `pec status`. Never interpreted.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Depend on jobs that already exist, running ones included. Takes
    /// anything that yields [`JobId`]s: `&runs`, `[a, b]`, `&[a]`.
    pub fn after<I>(mut self, deps: I) -> Self
    where
        I: IntoIterator,
        I::Item: Borrow<JobId>,
    {
        self.deps.extend(deps.into_iter().map(|d| *d.borrow()));
        self
    }

    pub fn cpus(mut self, n: u32) -> Self {
        self.resources.cpus = n;
        self
    }

    pub fn gpus(mut self, n: u32) -> Self {
        self.resources.gpus = n;
        self
    }

    pub fn mem_mb(mut self, mb: u64) -> Self {
        self.resources.mem_mb = mb;
        self
    }

    pub fn est_minutes(self, minutes: u64) -> Self {
        self.est(Duration::from_secs(minutes * 60))
    }

    pub fn est(mut self, d: Duration) -> Self {
        self.resources.est = Some(d);
        self
    }

    /// How to judge this job's output, written now, while the submitting
    /// agent still knows what success looks like. A fresh small context reads
    /// it when the work finishes.
    pub fn eval_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.eval_prompt = Some(prompt.into());
        self
    }

    /// How many times to re-run on failure. Only honoured for a job marked
    /// [`idempotent`](Job::idempotent).
    pub fn retries(mut self, n: u32) -> Self {
        self.retries = n;
        self
    }

    /// Whether re-running is safe. Defaults to `false`: a job that appends to
    /// a database goes straight to failed instead of being retried or
    /// reclaimed after a lease lapses.
    pub fn idempotent(mut self, yes: bool) -> Self {
        self.idempotent = yes;
        self
    }

    /// Barrier-only. Ignored on a command job.
    pub fn policy(mut self, policy: FailurePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// What to spawn when this node settles. Takes a [`Wake`] or anything a
    /// [`Command`] can be built from.
    pub fn on_done(mut self, wake: impl Into<Wake>) -> Self {
        self.wake = wake.into();
        self
    }

    /// When the supervising worker kills a job that overruns.
    pub fn kill_after(mut self, when: KillAfter) -> Self {
        self.kill_after = when;
        self
    }

    /// The resolved wall-clock cap, or `None` when nothing bounds this job.
    pub(crate) fn kill_after_duration(&self) -> Option<Duration> {
        match self.kill_after {
            KillAfter::Never => None,
            KillAfter::Fixed(d) => Some(d),
            KillAfter::EstimateTimes(n) => self.resources.est.map(|e| e * n),
        }
    }
}
