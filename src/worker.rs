use std::io::Read;
use std::path::Path;
use std::process::{Child, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use crate::error::{Error, Result};
use crate::id::JobId;
use crate::profile::Sample;
use crate::queue::Queue;
use crate::store::{Claim, Outcome};

/// What one host has to offer. Read once as a worker starts and written to
/// the store, so a single-machine store has exactly one row and a later
/// multi-node store still reads right.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Capacity {
    pub cpus: u32,
    pub mem_mb: u64,
    /// GPUs are named, not counted: device 0 and device 1 are not
    /// interchangeable once something is resident.
    pub gpus: Vec<u32>,
}

impl Capacity {
    /// What this machine appears to have.
    ///
    /// CPU count and memory come from the OS. GPUs are read from
    /// `PEKAREN_GPUS` (comma-separated device indices) or, failing that,
    /// `CUDA_VISIBLE_DEVICES`; probing the driver is still to come.
    pub fn detect() -> Result<Capacity> {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get() as u32)
            .unwrap_or(1);
        Ok(Capacity {
            cpus,
            mem_mb: total_mem_mb(),
            gpus: gpus_from_env(),
        })
    }

    /// How many cores to leave for the rest of the machine.
    pub fn reserve_cpus(mut self, n: u32) -> Self {
        self.cpus = self.cpus.saturating_sub(n).max(1);
        self
    }
}

/// What a worker did before it stopped.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerReport {
    pub committed: Vec<JobId>,
    pub failed: Vec<JobId>,
    /// Jobs this worker claimed but could not commit, because something
    /// else reclaimed them first. Normal, and always safe.
    pub lost_races: u32,
}

/// Runs jobs out of a store.
///
/// A worker is not a service: it is a loop any process can enter, including
/// the one that just submitted. It claims a job with a lease, supervises the
/// child, renews while alive, and commits conditionally.
pub struct Worker<'q> {
    queue: &'q Queue,
    capacity: Capacity,
    lease: Duration,
    poll: Duration,
    host: String,
}

impl<'q> Worker<'q> {
    pub fn new(queue: &'q Queue) -> Self {
        Worker {
            queue,
            capacity: Capacity::detect().unwrap_or(Capacity {
                cpus: 1,
                mem_mb: 0,
                gpus: Vec::new(),
            }),
            lease: Duration::from_secs(90),
            poll: Duration::from_millis(200),
            host: hostname(),
        }
    }

    /// Override what this worker believes the host has. Defaults to
    /// [`Capacity::detect`].
    pub fn capacity(mut self, capacity: Capacity) -> Self {
        self.capacity = capacity;
        self
    }

    /// How far out a lease expires. Renewed continuously while the worker
    /// lives; left to rot when it dies.
    pub fn lease(mut self, d: Duration) -> Self {
        self.lease = d;
        self
    }

    /// How often to look at the child, and at the store when idle.
    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll = d;
        self
    }

    /// Claim and run one job if anything is claimable right now. Returns
    /// `None` when nothing is.
    pub fn run_one(&mut self) -> Result<Option<JobId>> {
        self.queue.store().settle_barriers()?;
        let Some(claim) = self
            .queue
            .store()
            .claim_next(&self.host, &self.capacity, self.lease)?
        else {
            return Ok(None);
        };
        let id = claim.id;
        self.run_claim(claim)?;
        self.queue.store().settle_barriers()?;
        Ok(Some(id))
    }

    /// Run until the store holds nothing this worker can claim.
    pub fn run_until_idle(&mut self) -> Result<WorkerReport> {
        let stop = Arc::new(AtomicBool::new(false));
        self.run_inner(&stop, None, true)
    }

    /// Run for at most `d`, then return. Claims nothing new past the
    /// deadline, and never abandons a child it started.
    pub fn run_for(&mut self, d: Duration) -> Result<WorkerReport> {
        let stop = Arc::new(AtomicBool::new(false));
        self.run_inner(&stop, Some(Instant::now() + d), false)
    }

    /// Run until someone sets `stop`. This is what a background worker
    /// thread does: idle politely rather than exiting when the queue
    /// empties.
    pub fn run_while(&mut self, stop: &Arc<AtomicBool>) -> Result<WorkerReport> {
        self.run_inner(stop, None, false)
    }

    fn run_inner(
        &mut self,
        stop: &Arc<AtomicBool>,
        deadline: Option<Instant>,
        stop_when_idle: bool,
    ) -> Result<WorkerReport> {
        let mut report = WorkerReport::default();
        // Whatever else happens, first give back what dead workers left
        // holding leases.
        self.queue.reap()?;

        while !stop.load(Ordering::Relaxed) {
            if deadline.is_some_and(|d| Instant::now() >= d) {
                break;
            }
            self.queue.store().settle_barriers()?;

            match self
                .queue
                .store()
                .claim_next(&self.host, &self.capacity, self.lease)?
            {
                Some(claim) => {
                    let id = claim.id;
                    match self.run_claim(claim)? {
                        Committed::Done => report.committed.push(id),
                        Committed::Failed => report.failed.push(id),
                        Committed::LostRace => report.lost_races += 1,
                    }
                    self.queue.store().settle_barriers()?;
                }
                None if stop_when_idle => break,
                None => std::thread::sleep(self.poll),
            }
        }
        Ok(report)
    }

    /// Everything between holding a lease and having committed an outcome.
    fn run_claim(&mut self, claim: Claim) -> Result<Committed> {
        // Before anything expensive: is the job still the job that was
        // submitted? A binary rebuilt since is different code, and running
        // it under the old evaluation prompt is worse than not running it.
        let drift = self.queue.check_inputs(claim.id)?;
        if let Some(fatal) = drift.iter().find(|d| d.is_fatal()) {
            let note = format!("{} changed since submit", fatal.path.display());
            let committed =
                self.queue
                    .store()
                    .commit(&claim, Outcome::Failed, None, Some(&note))?;
            return Ok(if committed {
                Committed::Failed
            } else {
                Committed::LostRace
            });
        }

        let mut child = match self.spawn(&claim) {
            Ok(child) => child,
            Err(e) => {
                let note = e.to_string();
                let committed =
                    self.queue
                        .store()
                        .commit(&claim, Outcome::Failed, None, Some(&note))?;
                return Ok(if committed {
                    Committed::Failed
                } else {
                    Committed::LostRace
                });
            }
        };

        let pid = child.id();
        self.queue
            .store()
            .record_child(&claim, pid, process_start(pid).unwrap_or(0))?;

        let outcome = self.supervise(&claim, &mut child)?;
        if outcome.0 == Outcome::Killed {
            self.queue.store().record_event(
                claim.id,
                "warn",
                &format!("{} killed after running past its cap", claim.describe()),
            )?;
        }
        let exit = outcome.1;
        self.queue.store().record_samples(&claim, &outcome.2)?;

        // Files are not transactional, so the scratch directory only
        // becomes the job's output once the commit has actually landed.
        let committed = self.queue.store().commit(
            &claim,
            outcome.0,
            exit,
            match outcome.0 {
                Outcome::Done => None,
                Outcome::Killed => Some("killed after running past its cap"),
                _ => Some("exited non-zero"),
            },
        )?;
        if !committed {
            // Someone reclaimed the job while we ran it. Our output is not
            // this job's output; throw it away.
            let _ = std::fs::remove_dir_all(&claim.scratch);
            return Ok(Committed::LostRace);
        }
        if outcome.0 == Outcome::Done {
            self.publish_scratch(&claim)?;
        }

        Ok(match outcome.0 {
            Outcome::Done => Committed::Done,
            _ => Committed::Failed,
        })
    }

    fn spawn(&self, claim: &Claim) -> Result<Child> {
        let logs = self.queue.store().log_dir(claim.id);
        std::fs::create_dir_all(&logs).map_err(|e| Error::io(&logs, e))?;
        std::fs::create_dir_all(&claim.scratch).map_err(|e| Error::io(&claim.scratch, e))?;
        let out = std::fs::File::create(logs.join(format!("{}.out", claim.attempt)))
            .map_err(|e| Error::io(&logs, e))?;
        let err = std::fs::File::create(logs.join(format!("{}.err", claim.attempt)))
            .map_err(|e| Error::io(&logs, e))?;

        let cmd = &claim.command;
        let mut process = if cmd.shell {
            let mut p = std::process::Command::new("sh");
            p.arg("-c").arg(&cmd.program);
            p
        } else {
            let mut p = std::process::Command::new(&cmd.program);
            p.args(&cmd.args);
            p
        };
        if let Some(dir) = &cmd.cwd {
            process.current_dir(dir);
        }
        for (key, value) in &cmd.env {
            process.env(key, value);
        }
        process
            .env(crate::task::JOB_ENV, claim.id.to_string())
            .env("PEKAREN_STORE", self.queue.path())
            .env("PEKAREN_SCRATCH", &claim.scratch)
            // The child sees only the devices it was given, and calls them
            // 0 onward.
            .env(
                "CUDA_VISIBLE_DEVICES",
                claim
                    .gpus
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err));

        process
            .spawn()
            .map_err(|e| Error::io(Path::new(&cmd.program), e))
    }

    /// Watch the child: renew the lease, sample it, and enforce the cap.
    fn supervise(
        &self,
        claim: &Claim,
        child: &mut Child,
    ) -> Result<(Outcome, Option<i32>, Vec<Sample>)> {
        let started = Instant::now();
        let mut samples = Vec::new();
        let mut last_renew = Instant::now();
        let mut last_sample = Instant::now();
        let mut cpu = CpuReading::start(child.id());

        loop {
            if let Some(status) = child.try_wait().map_err(|e| Error::io(Path::new("."), e))? {
                let code = status.code();
                return Ok((
                    if code == Some(0) {
                        Outcome::Done
                    } else {
                        Outcome::Failed
                    },
                    code,
                    samples,
                ));
            }

            // A worker checks its own lease before it matters, not after:
            // this catches the frozen box, alive but long since reclaimed.
            if last_renew.elapsed() >= self.lease / 3 {
                if !self.queue.store().renew(claim, self.lease)? {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Ok((Outcome::Cancelled, None, samples));
                }
                last_renew = Instant::now();
            }

            if last_sample.elapsed() >= Duration::from_secs(1) {
                if let Some(sample) = cpu.sample(child.id()) {
                    samples.push(sample);
                }
                last_sample = Instant::now();
            }

            if claim.kill_after.is_some_and(|cap| started.elapsed() > cap) {
                let _ = child.kill();
                let _ = child.wait();
                return Ok((Outcome::Killed, None, samples));
            }

            std::thread::sleep(self.poll);
        }
    }

    /// Move a finished job's scratch directory to where readers look for
    /// it. Only ever called after the commit landed.
    fn publish_scratch(&self, claim: &Claim) -> Result<()> {
        if std::fs::read_dir(&claim.scratch)
            .map(|mut d| d.next().is_none())
            .unwrap_or(true)
        {
            let _ = std::fs::remove_dir_all(&claim.scratch);
            return Ok(());
        }
        let out = self
            .queue
            .path()
            .join("out")
            .join(claim.id.get().to_string());
        std::fs::create_dir_all(out.parent().unwrap_or(&out)).map_err(|e| Error::io(&out, e))?;
        let dest = out.join(claim.attempt.to_string());
        let _ = std::fs::remove_dir_all(&dest);
        std::fs::rename(&claim.scratch, &dest).map_err(|e| Error::io(&dest, e))?;
        Ok(())
    }
}

enum Committed {
    Done,
    Failed,
    LostRace,
}

/// The tail of a job's stderr, for an error message a human reads.
pub(crate) fn stderr_tail(path: &Path, bytes: usize) -> String {
    let Ok(mut file) = std::fs::File::open(path) else {
        return String::new();
    };
    let mut text = String::new();
    let _ = file.read_to_string(&mut text);
    let text = text.trim_end();
    match text.char_indices().nth_back(bytes) {
        Some((at, _)) => format!("…{}", &text[at..]),
        None => text.to_string(),
    }
}

/// Successive CPU readings, so a sample can report cores rather than
/// seconds. Linux only; elsewhere a job runs unsampled.
struct CpuReading {
    last_ticks: u64,
    last_at: Instant,
}

impl CpuReading {
    fn start(pid: u32) -> CpuReading {
        CpuReading {
            last_ticks: cpu_ticks(pid).unwrap_or(0),
            last_at: Instant::now(),
        }
    }

    fn sample(&mut self, pid: u32) -> Option<Sample> {
        let ticks = cpu_ticks(pid)?;
        let elapsed = self.last_at.elapsed().as_secs_f64().max(0.001);
        // USER_HZ is 100 everywhere this matters; a wrong guess here skews
        // a number nobody bills on.
        let cores = (ticks.saturating_sub(self.last_ticks) as f64 / 100.0) / elapsed;
        self.last_ticks = ticks;
        self.last_at = Instant::now();
        Some(Sample {
            at: SystemTime::now(),
            rss_mb: rss_mb(pid).unwrap_or(0),
            cpu_cores: cores,
            gpu_util: None,
        })
    }
}

#[cfg(target_os = "linux")]
fn stat_fields(pid: u32) -> Option<Vec<String>> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The second field is the executable name in parentheses and may
    // contain spaces, so everything is counted from the last ')'.
    let rest = text.rsplit_once(')')?.1;
    Some(rest.split_whitespace().map(str::to_owned).collect())
}

#[cfg(target_os = "linux")]
fn cpu_ticks(pid: u32) -> Option<u64> {
    let fields = stat_fields(pid)?;
    // utime and stime, counted from the field after the process state.
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

#[cfg(target_os = "linux")]
fn rss_mb(pid: u32) -> Option<u64> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/statm")).ok()?;
    let pages: u64 = text.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * 4096 / (1024 * 1024))
}

/// When the process started, in clock ticks since boot. Paired with the PID
/// it identifies a process precisely enough to kill it, which a PID alone
/// does not: they get recycled.
#[cfg(target_os = "linux")]
pub(crate) fn process_start(pid: u32) -> Option<i64> {
    stat_fields(pid)?.get(19)?.parse().ok()
}

#[cfg(not(target_os = "linux"))]
fn cpu_ticks(_pid: u32) -> Option<u64> {
    None
}

#[cfg(not(target_os = "linux"))]
fn rss_mb(_pid: u32) -> Option<u64> {
    None
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn process_start(_pid: u32) -> Option<i64> {
    None
}

fn total_mem_mb() -> u64 {
    // /proc/meminfo on Linux; other platforms declare nothing and rely on
    // admission control being advisory anyway.
    let Ok(text) = std::fs::read_to_string("/proc/meminfo") else {
        return 0;
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .unwrap_or(0);
            return kb / 1024;
        }
    }
    0
}

fn gpus_from_env() -> Vec<u32> {
    for key in ["PEKAREN_GPUS", "CUDA_VISIBLE_DEVICES"] {
        if let Ok(list) = std::env::var(key) {
            let devices: Vec<u32> = list
                .split(',')
                .filter_map(|d| d.trim().parse().ok())
                .collect();
            if !devices.is_empty() {
                return devices;
            }
        }
    }
    Vec::new()
}

pub(crate) fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|h| h.trim().to_string())
        .ok()
        .filter(|h| !h.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .unwrap_or_else(|| "localhost".into())
}
