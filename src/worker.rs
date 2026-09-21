use std::time::Duration;

use crate::error::{Error, Result};
use crate::id::JobId;
use crate::queue::Queue;

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
    /// `CUDA_VISIBLE_DEVICES`; probing the driver is milestone 2's job.
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
    pub started: Vec<JobId>,
    pub committed: Vec<JobId>,
    pub failed: Vec<JobId>,
    /// Jobs this worker lost the race for. Normal, and always safe.
    pub lost_races: u32,
}

/// Runs jobs out of a store.
///
/// A worker is not a service: it is a loop any process can enter, including
/// the one that just submitted. It claims a job with a lease, supervises the
/// child, renews while alive, commits conditionally, and — if it is the last
/// one out of a barrier's dependencies — spawns the handoff as its final act
/// before exiting.
pub struct Worker<'q> {
    queue: &'q Queue,
    capacity: Option<Capacity>,
    lease: Duration,
    poll: Duration,
}

impl<'q> Worker<'q> {
    pub fn new(queue: &'q Queue) -> Self {
        Worker {
            queue,
            capacity: None,
            lease: Duration::from_secs(90),
            poll: Duration::from_secs(2),
        }
    }

    /// Override what this worker believes the host has. Defaults to
    /// [`Capacity::detect`].
    pub fn capacity(mut self, capacity: Capacity) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// How far out a lease expires. Renewed continuously while the worker
    /// lives; left to rot when it dies.
    pub fn lease(mut self, d: Duration) -> Self {
        self.lease = d;
        self
    }

    /// How long to sleep when there is nothing claimable.
    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll = d;
        self
    }

    /// Claim and run one job if anything is claimable right now. Returns
    /// `None` when nothing is.
    pub fn run_one(&mut self) -> Result<Option<JobId>> {
        let _ = (&self.queue, self.lease, self.poll, &self.capacity);
        Err(Error::NotImplemented("Worker::run_one"))
    }

    /// Run until the store holds nothing this worker can claim.
    pub fn run_until_idle(&mut self) -> Result<WorkerReport> {
        Err(Error::NotImplemented("Worker::run_until_idle"))
    }

    /// Run for at most `d`, then return. This is what the grace window uses.
    pub fn run_for(&mut self, _d: Duration) -> Result<WorkerReport> {
        Err(Error::NotImplemented("Worker::run_for"))
    }
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
