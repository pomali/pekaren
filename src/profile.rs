use std::time::{Duration, SystemTime};

/// One reading of a running child, taken every few seconds by a thread in
/// the supervising worker. Cheap enough to ignore; enough to draw the crude
/// shape of a run.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sample {
    pub at: SystemTime,
    pub rss_mb: u64,
    /// Cores actually busy, averaged over the interval since the last
    /// sample. 1.0 means one core saturated.
    pub cpu_cores: f64,
    /// 0.0–1.0, where the platform reports it.
    pub gpu_util: Option<f64>,
}

/// What an attempt actually cost, rolled up from its samples.
///
/// The point is the comparison: "estimated 20 min, took 90 min, averaged 1.2
/// of 8 cores" tells an evaluator the job was I/O-bound or misconfigured,
/// and it sharpens the agent's next estimate. The same samples feed stall
/// detection.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Profile {
    pub wall: Duration,
    pub peak_rss_mb: u64,
    pub avg_cores: f64,
    pub gpu_util_avg: Option<f64>,
    pub gpu_util_peak: Option<f64>,
    /// Fraction of wall-clock spent waiting rather than computing.
    pub idle_fraction: f64,
}
