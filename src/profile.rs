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

/// Roll a run's samples into the shape the evaluator reads. `None` when
/// nothing was sampled — a job too short to see is not a profile.
pub(crate) fn roll_up(samples: &[Sample]) -> Option<Profile> {
    if samples.is_empty() {
        return None;
    }
    let n = samples.len() as f64;
    let peak_rss_mb = samples.iter().map(|s| s.rss_mb).max().unwrap_or(0);
    let avg_cores = samples.iter().map(|s| s.cpu_cores).sum::<f64>() / n;

    let gpu: Vec<f64> = samples.iter().filter_map(|s| s.gpu_util).collect();
    let (gpu_util_avg, gpu_util_peak) = if gpu.is_empty() {
        (None, None)
    } else {
        (
            Some(gpu.iter().sum::<f64>() / gpu.len() as f64),
            gpu.iter().copied().reduce(f64::max),
        )
    };

    // Idle means the job was waiting rather than computing: less than a
    // twentieth of a core, sampled.
    let idle = samples.iter().filter(|s| s.cpu_cores < 0.05).count() as f64;

    let wall = match (samples.first(), samples.last()) {
        (Some(first), Some(last)) => last.at.duration_since(first.at).unwrap_or_default(),
        _ => Duration::ZERO,
    };

    Some(Profile {
        wall,
        peak_rss_mb,
        avg_cores,
        gpu_util_avg,
        gpu_util_peak,
        idle_fraction: idle / n,
    })
}
