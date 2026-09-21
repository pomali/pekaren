//! A learning-rate sweep, the shape an agent writes it in.
//!
//! In practice this is a single `.rs` file with an inline manifest, run with
//! `cargo -Zscript sweep.rs`; here it is an example so it builds with the
//! crate. Loops and fan-outs are ordinary code: collect the handles, then
//! submit one barrier that depends on all of them.

use pekaren::prelude::*;

fn main() -> Result<()> {
    let q = Queue::options()
        .grace(std::time::Duration::from_secs(8))
        .open(std::env::var("PEKAREN_STORE").unwrap_or_else(|_| "~/.pekaren".into()))?;

    let runs: Vec<JobId> = [1e-3, 3e-4, 1e-4]
        .iter()
        .map(|lr| {
            q.submit(
                Job::cmd(format!("python train.py --lr {lr}"))
                    .name(format!("train lr={lr}"))
                    .gpus(1)
                    .mem_mb(16_000)
                    .est_minutes(40)
                    .retries(1)
                    .idempotent(true)
                    .eval_prompt(
                        "Loss should drop below 0.3 and not diverge in the last 500 steps",
                    ),
            )
        })
        .collect::<Result<_>>()?;

    let barrier = q.submit(
        Job::barrier()
            .name("sweep")
            .after(&runs)
            .policy(FailAtEnd)
            .on_done("pec-evaluate --sweep lr")
            .eval_prompt("Pick the run with the lowest final loss; say whether the sweep bracketed a minimum"),
    )?;

    println!("submitted {} runs under {barrier}", runs.len());
    for s in q.statuses(&runs)? {
        println!("  {} {}", s.id, s.state);
    }
    Ok(())
}
