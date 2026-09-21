//! A learning-rate sweep, the shape an agent writes it in.
//!
//! The job body is `train`, an ordinary function in this binary: the
//! compiler checks it, and the store holds a pointer to it — this
//! executable, that name — rather than a copy of its source. A worker runs
//! the job by re-executing this binary, where `bootstrap` dispatches to
//! `train` and exits before any of the submitting code below runs again.

use std::time::Duration;

use pekaren::prelude::*;

fn train(ctx: &JobCtx) -> TaskResult {
    let lr = ctx.arg(0).ok_or("no learning rate")?;
    println!("{}: training with lr={lr}", ctx.job());
    // …the real thing goes here, and returning Err fails the job.
    Ok(())
}

fn main() -> Result<()> {
    let mut tasks = Tasks::new();
    let train = tasks.add("train", train);
    tasks.bootstrap()?; // a worker running this binary stops here

    let q = Queue::options()
        .grace(Duration::from_secs(8))
        .open_default()?;

    let runs: Vec<JobId> = ["1e-3", "3e-4", "1e-4"]
        .iter()
        .map(|lr| {
            q.submit(
                Job::task(train)
                    .arg(*lr)
                    .name(format!("train lr={lr}"))
                    .gpus(1)
                    .mem_mb(16_000)
                    .est_minutes(40)
                    .retries(1)
                    .idempotent(true)
                    // Hashed now; a worker warns if the data moved under it.
                    .watch("data/train")
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
