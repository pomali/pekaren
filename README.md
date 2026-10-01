# Pekáreň

A daemon-free Rust job queue that lets an LLM agent submit long-running work,
walk away, and have the results evaluated without waking its expensive
context. Single machine first, with a path to several nodes later.

The crate is `pekaren` (Slovak for "bakery") and the binary is `pec` (Slovak
for "oven"): the bakery is the system, the oven is where jobs go.

The problem is context economics. An agent may hold 500k tokens of context,
but judging whether a training run succeeded rarely needs all of it, and
re-waking that context an hour later means paying for it again once the
prompt cache has gone cold. So each job carries an evaluation prompt written
at submit time, while the agent still knows what success looks like. When the
work finishes, a fresh small context can judge the output; the original agent
comes back only when it is cheap or when the user asks.

Full design: [`docs/design.md`](docs/design.md).

## Status — jobs run

Submitted work actually executes: a worker claims a job with a lease, runs
it, samples it, and commits the result only while the lease still holds its
token. Barriers settle, a job whose dependency failed is cancelled rather
than left pending, failed leases are reclaimed, `wait` waits.

- the public API (`Queue`, `Job`, `JobId`, `Task`, `Wake`, `Worker`)
- the SQLite schema, in [`src/store/schema.sql`](src/store/schema.sql)
- execution: claim, supervise, kill at the cap, conditional commit, retries
- change detection on everything a job depends on
- decisions written down in [`docs/adr/`](docs/adr/)

When a node with an `on_done` command settles, the worker that noticed
spawns it — exactly once, however many workers share the store — with
`PEKAREN_JOB`, `PEKAREN_STATE` and `PEKAREN_EVAL_PROMPT` set, and its
output in `logs/<n>/wake.{out,err}`.

Not built yet: GPU utilisation in the profile `pec status` shows, and a
wake that knows whether its session is still alive. See
[`docs/roadmap.md`](docs/roadmap.md).

With no daemon, someone has to be the worker. Either the submitting process
stays alive (it runs one in a background thread by default), or you start
one:

```
pec work          # run everything claimable, then stop
pec wait j3       # block until these jobs settle
pec reap          # reclaim rotted leases, kill orphans
pec status        # one line per job; `pec status j3` for one in full
```

`pec wait` answers in its exit code: 0 when every job is done, 1 when any
failed or was cancelled, 124 when `--timeout` came first. Exit 2 is `pec`
itself failing — a bad argument, a store it cannot open — so a script can
tell "the job failed" from "the question was wrong".

`pec status --json` prints one JSON object per job instead — state, exit
code, failure, dependencies, declared resources, timing, the command, log
paths, the evaluation prompt, the profile and the events — for a script or
an evaluator that should not parse the table.

## From a shell

Everything a submitting script can say, `pec submit` takes as flags, and it
prints the new id:

```
T=$(pec submit --name train --gpus 1 --cpus 24 --est 40m --cap 90m \
      --env LR=3e-4 --watch data/train --eval "Loss should drop below 0.3" \
      -- python train.py --lr '$LR')
V=$(pec submit --name eval --after $T --cpus 4 -- python eval.py)
W=$(pec barrier --after $T,$V --on-done 'pec status --json $PEKAREN_JOB > sweep.done')
pec wait --timeout 1h $W; echo $?     # 0, 1 or 124
pec logs $T                           # its stdout; --err for stderr
```

The words after `--` are one shell line, run through `sh -c` in the
directory `pec` was called from (`--cwd` to change it; `--exec` to run them
as a program and arguments with no shell). `--env-pass NAME` copies a
variable from the submitting shell, since the worker that runs the job has
an environment of its own; `--watch` paths are made absolute for the same
reason. `--retries N` implies the job is safe to run twice. `pec cancel`
stops jobs that have not started, and everything behind them. `pec help`
lists every flag.

## Shape of it

```rust
use pekaren::prelude::*;

let q = Queue::open_default()?;   // $PEKAREN_STORE, else ~/.pekaren

let runs: Vec<JobId> = lrs.iter().map(|lr| {
    q.submit(Job::cmd(format!("python train.py --lr {lr}"))
        .gpus(1)
        .est_minutes(40)
        .eval_prompt("Loss should drop below 0.3"))
}).collect::<Result<_>>()?;

q.submit(Job::barrier()
    .after(&runs)
    .policy(FailAtEnd)
    .on_done("pec-evaluate --sweep lr"))?;
```

A job can also *be* a function in your own program:

```rust
fn train(ctx: &JobCtx) -> TaskResult {
    println!("training with lr={}", ctx.arg(0).ok_or("no lr")?);
    Ok(())
}

fn main() -> Result<()> {
    let mut tasks = Tasks::new();
    let train = tasks.add("train", train);
    tasks.bootstrap()?;              // a worker running this binary stops here

    let q = Queue::open_default()?;
    q.submit(Job::task(train).arg("3e-4").gpus(1).watch("data/train"))?;
    Ok(())
}
```

A job has to outlive the process that submitted it, so the store can only
hold a *pointer* to code on disk, never a closure. A task is the narrowest
such pointer — this binary, this registered name — and the body stays
ordinary compiled Rust. `tasks.add` hands back a handle, so `Job::task`
cannot name a task nothing registers, and a rename is a compile error rather
than a failure an hour later. A worker runs the job by re-executing the
binary with `PEKAREN_TASK` set; `bootstrap` dispatches and exits before the
submitting code runs again.

For code that has no binary to live in — something an agent generated on the
spot — `Job::rust_file("analyze.rs")` runs a `.rs` file as a single-file
script, and `Job::rust("fn main() { … }")` takes the source directly and
writes it into the store. Both use `cargo -Zscript` by default, which needs
a nightly toolchain, or whatever `PEKAREN_RUST_RUNNER` names.

Rust is the scripting surface at both levels: an agent writes one `.rs` file
with an inline dependency manifest (cargo's single-file script support), so
there is no project scaffolding. Loops and fan-outs are ordinary code — collect the
handles into a `Vec`, then submit one barrier that depends on all of them.
See [`examples/sweep.rs`](examples/sweep.rs).

## What a job assumed

A job submitted now may run in an hour, by which time the code and the data
it named can have moved. Every path a job depends on is hashed at submit
time — the binary behind a task, the `.rs` behind a script, and anything
declared with `watch` — and re-hashed before it runs:

| What moved | Default | Why |
| --- | --- | --- |
| The binary or script the job runs | **Fail** | A rebuilt binary is different code; running it under an hour-old evaluation prompt is worse than not running it |
| A declared input (`watch`) | **Warn** | A dataset that grew is usually fine, and always worth knowing about |

Override either with `.on_code_change(OnChange::Warn)` or
`.watch_as(path, OnChange::Fail)`. Files are hashed by content, directories
by the shape of the tree — each entry's relative path, length and mtime — so
a dataset is not read end to end. Findings land in the job's events and
travel with the result; `pec check` runs the same check by hand.

## Build

```
cargo test        # unit + store tests, against real SQLite files
cargo clippy --all-targets
```

SQLite is vendored (`rusqlite/bundled`), so there is no system dependency
beyond a C compiler.

## Layout

| Path | What |
| --- | --- |
| `src/job.rs` | `Job` builder, `Command`, `Resources`, `Wake`, policies |
| `src/task.rs` | tasks: jobs that are functions in the submitting binary |
| `src/hash.rs` | change detection for the paths a job depends on |
| `src/queue.rs` | `Queue`, `JobStatus`, `State`, submit and read paths |
| `src/store/` | SQLite: schema, migrations, conditional writes |
| `src/worker.rs` | claim / supervise / commit loop |
| `src/bin/pec.rs` | the CLI, deliberately thin for now |
| `docs/adr/` | decisions and the alternatives they beat |

## License

MIT.
