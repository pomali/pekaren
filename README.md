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

## Status — milestone 1

This repository currently holds the **interface and the store**, which is
what the design needed to settle first:

- the public API (`Queue`, `Job`, `JobId`, `Wake`, `Worker`), built by value
- the SQLite schema, in [`src/store/schema.sql`](src/store/schema.sql)
- submit and read paths, working against a real store
- decisions written down in [`docs/adr/`](docs/adr/)

Execution is next and is not pretended: `Queue::wait`, `Queue::reap`,
`Queue::cancel` and the `Worker` loop return `Error::NotImplemented` rather
than a plausible-looking wrong answer. See [`docs/roadmap.md`](docs/roadmap.md).

## Shape of it

```rust
use pekaren::prelude::*;

let q = Queue::open("~/.pekaren")?;

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

Rust is the scripting surface: an agent writes one `.rs` file with an inline
dependency manifest (cargo's single-file script support), so there is no
project scaffolding. Loops and fan-outs are ordinary code — collect the
handles into a `Vec`, then submit one barrier that depends on all of them.
See [`examples/sweep.rs`](examples/sweep.rs).

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
| `src/queue.rs` | `Queue`, `JobStatus`, `State`, submit and read paths |
| `src/store/` | SQLite: schema, migrations, conditional writes |
| `src/worker.rs` | claim / supervise / commit loop (milestone 2) |
| `src/bin/pec.rs` | the CLI, deliberately thin for now |
| `docs/adr/` | decisions and the alternatives they beat |

## License

MIT.
