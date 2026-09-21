# ADR 0002 — the shape of the public interface

Status: accepted, 2026-09-21.

## Context

The caller is usually an LLM agent writing one `.rs` file it will never
open again, under a harness that charges it for every token of context it
re-reads. The API has to be writable in one pass, from memory, and it has to
fail loudly at submit time rather than silently an hour later.

## Decisions

### Handles by value, never futures

`submit` returns a `JobId` — `Copy`, printable as `j42`, parseable back.
Everything else takes `JobId`s. No lifetimes, no join handles, nothing that
has to be awaited or kept alive. The DAG is therefore ordinary data: collect
handles in a `Vec`, pass `&runs` to `after`.

`after` takes `I: IntoIterator, I::Item: Borrow<JobId>`, so `&runs`,
`[a, b]`, `&[a]` and `vec![a]` all work without the caller thinking about it.

### One builder, no required arguments but the command

`Job::cmd("...")` and `Job::barrier()` are complete on their own; everything
else is an optional chained call with a defensible default. The defaults
are the load-bearing part of the design:

| Default | Value | Why |
| --- | --- | --- |
| `policy` | `FailFast` | Wasted compute beats a wasted hour of waiting |
| `idempotent` | `false` | Re-running is the dangerous option; a job must say it is safe |
| `retries` | `0` | Only meaningful for an idempotent job |
| `cpus` | `1` | The honest floor |
| `kill_after` | `EstimateTimes(3)` | The design doc's runaway cap, inert when no estimate was given |
| `grace` | 5 s | Long enough to catch a bad path, short enough not to hold the agent |

### `Job::cmd` is a shell line; `Job::exec` is not

`Job::cmd("python train.py --lr 3e-4")` runs through `sh -c`, because that is
what the string in the design doc looks like and because redirection and
`&&` are how these commands get written. Quoting is the caller's, exactly as
in a terminal. `Job::exec("python", ["train.py", "--lr", "3e-4"])` execs
directly with no shell, for anything built from untrusted or awkward values.
Both produce the same `Command`; only the `shell` flag differs.

### The evaluation prompt is a field on the job, not a side table

`eval_prompt` sits on `Job` and on `JobStatus`, written at submit time and
read by whoever judges the output. It is the reason the crate exists, so it
is one call away, not behind a metadata map.

### Wake-up is data, not a callback

`Wake` is `Nothing | Always(Command) | ByWarmth { warm, cold, warm_until }`.
It stores commands; the library spawns them and does not interpret them.
Callbacks would have to outlive the submitting process, which is exactly
what the design says does not happen.

`ByWarmth` puts the cache-warm question in one place. Today the answer is a
wall-clock deadline written at submit time; when cache-warm detection is
solved (an open question in the design doc), the variant's meaning changes
and no caller has to move.

`on_done` takes `impl Into<Wake>`, so `.on_done("pec-evaluate ...")` works
and a full `Wake` also works.

### `FailurePolicy` variants are in the prelude

The design doc writes `.policy(FailAtEnd)`. `pekaren::prelude` re-exports
`FailurePolicy::*` so that line compiles as written, while the enum is still
namespaced for anyone who prefers `FailurePolicy::FailAtEnd`.

### There is one default store, and one definition of where it is

`Queue::open_default()` opens `$PEKAREN_STORE`, or `~/.pekaren` when that is
unset; `default_store_path()` answers the question without opening anything,
and `pec where` prints it. A submitting script, a worker, an evaluator
spawned an hour later and the CLI all land in the same place without having
agreed on a path first — which matters because the processes that share a
store here never talk to each other.

Resolution happens per call rather than once at startup, so a script that
sets the variable before opening gets what it set. An explicit path always
wins: `Queue::open(p)` never consults the environment.

### The worker is a loop, not a service

`Worker::new(&queue).run_until_idle()` is something any process enters,
including the one that just submitted. There is no `start()`/`stop()`, no
background thread the caller has to own, and no daemon to install.
`QueueOptions::work_on_submit` is how the submitting process volunteers.

### Nothing blocks except `wait`

`Queue::wait(&ids, timeout)` is the only call that waits on other people's
work, and it says so in its name. Everything else — `status`, `list`,
`runnable`, `logs` — is a read that returns now.

### Unfinished paths return `Error::NotImplemented`

`wait`, `cancel`, `reap` and the `Worker` loop exist in the signature and
return `Error::NotImplemented("Queue::wait")`. A caller can write the whole
script today and find out precisely what is missing, instead of getting a
plausible wrong answer. A test asserts this stays true.

## Consequences

- `Queue` is `Send` but not `Sync` (it holds one `rusqlite::Connection`).
  Per-process, per-thread queues are the intended use; sharing one across
  threads would need a mutex we do not want to pay for yet.
- `JobStatus` is `#[non_exhaustive]`: profiling and scheduling will add
  fields, and adding one should not break a caller.
- Adding a resource kind means touching `Resources`, the schema, and the
  admission query. That is deliberate: resources are declarations the
  scheduler must understand, not free-form tags.
