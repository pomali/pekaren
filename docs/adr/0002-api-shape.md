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

### A job that is a function: tasks, and why not a closure

`Job::task(handle)` runs a function registered in the submitting binary.
The obvious API — hand `submit` a closure — cannot exist: a job outlives
the process that submitted it, so the store can only hold a pointer to
code on disk. The choice is which pointer.

| Pointer | What it costs |
| --- | --- |
| Inline source (`Job::rust`) | No type checking, no tooling, compiled at claim time |
| A `.rs` path (`Job::rust_file`) | Real file, still compiled at claim time, still outside the program that submitted it |
| **This binary + a name** (`Job::task`) | Ordinary compiled Rust, checked when you queue the job; needs the binary to still be there when it runs |

The third is the default answer, and the first two remain for code with no
binary to live in — a script an agent wrote on the spot.

Registration returns a `Task` handle rather than taking a bare string at
the submit site:

```rust
let train = tasks.add("train", train);   // handle
q.submit(Job::task(train).arg("3e-4"))?; // not Job::task("trian")
```

so an unregistered task cannot be submitted and a rename is a compile
error. The name still exists — the store needs one — but it is written
once, next to the function it names.

Dispatch goes through the environment (`PEKAREN_TASK`, `PEKAREN_JOB`), not
argv, and the task's own arguments come from the store through `JobCtx`.
A binary with its own CLI is therefore never handed flags it did not
expect, which it would reject.

`bootstrap()` exits the process after running a task. The alternative,
returning and letting `main` continue, would re-run the submitting code
inside every job.

### What a job assumed: hashes, not hope

Every path a job depends on is hashed at submit time and re-hashed before
it runs: the binary behind a task, the script behind a Rust job, and
anything the submitter declares with `watch`. They are one table and one
check, differing only in policy — code defaults to `OnChange::Fail`,
declared inputs to `OnChange::Warn`.

Failing on a changed binary is the interesting default. The cheap
alternative, running whatever is at the path now, silently judges new code
against an evaluation prompt written for the old code. The other
alternative, freezing a copy of the binary into the store at submit time,
is reproducible but hides the same problem behind a stale snapshot; it is
worth adding later as an opt-in, not as the default.

Directories are hashed by the shape of the tree — each entry's relative
path, length and mtime — rather than by content, so declaring a dataset
costs one `stat` per file instead of a full read. The tradeoff is that
`touch` alone looks like a change, which is why a directory warns rather
than fails.

Hashes are non-cryptographic. They answer "is this the same thing I
submitted?" for a store only its owner writes; they are not a defence
against someone who can rewrite the store, and the code says so.

### Rust source is a job kind, not a command the caller assembles

`Job::rust("fn main() { .. }")` takes source; `Job::rust_file(p)` takes a
path; `rust_manifest` supplies the inline dependency manifest. The store
writes inline source under `<store>/scripts`, content-addressed, and records
the path in `JobStatus::script`.

The alternative was leaving it to the caller: write a file, then
`Job::cmd("cargo -Zscript ./thing.rs")`. That makes every caller invent a
place to put the file, and it puts the code out of the store's reach — an
evaluator waking an hour later gets the output but not the thing that
produced it.

Content-addressing rather than naming by job id means submitting the same
source twice is one file, and a rolled-back transaction leaves a reusable
script rather than litter.

The runner is resolved at submit time and stored as an ordinary argv, so the
job runs the same way later whatever the worker's environment looks like.
`cargo -Zscript` is the default and needs nightly; `PEKAREN_RUST_RUNNER`
overrides it with a command line the script path is appended to. Nothing
about this is special-cased downstream: by the time a worker sees it, a Rust
job is a command job.

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

### The grace window is a probe, not a full stop

The design asks the submitting script to stay attached for 5–10 seconds so
a job that dies on a bad path comes back as an error from `submit`. Taken
literally, a loop that submits ten jobs pays the window ten times over
while nothing is wrong.

So `submit` waits until the job settles, or until it has been *running* for
a second, whichever comes first, and gives up at the grace deadline.
Failures of the kind the window exists for — a missing binary, an
unreadable path — happen in milliseconds, so a second of life is enough
evidence, and a sweep submits at the speed of the store.

Work happens in a background thread of the submitting process, started by
the first submit. That keeps a script that submits and then keeps working a
complete system on its own. It also means a script that submits and exits
leaves its jobs for the next worker — `pec work`, or the next script — which
is what daemon-free costs.

## Consequences

- `Queue` is `Send` but not `Sync` (it holds one `rusqlite::Connection`).
  Per-process, per-thread queues are the intended use; sharing one across
  threads would need a mutex we do not want to pay for yet.
- `JobStatus` is `#[non_exhaustive]`: profiling and scheduling will add
  fields, and adding one should not break a caller.
- Adding a resource kind means touching `Resources`, the schema, and the
  admission query. That is deliberate: resources are declarations the
  scheduler must understand, not free-form tags.
