# Roadmap

Milestones, in the order the design forces them. Each one ends with the
store still readable by the previous build where the schema allows it.

## 1 — Interface and store (done)

The public API, the schema, submit and read paths, decisions written down.

## 2 — Execution (done)

Claim under one `BEGIN IMMEDIATE` against a free pool counted from live
leases; check the job's inputs before running anything; spawn the child
with `PEKAREN_JOB`, `PEKAREN_SCRATCH` and its GPUs; renew the lease and
sample the child while it runs; kill it at its cap; commit only while the
lease still holds our token, and publish the scratch directory after. Plus
retries for idempotent jobs, barrier settlement under both policies,
`Queue::wait`, `Queue::cancel`, `Queue::reap`, the grace window, and
`pec work` / `wait` / `reap`.

Left over from it: cancelling a *running* job (today cancel only stops one
that has not started), and reclaiming a lease left by another host.

## 3 — Handoff

- Barrier settlement, both failure policies.
- The notification claim: finishing workers race, one wins, an unfulfilled
  claim is picked up by the next process to touch the store.
- Spawning the stored wake command, `ByWarmth` included.
- A `sessions` table and a `SessionStart` hook, so a wake can reach the
  session that submitted the work: see
  [`notes/waking-claude-code.md`](notes/waking-claude-code.md).

## 4 — Reaping and stuck jobs

- `Queue::reap`: reclaim rotted leases, kill orphans by PID + start time,
  finish abandoned handoffs.
- Non-idempotent jobs fail rather than restart.

## 5 — Profiling

- Samples are collected and rolled up already; what is missing is GPU
  utilisation and surfacing any of it.
- Stall detection feeding the stuck-job path.
- "estimated 20 min, took 90 min, averaged 1.2 of 8 cores" in `pec status`.

## 6 — Surfaces

- `pec submit / status / wait / logs / reap` (the CLI open question).
- The agent-facing skill, once the Rust API has settled.

## Parked

- Freezing the binary: hard-link or copy a task's executable into the store
  at submit time, so a queued job can run the code it was submitted with
  instead of failing when the binary is rebuilt. Opt-in, once failing
  proves too strict.

- Starvation: a waiting job reserves capacity past some threshold. Needs the
  threshold, and needs scheduling to exist first.
- Multi-node: WAL does not work over network filesystems, so this means a
  tiny optional server, not a shared mount.
- Cache-warm detection: today `ByWarmth` is a wall-clock deadline. The
  replacement is socket liveness plus elapsed time, not a cache query —
  there is no cache query.
