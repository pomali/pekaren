# Roadmap

Milestones, in the order the design forces them. Each one ends with the
store still readable by the previous build where the schema allows it.

## 1 — Interface and store (done)

The public API, the schema, submit and read paths, decisions written down.
Execution entry points exist and return `Error::NotImplemented`.

## 2 — Execution

- Claim: pick a runnable job whose declaration fits the free pool, write a
  lease with a token and an expiry, all in one `BEGIN IMMEDIATE`.
- Supervise: spawn the child, record PID + process start time, renew the
  lease, enforce the wall-clock cap.
- Commit: write `done`/`failed` only `WHERE lease_token` is still ours, then
  move the scratch directory into place.
- `Queue::wait`, `Queue::cancel`, `Worker::run_*`, and the grace window
  turning an early death into `Error::EarlyFailure`.

## 3 — Handoff

- Barrier settlement, both failure policies.
- The notification claim: finishing workers race, one wins, an unfulfilled
  claim is picked up by the next process to touch the store.
- Spawning the stored wake command, `ByWarmth` included.

## 4 — Reaping and stuck jobs

- `Queue::reap`: reclaim rotted leases, kill orphans by PID + start time,
  finish abandoned handoffs.
- Non-idempotent jobs fail rather than restart.

## 5 — Profiling

- The sampler thread, the `samples` table, per-attempt rollups.
- Stall detection feeding the stuck-job path.
- "estimated 20 min, took 90 min, averaged 1.2 of 8 cores" in `pec status`.

## 6 — Surfaces

- `pec submit / status / wait / logs / reap` (the CLI open question).
- The agent-facing skill, once the Rust API has settled.

## Parked

- Starvation: a waiting job reserves capacity past some threshold. Needs the
  threshold, and needs scheduling to exist first.
- Multi-node: WAL does not work over network filesystems, so this means a
  tiny optional server, not a shared mount.
- Cache-warm detection: today `ByWarmth` is a wall-clock deadline.
