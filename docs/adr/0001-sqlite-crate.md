# ADR 0001 — rusqlite with bundled SQLite

Status: accepted, 2026-09-21.

## Context

The store is the only shared state in the system. It is opened by every
client and every worker, on one machine, with no daemon in the middle. That
gives us an unusual shape for a "database" choice:

- **Multi-process, not multi-connection.** Concurrency is between separate
  OS processes that may die at any moment, not between tasks in one runtime.
- **Synchronous and short.** Every query is a few rows out of a local file.
  Nothing here is worth an async runtime.
- **Correctness lives in conditional writes.** Leases, commits and
  notification claims are all `UPDATE ... WHERE the row still looks the way I
  read it`. We need direct control over transaction behaviour
  (`BEGIN IMMEDIATE`), `busy_timeout`, and `changes()`.
- **It must be there.** An agent runs `cargo -Zscript sweep.rs` on whatever
  box it landed on. "Install libsqlite3-dev first" is a failed job.

## Decision

**`rusqlite`, with the `bundled` feature.**

```toml
rusqlite = { version = "0.40", features = ["bundled"] }
```

- Synchronous, thin, and a direct mapping onto the C API, so the pragmas and
  the conditional writes read like what they compile to.
- `bundled` vendors the SQLite amalgamation and compiles it with the crate:
  one known version everywhere, no system package, no version skew between
  the machine that created a store and the machine that reads it. Cost is a
  C compiler and a few seconds of first build.
- No connection pool: each process opens one connection, which is exactly
  the concurrency model we have. `Connection`'s methods take `&self`, so
  `Queue` stays shareable within a process without a mutex.
- `Transaction::new_unchecked(&conn, TransactionBehavior::Immediate)` gives
  us the write lock up front from a `&self` method.

Settings, applied on every open (`src/store/mod.rs`):

| Pragma | Value | Why |
| --- | --- | --- |
| `journal_mode` | `WAL` | Readers never block the writer; `pec status` during a run is free |
| `synchronous` | `NORMAL` | WAL already survives a process crash, which is the failure we actually have; an OS crash may lose the last commits, and a re-run is cheaper than an fsync per job event |
| `foreign_keys` | `ON` | Deps, args and samples are cascade-deleted with their job |
| `busy_timeout` | 10 s | Contention between processes is the expected state here, not an error |

## Alternatives considered

| Option | Why not |
| --- | --- |
| `sqlx` | Async-first, built around pools and a server. We would add a runtime to await a local file read. Compile-time checked queries need a live database at build time, which fights `cargo -Zscript` usage. |
| `turso` / `libsql` | Aimed at remote replicas and embedded-with-sync. That is a real answer to the multi-node open question later, but today it buys nothing and adds a fork of SQLite to the trust surface. Revisit if multi-node happens. |
| `redb` / `sled` (pure-Rust KV) | No SQL, so the DAG queries, the runnable view and the resource ledger all become hand-rolled index maintenance — the exact code SQLite has already debugged. No `pec`-free inspection either: `sqlite3 store.db` is a real debugging tool. |
| `rusqlite` without `bundled` | Version skew across machines, and a missing system library turns into a failed job on a box the agent cannot fix. |
| Postgres | A daemon. The whole point is not having one. |

## Consequences

- SQLite's one-writer-at-a-time rule is our concurrency limit. Writes are
  small and short (claim, renew, commit), so this is fine at single-machine
  scale; if it ever is not, the fix is fewer writes (batch renewals), not a
  different database.
- WAL does not work on network filesystems. That rules out the
  "shared NFS mount" version of multi-node and confirms the design doc's
  note that multi-node likely needs a tiny optional server.
- A C compiler is required to build. Accepted.
- Schema changes go through `PRAGMA user_version` (`SCHEMA_VERSION` in
  `src/lib.rs`). Forward migrations are allowed; opening a store written by
  a newer build is refused rather than guessed at.
