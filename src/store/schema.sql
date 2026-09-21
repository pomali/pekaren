-- pekaren store, schema v3.
--
-- Every process that opens this file is a client and potentially a worker;
-- there is no daemon and no single writer. Three rules follow from that and
-- shape everything below:
--
--   1. Claims are leases with a token, never a boolean flag. A flag left by a
--      dead process is indistinguishable from work in progress; a lease rots
--      on its own.
--   2. Every state transition is a conditional write (UPDATE ... WHERE the
--      row still looks the way I read it). Losing the race is normal and
--      always safe.
--   3. Nothing that can be derived is stored. The free resource pool is a
--      query over live leases, so it cannot drift from reality.

CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE jobs (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  name           TEXT,
  kind           TEXT NOT NULL CHECK (kind IN ('command', 'barrier')),

  -- Command. NULL for a barrier.
  shell          INTEGER NOT NULL DEFAULT 0,  -- run through sh -c
  program        TEXT,
  cwd            TEXT,
  -- The .rs file behind a Rust job: content-addressed under <store>/scripts
  -- for inline source, or the caller's own path. NULL for everything else.
  script_path    TEXT,
  -- The registered function a task job runs, inside the binary named by
  -- `program`. NULL for everything else.
  task_name      TEXT,

  -- Declaration: admission control, and the baseline for the actual run.
  cpus           INTEGER NOT NULL DEFAULT 1,
  gpus           INTEGER NOT NULL DEFAULT 0,
  mem_mb         INTEGER NOT NULL DEFAULT 0,   -- 0 = no claim made
  est_secs       INTEGER,                      -- NULL = no estimate
  kill_after_secs INTEGER,                     -- resolved cap; NULL = uncapped

  -- Policy, all written at submit time.
  eval_prompt    TEXT,
  retries        INTEGER NOT NULL DEFAULT 0,
  idempotent     INTEGER NOT NULL DEFAULT 0,
  failure_policy TEXT NOT NULL DEFAULT 'fail_fast'
                 CHECK (failure_policy IN ('fail_fast', 'fail_at_end')),

  -- State.
  state          TEXT NOT NULL CHECK (state IN
                   ('pending', 'ready', 'running', 'done', 'failed', 'cancelled')),
  stalled        INTEGER NOT NULL DEFAULT 0,  -- flag, not a state: ~0 CPU on a
                                              -- job declared compute-bound
  attempt        INTEGER NOT NULL DEFAULT 0,
  exit_code      INTEGER,
  failure        TEXT,                        -- why, in one line, for humans
  submitted_at   INTEGER NOT NULL,            -- unix millis, here and below
  started_at     INTEGER,
  finished_at    INTEGER,

  -- Lease. The CAS target: a worker writes `done` only WHERE lease_token is
  -- still its own. lease_pid + lease_pid_start identify the child precisely
  -- enough to kill it, since PIDs get recycled and killing a stranger is
  -- worse than leaving an orphan.
  lease_token      TEXT,
  lease_host       TEXT,
  lease_expires_at INTEGER,
  lease_pid        INTEGER,
  lease_pid_start  INTEGER,
  scratch_dir      TEXT   -- keyed by lease token; moved into place on commit
);

CREATE INDEX jobs_by_state ON jobs (state);
-- Reclaiming a rotted lease, and the runnable set, are the two hot reads.
CREATE INDEX jobs_by_lease ON jobs (state, lease_expires_at);

-- argv and env as rows rather than an encoded blob: no serialization format
-- to keep compatible, and `pec status` can read them with plain SQL.
CREATE TABLE job_args (
  job_id INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  pos    INTEGER NOT NULL,
  arg    TEXT NOT NULL,
  PRIMARY KEY (job_id, pos)
) WITHOUT ROWID;

CREATE TABLE job_env (
  job_id INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  key    TEXT NOT NULL,
  val    TEXT NOT NULL,
  PRIMARY KEY (job_id, key)
) WITHOUT ROWID;

-- The DAG. A job may only depend on ids that already exist, so a cycle is
-- unreachable by construction.
CREATE TABLE deps (
  child_id  INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  parent_id INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  PRIMARY KEY (child_id, parent_id)
) WITHOUT ROWID;

CREATE INDEX deps_by_parent ON deps (parent_id);

-- What to spawn when a node settles. claimed_by is the notification claim:
-- finishing workers race for it, exactly one wins, and a claim without a
-- fulfilled_at is picked up by the next process to touch the store.
CREATE TABLE wakes (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id       INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  trigger      TEXT NOT NULL CHECK (trigger IN ('always', 'warm', 'cold')),
  warm_until   INTEGER,   -- only for 'warm' / 'cold'
  shell        INTEGER NOT NULL DEFAULT 1,
  program      TEXT NOT NULL,
  cwd          TEXT,
  claimed_by   TEXT,
  claimed_at   INTEGER,
  fulfilled_at INTEGER,
  spawn_pid    INTEGER
);

CREATE INDEX wakes_by_job ON wakes (job_id);

CREATE TABLE wake_args (
  wake_id INTEGER NOT NULL REFERENCES wakes (id) ON DELETE CASCADE,
  pos     INTEGER NOT NULL,
  arg     TEXT NOT NULL,
  PRIMARY KEY (wake_id, pos)
) WITHOUT ROWID;

-- One row per run of a job. Retries and reclaims append; nothing is
-- overwritten, so "declared 20 min, took 90 min" survives a restart.
CREATE TABLE attempts (
  job_id        INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  n             INTEGER NOT NULL,
  host          TEXT NOT NULL,
  lease_token   TEXT NOT NULL,
  pid           INTEGER,
  pid_start     INTEGER,
  started_at    INTEGER NOT NULL,
  finished_at   INTEGER,
  exit_code     INTEGER,
  outcome       TEXT CHECK (outcome IN
                  ('done', 'failed', 'killed', 'reclaimed', 'cancelled')),
  scratch_dir   TEXT,
  -- Profile summary, rolled up from samples when the attempt ends.
  wall_secs     REAL,
  peak_rss_mb   INTEGER,
  avg_cores     REAL,
  gpu_util_avg  REAL,
  gpu_util_peak REAL,
  idle_fraction REAL,
  PRIMARY KEY (job_id, n)
) WITHOUT ROWID;

-- Raw samples, every few seconds while a job runs. Cheap to write, and the
-- stall detector reads the tail of them.
CREATE TABLE samples (
  job_id   INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  attempt  INTEGER NOT NULL,
  at       INTEGER NOT NULL,
  rss_mb   INTEGER,
  cpu_cores REAL,
  gpu_util REAL,
  PRIMARY KEY (job_id, attempt, at)
) WITHOUT ROWID;

-- GPUs are named, not counted: device 0 and device 1 are not interchangeable
-- once something is resident. One row per device currently held.
CREATE TABLE gpu_claims (
  host        TEXT NOT NULL,
  device      INTEGER NOT NULL,
  job_id      INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  lease_token TEXT NOT NULL,
  PRIMARY KEY (host, device)
) WITHOUT ROWID;

-- What each host has. Written by a worker as it starts, so a single-machine
-- store has exactly one row and a later multi-node store still reads right.
CREATE TABLE hosts (
  host        TEXT PRIMARY KEY,
  cpus        INTEGER NOT NULL,
  mem_mb      INTEGER NOT NULL,
  gpu_devices TEXT NOT NULL DEFAULT '',  -- comma-separated device indices
  seen_at     INTEGER NOT NULL
) WITHOUT ROWID;

-- Arguments for a task job. Separate from job_args, which is argv: a task
-- takes these through its JobCtx, and the binary's own argv stays empty so
-- a program with its own CLI is never handed flags it did not expect.
CREATE TABLE task_args (
  job_id INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  pos    INTEGER NOT NULL,
  arg    TEXT NOT NULL,
  PRIMARY KEY (job_id, pos)
) WITHOUT ROWID;

-- Paths whose content the job assumed when it was submitted: the binary a
-- task re-runs, the .rs file a Rust job compiles, and whatever the
-- submitter declared with `watch`. Hashed at submit, re-hashed before the
-- job runs. `hash` is NULL when the path was absent at submit time, which
-- is itself a state worth comparing against.
CREATE TABLE job_inputs (
  job_id      INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  pos         INTEGER NOT NULL,
  kind        TEXT NOT NULL CHECK (kind IN ('binary', 'script', 'param')),
  path        TEXT NOT NULL,
  is_dir      INTEGER NOT NULL DEFAULT 0,
  hash        TEXT,
  recorded_at INTEGER NOT NULL,
  on_change   TEXT NOT NULL CHECK (on_change IN ('fail', 'warn', 'ignore')),
  PRIMARY KEY (job_id, pos)
) WITHOUT ROWID;

-- Anything worth telling whoever reads the job later: an input that moved,
-- a stall, a reclaim. The wake command carries these along with the result,
-- which is the whole point of noticing.
CREATE TABLE job_events (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id  INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
  at      INTEGER NOT NULL,
  level   TEXT NOT NULL CHECK (level IN ('info', 'warn', 'error')),
  message TEXT NOT NULL
);

CREATE INDEX job_events_by_job ON job_events (job_id, at);

-- A job is runnable when every parent has settled successfully. Kept as a
-- view so the ready set has exactly one definition.
CREATE VIEW runnable AS
SELECT j.*
FROM jobs j
WHERE j.state IN ('pending', 'ready')
  AND NOT EXISTS (
    SELECT 1 FROM deps d
    JOIN jobs p ON p.id = d.parent_id
    WHERE d.child_id = j.id AND p.state <> 'done'
  );
