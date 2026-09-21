# Waking a Claude Code session when a job finishes

Research note, 2026-09-21. Pekáreň's handoff spawns a stored command when a
barrier settles; this is what that command can usefully *be* when the thing
to wake is a Claude Code session. Sources are linked at the bottom; versions
and flags move, so re-check before relying on a detail.

There are four mechanisms, and they differ in one thing that matters more
than the rest: **whether the original session is still running.**

## 1. Post to the session's inbox socket — the original session, still alive

Every Claude Code session with cross-session messaging binds an inbox
socket: a Unix domain socket on macOS and Linux, a named pipe on native
Windows. The docs name this explicitly as the path for "a script or hook to
post into a session". Two environment variables are exported to hooks and to
Bash commands run inside that session:

| Variable | What |
| --- | --- |
| `CLAUDE_CODE_MESSAGING_SOCKET` | the session's own socket path (also shown as `Peer address` in `/status`, prefixed `uds:`) |
| `CLAUDE_CODE_MESSAGING_TOKEN` | a per-session token, sent as `{"type":"auth","token":"…"}` on the first line |

The auth line is optional on macOS and Linux and required on native
Windows. The receiving Claude reads the message between tool calls during a
turn, and if the session is idle, Claude Code starts a new turn with it.

That is exactly the "resume the original session" row of the design doc's
wake-up table, and it is the cheapest one: no new process, no transcript
replay, and the session's context is already in memory.

Constraints worth designing around:

- **The session must still be running.** A socket for a session that exited
  is gone; the wake has to fall through to mechanism 3 or 4.
- **Same machine, same filesystem namespace.** A session inside a container
  and one on the host cannot see each other's sockets.
- **The token is a secret.** Pekáreň should record the *socket path* in the
  store and not the token: on macOS and Linux nothing else is needed, and a
  store file full of session tokens is a liability out of proportion to what
  it buys. Windows support for this path can wait.
- Delivery counts as a prompt for usage, like anything else the session
  reads.
- Open the connection only when the message is ready: Claude Code closes a
  connection that has not sent a complete line within 30 seconds.

## 2. An `asyncRewake` hook — the session waits for us

A command hook with `asyncRewake: true` runs in the background and wakes
Claude when it exits with code 2; its stderr (or stdout, if stderr is empty)
is shown to Claude as a system reminder.

```json
{ "type": "command", "command": "pec wait --barrier j42 --wake", "asyncRewake": true }
```

This inverts the direction: instead of pekáreň knowing how to reach the
session, the session blocks on pekáreň. It needs no socket path, no session
id and no secret, and it is the only mechanism here that works without
pekáreň storing anything about the caller. It also costs a process sitting
on a `pec wait` for as long as the job runs.

`Stop`, `PostToolUse`, `Notification` and the other hook events are
display-only and cannot wake a session, so `asyncRewake` is the whole of
this option.

## 3. `claude -p --resume <session-id>` — the session is gone, the context isn't

```bash
claude -p --resume "$SESSION_ID" "The sweep finished. $(pec status j42)"
```

Claude Code finds a session by id in any project on this machine (v2.1.223
and later; before that, only from the same directory). `--continue` takes
the most recent conversation instead. A session id reaches a hook in the
common JSON fields (`session_id`, alongside `transcript_path` and `cwd`), so
a `SessionStart` hook is the natural place for pekáreň to learn it.

This spawns a fresh process that replays the transcript. Whether the prompt
cache is still warm is not something the docs expose, and not something we
can query — see below.

## 4. `claude --bare -p` — a fresh evaluator, no original context at all

```bash
claude --bare -p "$(pec eval-prompt j42)" --allowedTools "Read,Bash(pec *)" \
  --permission-prompts none --output-format json
```

`--bare` skips hooks, plugins, MCP servers, CLAUDE.md and auto memory, which
is what you want for a small judging context that should cost the same on
every machine. `--permission-prompts none` is the documented way to say
nobody is there to approve anything. This is the design doc's "cache cold →
spawn a fresh evaluator with the stored eval prompt and the job outputs",
and it is the case pekáreň exists to make cheap.

Note that `--bare` does not read OAuth credentials, so it needs
`ANTHROPIC_API_KEY` in the environment.

## Also considered

- **Channels** (`claude --channels plugin:…`) push external events into a
  running session over an MCP server, and a webhook receiver is one of the
  documented shapes. It is the "supported" way to push CI-like events in,
  but it is a research preview, needs a plugin from an allowlist plus Bun,
  and only delivers while the session is open — mechanism 1 gets the same
  result from a shell script with no plugin.
- **`SendMessage` / `notify_when_idle`** is Claude-to-Claude only: a session
  subscribes to be told when another session goes idle. Not reachable from
  an external process, and it watches sessions, not jobs.
- **Scheduled tasks** poll on a timer. That is the thing pekáreň is supposed
  to remove.

## What this says about the design

**Cache-warm detection** is an open question in the design doc. Nothing here
exposes cache state, so it stays a heuristic — but a better one than a bare
deadline: *the socket still existing* means the session is alive, and the
time since it was last active bounds how cold its cache can be. Liveness is
cheap to check and is the thing the wake actually depends on, since a dead
session cannot be posted to at all.

So the wake ladder becomes, in order:

1. Socket exists and the session was active recently → post to the socket.
2. Socket gone, session id known → `claude -p --resume`.
3. Otherwise → `claude --bare -p` with the stored eval prompt.

Which maps onto `Wake::ByWarmth { warm, cold, warm_until }` with one change:
the warm branch should be conditional on liveness, not only on a deadline
written at submit time.

**How pekáreň learns the socket and session id.** A `SessionStart` hook,
shipped with the crate, writing `session_id`, `CLAUDE_CODE_MESSAGING_SOCKET`
and `cwd` into the store — one row per live session, refreshed on every
start and resume. That gives `Job::on_done` something to name, and gives
`pec` a way to list who can be woken. It needs a `sessions` table and a
schema bump; not built yet.

## Sources

- [Message your other Claude Code sessions](https://code.claude.com/docs/en/cross-session-messaging) — the inbox socket, `CLAUDE_CODE_MESSAGING_SOCKET`, `CLAUDE_CODE_MESSAGING_TOKEN`, own-child verification, delivery rules
- [Automate actions with hooks](https://code.claude.com/docs/en/hooks) — `asyncRewake`, the common JSON fields, which events are display-only
- [Run Claude Code programmatically](https://code.claude.com/docs/en/headless) — `-p`, `--resume`, `--continue`, `--bare`, `--permission-prompts none`, `--output-format`
- [Push events into a running session with channels](https://code.claude.com/docs/en/channels) — channels, and how they compare to the alternatives
