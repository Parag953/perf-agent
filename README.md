# perf-agent — Agent Orchestrator

Performance-triage orchestrator, in Go. A trigger arrives, a cheap model
fingerprints the issue and checks **memory**; on a miss the orchestrator
**leases a VM** from the pool manager (Poold), **SSHes a `claude -p`** investigation
onto it, distills the result back into memory, and returns the VM. The agent opens
a **draft PR** when it lands a fix. A dashboard watches the whole thing over a
small read-only JSON contract.

> This replaces the previous single-box Python service (`app.py`). Design doc:
> the orchestrator artifact (VM leasing, memory, dashboard contract).

## Flow

```
trigger ─▶ [haiku] signature ─▶ memory lookup ─┬─ hit  ─▶ return stored analysis + PR   (no VM)
                                               └─ miss ─▶ QUEUE ─▶ scheduler leases ─▶ ssh claude -p ─▶
                                                          collect ─▶ PR check ─▶ [haiku] distill ─▶
                                                          memory.insert ─▶ poold.release()
```

One task per VM, no retry. A VM is `ready`/`free` → `allocated` (on lease) →
`degraded` (after its one task) → Poold recycles it → `ready`.

### The queue

Fingerprinting and the memory lookup run **off the queue**, so a known issue never
waits for a VM. On a miss the task joins a FIFO queue ordered by submission, and a
**single scheduler goroutine** — the only thing that calls `Lease` — serves the head.
When the pool is exhausted Poold answers `409 no_capacity`; the task simply stays
queued (with a visible `queue_pos`) and the scheduler retries every `QUEUE_POLL_SECS`
and immediately whenever a VM is released. An exhausted pool is never an error.

Because Poold resets a box on release, freeing a VM takes minutes. `/api/state`
surfaces that as `vms[].prepare` (`step`, `elapsed_s`) plus the gate sample, read
from Poold's `GET /boxes`, so a queue wait is explained rather than silent.

### Live agent transcripts

The dispatcher runs `claude -p --output-format stream-json --verbose` and parses the
NDJSON as it arrives, turning it into typed events — `thinking`, `text`, `tool`,
`tool_result`, `bootstrap`, `system`, `result`, `failure`. Each is appended to
SQLite and fanned out to subscribers, so the dashboard can watch a run in flight and
replay it in full afterwards. Bootstrap emits its own progress, so VM provisioning
is not dead air either.

## Layout

| Path | What |
|---|---|
| `cmd/orchestrator` | the service — webhook intake, scheduler, dashboard API |
| `cmd/mock-poold` | stand-in pool manager (real Poold API is mocked for now) |
| `internal/orchestrator` | core loop, task registry, phase transitions, snapshot |
| `internal/store` | SQLite: memory rows, tasks, and agent transcripts (pure-Go driver, no cgo) |
| `internal/llm` | Haiku signature / match / distill — `api`, `cli`, `stub` backends |
| `internal/poold` | pool-manager client (`Lease`/`Release`/`Status`) |
| `internal/dispatch` | SSH the `claude -p` job to a VM; the investigation prompt; stream-json parsing |
| `internal/api` | `/webhook`, `/api/state`, `/api/stream`, `/api/task/:id`, `/api/task/:id/stream`, `/api/memory/:id`, embedded dashboard |

## Run it locally (no external dependencies)

```bash
make demo        # mock-poold + orchestrator with mock dispatch + stub LLM
```

Then open <http://localhost:8080> and fire an alert:

```bash
curl -XPOST localhost:8080/webhook \
  -d '{"title":"memory > 40%","tags":["kube_deployment:tachyon"]}'
```

Fire the same alert twice: the first opens a (mock) PR and writes a memory row;
the second is served from memory with **no VM spent**. Watch it live on the dashboard.

`make test` runs an end-to-end test (miss → PR → memory, then hit) with fakes.

## Dashboard contract (build against this)

Read-only. The dashboard needs nothing but these.

| Endpoint | Returns |
|---|---|
| `GET /api/state` | one snapshot: `stats`, `vms`, `tasks`, `memory` (see below) |
| `GET /api/stream` | SSE — the same `state` JSON on every change; falls back to polling `/api/state` |
| `GET /api/memory/:id` | one full memory row incl. `full_analysis` (drill-down) |
| `GET /api/task/:id` | one task incl. its trigger `payload`, full `analysis`, `summary` and the whole `events` transcript |
| `GET /api/task/:id/stream` | SSE — one `AgentEvent` per frame, live, for a task in flight |

`/api/state` shape:

```jsonc
{
  "stats":  { "vms_total":8, "vms_ready":3, "vms_allocated":4, "vms_degraded":1,
              "queue_depth":2, "running":4, "completed":143, "tasks_total":147,
              "cache_hits":58, "hit_rate":0.39, "avg_task_secs":472 },
  "vms":    [ { "vm_id":"VM101", "name":"andro-a", "state":"allocated", "host":"…",
               "task_id":"t-9a1", "service":"tachyon", "alert":"memory > 40%", "since":"…" },
              { "vm_id":"VM102", "name":"andro-b", "state":"degraded", "degraded_reason":"preparing",
               "prepare":{ "step":"gate", "attempt":1, "elapsed_s":118.4,
                           "gate_sample":"deploys=13 pods_not_ready=0 restarts=0" } } ],
  "tasks":  [ { "task_id":"t-9a1", "service":"tachyon", "alert":"memory > 40%",
               "phase":"running", "vm_id":"VM101", "outcome":"", "pr_url":"",
               "queue_pos":0, "enqueued_at":"…", "started_at":"…" } ],
  "memory": [ { "id":12, "signature":"tachyon/scope-map-churn", "service":"tachyon",
               "root_cause":"…", "pr_url":"…", "hit_count":6, "last_seen":"…" } ]
}
```

**Task `phase`**: `matching →` (`done`/memory hit) or
`queued → dispatched → running → collecting → pr_check → distilling → done`.
A task in `queued` carries `queue_pos` (1 = next to be served).
A finished task also carries an **`outcome`**: `pr_opened` (with `pr_url`),
`analysis_only`, `memory_hit`, `need_queries`, or `error`.

**VM `state`**: `ready` / `free` (leasable), `allocated`, `degraded`.

## Configuration

Everything is env-driven; see [`.env.example`](.env.example). Key knobs:
`POOLD_URL`, `WORKERS`, `QUEUE_POLL_SECS`, `DB_PATH`, `DISPATCH_MODE` (`ssh`|`mock`),
`LLM_BACKEND` (`api`|`cli`|`stub`), `ANTHROPIC_API_KEY`, `CLAUDE_TIMEOUT`,
`MOCK_STEP_DELAY_MS` (paces the mock dispatcher for demos),
`SLACK_BOT_TOKEN` + `SLACK_CHANNEL` (see below).

### Slack notifications (optional)

Set `SLACK_BOT_TOKEN` (a `xoxb-…` bot token with the `chat:write` scope, with the
app added to the channel) and `SLACK_CHANNEL` (a channel ID like `C0C1SH00CP9` or
`#name`). When both are present, the orchestrator posts each finished *new*
investigation to the channel via `chat.postMessage` — symptom, root cause, fix
summary, and the PR link. It fires only for investigations that actually run a VM
(memory cache hits are not posted); posting is best-effort and off the critical
path, so a Slack failure is logged and never affects the task. Leave either var
empty to disable.

`WORKERS` now bounds only concurrent *memory lookups*; how many investigations run
at once is bounded by the pool itself, since one scheduler owns every lease.

The **LLM backend** the orchestrator uses for its own Haiku calls:
`api` (Anthropic key), `cli` (shells out to the local `claude`, reusing the
subscription OAuth token), or `stub` (dependency-free, for demos/tests).

## Deploy (EC2, Amazon Linux 2023, aarch64)

```bash
make deploy      # cross-compiles linux/arm64, ships binary + unit, restarts service
```

Host needs `/opt/agent/.env` (root-owned 0600). A leased VM comes back blank.

## VM bootstrap (provisioning at dispatch)

Before each `claude -p` run the SSH dispatcher **provisions the leased VM** so the
agent can actually do its job (`internal/dispatch/dispatch.go`, `VM_BOOTSTRAP=true`):

1. **Secrets** — writes `CLAUDE_CODE_OAUTH_TOKEN`, `GH_TOKEN`/`GITHUB_TOKEN`,
   `DD_API_KEY`/`DD_APP_KEY`, and `AWS_*` from the box's `/opt/agent/.env` to
   `~/.config/agent/env` on the VM (chmod 600). Travels over the ssh *stdin*, never
   argv, so it isn't in `ps`. The run `source`s it so `claude`/`gh`/`aws` are
   authenticated and claude can expand the `${DD_API_KEY}` refs in mcp.json.
2. **`mcp.json`** — the pool VMs boot from a stable snapshot that has no mcp.json,
   so the box ships its own (`MCP_CONFIG_SRC`) to `~/.config/agent/mcp.json` and
   points `--mcp-config` there. Env refs stay literal; claude expands them at run time.
3. **`claude`** — `command -v claude || curl -fsSL https://claude.ai/install.sh | bash`.
4. **`gh`** — package manager, then a pinned tarball (`GH_VERSION`) into
   `~/.local/bin`; then `gh auth setup-git` so `git push` and `gh pr create` both work.
5. **`aws` CLI v2** — the snapshot also lacks it; installed into `~/.local/bin`.

Every step is guarded by `command -v`, so a **pre-baked golden image no-ops** (only
the secrets + mcp.json files are rewritten) — bootstrap is a self-healing safety net,
not a per-task installer. The snapshot is expected to carry `go`, the voyager
checkout, kubectl and the local k8s cluster. Mint the Claude token with
`claude setup-token`.

## Not built yet (intentionally mocked / deferred)

- **Poold** is mocked (`cmd/mock-poold`). The client depends only on the HTTP
  contract, so the real service drops in unchanged.
- **`need_queries`** (agent pausing for DB data) currently parks the task; the
  human-in-the-loop resume is future work.
- **`claude` session limits** are shared between the orchestrator's own Haiku calls
  (`LLM_BACKEND=cli`) and the agent runs on the VMs, because both use the same
  subscription token. Hitting the cap shows up as `signature failed` and
  `ssh dispatch: exit status 1`.

Task state is no longer in-memory only: tasks, their trigger payloads, their
analyses and their full transcripts persist to SQLite. On startup the orchestrator
restores them, fails anything left mid-flight, and **releases the VM it was
holding** — previously a restart stranded that VM as `allocated` forever.
