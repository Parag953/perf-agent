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
                                               └─ miss ─▶ poold.lease() ─▶ ssh claude -p ─▶
                                                          collect ─▶ PR check ─▶ [haiku] distill ─▶
                                                          memory.insert ─▶ poold.release()
```

One task per VM, no retry. A VM is `ready`/`free` → `allocated` (on lease) →
`degraded` (after its one task) → Poold recycles it → `ready`.

## Layout

| Path | What |
|---|---|
| `cmd/orchestrator` | the service — webhook intake, workers, dashboard API |
| `cmd/mock-poold` | stand-in pool manager (real Poold API is mocked for now) |
| `internal/orchestrator` | core loop, task registry, phase transitions, snapshot |
| `internal/store` | SQLite memory store (pure-Go driver, no cgo) |
| `internal/llm` | Haiku signature / match / distill — `api`, `cli`, `stub` backends |
| `internal/poold` | pool-manager client (`Lease`/`Release`/`Status`) |
| `internal/dispatch` | SSH the `claude -p` job to a VM; the investigation prompt; output parsing |
| `internal/api` | `/webhook`, `/api/state`, `/api/stream` (SSE), `/api/memory/:id`, embedded reference dashboard |

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

`/api/state` shape:

```jsonc
{
  "stats":  { "vms_total":8, "vms_ready":3, "vms_allocated":4, "vms_degraded":1,
              "queue_depth":2, "tasks_total":147, "cache_hits":58,
              "hit_rate":0.39, "avg_task_secs":472 },
  "vms":    [ { "vm_id":"VM101", "state":"allocated", "host":"…", "task_id":"t-9a1", "since":"…" } ],
  "tasks":  [ { "task_id":"t-9a1", "service":"tachyon", "alert":"memory > 40%",
               "phase":"running", "vm_id":"VM101", "outcome":"", "pr_url":"",
               "enqueued_at":"…", "started_at":"…" } ],
  "memory": [ { "id":12, "signature":"tachyon/scope-map-churn", "service":"tachyon",
               "root_cause":"…", "pr_url":"…", "hit_count":6, "last_seen":"…" } ]
}
```

**Task `phase`**: `queued → matching →` (`done`/memory hit) or
`leasing → dispatched → running → collecting → pr_check → distilling → done`.
A finished task also carries an **`outcome`**: `pr_opened` (with `pr_url`),
`analysis_only`, `memory_hit`, `need_queries`, or `error`.

**VM `state`**: `ready` / `free` (leasable), `allocated`, `degraded`.

## Configuration

Everything is env-driven; see [`.env.example`](.env.example). Key knobs:
`POOLD_URL`, `WORKERS`, `DB_PATH`, `DISPATCH_MODE` (`ssh`|`mock`),
`LLM_BACKEND` (`api`|`cli`|`stub`), `ANTHROPIC_API_KEY`, `CLAUDE_TIMEOUT`.

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

1. **Secrets** — writes `CLAUDE_CODE_OAUTH_TOKEN`, `GH_TOKEN`/`GITHUB_TOKEN`, and
   `AWS_*` from the box's `/opt/agent/.env` to `~/.config/agent/env` on the VM
   (chmod 600). Travels over the ssh *stdin*, never argv, so it isn't in `ps`. The
   investigation run then `source`s that file so `claude`, `gh`, and `aws` are
   authenticated.
2. **`claude`** — `command -v claude || curl -fsSL https://claude.ai/install.sh | bash`.
3. **`gh`** — package manager, then a pinned tarball (`GH_VERSION`) into
   `~/.local/bin`; then `gh auth setup-git` so `git push` and `gh pr create` both work.

Every step is guarded by `command -v`, so a **pre-baked golden image no-ops** (only
the secrets file is rewritten) — bootstrap is a self-healing safety net, not a
per-task installer. `go`, `aws`, and the voyager checkout are expected on the image;
mint the Claude token with `claude setup-token`.

## Not built yet (intentionally mocked / deferred)

- **Poold** is mocked (`cmd/mock-poold`). The client depends only on the HTTP
  contract, so the real service drops in unchanged.
- **`need_queries`** (agent pausing for DB data) currently parks the task; the
  human-in-the-loop resume is future work.
- **Task state** lives in memory (the SQLite store holds *memory rows*, not the
  queue) — a restart drops in-flight tasks. Persisting them is a known follow-up.
