# Agent Orchestrator — dashboard

Operational dashboard for the orchestrator + Poold pool, built against the
dashboard contract while the real orchestrator is still being built.

## What's here

- `server.py` — a **mock backend** implementing the dashboard contract:
  `GET /api/state`, `GET /api/stream` (SSE), `GET /api/memory/<id>`. It runs an
  in-memory simulation of the VM pool and the task phase flow
  (`queued → matching → (hit ✓ | leasing → dispatched → running → collecting →
  pr_check → distilling → done)`) on a fixed clock, so the dashboard has
  something real to render against without the orchestrator, Poold, or Claude
  existing yet. It also exposes the *assumed* Poold API
  (`POST /poold/lease`, `POST /poold/release`, `GET /poold/status`) as real
  routes for whoever builds the orchestrator's Poold client to test against.
- `static/` — the dashboard itself: stat tiles, VM pool grid, task list with
  phase pills, and a memory ("known issues") panel that opens a detail modal
  via `GET /api/memory/<id>`. Plain HTML/CSS/JS, no build step, no framework —
  talks to the backend with `fetch` and `EventSource`.

**Nothing here is the real pipeline.** Task phases advance on a clock, not a
real `claude -p` run; "PRs" are just generated URLs. Swap `server.py` for the
real orchestrator once it exists — the dashboard should not need to change,
since it only ever consumes `/api/state` and `/api/stream`.

## Run it

```bash
pip install -r dashboard/requirements.txt
python dashboard/server.py        # http://localhost:8090
```

`PORT` env var overrides the port.

## Notes / assumptions carried over from the design doc

- Memory-hit tasks skip the VM lease entirely (`matching` → `done` directly) —
  that shortcut is the whole point of memory, so it's simulated too, not just
  drawn.
- A VM only ever runs one task, then goes `degraded` until Poold recycles it
  back to `ready` — enforced in `tick()`, not just in the state labels.
- `queue_depth` reflects tasks that rolled `matching` → `leasing` and found no
  free VM (mocked as a `409 no_capacity` in `/poold/lease`) and fell back to
  `queued`.
- The dashboard falls back from SSE to polling `/api/state` every 4s if the
  stream drops, per the contract's note — watch the pill in the header.
