#!/usr/bin/env python3
"""Mock orchestrator backend for the Agent Orchestrator dashboard.

The real orchestrator (trigger intake, Haiku fingerprinting, memory, Poold
client, SSH dispatch) is being built in parallel. This stands in against the
dashboard contract only:

    GET /api/state        one snapshot: stats + vms + tasks + memory
    GET /api/stream        the same snapshot, pushed over SSE on every change
    GET /api/memory/<id>   full detail for one memory row

It also exposes the *assumed* Poold API (lease/release/status) as real routes,
mocked in-process, so the seam the orchestrator will eventually call through
is visible and testable independent of the dashboard.

Nothing here is the real pipeline: phases advance on a fixed clock instead of
real `claude -p` runs, and "PRs" are just URLs. Swap this file out once the
orchestrator exists; the dashboard should not need to change.
"""
import json
import os
import queue
import random
import threading
import time
from datetime import datetime, timezone

from flask import Flask, Response, abort, jsonify, send_from_directory

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
TICK_SECONDS = 3
RUNNING_TICKS = (2, 5)          # how long a task sits in "running"
RECYCLE_TICKS = (1, 2)          # how long a VM stays "degraded" before Poold recycles it
MEMORY_HIT_CHANCE = 0.35
PR_OPENED_CHANCE = 0.55
SPAWN_CHANCE = 0.45
MAX_ACTIVE_TASKS = 10
KEEP_DONE_TASKS = 12
REPO = "andromedasec/voyager"

PHASE_FLOW = ["queued", "matching", "leasing", "dispatched", "running",
              "collecting", "pr_check", "distilling", "done"]

SERVICES = ["tachyon", "kepler", "kuiper", "ariel", "apiserver", "ingester-worker"]
ALERTS = [
    "memory > 40%", "p99 latency spike", "cpu throttling sustained",
    "oom kill risk", "goroutine count climbing", "disk IO saturation",
    "heap growth", "cpu spike", "connection pool exhausted",
]

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path="")

lock = threading.Lock()
subscribers = []          # list[queue.Queue] — one per open SSE connection
_task_seq = 0
_pr_seq = 42               # doc's example PR is already #42
_vm_seq = 0

VMS = {
    "VM101": {"vm_id": "VM101", "state": "allocated", "host": "10.4.1.101", "task_id": None, "since": None},
    "VM102": {"vm_id": "VM102", "state": "allocated", "host": "10.4.1.102", "task_id": None, "since": None},
    "VM103": {"vm_id": "VM103", "state": "allocated", "host": "10.4.1.103", "task_id": None, "since": None},
    "VM104": {"vm_id": "VM104", "state": "allocated", "host": "10.4.1.104", "task_id": None, "since": None},
    "VM105": {"vm_id": "VM105", "state": "ready",     "host": "10.4.1.105", "task_id": None, "since": None},
    "VM106": {"vm_id": "VM106", "state": "ready",     "host": "10.4.1.106", "task_id": None, "since": None},
    "VM107": {"vm_id": "VM107", "state": "ready",     "host": "10.4.1.107", "task_id": None, "since": None},
    "VM108": {"vm_id": "VM108", "state": "degraded",  "host": "10.4.1.108", "task_id": None, "since": None},
}

MEMORY = {
    12: {
        "id": 12, "signature": "tachyon/scope-map-churn", "service": "tachyon",
        "symptom": "heap climbing under sustained query load, GC pause p99 > 800ms",
        "root_cause": "ResourcesQuery rebuilt per request instead of memoized per scope",
        "fix_summary": "memoize ResourcesQuery per request context; invalidate on scope write",
        "pr_url": f"https://github.com/{REPO}/pull/42",
        "full_analysis": (
            "pprof heap diff across two 10-minute windows showed ResourcesQuery "
            "allocations dominating retained heap. Traced to resolve.go rebuilding "
            "the full scope map on every apiserver request instead of reusing the "
            "one computed at session start. Confirmed via allocation profile "
            "matching request rate 1:1. Fix memoizes per request context and "
            "invalidates on scope write; verified with a 30-minute soak, heap "
            "growth flat."
        ),
        "hit_count": 6, "created_at": "2026-08-30T14:12:00Z", "last_seen": "2026-09-17T09:40:00Z",
    },
    7: {
        "id": 7, "signature": "kepler/log-buffer", "service": "kepler",
        "symptom": "kepler-logs OOMKilled every 40-60 min under bursty ingest",
        "root_cause": "unbounded log batch retention — buffer grows with no upper bound between flushes",
        "fix_summary": None,
        "pr_url": None,
        "full_analysis": (
            "Container RSS climbs linearly with ingest rate then OOMKills. No fix "
            "landed yet — flagged that the batch buffer has no size cap and no "
            "backpressure signal to the producer; needs a design call on drop-vs-block."
        ),
        "hit_count": 3, "created_at": "2026-09-02T08:05:00Z", "last_seen": "2026-09-16T22:10:00Z",
    },
    5: {
        "id": 5, "signature": "kuiper/oom-risk", "service": "kuiper",
        "symptom": "kuiper pods restart in a tight loop after a Kafka reconnect storm",
        "root_cause": "goroutine leak in the consumer poller on reconnect — old poller never exits",
        "fix_summary": "cancel the poller's context on reconnect before starting a new one",
        "pr_url": f"https://github.com/{REPO}/pull/37",
        "full_analysis": (
            "goroutine dump showed N pollers alive where N == reconnect count since "
            "boot. Each reconnect started a new poller without cancelling the old "
            "one's context. Fixed by deriving the poller context from the connection "
            "lifetime and cancelling on teardown."
        ),
        "hit_count": 9, "created_at": "2026-07-14T11:00:00Z", "last_seen": "2026-09-15T13:22:00Z",
    },
    3: {
        "id": 3, "signature": "kepler/heap-growth-cache", "service": "kepler",
        "symptom": "kepler-inventory heap grows ~200MB/hr under tenant fan-out load",
        "root_cause": "LRU cache without eviction wired for a single-tenant assumption",
        "fix_summary": None,
        "pr_url": None,
        "full_analysis": (
            "Cache keyed by resource id only, not tenant id, so eviction never "
            "triggers under fan-out because the working set looks small per key but "
            "is actually unbounded across tenants. Needs a scoped eviction policy."
        ),
        "hit_count": 2, "created_at": "2026-09-10T19:30:00Z", "last_seen": "2026-09-14T07:15:00Z",
    },
}

TASKS = {}   # task_id -> dict, insertion-ordered
COUNTERS = {"tasks_total": 147, "cache_hits": 58, "durations": [472, 390, 610, 340, 505, 288, 455]}


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def next_task_id():
    global _task_seq
    _task_seq += 1
    return f"t-{_task_seq:03x}"


def next_pr_url():
    global _pr_seq
    _pr_seq += 1
    return f"https://github.com/{REPO}/pull/{_pr_seq}"


def seed_initial_tasks():
    mk = lambda **kw: TASKS.__setitem__(kw["task_id"], kw)
    mk(task_id="t-9a1", service="tachyon", alert="memory > 40%", phase="running",
       vm_id="VM101", enqueued_at=now_iso(), started_at=now_iso(), _remaining=3)
    mk(task_id="t-9a2", service="kepler", alert="cpu spike", phase="running",
       vm_id="VM102", enqueued_at=now_iso(), started_at=now_iso(), _remaining=2)
    mk(task_id="t-9a3", service="kuiper", alert="oom kill risk", phase="collecting",
       vm_id="VM103", enqueued_at=now_iso(), started_at=now_iso())
    mk(task_id="t-9a4", service="kepler", alert="heap growth", phase="pr_check",
       vm_id="VM104", enqueued_at=now_iso(), started_at=now_iso())
    mk(task_id="t-8f2", service="kepler", alert="heap growth", phase="done", vm_id=None,
       outcome="pr_opened", pr_url=f"https://github.com/{REPO}/pull/42",
       enqueued_at=now_iso(), started_at=now_iso())
    mk(task_id="t-8e1", service="kuiper", alert="reconnect storm", phase="done", vm_id=None,
       outcome="memory_hit", enqueued_at=now_iso(), started_at=now_iso())
    mk(task_id="t-9a5", service="kepler", alert="oom kill", phase="queued", vm_id=None,
       enqueued_at=now_iso())
    mk(task_id="t-9a6", service="tachyon", alert="disk IO saturation", phase="queued", vm_id=None,
       enqueued_at=now_iso())
    VMS["VM101"]["task_id"], VMS["VM101"]["since"] = "t-9a1", now_iso()
    VMS["VM102"]["task_id"], VMS["VM102"]["since"] = "t-9a2", now_iso()
    VMS["VM103"]["task_id"], VMS["VM103"]["since"] = "t-9a3", now_iso()
    VMS["VM104"]["task_id"], VMS["VM104"]["since"] = "t-9a4", now_iso()


def public_task(t):
    """Strip internal bookkeeping (_remaining, _degrade_at, _vm_id) before serving."""
    return {k: v for k, v in t.items() if not k.startswith("_")}


def snapshot():
    with lock:
        vms = list(VMS.values())
        ready = sum(1 for v in vms if v["state"] in ("ready", "free"))
        allocated = sum(1 for v in vms if v["state"] == "allocated")
        degraded = sum(1 for v in vms if v["state"] == "degraded")
        queued = sum(1 for t in TASKS.values() if t["phase"] == "queued")
        durations = COUNTERS["durations"][-20:] or [0]
        tasks_total = COUNTERS["tasks_total"]
        cache_hits = COUNTERS["cache_hits"]
        return {
            "stats": {
                "vms_total": len(vms), "vms_ready": ready, "vms_allocated": allocated,
                "vms_degraded": degraded, "queue_depth": queued, "tasks_total": tasks_total,
                "cache_hits": cache_hits,
                "hit_rate": round(cache_hits / tasks_total, 2) if tasks_total else 0,
                "avg_task_secs": round(sum(durations) / len(durations)),
            },
            "vms": [{k: v[k] for k in ("vm_id", "state", "host", "task_id", "since")} for v in vms],
            "tasks": [public_task(t) for t in TASKS.values()],
            "memory": [
                {k: m[k] for k in ("id", "signature", "service", "root_cause", "pr_url", "hit_count", "last_seen")}
                for m in sorted(MEMORY.values(), key=lambda m: m["last_seen"], reverse=True)
            ],
        }


def broadcast():
    snap = snapshot()
    for q in list(subscribers):
        try:
            q.put_nowait(snap)
        except queue.Full:
            pass


def lease_vm():
    for v in VMS.values():
        if v["state"] in ("ready", "free"):
            return v
    return None


def match_memory(service):
    candidates = [m for m in MEMORY.values() if m["service"] == service]
    if not candidates or random.random() > MEMORY_HIT_CHANCE:
        return None
    return random.choice(candidates)


def tick():
    with lock:
        for t in list(TASKS.values()):
            phase = t["phase"]

            if phase == "queued":
                t["phase"] = "matching"

            elif phase == "matching":
                hit = match_memory(t["service"])
                if hit:
                    hit["hit_count"] += 1
                    hit["last_seen"] = now_iso()
                    t["phase"] = "done"
                    t["outcome"] = "memory_hit"
                    t["memory_id"] = hit["id"]
                    COUNTERS["tasks_total"] += 1
                    COUNTERS["cache_hits"] += 1
                else:
                    vm = lease_vm()
                    if vm:
                        vm["state"] = "allocated"
                        vm["task_id"] = t["task_id"]
                        vm["since"] = now_iso()
                        t["vm_id"] = vm["vm_id"]
                        t["started_at"] = now_iso()
                        t["phase"] = "dispatched"
                    else:
                        t["phase"] = "queued"   # no capacity — Poold would 409; stay queued

            elif phase == "dispatched":
                t["phase"] = "running"
                t["_remaining"] = random.randint(*RUNNING_TICKS)

            elif phase == "running":
                t["_remaining"] = t.get("_remaining", 1) - 1
                if t["_remaining"] <= 0:
                    t["phase"] = "collecting"

            elif phase == "collecting":
                t["phase"] = "pr_check"

            elif phase == "pr_check":
                t["phase"] = "distilling"

            elif phase == "distilling":
                vm_id = t["vm_id"]
                if random.random() < PR_OPENED_CHANCE:
                    t["outcome"] = "pr_opened"
                    t["pr_url"] = next_pr_url()
                else:
                    t["outcome"] = "analysis_only"
                t["phase"] = "done"
                if t.get("started_at"):
                    started = datetime.strptime(t["started_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    COUNTERS["durations"].append(int((datetime.now(timezone.utc) - started).total_seconds()) or random.randint(240, 600))
                COUNTERS["tasks_total"] += 1
                t["vm_id"] = None
                if vm_id and vm_id in VMS:
                    VMS[vm_id]["state"] = "degraded"
                    VMS[vm_id]["_degrade_at"] = time.time()
                    VMS[vm_id]["task_id"] = None

        for v in VMS.values():
            if v["state"] == "degraded" and time.time() - v.get("_degrade_at", 0) > random.randint(*RECYCLE_TICKS) * TICK_SECONDS:
                v["state"] = "ready"
                v["since"] = None

        done_ids = [tid for tid, t in TASKS.items() if t["phase"] == "done"]
        if len(done_ids) > KEEP_DONE_TASKS:
            for tid in done_ids[:len(done_ids) - KEEP_DONE_TASKS]:
                del TASKS[tid]

        active = sum(1 for t in TASKS.values() if t["phase"] != "done")
        if active < MAX_ACTIVE_TASKS and random.random() < SPAWN_CHANCE:
            tid = next_task_id()
            TASKS[tid] = {
                "task_id": tid, "service": random.choice(SERVICES), "alert": random.choice(ALERTS),
                "phase": "queued", "vm_id": None, "enqueued_at": now_iso(),
            }

    broadcast()


def simulate_loop():
    while True:
        time.sleep(TICK_SECONDS)
        try:
            tick()
        except Exception as e:
            app.logger.exception("simulate tick failed: %s", e)


# ---------------------------------------------------------------------------
# Dashboard contract
# ---------------------------------------------------------------------------
@app.get("/api/state")
def api_state():
    return jsonify(snapshot())


@app.get("/api/stream")
def api_stream():
    def gen():
        q = queue.Queue(maxsize=10)
        with lock:
            subscribers.append(q)
        try:
            q.put_nowait(snapshot())
            while True:
                try:
                    data = q.get(timeout=15)
                    yield f"data: {json.dumps(data)}\n\n"
                except queue.Empty:
                    yield ": heartbeat\n\n"
        finally:
            with lock:
                if q in subscribers:
                    subscribers.remove(q)
    return Response(gen(), mimetype="text/event-stream",
                     headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/memory/<int:mem_id>")
def api_memory_detail(mem_id):
    m = MEMORY.get(mem_id)
    if not m:
        abort(404, description="no memory row with that id")
    return jsonify(m)


# ---------------------------------------------------------------------------
# Mocked Poold — not used by the dashboard, kept for orchestrator dev/testing
# ---------------------------------------------------------------------------
@app.post("/poold/lease")
def poold_lease():
    with lock:
        vm = lease_vm()
        if not vm:
            return jsonify({"error": "no_capacity"}), 409
        vm["state"] = "allocated"
        vm["since"] = now_iso()
    return jsonify({"vm_id": vm["vm_id"], "host": vm["host"],
                     "ssh_target": f"agent@{vm['host']}", "state": "allocated"})


@app.post("/poold/release")
def poold_release():
    from flask import request
    body = request.get_json(force=True, silent=True) or {}
    vm_id = body.get("vm_id")
    with lock:
        vm = VMS.get(vm_id)
        if not vm:
            abort(404, description="unknown vm_id")
        vm["state"] = "degraded"
        vm["_degrade_at"] = time.time()
        vm["task_id"] = None
    return jsonify({"ok": True})


@app.get("/poold/status")
def poold_status():
    with lock:
        return jsonify({"vms": [{"vm_id": v["vm_id"], "state": v["state"]} for v in VMS.values()]})


# ---------------------------------------------------------------------------
# Static frontend
# ---------------------------------------------------------------------------
@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    seed_initial_tasks()
    threading.Thread(target=simulate_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 8090))
    app.run(host="0.0.0.0", port=port, threaded=True)
