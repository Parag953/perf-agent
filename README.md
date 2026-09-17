# poold

Lease and reset authority for the andromeda VM pool. One box per lease; every acquire
rolls the VM back to its `warm-live` snapshot, resolves its address from the qemu guest
agent, steps the guest clock, and hands over only after the gate window is clean.

Design: `docs/superpowers/specs/2026-09-16-agent-swarm-cluster-pool-design.md` in voyager (§4.1, §14).

    python3 -m unittest discover -p 'test_*.py'
    ./deploy.sh andro-1@10.0.0.155        # rsync + systemd --user restart

API: `GET /status`, `GET /boxes/<name>`, `POST /run {task, owner, source, ttl_s, callback_url, reset_on_release}`,
`GET /runs/<id>`, `GET /runs/<id>/trace` (SSE), `DELETE /runs/<id>`, `POST /boxes/<name>/reset|quarantine|unquarantine`,
`POST /leases/<id>/heartbeat`.

Orchestrator contract (kept alongside the native API): `POST /poold/lease` → `200 {vm_id, host, ssh_target,
state:"allocated"}` or `409 {error:"no_capacity"}`; `POST /poold/release {vm_id}` → `{ok:true}`;
`GET /poold/status` → `{vms:[{vm_id, state}]}` with `state ∈ allocated|ready|free|degraded`
(allocated=leased, ready=prepared and gate-passed, free=dirty or preparing, degraded=quarantined).
With `auto_prepare = true` every dirty box is rolled back and gated as soon as it is idle, so a lease
is instant when a box is `ready`; `release` marks the box dirty and it returns to `ready` in ~3 min.
