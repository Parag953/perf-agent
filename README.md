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
