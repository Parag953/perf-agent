# pool — the VM pool the orchestrator leases from

The orchestrator does not own VMs; it asks **poold** for one. This directory holds
poold itself and the operational tooling around the pool, vendored here so the lease
contract and its only consumer live in one place.

| Path | What |
|---|---|
| `poold/` | the pool daemon — lease/release, prepare (rollback → gate), Proxmox API driver |
| `scripts/andro-phase0.sh` | PVE harness: `probe`, `snapshot`, `rollback`, `waitgate`, `stepclock`, `storm`, `scalefloor` |
| `docs/andro-vm-pool.md` | the pool runbook: topology, access, health gate, and the traps that cost real time |

## Why a whole VM per agent

Four things in the Voyager build system are hardcoded per host — the kube context name,
the singleton `localhost:5000` registry, the image tag `0.9.0` in a fixed namespace, and
one Docker daemon. Two agents on one cluster both push `<svc>:0.9.0` and silently
overwrite each other. So the lease unit is a **whole VM**, not a cluster.

## The contract the orchestrator depends on

| Endpoint | Meaning |
|---|---|
| `POST /poold/lease` | `200` with `{vm_id, host, ssh_target, state}`, or `409 no_capacity` |
| `POST /poold/release` | hand the box back; poold resets it |
| `GET /poold/status` | contract view: `ready` (leasable), `allocated`, `degraded` (+ `degraded_reason`) |
| `GET /boxes` | richer per-box view incl. `prepare.step`, `prepare.elapsed_s` and the gate sample |

**Lease never blocks.** It is instant on a `ready` box and `409` otherwise — the ~4
minutes is the box *preparing* after release (full vmstate rollback, then a health gate).
That is why the orchestrator queues on `409` rather than treating it as an error, and why
it reads `/boxes` to show what a preparing box is actually doing.

## The gate is a window, not a sample

A box is only `ready` after `GATE_PASSES=4` consecutive clean samples, 10s apart, with a
120s settle floor and a flat pod-restart count — plus real datastore probes. This exists
because the gate once went green at 67.6s while TimescaleDB was still refusing
connections. Every hollow-green on this project was a single-sample check.

## Deploy

```bash
cd poold && ./deploy.sh andro-3@10.0.0.192
```

Runs the unit tests, tars the tree over SSH, and restarts the systemd --user unit.
poold needs a Proxmox API token at `~/.config/pve/token` (never committed) with
`PVEVMAdmin` on the pool VMs plus `Datastore.AllocateSpace` on `local-lvm`.

**Every poold restart rolls back any box that was `preparing` or internally leased**, so
deploy only when the pool is idle.
