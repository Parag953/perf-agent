# Andromeda VM Pool on Proxmox

Built 2026-09-16, both stacks verified healthy 2026-09-17.
Everything here was observed on the running machines. Where something is an
estimate rather than a measurement, it says so.

---

## 1. Topology

```
server1  (Proxmox 9.2.10, 10.0.0.213)  ← NOT YOURS, see §2
  96 cores · 251 GB RAM · local-lvm 342 GB · storagehdd 3.56 TB (88% full, off limits)
│
├── VM 100  officecluster6      ← someone else's
├── VM 101  andro-a   8 vCPU · 24 GB · 130 GB   user andro-1
└── VM 102  andro-b   8 vCPU · 24 GB · 130 GB   user andro-2
```

Both: Debian 13 (trixie), kernel 6.12, disks on `local-lvm` only.

### The VMs are peers, not a cluster

**The stack cannot span machines.** `create-cluster` is
`minikube start --driver=docker` (`scripts/build/kube.mk:240`); the k3d path is
`--servers 1 --agents 1` (`kube.mk:295`). Both are node-*containers* on one
Docker daemon, with the registry container on that same Docker network. There is
no join token and no worker enrollment. A "master + workers" layout is not
buildable here.

Each VM is therefore a **complete self-contained stack**: own minikube, own
registry on `localhost:5000`, own checkout. Both use the kube context name
`andromeda` deliberately — they never see each other, and `context-verifier`
accepts only three fixed names (§5).

---

## 2. server1 belongs to someone else

`server1` is on **`meetclassmeet2015@`'s tailnet**, alongside
`andromeda-patch-01/02/03`. Your Mac and both VMs are on **`abhishekh@`'s**
tailnet — `server1` does not appear in your peer list at all. It also runs VM 100
and the 88%-full `storagehdd`.

The tell, when we installed Tailscale on it: `tailscale up` returned instantly
with **no auth URL**, because it already held credentials for that tailnet.

**Consequences:**
- Do not `tailscale logout` on it — that would disconnect it from its owners.
- No root SSH (publickey denied). Host-level changes need its administrator.
- Anything requiring the Proxmox API needs a token *they* create (§8).

---

## 3. Access — all verified working

| Target | LAN | Tailscale (works anywhere) |
|---|---|---|
| `andro-a` | `ssh andro-1@10.0.0.155` | `ssh andro-1@100.75.158.42` |
| `andro-b` | `ssh andro-2@10.0.0.38` | `ssh andro-2@100.73.230.48` |
| Proxmox UI/API | `https://10.0.0.213:8006` | same URL, routed via `andro-a` |

Key auth (`~/.ssh/id_ed25519`), passwordless sudo on both. `root` is locked on
the VMs by design — Debian installs `sudo` only when the root password is left
blank at install time.

### The Proxmox route

`andro-a` is a Tailscale subnet router advertising **`10.0.0.213/32`** only — the
Proxmox host, nothing else on the LAN — on **your** tailnet. IP forwarding is set
in `/etc/sysctl.d/99-tailscale.conf`, and the UDP-GRO tuning that subnet routers
need is persisted by a systemd unit `tailscale-gro.service`.

Verified end to end from off-network: `HTTP 401` from the Proxmox API with
`route -n get 10.0.0.213` reporting `interface: utun8`, i.e. through the tunnel,
not the LAN.

**This depends on `andro-a` being up.** If you roll it back or shut it down you
lose remote Proxmox access until it returns. To remove that single point of
failure, advertise the same route from `andro-b` too — Tailscale supports
multiple subnet routers for one prefix with automatic failover.

**Validating from inside the office is misleading**: your Mac reaches
`10.0.0.213` directly over the LAN, which masks whether the tunnel works. Check
`route -n get 10.0.0.213` shows `utun*`, or test from a phone hotspot.

---

## 4. AWS SSO — required before any image work

Everything image-related is hard-wired to ECR
`741798470562.dkr.ecr.us-west-2.amazonaws.com`. The profile **must** be named
`andromeda-dev01` (`scripts/build/env.mk:177`). `~/.aws/config` is already
written on both VMs.

```bash
ssh -t andro-1@10.0.0.155 'aws sso login --profile andromeda-dev01 --use-device-code'
ssh -t andro-2@10.0.0.38  'aws sso login --profile andromeda-dev01 --use-device-code'
```

**`--use-device-code` is mandatory on these boxes.** The default flow registers a
loopback `redirect_uri` (`http://127.0.0.1:<port>/oauth/callback`) on the *VM*;
you approve in your Mac's browser, the callback hits *your Mac's* port, and
nothing is listening. Device code has no callback.

Sessions expire in ~8-12 h; after that image work fails with
`no basic auth credentials`. Check before handing a box to an agent:

```bash
aws sts get-caller-identity --profile andromeda-dev01 --no-cli-pager
```

---

## 5. Why an agent needs a whole VM

Four hardcoded things in the build system. These are why a shared cluster is
unsafe and the pool leases entire VMs.

1. **`context-verifier` accepts exactly three context names** — `andromeda`,
   `docker-desktop`, `k3d-andromeda` (`scripts/build/common.mk:5`); anything else
   `exit 1`. Every `dev.*` target depends on it.
2. **The registry is a singleton.** `push_image_to_minikube` is
   `docker tag $1:$2 localhost:5000/...; docker push ...` (`kube.mk:380`), and the
   registry binds `127.0.0.1:5000`. One per host, not parameterised.
3. **The image tag is hardcoded `0.9.0`, in a hardcoded namespace.**
   `dev.refresh.%` and `dev.delta.%` push `$*:0.9.0` then
   `kubectl delete pods -l service=$* -n andromeda-system`.
   → **Two agents on one cluster both pushing `apiserver:0.9.0` silently
   overwrite each other, and both pods roll to whoever pushed last. No error.**
4. **Everything is bound to one Docker daemon** (§1).

**So: one agent = one whole VM, held under a lock.** Raising depth without
VM-level isolation means plumbing `TAG` and `DEPLOY_NAMESPACE` through
`dev.refresh` / `dev.delta` / `helm-install` and `context-verifier` — a real
change to `scripts/build/`, not config.

### Lease mechanism — designed, NOT BUILT

Distributed, no broker: the VMs share no filesystem, so a central lock adds a box
and a SPOF for what two files can do. Each VM owns `/var/lock/andro.lease` with
`owner, pid, acquired_at`. A client-side wrapper tries `andro-a`, falls back to
`andro-b`, blocks if both are held. TTL so a crashed agent's claim expires;
`--steal` forces it loudly.

---

## 6. The health gate — get this right

**`kubectl get pods -A | grep -vcE "Running|Completed"` returning 0 is NOT
sufficient.** It returned 0 twice on `andro-b` while `andromeda-system` was
*empty*: a failed helm release has no Deployments, so it has no pods to be
unhealthy. `helm list` also showed the release present, because a **failed**
release still lists.

Two hollow green signals. Use all three checks:

```bash
helm list -A --no-headers | grep -vc deployed              # must be 0
kubectl get deploy -n andromeda-system --no-headers | wc -l # must be > 0 (13 today)
kubectl get pods -A --no-headers | grep -vcE "Running|Completed"  # must be 0
```

**Exit codes also lie here.** `make setup-cluster` returns **non-zero on a
perfectly good install** (it syncs all 39 andromeda images; a profile has ~14, so
the missing ones fail the sync after the needed ones were pushed). Meanwhile
`dev.profile.inventory` can return **zero** with pods that will never start.

---

## 7. Daily commands

From `~/voyager` on either VM.

```bash
make registry-login                      # docker+helm login to ECR; do this first
make dev.profile.inventory               # helm install only — does NOT build
make dev.images.inventory DOCKER_JOBS=1  # build the 14 inventory images locally
make dev.delta.<svc>                     # fast: compile Go binary, overlay on image
make dev.refresh.<svc>                   # full rebuild of one service
```

### Pull prebuilt images instead of building — much faster

CI publishes every service to ECR daily. Building all 14 locally took **~2-2.5 h**
(measured: `apiserver` alone 11m09s). Pulling ~1.5 GB takes minutes.

Find the newest tag:

```bash
aws ecr describe-images --repository-name apiserver --region us-west-2 \
  --profile andromeda-dev01 --no-cli-pager --filter tagStatus=TAGGED --output json \
| jq -r '.imageDetails[] | select(.imageTags!=null) | "\(.imagePushedAt) \(.imageTags|join(","))"' \
| sort -r | head -5
```

Then pull and retag to `0.9.0` (the tag the charts expect, `kube.mk:176`):

```bash
ECR=741798470562.dkr.ecr.us-west-2.amazonaws.com
RTAG=26.09.16-alpha.3          # use a current tag
for s in apiserver ui-app ingester kepler knot batch-compute init-sql init-tsdb \
         init-neo4j init-model init-data risk lsp-eps query-cache graph-janitor \
         debezium kuiper tachyon; do
  docker pull -q $ECR/$s:$RTAG && docker tag $ECR/$s:$RTAG $s:0.9.0 && echo "PULLED $s"
done
```

`ANDROMEDA_REGISTRY ?= local` (`kube.mk:191`) means "use what's in the daemon",
so no override is needed — `setup-cluster` picks the retagged images up.

Caveat: these are release images, not your working tree. Use `dev.delta.<svc>`
to overlay local changes.

### The profile module list is a BUILD list, not a DEPLOY list

`PROFILE_MODULES_inventory` (`profiles.mk:119`) has 14 entries, but the charts
deploy more — `init-data`, `debezium`, `kuiper`, `tachyon` are rendered by the
inventory profile and absent from that list. Pulling only the 14 produces
`ImagePullBackOff` ten minutes into an install. The 18 names above are the
corrected set.

### Registry parity beats reconciliation

The durable way to prepare a new box is to **mirror a known-good box's catalogue
before installing**:

```bash
# on the good box
curl -s localhost:5000/v2/_catalog | python3 -c "import json,sys; print(' '.join(sorted(json.load(sys.stdin)['repositories'])))"
```

Both boxes today hold **33 repos**. Reconciling after the fact also works but is
inherently late — a reconciler can only see images referenced by pods that
already exist, so each install round can reveal new gaps:

```bash
# /tmp/reconcile.sh on both VMs — diff cluster wants vs registry has, pull the gaps
WANT=$(kubectl get pods -A -o jsonpath='{range .items[*]}{range .spec.containers[*]}{.image}{"\n"}{end}{end}' \
  | grep "registry.local:5000/" | sed 's|registry.local:5000/||' | cut -d: -f1 | sort -u)
HAVE=$(curl -s localhost:5000/v2/_catalog | tr ',' '\n' | tr -d '{}[]"' | sed 's/repositories://' | sort -u)
for w in $WANT; do echo "$HAVE" | grep -qx "$w" || echo "MISSING $w"; done
```

---

## 8. Proxmox API — for growing the pool

The API is reachable from `andro-a` (`:8006`, returns `401`). With a token you
can create, clone, start, stop **and snapshot** VMs — no host SSH needed.
`qm snapshot --vmstate 1` has an API equivalent
(`POST /nodes/server1/qemu/{id}/snapshot` with `vmstate=1`), covered by
`VM.Snapshot` / `VM.Snapshot.Rollback` in the built-in **`PVEVMAdmin`** role.

**Ask the host's owner for the narrow thing, not root:**

> A pool `andro-agents` containing VMs 101 and 102, and an API token with
> `PVEVMAdmin` on that pool plus `Datastore.AllocateSpace` on `local-lvm`.

That lets an orchestrator manage pool VMs from `andro-a` while being
structurally incapable of touching VM 100, `storagehdd`, or host config.

**Starting daemons on the host is not possible via the API** — by design, and
correctly so: `andro-a` is a guest, and a guest controlling its hypervisor would
defeat the isolation the pool relies on. That needs root SSH from the owner.

### If you pursue vmstate snapshots

`local-lvm` is **LVM-thin**, and thin snapshots consume real space as blocks
diverge — Kafka, etcd, Postgres and ClickHouse write constantly even idle. Budget
for the vmstate (~RAM size) **plus** ongoing divergence, and measure
`lvs -o lv_name,data_percent pve` a day after cutting a baseline rather than
assuming.

Clock jump is the other hazard: a resumed VM believes it is snapshot-time, then
NTP steps it forward — node leases expire, ServiceAccount tokens go stale, Kafka
sessions rebalance, and every Temporal timer fires at once. The health gate in §6
absorbs it; `chrony` with `makestep 1000 -1` helps; refreshing baselines often
keeps the jump small.

---

## 9. Gotchas that cost real time

### Debian install
**Leave the root password blank.** Debian reads that as "lock root, use sudo" and
adds your user to `sudo`. Set a root password and the user gets no sudo at all —
discovered only when the first `sudo` fails, long after the installer is gone.

Also uncheck the desktop environment; keep SSH server.

### The repo's dependency installer does not work on Debian
`scripts/dev/setup/install-dependencies.sh` shells out to `snap` for yq/jq
(`:57`), points Docker and helm at **Ubuntu** apt repos, and builds the terraform
repo line from `lsb_release -cs` → `trixie`, which HashiCorp does not publish.
Install those directly.

**Do not `apt install golang`** — the workspace needs **Go 1.27.1**
(`go.work:1`); Debian 13 ships older. Use the upstream tarball.

**`crane` is required** by `load-external-images-minikube`. Missing it aborts
`setup-cluster` *before* `load-images` runs, so the registry is left empty while
all the images sit in the docker daemon — a confusing failure shape.
`go install github.com/google/go-containerregistry/cmd/crane@latest`

### Python: undeclared deps and unset PYTHONPATH
`requirements.txt` lists only `pyyaml`, but `scripts/ci/create_external_secret.py`
→ `tests/lib/python/as_aws_utils.py` needs **nine**: `yaml`, `kubernetes`,
`boto3`, `botocore`, `httpx`, `polling2`, `azure-identity`,
`azure-mgmt-authorization`, `msgraph-sdk`.

```bash
sudo apt-get install -y python3-yaml python3-kubernetes python3-boto3
sudo pip3 install --break-system-packages httpx polling2 azure-identity \
  azure-mgmt-authorization msgraph-sdk      # Debian 13 enforces PEP 668
```

**And `PYTHONPATH` is never set by the build.** `helm.mk:197` runs
`python3 $(ROOT)/scripts/ci/create_external_secret.py`, but `as_aws_utils.py`
lives in `tests/lib/python/` — never on `sys.path`. It works on a dev machine
only because the *developer's shell* provides it (see
`.claude/skills/voyager-worktree/scripts/run-make-gen.sh:28`). Both VMs now have
`/etc/profile.d/voyager-python.sh` with that list.

### Submodules: one at a time, then verify with `du`
`git submodule update --init --recursive` wedged, a second attempt raced it into
the same `.git/modules` path, and the result was an **8 KB half-clone that
`git submodule status` reported as clean**. `make gen` then died at `Makefile:78`
with `Unable to find current revision`. Four attempts on `andro-a`; one on
`andro-b` using:

```bash
for m in thirdparty/googleapis helm/andromeda/charts/temporal thirdparty/iam-dataset; do
  git submodule update --init "$m"
done
du -sh thirdparty/iam-dataset   # must be ~430 MB, NOT 8 KB
```

### A failed `andromeda` install POISONS every retry
This is the big one. It cost four failed reinstalls on `andro-b`.

The init hooks carry `helm.sh/hook-delete-policy: hook-succeeded` — successful
hook Jobs are deleted, **failed ones are deliberately kept**. So one failed
install leaves an `init-data` Job behind, and every later install dies with:

```
Error: failed pre-install: ... * jobs.batch "init-data" already exists
```

`helm uninstall` does **not** remove hook resources. And separately,
`helm upgrade --install` over a failed release **skips `pre-install` hooks
entirely**, so `temporal-externalsecret` never gets created, there is no
`temporal-visibility-stores` secret, temporal-frontend cannot start, and the
apiserver dies with `failed to create temporal client: context deadline exceeded`.

**Correct cleanup for a failed release — both halves, always:**

```bash
helm uninstall andromeda -n andromeda-system
kubectl delete job -n andromeda-system --field-selector status.successful=0
```

Use the field-selector form. `kubectl delete job --all` also removes *completed*
init Jobs, which loses the record of what has been seeded.

**Read the hook error, not the make error.** `make: *** [profiles.mk:175] Error 2`
said nothing useful for four attempts; `jobs.batch "init-data" already exists`
was in the same log the whole time.

### kuiper is OOMKilled by an unmeasured memory cap — STILL UNFIXED IN REPO
`helm/andromeda/profiles/local-resources.yaml:140` caps `kuiper` at **256Mi**.
The comment at line 42 of that same file says:

> "ALSO UNMEASURED but DO ship with the inventory profile: **kuiper**,
> batch-compute... their caps are the ones most worth re-measuring."

It is too low. kuiper OOMKills on startup while loading Custom Application
permissions. The log line is a red herring —
`unable to fetch Custom Application permissions from DB` looks like a data
problem, but the pod state says `OOMKilled`. **Read the pod state, not just the
logs.**

`andro-a`'s "4 restarts then stable" was the same OOM surviving by luck; it would
have died on its next inventory sync.

Both boxes are patched at runtime to `1Gi`:
```bash
kubectl set resources deploy -n andromeda-system kuiper --limits=memory=1Gi --requests=memory=256Mi
```

**This does not survive `dev.profile.inventory`.** Fixing
`local-resources.yaml:140` is a small PR worth raising — and `batch-compute`
carries the same "unmeasured" warning and has never been measured either.

### Never gate a wait on a pattern your own command contains
Three separate times a supervising command matched **itself**:

- `pgrep -f "make gen"` — the watcher's own `bash -c` contained that string, so it
  reported RUNNING for an hour after the job had finished. Cost ~1 h of idle time.
- `ps -ef | grep -cE "[s]etup-cluster"` inside a wrapper whose heredoc contained
  that text — the `[s]` trick stops *grep* self-matching, not a parent whose
  arguments contain the pattern.
- `pkill -f finish.sh` issued from an SSH command containing `finish.sh` — killed
  its own session.

Gate on a PID captured at launch (`$!`), or on a **marker the job writes to a
file**. Never on a string the watcher also contains.

### Build output is suppressed
`profiles.mk:207` redirects each module build to `>/dev/null 2>&1` and prints only
`OK`/`FAIL` plus a duration. To see a failure:
`make -C ~/voyager/services/<svc> docker PROJECT=<svc>`.

The `xargs` warning there (`-n1` with `-I{}`) is harmless: `-I{}` supersedes it
but already implies one argument per command, and `-P $(DOCKER_JOBS)` still
controls concurrency.

### Ballooning is OFF on both VMs, deliberately
Under a 16 GB minikube it lets the host reclaim pages from Kafka and Neo4j, which
surfaces as random pod restarts rather than as memory pressure.

### minikube's kicbase is a cold WAN download
~508 MB from `gcr.io` per fresh box, not from your ECR mirror. Worth mirroring
alongside the k3d images if you provision often —
`scripts/ci/pull_and_retag.sh:11` already does exactly this pattern for k3d.

---

## 10. Sizing — the measurement that changes the pool

**A running stack uses far less disk than estimated.** Measured today:

| | `andro-a` | `andro-b` |
|---|---|---|
| Disk used | **62 GB** of 121 GB | **51 GB** of 121 GB |
| RAM used | 8 GB of 23 GB | 8 GB of 23 GB |
| Registry | 33 repos | 33 repos |
| Deployments | 13 | 13 |

`andro-a` is ~11 GB larger only because it ran `make gen` and compiled two
modules before switching to the ECR path; `andro-b` is pull-only. **A pull-only
box needs ~55 GB, not the 130 GB allocated and not the 150 GB originally
estimated.**

### Consequences for pool depth

| Resource | Host | Per VM (current) | Ceiling |
|---|---|---|---|
| Cores | 96 | 8 | 12 VMs |
| RAM | 251 GB | 24 GB | 10 VMs |
| `local-lvm` | 342 GB | 130 GB allocated / **~55 GB used** | 2 allocated, **~5 real** |

Disk was thought to be the hard ceiling at 2 VMs. At ~55 GB actual, **70 GB
allocations would fit 4-5 VMs** in the same 342 GB — a materially better pool.
Two caveats: LVM-thin over-provisioning fails as ENOSPC *mid-build* (which looks
like a compiler error, not a disk error), and snapshots add divergence on top
(§8).

**Both VMs are also under-provisioned on CPU and RAM** — 8 cores / 24 GB out of
96 / 251. At 16 cores / 48 GB each they would run the *full* stack (ClickHouse,
cooper/datapath, Kafka) rather than the cut-down inventory profile, and
`DOCKER_JOBS` could go to 4-6. The serial-build guidance at `profiles.mk:197` was
measured on a **21.5 GiB** box that swapped; it does not apply at 48 GB.
Resizing needs a shutdown, so decide before cutting snapshot baselines.

---

## 11. Verified state — 2026-09-17

```
andro-a   10.0.0.155 / 100.75.158.42   HEALTHY   33 Running   13 deploys   5/5 helm deployed
andro-b   10.0.0.38  / 100.73.230.48   HEALTHY   33 Running   13 deploys   5/5 helm deployed
```

Both pass the full §6 gate. Services on each: `apiserver`, `ui-app`,
`ingester-server`, `ingester-worker-inventory`, `kepler-inventory`, `knot`,
`batch-compute`, `kuiper`, `tachyon`, `temporal-frontend/history/matching/worker`.
Plus `andromeda-database` (postgresql, timescaledb, redis), `andromeda-kafka`
(broker, controller, strimzi-operator, entity-operator, connect-cluster),
`andromeda-neo4j`, `external-secrets`.

Helm releases on both: `andromeda`, `andromeda-database`, `andromeda-kafka`,
`andromeda-neo4j-1`, `external-secrets` — all `deployed`.

### Not done
- **The lease scripts (§5)** — designed, not written.
- **`local-resources.yaml` kuiper cap** — patched at runtime only; reverts on the
  next `dev.profile.inventory`.
- **No functional test beyond pod health** — nothing has driven the apiserver or
  ui-app end to end on these boxes.
- **Proxmox API token (§8)** — needs the host owner.
