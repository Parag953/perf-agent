#!/usr/bin/env bash
set -euo pipefail

PVE=${PVE:-https://10.0.0.213:8006}
NODE=${NODE:-server1}
VMID=${VMID:-102}
BOX=${BOX:-andro-2@10.0.0.38}
STORAGE=${STORAGE:-local-lvm}
LOG=${LOG:-$(dirname "$0")/phase0.log}
TOKEN_FILE=${TOKEN_FILE:-$HOME/.config/pve/token}



ALLOWED_VMIDS="${ALLOWED_VMIDS:-101 102}"
ALLOWED_STORAGE="${ALLOWED_STORAGE:-local-lvm}"
ALLOWED_NODE="${ALLOWED_NODE:-server1}"

guard() {
  local ok=0 v
  for v in $ALLOWED_VMIDS; do [[ "$VMID" == "$v" ]] && ok=1; done
  if (( ! ok )); then
    echo "REFUSED: VMID=$VMID is not in the allowlist ($ALLOWED_VMIDS)." >&2
    echo "  VM 100 (officecluster6) belongs to another team and must never be touched." >&2
    exit 3
  fi
  ok=0
  for v in $ALLOWED_STORAGE; do [[ "$STORAGE" == "$v" ]] && ok=1; done
  if (( ! ok )); then
    echo "REFUSED: STORAGE=$STORAGE is not in the allowlist ($ALLOWED_STORAGE)." >&2
    echo "  storagehdd holds older important disks and is off limits." >&2
    exit 3
  fi
  if [[ "$NODE" != "$ALLOWED_NODE" ]]; then
    echo "REFUSED: NODE=$NODE != $ALLOWED_NODE." >&2; exit 3
  fi
  case "$BOX" in
    andro-1@*|andro-2@*) : ;;
    *) echo "REFUSED: BOX=$BOX is not an andro pool box." >&2; exit 3 ;;
  esac
}

api_guard_path() {
  case "$1" in
    *storagehdd*) echo "REFUSED: API path touches storagehdd: $1" >&2; exit 3 ;;
    */qemu/100/*|*/qemu/100) echo "REFUSED: API path touches VM 100: $1" >&2; exit 3 ;;
  esac
}

usage() {
  cat <<'U'
usage: andro-phase0.sh <cmd>
  probe               storage backend, vmstate support, vm config, snapshots, live gate
  list                snapshots + volumes for the target vm
  snapshot <n> [0|1]  take snapshot <n>; 1 = --vmstate (live), 0 = disk-only
  dirty               plant dirt markers (file, docker tag, deploy annotation)
  rollback <n> [0|1]  roll back to <n>, timing task -> first ssh -> gate pass
  storm               post-rollback timer/lease/rebalance indicators
  coldsnap [n]        graceful shutdown -> disk-only snapshot -> start, timed
  ensure              repair an empty cluster behind a reachable box (frees 192.168.49.2)
  gate                one health-gate sample
  scalefloor          statefulset scale 0 -> ready floor (the PVC-swap comparison)

env: PVE_TOKEN or ~/.config/pve/token   VMID (default 102)   BOX (default andro-2@10.0.0.38)
     NODE (server1)  STORAGE (local-lvm)  GATE_TIMEOUT (900)
gate: GATE_PASSES (4 consecutive clean samples)  GATE_SETTLE (120s floor)
      GATE_INTERVAL (10s between samples)  EXPECT_DEPLOYS (13)  EXPECT_CTX (andromeda)
addr: BOX_DISCOVER=1 resolves the box IP from the qemu guest agent; set 0 to trust BOX
U
}

need_token() {
  if [[ -z "${PVE_TOKEN:-}" ]]; then
    [[ -r "$TOKEN_FILE" ]] || { echo "no PVE_TOKEN and no $TOKEN_FILE" >&2; exit 2; }
    PVE_TOKEN=$(tr -d '[:space:]' < "$TOKEN_FILE")
  fi
  AUTH="Authorization: PVEAPIToken=$PVE_TOKEN"
}

SSH=(ssh -o BatchMode=yes -o ConnectTimeout=3 -o StrictHostKeyChecking=accept-new "$BOX")
set_box() { BOX="$1"; SSH=(ssh -o BatchMode=yes -o ConnectTimeout=3 -o StrictHostKeyChecking=accept-new "$BOX"); }
now() { date +%s.%N; }
ts() { date -u +%H:%M:%S; }
log() { echo "[$(ts)] $*" | tee -a "$LOG"; }
api() { local m=$1 p=$2; shift 2; api_guard_path "$p"; curl -sk -X "$m" -H "$AUTH" "$PVE/api2/json$p" "$@"; }
jqd() { python3 -c "import json,sys; d=json.load(sys.stdin); $1"; }

wait_task() {
  local upid=$1 t0
  t0=$(now)
  while :; do
    local st
    st=$(api GET "/nodes/$NODE/tasks/$upid/status" | jqd 'x=d["data"]; print(x["status"], x.get("exitstatus",""))')
    case "$st" in
      "stopped OK") printf '%.1f' "$(echo "$(now) - $t0" | bc)"; return 0 ;;
      stopped*) log "task failed: $st"; api GET "/nodes/$NODE/tasks/$upid/log" | jqd 'print("\n".join(l["t"] for l in d["data"]))' | tail -20; return 1 ;;
    esac
    sleep 1
  done
}

read -r -d '' PICK_IPV4 <<'PY' || true
import json,sys
SKIP = ("lo","docker","br-","veth","tailscale","cni","flannel","kube","virbr")
try:
    d = json.load(sys.stdin)["data"]["result"]
except Exception:
    sys.exit(1)
for i in d:
    if str(i.get("name","")).startswith(SKIP):
        continue
    for a in i.get("ip-addresses",[]) or []:
        ip = a.get("ip-address","")
        if a.get("ip-address-type") == "ipv4" and not ip.startswith(("127.","169.254.")):
            print(ip); sys.exit(0)
sys.exit(1)
PY

# A cold boot re-runs DHCP and the box can come back on a different address.
# Never trust the configured IP after a reboot -- ask the qemu guest agent.
resolve_box() {
  [[ "${BOX_DISCOVER:-1}" == 1 ]] || return 0
  local user ip
  user=${BOX%%@*}
  ip=$(api GET "/nodes/$NODE/qemu/$VMID/agent/network-get-interfaces" 2>/dev/null \
         | python3 -c "$PICK_IPV4" 2>/dev/null) || return 0
  [[ -n "$ip" ]] || return 0
  if [[ "$BOX" != "$user@$ip" ]]; then
    log "box address moved: $BOX -> $user@$ip (via guest agent)"
    set_box "$user@$ip"
  fi
  return 0
}

gfield() { sed -n "s/.*$2=\\([A-Za-z0-9_-]*\\).*/\\1/p" <<<"$1"; }

gate_ok() {
  local g=$1 hb db pb pr cx ds
  [[ "$g" == ssh_down ]] && return 1
  hb=$(gfield "$g" helm_bad)
  db=$(gfield "$g" deploys)
  pb=$(gfield "$g" pods_bad_phase)
  pr=$(gfield "$g" pods_not_ready)
  ds=$(gfield "$g" ds_bad)
  cx=$(gfield "$g" ctx)
  [[ "$hb" == 0 && "$pb" == 0 && "$pr" == 0 && "$cx" == "${EXPECT_CTX:-andromeda}" ]] || return 1
  [[ "$ds" == 0 ]] || return 1
  [[ -n "$db" && "$db" -ge "${EXPECT_DEPLOYS:-13}" ]] || return 1
  return 0
}

read -r -d '' REMOTE_GATE <<'RG' || true
h=$(helm list -A --no-headers 2>/dev/null | grep -vc deployed)
d=$(kubectl get deploy -n andromeda-system --no-headers 2>/dev/null | wc -l)
p=$(kubectl get pods -A --field-selector=status.phase!=Running,status.phase!=Succeeded --no-headers 2>/dev/null | wc -l)
nr=$(kubectl get pods -A --no-headers 2>/dev/null | awk '{split($3,a,"/"); if (a[1]!=a[2] && $4!="Completed" && $4!="Succeeded") n++} END{print n+0}')
rs=$(kubectl get pods -A --no-headers 2>/dev/null | awk '{s+=$5} END{print s+0}')
c=$(kubectl config current-context 2>/dev/null)

# A pod can be Running and Ready while its datastore refuses connections.
# Probe the wire, not the pod phase.
ds=0
probe() {
  kubectl -n "$1" get sts "$2" >/dev/null 2>&1 || return 0
  kubectl -n "$1" exec "sts/$2" -- "${@:3}" >/dev/null 2>&1 || ds=$((ds+1))
}
if [ "$c" = "andromeda" ] && [ "$nr" = 0 ]; then
  probe andromeda-database postgresql  pg_isready -q -U postgres -h 127.0.0.1
  probe andromeda-database timescaledb pg_isready -q -U postgres -h 127.0.0.1
  probe andromeda-database redis       redis-cli ping
  probe andromeda-neo4j    neo4j-1     bash -c 'exec 3<>/dev/tcp/127.0.0.1/7687'
fi
echo "helm_bad=$h deploys=$d pods_bad_phase=$p pods_not_ready=$nr ds_bad=$ds restarts=$rs ctx=$c"
RG

# A cold boot starts the docker containers in restart-policy order. If any of them
# takes 192.168.49.2 before minikube does, `minikube start` dies with
# "Address already in use" and the node container stays Exited(255) -- an empty
# cluster behind a box that answers SSH. Free the address, then start the node.
read -r -d '' REMOTE_ENSURE_CLUSTER <<'REC' || true
if kubectl get nodes >/dev/null 2>&1; then echo "cluster_up"; exit 0; fi
if ! docker network inspect andromeda >/dev/null 2>&1; then echo "no_andromeda_network"; fi
squatter=$(docker network inspect andromeda \
  --format '{{range .Containers}}{{.Name}} {{.IPv4Address}}{{"\n"}}{{end}}' 2>/dev/null \
  | awk '$2 ~ /^192\.168\.49\.2\// && $1 != "andromeda" {print $1}')
for c in $squatter; do
  # DNSNames carries the aliases other containers resolve this one by
  # (registry.local). Reconnecting without them breaks every image pull, so
  # capture them first and hand them back.
  aliases=$(docker inspect "$c" \
    --format '{{range .NetworkSettings.Networks.andromeda.DNSNames}}{{println .}}{{end}}' 2>/dev/null \
    | grep -v "^$c$" | grep -vE "^[0-9a-f]{12}$")
  args=""
  for a in $aliases; do args="$args --alias $a"; done
  docker network disconnect andromeda "$c" >/dev/null 2>&1 || true
  # shellcheck disable=SC2086
  docker network connect --ip "${RELOCATE_IP:-192.168.49.10}" $args andromeda "$c" >/dev/null 2>&1 || \
    docker network connect $args andromeda "$c" >/dev/null 2>&1 || true
  echo "freed 192.168.49.2 from container: $c (aliases kept:$(echo $aliases | tr '\n' ' '))"
done
if minikube start -p andromeda >/tmp/ensure_cluster.log 2>&1; then
  echo "cluster_started"
else
  echo "cluster_start_failed"; tail -6 /tmp/ensure_cluster.log
  exit 1
fi
REC

gate() {
  "${SSH[@]}" bash -s <<<"$REMOTE_GATE" 2>/dev/null || echo "ssh_down"
}

ensure_cluster() {
  log "-- cluster not up behind a reachable box; running ensure_cluster --"
  "${SSH[@]}" bash -s <<<"$REMOTE_ENSURE_CLUSTER" 2>&1 | sed "s/^/[$(ts)]   /" | tee -a "$LOG"
}

# Hand a box over only when the gate has been clean for N consecutive samples with a
# flat restart count AND the settle floor has elapsed. A single sample is a hollow green.
wait_gate() {
  local t0=$1 label=$2 tmo=${3:-900}
  local passes=${GATE_PASSES:-4} settle=${GATE_SETTLE:-120} iv=${GATE_INTERVAL:-10}
  local streak=0 rs_prev="" ensured=0 nossh=0
  FIRST_SSH="" FIRST_PASS="" GATE_PASS=""
  local g el rs last=""
  while :; do
    g=$(gate)
    el=$(printf '%.1f' "$(echo "$(now) - $t0" | bc)")
    if [[ "$g" == ssh_down ]]; then
      streak=0; rs_prev=""
      nossh=$((nossh+1))
      (( nossh % 10 == 0 )) && resolve_box
    else
      [[ -z "$FIRST_SSH" ]] && { FIRST_SSH=$el; log "T+${el}s first ssh ($label)"; }
      if [[ "$(gfield "$g" ctx)" != "${EXPECT_CTX:-andromeda}" && $ensured == 0 ]]; then
        ensured=1; ensure_cluster || true; last=""; continue
      fi
      rs=$(gfield "$g" restarts)
      if gate_ok "$g" && [[ -n "$rs" && "$rs" == "$rs_prev" ]]; then
        streak=$((streak+1))
      elif gate_ok "$g"; then
        streak=1
        [[ -z "$FIRST_PASS" ]] && { FIRST_PASS=$el; log "T+${el}s first clean sample"; }
      else
        streak=0
      fi
      rs_prev=$rs
    fi
    [[ "$g" != "$last" ]] && log "T+${el}s $g  streak=$streak"
    last=$g
    if (( streak >= passes )) && (( ${el%.*} >= settle )); then
      GATE_PASS=$el
      log "T+${el}s GATE PASS ($label): $passes consecutive clean samples, restarts flat at $rs_prev, settle floor ${settle}s met"
      return 0
    fi
    if (( ${el%.*} > tmo )); then
      log "GATE TIMEOUT ($label) after ${el}s; last=$g streak=$streak"
      "${SSH[@]}" 'kubectl get pods -A --no-headers | grep -vE "Running|Completed"' 2>/dev/null | tee -a "$LOG"
      return 1
    fi
    if [[ -n "$FIRST_PASS" ]]; then sleep "$iv"; else sleep 2; fi
  done
}

cmd_probe() {
  log "== probe =="
  api GET /version | python3 -c '
import json,sys
d=json.load(sys.stdin)["data"]; print("  pve", d.get("version"), d.get("release"))
' | tee -a "$LOG"
  api GET /nodes | python3 -c '
import json,sys
for n in json.load(sys.stdin)["data"]:
    print("  node {:12} status={} cpu={:.0%} mem={:.1f}/{:.1f}G".format(n["node"], n.get("status"), n.get("cpu",0), n.get("mem",0)/2**30, n.get("maxmem",1)/2**30))
' | tee -a "$LOG"
  log "-- storage $STORAGE (the ONLY storage this tool will touch) --"
  api GET "/nodes/$NODE/storage/$STORAGE/status" | python3 -c '
import json,sys
d=json.load(sys.stdin)["data"]
print("  type={} content={} active={}".format(d.get("type"), d.get("content"), d.get("active")))
print("  total={:.1f}G used={:.1f}G avail={:.1f}G".format(d.get("total",0)/2**30, d.get("used",0)/2**30, d.get("avail",0)/2**30))
print("  VMSTATE-CAPABLE:", "YES" if "images" in str(d.get("content","")) else "NO -- images content type missing")
' | tee -a "$LOG"
  log "-- vm $VMID --"
  api GET "/nodes/$NODE/qemu/$VMID/config" | python3 -c '
import json,sys
d=json.load(sys.stdin)["data"]
print("  name={} cores={} memory={}M agent={} parent={}".format(d.get("name"), d.get("cores"), d.get("memory"), d.get("agent"), d.get("parent","-")))
for k,v in sorted(d.items()):
    if k[:4] in ("scsi","virt","sata","ide0","ide1","ide2","ide3") and "vm-" in str(v):
        print("  disk {}: {}".format(k, v))
' | tee -a "$LOG"
  api GET "/nodes/$NODE/qemu/$VMID/status/current" | python3 -c '
import json,sys
d=json.load(sys.stdin)["data"]
print("  status={} qmpstatus={} uptime={}s".format(d.get("status"), d.get("qmpstatus"), d.get("uptime")))
' | tee -a "$LOG"
  log "-- snapshots + volumes --"
  cmd_list
  log "-- live health gate --"
  log "  $(gate)"
}

cmd_list() {
  api GET "/nodes/$NODE/qemu/$VMID/snapshot" | python3 -c '
import json,sys,datetime
for x in json.load(sys.stdin)["data"]:
    t = datetime.datetime.utcfromtimestamp(x["snaptime"]).strftime("%Y-%m-%dT%H:%M:%SZ") if x.get("snaptime") else "-"
    print("  snap {:14} vmstate={} at={} parent={}".format(x["name"], x.get("vmstate",0), t, x.get("parent","-")))
' | tee -a "$LOG"
  api GET "/nodes/$NODE/storage/$STORAGE/content" --data-urlencode "vmid=$VMID" -G | python3 -c '
import json,sys
for v in json.load(sys.stdin)["data"]:
    print("  vol  {:44} {:7.2f}G".format(v["volid"], v["size"]/2**30))
' | tee -a "$LOG"
}

cmd_snapshot() {
  local name=$1 vmstate=${2:-1}
  log "== snapshot $name vmstate=$vmstate on vm $VMID =="
  log "quiescence: $(gate)"
  "${SSH[@]}" 'date -u +%s > ~/SNAPSHOT_TAKEN_AT; sync' 
  local t0 upid dur
  t0=$(now)
  upid=$(api POST "/nodes/$NODE/qemu/$VMID/snapshot" --data-urlencode "snapname=$name" --data-urlencode "vmstate=$vmstate" --data-urlencode "description=phase0 $(date -u +%FT%TZ)" | jqd 'print(d["data"])')
  log "task $upid"
  dur=$(wait_task "$upid") || exit 1
  log "snapshot $name done in ${dur}s"
  log "ssh after snapshot: $(gate)"
  cmd_list
}

cmd_dirty() {
  log "== dirtying vm $VMID =="
  "${SSH[@]}" 'echo "$(date -u) dirty" > ~/PHASE0_DIRTY; docker tag knot:0.9.0 knot:phase0-dirty; kubectl -n andromeda-system annotate deploy knot phase0/dirty="$(date -u +%s)" --overwrite >/dev/null; echo "marker: $(cat ~/PHASE0_DIRTY); tag: $(docker images --format "{{.Repository}}:{{.Tag}}" | grep -c phase0-dirty); anno: $(kubectl -n andromeda-system get deploy knot -o jsonpath="{.metadata.annotations.phase0/dirty}")"' | tee -a "$LOG"
}

cmd_rollback() {
  local name=$1 start=${2:-1}
  log "== rollback vm $VMID to $name (start=$start) =="
  local snap_age=""
  snap_age=$("${SSH[@]}" 'cat ~/SNAPSHOT_TAKEN_AT 2>/dev/null' || true)
  log "pre-rollback: $(gate)"
  local t0 upid dur
  t0=$(now)
  upid=$(api POST "/nodes/$NODE/qemu/$VMID/snapshot/$name/rollback" --data-urlencode "start=$start" | jqd 'print(d["data"])')
  log "task $upid"
  dur=$(wait_task "$upid") || exit 1
  log "T+${dur}s rollback task done; vm status: $(api GET "/nodes/$NODE/qemu/$VMID/status/current" | jqd 'print(d["data"]["status"])')"
  resolve_box
  if [[ -n "$snap_age" ]]; then log "snapshot was taken $(( $(date -u +%s) - snap_age ))s ago (expected guest clock jump)"; fi
  wait_gate "$t0" rollback "${GATE_TIMEOUT:-900}" || exit 1
  "${SSH[@]}" 'echo "guest_clock=$(date -u +%s) host_boot=$(uptime -s) marker_present=$([ -f ~/PHASE0_DIRTY ] && echo yes || echo no) dirty_tag=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep -c phase0-dirty) anno=$(kubectl -n andromeda-system get deploy knot -o jsonpath="{.metadata.annotations.phase0/dirty}" 2>/dev/null)"' 2>/dev/null | tr '\n' ' ' | sed "s/^/[$(ts)] dirt-check /" | tee -a "$LOG"; echo
  log "RESULT rollback=${dur}s first_ssh=${FIRST_SSH}s first_clean=${FIRST_PASS}s gate_pass=${GATE_PASS}s"
  log "post-gate clock: $("${SSH[@]}" 'timedatectl show -p NTPSynchronized -p NTP --value | tr "\n" " "; echo "guest=$(date -u +%s) mac=$(date -u +%s)"' 2>/dev/null)"
  cmd_storm
}

cmd_coldsnap() {
  local name=${1:-warm-cold}
  log "== cold snapshot $name on vm $VMID (graceful shutdown -> disk-only snap -> start) =="
  local g; g=$(gate)
  gate_ok "$g" || { log "REFUSED: stack not healthy before shutdown: $g"; exit 1; }
  log "pre: $g"
  "${SSH[@]}" 'sync' 2>/dev/null || true
  local t0 upid dur
  t0=$(now)
  log "-- graceful shutdown --"
  upid=$(api POST "/nodes/$NODE/qemu/$VMID/status/shutdown" --data-urlencode "timeout=300" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"])')
  dur=$(wait_task "$upid") || { log "shutdown task failed"; exit 1; }
  log "shutdown in ${dur}s; status=$(api GET "/nodes/$NODE/qemu/$VMID/status/current" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["status"])')"
  log "-- disk-only snapshot --"
  upid=$(api POST "/nodes/$NODE/qemu/$VMID/snapshot" --data-urlencode "snapname=$name" --data-urlencode "vmstate=0" --data-urlencode "description=phase0 cold $(date -u +%FT%TZ)" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"])')
  dur=$(wait_task "$upid") || { log "snapshot failed; STARTING VM BACK UP"; api POST "/nodes/$NODE/qemu/$VMID/status/start" >/dev/null; exit 1; }
  log "cold snapshot taken in ${dur}s"
  log "-- start --"
  local tb; tb=$(now)
  upid=$(api POST "/nodes/$NODE/qemu/$VMID/status/start" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"])')
  wait_task "$upid" >/dev/null || true
  resolve_box
  if wait_gate "$tb" cold "${GATE_TIMEOUT:-1800}"; then
    log "RESULT cold_boot_first_ssh=${FIRST_SSH}s cold_first_clean=${FIRST_PASS}s cold_gate_pass=${GATE_PASS}s"
  else
    log "RESULT cold path FAILED -- do not use warm-cold as a live-rollback fallback"
  fi
  log "total cold cycle: $(printf '%.1f' "$(echo "$(now) - $t0" | bc)")s"
  cmd_list
}

cmd_ensure() {
  log "== ensure_cluster on $BOX =="
  resolve_box 2>/dev/null || true
  ensure_cluster
  log "gate: $(gate)"
}

cmd_storm() {
  log "== timer-storm indicators (last 5m) =="
  "${SSH[@]}" 'for d in knot temporal-history batch-compute ada-orchestrator; do kubectl -n andromeda-system get deploy $d >/dev/null 2>&1 || continue; n=$(kubectl -n andromeda-system logs deploy/$d --since=5m 2>/dev/null | wc -l); e=$(kubectl -n andromeda-system logs deploy/$d --since=5m 2>/dev/null | grep -ciE "error|timeout|lease|expired|rebalanc" ); echo "  $d lines=$n err_like=$e"; done; echo "  restarts: $(kubectl get pods -A --no-headers | awk "{s+=\$5} END {print s+0}")"; echo "  node: $(kubectl get nodes --no-headers)"' | tee -a "$LOG"
}

read -r -d '' REMOTE_SCALEFLOOR <<'RS' || true
set -e
ready_bad() { kubectl get pods -A --no-headers | awk '{split($3,a,"/"); if (a[1]!=a[2] && $4!="Completed" && $4!="Succeeded") n++} END{print n+0}'; }
phase_bad() { kubectl get pods -A --field-selector=status.phase!=Running,status.phase!=Succeeded --no-headers | wc -l; }
mapfile -t STS < <(kubectl get sts -A --no-headers | awk '{print $1" "$2" "$3}' | sed 's|/.*||')
printf 'statefulsets:'; for l in "${STS[@]}"; do printf ' %s' "$l"; done; echo
t0=$(date +%s)
for l in "${STS[@]}"; do set -- $l; kubectl -n "$1" scale sts "$2" --replicas=0 >/dev/null; done
while :; do
  n=0
  for l in "${STS[@]}"; do set -- $l; c=$(kubectl -n "$1" get pods -l "app.kubernetes.io/name=$2" --no-headers 2>/dev/null | wc -l); n=$((n+c)); done
  tot=$(kubectl get pods -A --no-headers | grep -cE 'postgresql-|timescaledb-|redis-|neo4j-1-|kafka-cluster-(broker|controller)' || true)
  [ "$tot" = 0 ] && break
  [ $(( $(date +%s)-t0 )) -gt 600 ] && { echo "SCALEDOWN TIMEOUT, still: $tot"; break; }
  sleep 2
done
echo "scaled_down_in=$(( $(date +%s)-t0 ))s"
t1=$(date +%s)
for l in "${STS[@]}"; do set -- $l; kubectl -n "$1" scale sts "$2" --replicas="$3" >/dev/null; done
while [ "$(phase_bad)" != 0 ] || [ "$(ready_bad)" != 0 ]; do
  [ $(( $(date +%s)-t1 )) -gt 1200 ] && { echo "SCALEUP TIMEOUT"; kubectl get pods -A --no-headers | grep -vE 'Running|Completed'; break; }
  sleep 3
done
echo "RESULT scale_up_to_ready=$(( $(date +%s)-t1 ))s total_floor=$(( $(date +%s)-t0 ))s"
RS

cmd_scalefloor() {
  log "== statefulset scale floor (PVC-swap comparison) on vm $VMID =="
  log "pre: $(gate)"
  "${SSH[@]}" bash -s <<<"$REMOTE_SCALEFLOOR" 2>&1 | tee -a "$LOG"
  log "post: $(gate)"
}

case "${1:-}" in
  probe) guard; need_token; resolve_box; cmd_probe ;;
  list) guard; need_token; cmd_list ;;
  snapshot) guard; need_token; resolve_box; cmd_snapshot "${2:?name}" "${3:-1}" ;;
  rollback) guard; need_token; resolve_box; cmd_rollback "${2:?name}" "${3:-1}" ;;
  dirty) guard; need_token; resolve_box; cmd_dirty ;;
  coldsnap) guard; need_token; resolve_box; cmd_coldsnap "${2:-warm-cold}" ;;
  ensure) guard; need_token; cmd_ensure ;;
  gate) guard; need_token; resolve_box; log "$(gate)" ;;
  waitgate) guard; need_token; resolve_box; t0=$(now); log "== waitgate vm $VMID box $BOX =="; wait_gate "$t0" waitgate "${GATE_TIMEOUT:-900}" ;;
  stepclock) guard; need_token; resolve_box; log "clock before: guest=$("${SSH[@]}" 'date -u +%s') mac=$(date -u +%s)"; "${SSH[@]}" 'sudo systemctl restart systemd-timesyncd; sleep 3'; log "clock after:  guest=$("${SSH[@]}" 'date -u +%s') mac=$(date -u +%s)" ;;
  storm) guard; need_token; resolve_box; cmd_storm ;;
  scalefloor) guard; need_token; resolve_box; cmd_scalefloor ;;
  -h|--help|help|"") usage; exit 0 ;;
  *) echo "unknown command: $1"; echo; usage; exit 2 ;;
esac
