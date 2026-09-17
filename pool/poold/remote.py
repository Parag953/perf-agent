import os
import signal
import subprocess
from typing import Callable, List, Optional

REMOTE_GATE = r'''
h=$(helm list -A --no-headers 2>/dev/null | grep -vc deployed)
d=$(kubectl get deploy -n andromeda-system --no-headers 2>/dev/null | wc -l)
p=$(kubectl get pods -A --field-selector=status.phase!=Running,status.phase!=Succeeded --no-headers 2>/dev/null | wc -l)
nr=$(kubectl get pods -A --no-headers 2>/dev/null | awk '{split($3,a,"/"); if (a[1]!=a[2] && $4!="Completed" && $4!="Succeeded") n++} END{print n+0}')
rs=$(kubectl get pods -A --no-headers 2>/dev/null | awk '{s+=$5} END{print s+0}')
c=$(kubectl config current-context 2>/dev/null)
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
'''

STEP_CLOCK = "sudo -n systemctl restart systemd-timesyncd && sleep 3 && date -u +%s"


def ssh_argv(user: str, ip: str, command: str, connect_timeout: int = 3) -> List[str]:
    return [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", f"ConnectTimeout={connect_timeout}",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=ERROR",
        "-o", "ServerAliveInterval=15",
        "-o", "ServerAliveCountMax=4",
        f"{user}@{ip}",
        command,
    ]


class Remote:
    def ssh_ok(self, user: str, ip: str) -> bool:
        try:
            r = subprocess.run(ssh_argv(user, ip, "true"), capture_output=True, timeout=10)
            return r.returncode == 0
        except subprocess.TimeoutExpired:
            return False

    def step_clock(self, user: str, ip: str) -> None:
        subprocess.run(ssh_argv(user, ip, STEP_CLOCK), capture_output=True, timeout=30)

    def gate_sample(self, user: str, ip: str) -> str:
        try:
            r = subprocess.run(ssh_argv(user, ip, "bash -s"), input=REMOTE_GATE.encode(),
                               capture_output=True, timeout=60)
        except subprocess.TimeoutExpired:
            return "ssh_down"
        if r.returncode != 0:
            return "ssh_down"
        line = r.stdout.decode(errors="replace").strip().splitlines()
        return line[-1] if line else "ssh_down"

    def run_task(self, user: str, ip: str, task: str, log_path: str,
                 on_start: Optional[Callable[[subprocess.Popen], None]] = None) -> int:
        os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
        with open(log_path, "ab", buffering=0) as log:
            p = subprocess.Popen(ssh_argv(user, ip, f"bash -lc {_shq(task)}", connect_timeout=10),
                                 stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                 start_new_session=True)
            if on_start:
                on_start(p)
            return p.wait()


def kill_task(p: subprocess.Popen) -> None:
    try:
        os.killpg(p.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass


def _shq(s: str) -> str:
    return "'" + s.replace("'", "'\\''") + "'"
