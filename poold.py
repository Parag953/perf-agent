import json
import os
import sys
import threading
import time
import tomllib
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from gate import GateConfig
from prepare import Prepare
from store import Store


def log(msg: str) -> None:
    sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()


class Poold:
    def __init__(self, cfg: dict, pve, remote, gate_cfg: GateConfig):
        self.cfg, self.pve, self.remote, self.gate_cfg = cfg, pve, remote, gate_cfg
        self.store = Store(cfg.get("db", "pool.db"))
        self.trace_dir = cfg.get("trace_dir", "traces")
        os.makedirs(self.trace_dir, exist_ok=True)
        self.lan_prefix = cfg.get("lan_prefix", "10.0.0.")
        self.ts_prefix = cfg.get("tailscale_prefix", "100.")
        for b in cfg["boxes"]:
            self.store.ensure_box(b["name"], b["vmid"], b["ssh_user"], b.get("ip"), b.get("snapshot", "warm-live"))
        self.store.recover_on_startup()
        self.progress = {}
        self.procs = {}
        self.vm_cache = {}
        self.pause_dispatch = False
        self._stop = threading.Event()
        self._threads = []
        host, port = cfg.get("listen", "0.0.0.0:7070").rsplit(":", 1)
        self.server = ThreadingHTTPServer((host, int(port)), _handler(self))
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]

    def refresh_addresses(self):
        for b in self.store.boxes():
            try:
                ip = self.pve.guest_ipv4(b["vmid"], self.lan_prefix)
                if ip:
                    self.store.set_ip(b["name"], ip)
                ts = self.pve.guest_ipv4(b["vmid"], self.ts_prefix)
                if ts:
                    self.store.set_ts_ip(b["name"], ts)
            except Exception as e:
                log(f"{b['name']}: address refresh failed: {e}")

    def start(self):
        self.refresh_addresses()
        for fn in (self._dispatch_loop, self._sweep_loop, self.server.serve_forever):
            t = threading.Thread(target=fn, daemon=True)
            t.start()
            self._threads.append(t)
        log(f"poold listening on :{self.port} boxes={[b['name'] for b in self.store.boxes()]}")

    def stop(self):
        self._stop.set()
        self.server.shutdown()
        self.server.server_close()

    def _dispatch_loop(self):
        iv = self.cfg.get("dispatch_interval_s", 2.0)
        while not self._stop.wait(iv):
            if self.pause_dispatch:
                continue
            try:
                d = self.store.dispatch()
                if d and d.get("direct"):
                    log(f"{d['box']}: ready box handed to run {d['run_id']} without a rollback")
                    threading.Thread(target=self._run_only, args=(d["box"], d["run_id"]), daemon=True).start()
                elif d:
                    threading.Thread(target=self._prepare_and_run, args=(d["box"], d["run_id"]), daemon=True).start()
                elif self.cfg.get("auto_prepare", False):
                    name = self.store.pick_dirty_idle()
                    if name:
                        d = self.store.request_reset(name)
                        threading.Thread(target=self._prepare_and_run, args=(d["box"], None), daemon=True).start()
            except Exception:
                log(traceback.format_exc())

    def _sweep_loop(self):
        iv = self.cfg.get("sweep_interval_s", 30.0)
        grace = self.cfg.get("lease", {}).get("heartbeat_grace_s", 90)
        while not self._stop.wait(iv):
            try:
                for rid in self.store.sweep(heartbeat_grace_s=grace):
                    log(f"sweeper: lease for run {rid} expired")
                    self._kill(rid)
            except Exception:
                log(traceback.format_exc())

    def _kill(self, run_id):
        p = self.procs.pop(run_id, None)
        if p:
            from remote import kill_task
            kill_task(p)

    def _prepare_and_run(self, box_name: str, run_id):
        try:
            self._prepare_and_run_inner(box_name, run_id)
        except Exception as e:
            log(f"{box_name}: unexpected error in prepare/run: {traceback.format_exc()}")
            self.progress.pop(box_name, None)
            try:
                self.store.prepare_failed(box_name, f"internal: {type(e).__name__}: {e}")
            except Exception:
                log(traceback.format_exc())

    def _prepare_and_run_inner(self, box_name: str, run_id):
        b = self.store.box(box_name)
        user, vmid, snap = b["ssh_user"], b["vmid"], b["snapshot"]
        t0 = time.time()

        def on_step(step, attempt):
            self.progress[box_name] = {"step": step, "attempt": attempt, "started_at": t0}
            log(f"{box_name}: prepare step={step} attempt={attempt} t+{time.time()-t0:.0f}s")

        def rollback():
            upid = self.pve.rollback(vmid, snap, start=True)
            self.pve.wait_task(upid)

        def resolve_ip():
            ip = self.pve.guest_ipv4(vmid, self.lan_prefix)
            if ip:
                self.store.set_ip(box_name, ip)
                ts = self.pve.guest_ipv4(vmid, self.ts_prefix)
                if ts:
                    self.store.set_ts_ip(box_name, ts)
            return ip

        def gate_sample(ip):
            line = self.remote.gate_sample(user, ip)
            self.progress.setdefault(box_name, {})["gate"] = {"sample": line, "sampled_at": time.time()}
            return line

        prep = Prepare(rollback=rollback, resolve_ip=resolve_ip,
                       ssh_ok=lambda ip: self.remote.ssh_ok(user, ip),
                       step_clock=lambda ip: self.remote.step_clock(user, ip),
                       gate_sample=gate_sample, gate_cfg=self.gate_cfg,
                       retries=self.cfg.get("prepare_retries", 1), on_step=on_step, log=log)
        res = prep.run()
        self.progress.pop(box_name, None)
        if not res.ok:
            state = self.store.prepare_failed(box_name, res.error)
            log(f"{box_name}: prepare FAILED -> {state}: {res.error}")
            return
        self.store.db.execute("UPDATE boxes SET last_error=NULL WHERE name=?", (box_name,))
        log(f"{box_name}: prepare ok ip={res.ip} " + " ".join(f"{k}={v:.1f}s" for k, v in res.timings.items()))
        self.last_prepare = getattr(self, "last_prepare", {})
        self.last_prepare[box_name] = {"at": time.time(), "ip": res.ip, **{k: round(v, 1) for k, v in res.timings.items()}}
        if run_id is None:
            self.store.mark_leased(box_name, None, ttl_s=0)
            return
        run = self.store.run(run_id)
        self.store.mark_leased(box_name, run_id, ttl_s=run["ttl_s"])
        self._execute(box_name, run_id, res.ip, user)

    def _run_only(self, box_name: str, run_id: str):
        try:
            b = self.store.box(box_name)
            self._execute(box_name, run_id, b["ip"], b["ssh_user"])
        except Exception as e:
            log(f"{box_name}: unexpected error in run: {traceback.format_exc()}")
            if self.store.run(run_id)["state"] == "running":
                self.store.end_run(run_id, exit_code=255, error=f"internal: {type(e).__name__}: {e}")

    def _execute(self, box_name: str, run_id: str, ip: str, user: str):
        run = self.store.run(run_id)
        log_path = os.path.join(self.trace_dir, f"{run_id}.log")
        with open(log_path, "a") as f:
            f.write(f"# run {run_id} on {box_name} ({ip}) task: {run['task']}\n")
        try:
            rc = self.remote.run_task(user, ip, run["task"], log_path,
                                      on_start=lambda p: self.procs.__setitem__(run_id, p))
        except Exception as e:
            rc = 255
            with open(log_path, "a") as f:
                f.write(f"# runner error: {e}\n")
        self.procs.pop(run_id, None)
        if self.store.run(run_id)["state"] == "running":
            self.store.end_run(run_id, exit_code=rc, error=None if rc == 0 else f"exit {rc}")
        log(f"{box_name}: run {run_id} exited {rc} -> box dirty")
        self._callback(run_id)
        if run["reset_on_release"]:
            try:
                d = self.store.request_reset(box_name)
                threading.Thread(target=self._prepare_and_run, args=(d["box"], None), daemon=True).start()
            except ValueError as e:
                log(f"{box_name}: reset_on_release skipped: {e}")

    def _callback(self, run_id):
        r = self.store.run(run_id)
        if not r or not r.get("callback_url"):
            return
        try:
            import urllib.request
            req = urllib.request.Request(r["callback_url"], data=json.dumps(self.run_view(run_id)).encode(),
                                         headers={"Content-Type": "application/json"}, method="POST")
            urllib.request.urlopen(req, timeout=10).read()
        except Exception as e:
            log(f"callback for {run_id} failed: {e}")

    def vm_state(self, vmid):
        c = self.vm_cache.get(vmid)
        if c and time.time() - c[0] < 5:
            return c[1]
        try:
            s = self.pve.status(vmid)
            v = {"status": s.get("status"), "uptime_s": s.get("uptime"), "mem_bytes": s.get("mem")}
        except Exception as e:
            v = {"status": "unknown", "error": str(e)}
        self.vm_cache[vmid] = (time.time(), v)
        return v

    def box_view(self, b: dict) -> dict:
        v = {k: b.get(k) for k in ("name", "vmid", "ssh_user", "ip", "ts_ip", "state", "since", "lease_id", "run_id",
                                    "snapshot", "fail_count", "last_error", "expires_at", "last_heartbeat")}
        v["vm"] = self.vm_state(b["vmid"])
        p = self.progress.get(b["name"])
        if p and b["state"] == "preparing":
            v["prepare"] = {"step": p.get("step"), "attempt": p.get("attempt"), "started_at": p.get("started_at"),
                            "elapsed_s": round(time.time() - p.get("started_at", time.time()), 1)}
            if "gate" in p:
                v["gate"] = p["gate"]
        v["last_prepare"] = getattr(self, "last_prepare", {}).get(b["name"])
        return v

    CONTRACT_STATE = {"leased": "allocated", "ready": "ready", "quarantined": "degraded",
                      "dirty": "free", "preparing": "free"}

    def vm_id(self, b: dict) -> str:
        return f"VM{b['vmid']}"

    def box_by_vm_id(self, vm_id: str):
        for b in self.store.boxes():
            if vm_id in (self.vm_id(b), b["name"]):
                return b
        return None

    def contract_vm(self, b: dict) -> dict:
        v = {"vm_id": self.vm_id(b), "name": b["name"], "state": self.CONTRACT_STATE.get(b["state"], b["state"]),
             "poold_state": b["state"], "host": b.get("ip"),
             "ssh_target": f"{b['ssh_user']}@{b['ip']}" if b.get("ip") else None,
             "tailscale_host": b.get("ts_ip"),
             "tailscale_target": f"{b['ssh_user']}@{b['ts_ip']}" if b.get("ts_ip") else None,
             "since": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(b["since"])) if b.get("since") else None}
        p = self.progress.get(b["name"])
        if p and b["state"] == "preparing":
            v["step"] = p.get("step")
        if b["state"] == "quarantined":
            v["error"] = b.get("last_error")
        return v

    def lease_for_orchestrator(self):
        name = self.store.pick_ready()
        if not name:
            return None
        ttl = self.cfg.get("lease", {}).get("external_ttl_s", 7200)
        try:
            lease_id = self.store.lease_external(name, ttl_s=ttl)
        except ValueError:
            return None
        b = self.store.box(name)
        log(f"{name}: leased to orchestrator lease={lease_id} host={b['ip']}")
        return {"vm_id": self.vm_id(b), "host": b["ip"], "ssh_target": f"{b['ssh_user']}@{b['ip']}",
                "tailscale_host": b.get("ts_ip"),
                "tailscale_target": f"{b['ssh_user']}@{b['ts_ip']}" if b.get("ts_ip") else None,
                "state": "allocated", "lease_id": lease_id, "expires_at": b["expires_at"]}

    def run_view(self, run_id):
        return self.store.run(run_id)

    def status_view(self):
        return {"ts": time.time(), "boxes": [self.box_view(b) for b in self.store.boxes()],
                "runs": self.store.runs(limit=30), "queue": self.store.queue()}


def _handler(p: Poold):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _json(self, code, obj):
            body = json.dumps(obj, default=str).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(n) if n else b""
            if not raw:
                return {}
            return json.loads(raw.decode())

        def do_GET(self):
            u = urlparse(self.path)
            parts = [x for x in u.path.split("/") if x]
            try:
                if parts == ["status"]:
                    return self._json(200, p.status_view())
                if parts == ["poold", "status"]:
                    return self._json(200, {"vms": [p.contract_vm(b) for b in p.store.boxes()]})
                if parts == ["boxes"]:
                    return self._json(200, [p.box_view(b) for b in p.store.boxes()])
                if len(parts) == 2 and parts[0] == "boxes":
                    b = p.store.box(parts[1])
                    return self._json(200, p.box_view(b)) if b else self._json(404, {"error": "no such box"})
                if parts == ["runs"]:
                    return self._json(200, p.store.runs())
                if len(parts) == 2 and parts[0] == "runs":
                    r = p.store.run(parts[1])
                    return self._json(200, r) if r else self._json(404, {"error": "no such run"})
                if len(parts) == 3 and parts[0] == "runs" and parts[2] == "trace":
                    return self._trace(parts[1], parse_qs(u.query))
                return self._json(404, {"error": "not found"})
            except Exception as e:
                log(traceback.format_exc())
                return self._json(500, {"error": str(e)})

        def _trace(self, run_id, q):
            r = p.store.run(run_id)
            if not r:
                return self._json(404, {"error": "no such run"})
            follow = q.get("follow", ["1"])[0] != "0"
            path = os.path.join(p.trace_dir, f"{run_id}.log")
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            pos = 0
            while True:
                if os.path.exists(path):
                    with open(path, "rb") as f:
                        f.seek(pos)
                        chunk = f.read()
                        pos = f.tell()
                    for line in chunk.decode(errors="replace").splitlines():
                        self.wfile.write(f"data: {line}\n\n".encode())
                    self.wfile.flush()
                state = p.store.run(run_id)["state"]
                if not follow or state in ("done", "failed", "abandoned"):
                    if follow:
                        self.wfile.write(f"event: end\ndata: {state}\n\n".encode())
                        self.wfile.flush()
                    return
                time.sleep(0.5)

        def do_POST(self):
            parts = [x for x in urlparse(self.path).path.split("/") if x]
            try:
                body = self._body()
            except Exception:
                return self._json(400, {"error": "bad json"})
            try:
                if parts == ["poold", "lease"]:
                    lease = p.lease_for_orchestrator()
                    return self._json(200, lease) if lease else self._json(409, {"error": "no_capacity"})
                if parts == ["poold", "release"]:
                    b = p.box_by_vm_id(str(body.get("vm_id", "")))
                    if not b:
                        return self._json(404, {"error": "no_such_vm"})
                    if not p.store.release(b["name"]):
                        return self._json(409, {"error": "not_allocated"})
                    log(f"{b['name']}: released by orchestrator")
                    return self._json(200, {"ok": True})
                if parts == ["run"]:
                    if not body.get("task"):
                        return self._json(400, {"error": "task required"})
                    r = p.store.enqueue(task=body["task"], owner=body.get("owner"), source=body.get("source"),
                                        ttl_s=int(body.get("ttl_s") or p.cfg.get("lease", {}).get("default_ttl_s", 3600)),
                                        callback_url=body.get("callback_url"),
                                        reset_on_release=bool(body.get("reset_on_release", False)))
                    return self._json(202, r)
                if len(parts) == 3 and parts[0] == "boxes":
                    name, action = parts[1], parts[2]
                    if not p.store.box(name):
                        return self._json(404, {"error": "no such box"})
                    if action == "reset":
                        d = p.store.request_reset(name)
                        threading.Thread(target=p._prepare_and_run, args=(name, None), daemon=True).start()
                        return self._json(202, d)
                    if action == "quarantine":
                        p.store.set_state(name, "quarantined", error="manual")
                        return self._json(200, p.box_view(p.store.box(name)))
                    if action == "unquarantine":
                        p.store.unquarantine(name)
                        return self._json(200, p.box_view(p.store.box(name)))
                if len(parts) == 3 and parts[0] == "leases" and parts[2] == "heartbeat":
                    ok = p.store.heartbeat(parts[1])
                    return self._json(204 if ok else 404, None if ok else {"error": "no such lease"})
                return self._json(404, {"error": "not found"})
            except ValueError as e:
                return self._json(409, {"error": str(e)})
            except Exception as e:
                log(traceback.format_exc())
                return self._json(500, {"error": str(e)})

        def do_DELETE(self):
            parts = [x for x in urlparse(self.path).path.split("/") if x]
            if len(parts) == 2 and parts[0] == "runs":
                try:
                    box = p.store.cancel(parts[1])
                except KeyError:
                    return self._json(404, {"error": "no such run"})
                p._kill(parts[1])
                return self._json(200, {"run_id": parts[1], "box": box})
            return self._json(404, {"error": "not found"})

    return H


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "poold.toml"
    with open(path, "rb") as f:
        cfg = tomllib.load(f)
    from pve import PVE
    from remote import Remote
    token = open(os.path.expanduser(cfg["pve"]["token_file"])).read().strip()
    pve = PVE(cfg["pve"]["url"], token, cfg["pve"]["node"])
    gate_cfg = GateConfig(**cfg.get("gate", {}))
    p = Poold(cfg, pve=pve, remote=Remote(), gate_cfg=gate_cfg)
    p.start()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        p.stop()


if __name__ == "__main__":
    main()
