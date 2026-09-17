import json
import threading
import time
import unittest
import urllib.error
import urllib.request

from gate import GateConfig
from poold import Poold

CLEAN = "helm_bad=0 deploys=13 pods_bad_phase=0 pods_not_ready=0 ds_bad=0 restarts=14 ctx=andromeda"


class FakePVE:
    def __init__(self):
        self.rollbacks = []

    def status(self, vmid):
        return {"status": "running", "uptime": 1234, "mem": 1 << 30}

    def rollback(self, vmid, snapshot, start=True):
        self.rollbacks.append((vmid, snapshot))
        return "UPID:fake"

    def wait_task(self, upid, timeout_s=600.0, poll_s=2.0):
        return {"status": "stopped", "exitstatus": "OK"}

    def guest_ipv4(self, vmid, prefix):
        return "10.0.0.38"


class FakeRemote:
    def __init__(self):
        self.tasks = []
        self.clock_steps = 0

    def ssh_ok(self, user, ip):
        return True

    def step_clock(self, user, ip):
        self.clock_steps += 1

    def gate_sample(self, user, ip):
        return CLEAN

    def run_task(self, user, ip, task, log_path, on_start=None):
        self.tasks.append(task)
        with open(log_path, "a") as f:
            f.write(f"ran: {task}\n")
        return 0 if "fail" not in task else 7


def http(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read().decode() or "null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "null")


def wait_for(pred, timeout=5.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return True
        time.sleep(0.02)
    return False


class Api(unittest.TestCase):
    def setUp(self):
        self.pve, self.remote = FakePVE(), FakeRemote()
        self.tmp = __import__("tempfile").mkdtemp()
        cfg = {
            "listen": "127.0.0.1:0",
            "db": ":memory:",
            "trace_dir": self.tmp,
            "lan_prefix": "10.0.0.",
            "boxes": [{"name": "andro-b", "vmid": 102, "ssh_user": "andro-2", "snapshot": "warm-live"}],
            "gate": {"passes": 2, "interval_s": 0, "settle_s": 0, "timeout_s": 30, "expect_deploys": 13, "expect_ctx": "andromeda"},
            "lease": {"default_ttl_s": 600, "heartbeat_grace_s": 90},
            "dispatch_interval_s": 0.05,
            "sweep_interval_s": 0.05,
        }
        self.p = Poold(cfg, pve=self.pve, remote=self.remote, gate_cfg=GateConfig(**cfg["gate"]))
        self.p.start()
        self.base = f"http://127.0.0.1:{self.p.port}"

    def tearDown(self):
        self.p.stop()

    def box(self):
        return http("GET", f"{self.base}/boxes/andro-b")[1]

    def test_status_lists_boxes_with_live_vm_state(self):
        code, st = http("GET", f"{self.base}/status")
        self.assertEqual(code, 200)
        b = st["boxes"][0]
        self.assertEqual(b["name"], "andro-b")
        self.assertEqual(b["vm"]["status"], "running")
        self.assertIn(b["state"], ("dirty", "free", "preparing"))

    def test_run_goes_queued_preparing_running_done_and_box_returns_dirty(self):
        code, r = http("POST", f"{self.base}/run", {"task": "hostname", "owner": "abhi", "source": "test"})
        self.assertEqual(code, 202)
        rid = r["run_id"]
        self.assertTrue(wait_for(lambda: http("GET", f"{self.base}/runs/{rid}")[1]["state"] == "done"))
        self.assertEqual(self.pve.rollbacks, [(102, "warm-live")])
        self.assertEqual(self.remote.clock_steps, 1)
        self.assertEqual(self.remote.tasks, ["hostname"])
        self.assertEqual(self.box()["state"], "dirty")
        code, trace = http("GET", f"{self.base}/runs/{rid}")
        self.assertEqual(trace["exit_code"], 0)

    def test_failed_task_marks_run_failed(self):
        _, r = http("POST", f"{self.base}/run", {"task": "please fail", "owner": "abhi"})
        rid = r["run_id"]
        self.assertTrue(wait_for(lambda: http("GET", f"{self.base}/runs/{rid}")[1]["state"] == "failed"))
        self.assertEqual(http("GET", f"{self.base}/runs/{rid}")[1]["exit_code"], 7)

    def test_second_run_queues_behind_the_first(self):
        self.p.pause_dispatch = True
        _, r1 = http("POST", f"{self.base}/run", {"task": "a", "owner": "o"})
        _, r2 = http("POST", f"{self.base}/run", {"task": "b", "owner": "o"})
        self.assertEqual((r1["position"], r2["position"]), (1, 2))
        self.assertEqual(http("GET", f"{self.base}/status")[1]["queue"], [r1["run_id"], r2["run_id"]])
        self.p.pause_dispatch = False
        self.assertTrue(wait_for(lambda: http("GET", f"{self.base}/runs/{r2['run_id']}")[1]["state"] == "done", 10))
        self.assertEqual(self.remote.tasks, ["a", "b"])

    def test_reset_on_release_brings_box_back_to_free(self):
        _, r = http("POST", f"{self.base}/run", {"task": "x", "owner": "o", "reset_on_release": True})
        self.assertTrue(wait_for(lambda: self.box()["state"] == "free", 10))
        self.assertEqual(len(self.pve.rollbacks), 2)

    def test_manual_reset_endpoint(self):
        self.assertTrue(wait_for(lambda: self.box()["state"] in ("dirty", "free")))
        code, _ = http("POST", f"{self.base}/boxes/andro-b/reset")
        self.assertEqual(code, 202)
        self.assertTrue(wait_for(lambda: self.box()["state"] == "free", 10))

    def test_trace_endpoint_returns_task_output(self):
        _, r = http("POST", f"{self.base}/run", {"task": "hostname", "owner": "o"})
        rid = r["run_id"]
        self.assertTrue(wait_for(lambda: http("GET", f"{self.base}/runs/{rid}")[1]["state"] == "done"))
        req = urllib.request.Request(f"{self.base}/runs/{rid}/trace?follow=0")
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read().decode()
            self.assertEqual(resp.headers["Content-Type"].split(";")[0], "text/event-stream")
        self.assertIn("ran: hostname", body)

    def test_unknown_routes_404_and_bad_json_400(self):
        self.assertEqual(http("GET", f"{self.base}/nope")[0], 404)
        self.assertEqual(http("POST", f"{self.base}/run", {"owner": "o"})[0], 400)
        self.assertEqual(http("GET", f"{self.base}/runs/zzz")[0], 404)


if __name__ == "__main__":
    unittest.main()
