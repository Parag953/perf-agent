import unittest
from gate import GateConfig
from prepare import Prepare, PrepareResult

CLEAN = "helm_bad=0 deploys=13 pods_bad_phase=0 pods_not_ready=0 ds_bad=0 restarts=14 ctx=andromeda"
CFG = GateConfig(passes=2, interval_s=0, settle_s=0, timeout_s=50, expect_deploys=13, expect_ctx="andromeda")


class Fake:
    def __init__(self, gate_lines=None, ip_lines=None, ssh_ok_after=0, rollback_fail=False, gate_bad_until=0.0):
        self.calls = []
        self.t = 0.0
        self.gate_bad_until = gate_bad_until
        self.gate_lines = list(gate_lines or [CLEAN] * 5)
        self.ip_lines = list(ip_lines or ["10.0.0.38"])
        self.ssh_ok_after = ssh_ok_after
        self.ssh_attempts = 0
        self.rollback_fail = rollback_fail

    def clock(self):
        self.t += 1.0
        return self.t

    def sleep(self, s):
        self.t += s

    def rollback(self):
        self.calls.append("rollback")
        if self.rollback_fail:
            raise RuntimeError("qmrollback failed")

    def resolve_ip(self):
        self.calls.append("resolve_ip")
        if len(self.ip_lines) > 1:
            return self.ip_lines.pop(0)
        return self.ip_lines[0] if self.ip_lines else None

    def ssh_ok(self, ip):
        self.calls.append(f"ssh_ok:{ip}")
        self.ssh_attempts += 1
        return self.ssh_attempts > self.ssh_ok_after

    def step_clock(self, ip):
        self.calls.append(f"step_clock:{ip}")

    def gate_sample(self, ip):
        self.calls.append("gate_sample")
        if self.t < self.gate_bad_until:
            return CLEAN.replace("ds_bad=0", "ds_bad=1")
        return self.gate_lines.pop(0) if self.gate_lines else CLEAN


def mk(fake, **kw):
    return Prepare(rollback=fake.rollback, resolve_ip=fake.resolve_ip, ssh_ok=fake.ssh_ok,
                   step_clock=fake.step_clock, gate_sample=fake.gate_sample, gate_cfg=CFG,
                   clock=fake.clock, sleep=fake.sleep, **kw)


class Sequence(unittest.TestCase):
    def test_steps_run_in_fixed_order_and_clock_step_precedes_gate(self):
        f = Fake()
        r = mk(f).run()
        self.assertTrue(r.ok)
        first_gate = f.calls.index("gate_sample")
        self.assertEqual(f.calls[:4], ["rollback", "resolve_ip", "ssh_ok:10.0.0.38", "step_clock:10.0.0.38"])
        self.assertGreater(first_gate, f.calls.index("step_clock:10.0.0.38"))

    def test_result_carries_ip_and_step_timings(self):
        f = Fake()
        r = mk(f).run()
        self.assertEqual(r.ip, "10.0.0.38")
        for k in ("rollback", "resolve_ip", "wait_ssh", "step_clock", "gate"):
            self.assertIn(k, r.timings)

    def test_waits_for_guest_agent_to_report_an_address(self):
        f = Fake(ip_lines=[None, None, "10.0.0.37"])
        r = mk(f).run()
        self.assertTrue(r.ok)
        self.assertEqual(r.ip, "10.0.0.37")

    def test_waits_for_ssh(self):
        f = Fake(ssh_ok_after=3)
        r = mk(f).run()
        self.assertTrue(r.ok)
        self.assertEqual(f.ssh_attempts, 4)

    def test_gate_failure_retries_the_whole_sequence_once(self):
        f = Fake(gate_bad_until=60.0)
        r = mk(f).run()
        self.assertTrue(r.ok)
        self.assertEqual(f.calls.count("rollback"), 2)
        self.assertEqual(r.attempt, 2)

    def test_two_gate_failures_return_not_ok(self):
        f = Fake(gate_bad_until=10_000.0)
        r = mk(f).run()
        self.assertFalse(r.ok)
        self.assertEqual(f.calls.count("rollback"), 2)
        self.assertIn("gate", r.error)

    def test_rollback_exception_counts_as_a_failed_attempt(self):
        f = Fake(rollback_fail=True)
        r = mk(f).run()
        self.assertFalse(r.ok)
        self.assertEqual(f.calls.count("rollback"), 2)
        self.assertIn("qmrollback", r.error)

    def test_progress_callback_sees_each_step(self):
        f = Fake()
        seen = []
        mk(f, on_step=lambda step, attempt: seen.append(step)).run()
        self.assertEqual(seen, ["rollback", "resolve_ip", "wait_ssh", "step_clock", "gate", "handover"])


if __name__ == "__main__":
    unittest.main()
