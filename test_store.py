import unittest
from store import Store


def mk():
    s = Store(":memory:")
    s.ensure_box("andro-b", vmid=102, ssh_user="andro-2", ip="10.0.0.38", snapshot="warm-live")
    return s


class Dispatch(unittest.TestCase):
    def test_new_box_starts_dirty(self):
        s = mk()
        self.assertEqual(s.box("andro-b")["state"], "dirty")

    def test_enqueue_returns_position(self):
        s = mk()
        r1 = s.enqueue(task="true", owner="abhi", source="test", ttl_s=60)
        r2 = s.enqueue(task="true", owner="abhi", source="test", ttl_s=60)
        self.assertEqual(r1["position"], 1)
        self.assertEqual(r2["position"], 2)

    def test_dispatch_pairs_oldest_run_with_a_free_or_dirty_box(self):
        s = mk()
        r1 = s.enqueue(task="a", owner="o", source="t", ttl_s=60)
        s.enqueue(task="b", owner="o", source="t", ttl_s=60)
        d = s.dispatch()
        self.assertEqual(d["run_id"], r1["run_id"])
        self.assertEqual(d["box"], "andro-b")
        self.assertEqual(s.box("andro-b")["state"], "preparing")
        self.assertEqual(s.run(r1["run_id"])["state"], "preparing")

    def test_dispatch_with_no_available_box_returns_none(self):
        s = mk()
        s.enqueue(task="a", owner="o", source="t", ttl_s=60)
        s.enqueue(task="b", owner="o", source="t", ttl_s=60)
        s.dispatch()
        self.assertIsNone(s.dispatch())

    def test_dispatch_with_empty_queue_returns_none(self):
        s = mk()
        self.assertIsNone(s.dispatch())

    def test_reset_request_prepares_box_without_a_run(self):
        s = mk()
        d = s.request_reset("andro-b")
        self.assertEqual(d["box"], "andro-b")
        self.assertIsNone(d["run_id"])
        self.assertEqual(s.box("andro-b")["state"], "preparing")

    def test_reset_request_on_leased_box_is_refused(self):
        s = mk()
        r = s.enqueue(task="a", owner="o", source="t", ttl_s=60)
        s.dispatch()
        s.mark_leased("andro-b", r["run_id"], ttl_s=60)
        with self.assertRaises(ValueError):
            s.request_reset("andro-b")


class Lifecycle(unittest.TestCase):
    def test_mark_leased_sets_lease_and_run_running(self):
        s = mk()
        r = s.enqueue(task="a", owner="o", source="t", ttl_s=60)
        s.dispatch()
        lease = s.mark_leased("andro-b", r["run_id"], ttl_s=60)
        b = s.box("andro-b")
        self.assertEqual(b["state"], "leased")
        self.assertEqual(b["lease_id"], lease)
        self.assertEqual(s.run(r["run_id"])["state"], "running")

    def test_prepare_without_run_ends_free(self):
        s = mk()
        s.request_reset("andro-b")
        s.mark_leased("andro-b", None, ttl_s=0)
        self.assertEqual(s.box("andro-b")["state"], "free")

    def test_run_exit_zero_is_done_and_box_dirty(self):
        s = mk()
        r = s.enqueue(task="a", owner="o", source="t", ttl_s=60)
        s.dispatch(); s.mark_leased("andro-b", r["run_id"], ttl_s=60)
        s.end_run(r["run_id"], exit_code=0)
        self.assertEqual(s.run(r["run_id"])["state"], "done")
        self.assertEqual(s.box("andro-b")["state"], "dirty")
        self.assertIsNone(s.box("andro-b")["lease_id"])

    def test_run_exit_nonzero_is_failed(self):
        s = mk()
        r = s.enqueue(task="a", owner="o", source="t", ttl_s=60)
        s.dispatch(); s.mark_leased("andro-b", r["run_id"], ttl_s=60)
        s.end_run(r["run_id"], exit_code=3, error="boom")
        self.assertEqual(s.run(r["run_id"])["state"], "failed")
        self.assertEqual(s.run(r["run_id"])["error"], "boom")

    def test_prepare_failure_increments_and_quarantines_on_second(self):
        s = mk()
        r = s.enqueue(task="a", owner="o", source="t", ttl_s=60)
        s.dispatch()
        st = s.prepare_failed("andro-b", "gate timeout")
        self.assertEqual(st, "dirty")
        self.assertEqual(s.box("andro-b")["fail_count"], 1)
        s.dispatch()
        st = s.prepare_failed("andro-b", "gate timeout")
        self.assertEqual(st, "quarantined")
        self.assertEqual(s.run(r["run_id"])["state"], "queued")

    def test_successful_lease_resets_fail_count(self):
        s = mk()
        r = s.enqueue(task="a", owner="o", source="t", ttl_s=60)
        s.dispatch(); s.prepare_failed("andro-b", "x")
        s.dispatch(); s.mark_leased("andro-b", r["run_id"], ttl_s=60)
        self.assertEqual(s.box("andro-b")["fail_count"], 0)

    def test_unquarantine_lands_dirty(self):
        s = mk()
        s.set_state("andro-b", "quarantined")
        s.unquarantine("andro-b")
        self.assertEqual(s.box("andro-b")["state"], "dirty")

    def test_cancel_queued_run(self):
        s = mk()
        r = s.enqueue(task="a", owner="o", source="t", ttl_s=60)
        s.cancel(r["run_id"])
        self.assertEqual(s.run(r["run_id"])["state"], "abandoned")
        self.assertIsNone(s.dispatch())


class Sweeper(unittest.TestCase):
    def test_expired_lease_goes_dirty_and_run_abandoned(self):
        s = mk()
        r = s.enqueue(task="a", owner="o", source="t", ttl_s=1)
        s.dispatch(); s.mark_leased("andro-b", r["run_id"], ttl_s=1, now=1000.0)
        swept = s.sweep(now=1002.0, heartbeat_grace_s=90)
        self.assertEqual(swept, [r["run_id"]])
        self.assertEqual(s.box("andro-b")["state"], "dirty")
        self.assertEqual(s.run(r["run_id"])["state"], "abandoned")

    def test_heartbeat_extends_survival_within_ttl(self):
        s = mk()
        r = s.enqueue(task="a", owner="o", source="t", ttl_s=1000)
        s.dispatch(); lease = s.mark_leased("andro-b", r["run_id"], ttl_s=1000, now=1000.0)
        s.heartbeat(lease, now=1050.0)
        self.assertEqual(s.sweep(now=1100.0, heartbeat_grace_s=90), [])
        self.assertEqual(s.sweep(now=1150.0, heartbeat_grace_s=90), [r["run_id"]])

    def test_startup_recovery(self):
        s = mk()
        r = s.enqueue(task="a", owner="o", source="t", ttl_s=60)
        s.dispatch()
        s.recover_on_startup()
        self.assertEqual(s.box("andro-b")["state"], "dirty")
        self.assertEqual(s.run(r["run_id"])["state"], "queued")
        s.dispatch(); s.mark_leased("andro-b", r["run_id"], ttl_s=60)
        s.recover_on_startup()
        self.assertEqual(s.box("andro-b")["state"], "dirty")
        self.assertEqual(s.run(r["run_id"])["state"], "abandoned")


if __name__ == "__main__":
    unittest.main()
