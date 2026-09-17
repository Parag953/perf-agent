import unittest
from gate import parse_sample, sample_ok, GateWindow, GateConfig

CLEAN = "helm_bad=0 deploys=13 pods_bad_phase=0 pods_not_ready=0 ds_bad=0 restarts=14 ctx=andromeda"
CFG = GateConfig(passes=4, interval_s=10, settle_s=120, timeout_s=600, expect_deploys=13, expect_ctx="andromeda")


class SampleOk(unittest.TestCase):
    def test_clean_sample_is_ok(self):
        self.assertTrue(sample_ok(parse_sample(CLEAN), CFG))

    def test_ssh_down_is_not_ok(self):
        self.assertFalse(sample_ok(parse_sample("ssh_down"), CFG))

    def test_zero_deploys_while_resuming_is_not_ok(self):
        s = CLEAN.replace("deploys=13", "deploys=0")
        self.assertFalse(sample_ok(parse_sample(s), CFG))

    def test_datastore_refusing_behind_ready_pods_is_not_ok(self):
        s = CLEAN.replace("ds_bad=0", "ds_bad=1")
        self.assertFalse(sample_ok(parse_sample(s), CFG))

    def test_unready_pod_is_not_ok(self):
        s = CLEAN.replace("pods_not_ready=0", "pods_not_ready=1")
        self.assertFalse(sample_ok(parse_sample(s), CFG))

    def test_wrong_context_is_not_ok(self):
        s = CLEAN.replace("ctx=andromeda", "ctx=minikube")
        self.assertFalse(sample_ok(parse_sample(s), CFG))

    def test_missing_field_is_not_ok(self):
        s = CLEAN.replace(" ds_bad=0", "")
        self.assertFalse(sample_ok(parse_sample(s), CFG))


class Window(unittest.TestCase):
    def feed(self, w, samples, t0=0.0, step=10.0):
        verdict = None
        for i, s in enumerate(samples):
            verdict = w.feed(s, t0 + i * step)
        return verdict

    def test_four_clean_flat_samples_after_floor_pass(self):
        w = GateWindow(CFG, 0.0)
        # samples at t=0..150; 4 consecutive clean at t=120,130,140,150 with flat restarts
        v = self.feed(w, [CLEAN] * 16)
        self.assertEqual(v, "pass")

    def test_single_clean_sample_does_not_pass(self):
        w = GateWindow(CFG, 0.0)
        self.assertEqual(w.feed(CLEAN, 200.0), "wait")

    def test_clean_streak_before_settle_floor_does_not_pass(self):
        w = GateWindow(CFG, 0.0)
        v = self.feed(w, [CLEAN] * 6)  # t=0..50, streak 6 but floor is 120
        self.assertEqual(v, "wait")
        self.assertEqual(w.streak, 6)

    def test_restart_drift_resets_streak_to_one(self):
        w = GateWindow(CFG, 0.0)
        self.feed(w, [CLEAN] * 3)
        w.feed(CLEAN.replace("restarts=14", "restarts=15"), 30.0)
        self.assertEqual(w.streak, 1)

    def test_bad_sample_resets_streak_to_zero(self):
        w = GateWindow(CFG, 0.0)
        self.feed(w, [CLEAN] * 3)
        w.feed(CLEAN.replace("ds_bad=0", "ds_bad=1"), 30.0)
        self.assertEqual(w.streak, 0)

    def test_ssh_down_resets_streak_and_restart_baseline(self):
        w = GateWindow(CFG, 0.0)
        self.feed(w, [CLEAN] * 3)
        w.feed("ssh_down", 30.0)
        self.assertEqual(w.streak, 0)
        w.feed(CLEAN, 40.0)
        self.assertEqual(w.streak, 1)

    def test_timeout_fails(self):
        w = GateWindow(CFG, 0.0)
        v = w.feed(CLEAN.replace("ds_bad=0", "ds_bad=1"), 601.0)
        self.assertEqual(v, "fail")

    def test_records_first_ssh_and_first_clean(self):
        w = GateWindow(CFG, 0.0)
        w.feed("ssh_down", 0.0)
        w.feed(CLEAN.replace("pods_not_ready=0", "pods_not_ready=2"), 5.0)
        w.feed(CLEAN, 15.0)
        self.assertEqual(w.first_ssh_at, 5.0)
        self.assertEqual(w.first_clean_at, 15.0)


if __name__ == "__main__":
    unittest.main()
