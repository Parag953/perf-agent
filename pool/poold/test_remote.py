import unittest
from remote import ssh_argv, REMOTE_GATE


class SshArgv(unittest.TestCase):
    def test_batch_mode_short_timeout_and_accept_new_host_keys(self):
        argv = ssh_argv("andro-2", "10.0.0.38", "hostname", connect_timeout=3)
        self.assertEqual(argv[0], "ssh")
        self.assertIn("BatchMode=yes", argv)
        self.assertIn("ConnectTimeout=3", argv)
        self.assertIn("StrictHostKeyChecking=accept-new", argv)
        self.assertEqual(argv[-2:], ["andro-2@10.0.0.38", "hostname"])

    def test_box_ip_changes_must_not_trip_known_hosts(self):
        argv = ssh_argv("andro-2", "10.0.0.37", "true")
        self.assertIn("UserKnownHostsFile=/dev/null", argv)

    def test_gate_script_probes_postgres_as_the_postgres_user(self):
        self.assertIn("pg_isready -q -U postgres -h 127.0.0.1", REMOTE_GATE)
        self.assertIn("split($3,a,\"/\")", REMOTE_GATE)


if __name__ == "__main__":
    unittest.main()
