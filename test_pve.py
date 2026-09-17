import unittest
from pve import pick_ipv4

IFACES = [
    {"name": "lo", "ip-addresses": [{"ip-address-type": "ipv4", "ip-address": "127.0.0.1"}]},
    {"name": "tailscale0", "ip-addresses": [{"ip-address-type": "ipv4", "ip-address": "100.73.230.48"}]},
    {"name": "ens18", "ip-addresses": [
        {"ip-address-type": "ipv6", "ip-address": "fe80::1"},
        {"ip-address-type": "ipv4", "ip-address": "10.0.0.38"}]},
    {"name": "docker0", "ip-addresses": [{"ip-address-type": "ipv4", "ip-address": "172.17.0.1"}]},
    {"name": "br-abc", "ip-addresses": [{"ip-address-type": "ipv4", "ip-address": "192.168.49.1"}]},
]


class PickIPv4(unittest.TestCase):
    def test_prefers_the_lan_interface_over_tailscale_docker_and_loopback(self):
        self.assertEqual(pick_ipv4(IFACES, prefix="10.0.0."), "10.0.0.38")

    def test_returns_none_when_no_lan_address_yet(self):
        no_lan = [i for i in IFACES if i["name"] != "ens18"]
        self.assertIsNone(pick_ipv4(no_lan, prefix="10.0.0."))

    def test_handles_missing_ip_addresses_key(self):
        self.assertIsNone(pick_ipv4([{"name": "ens18"}], prefix="10.0.0."))


if __name__ == "__main__":
    unittest.main()
