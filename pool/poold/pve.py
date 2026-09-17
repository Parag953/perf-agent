import json
import ssl
import time
import urllib.parse
import urllib.request
from typing import Optional


def pick_ipv4(ifaces: list, prefix: str) -> Optional[str]:
    for i in ifaces:
        for a in i.get("ip-addresses", []) or []:
            if a.get("ip-address-type") == "ipv4" and str(a.get("ip-address", "")).startswith(prefix):
                return a["ip-address"]
    return None


class PVE:
    def __init__(self, url: str, token: str, node: str, verify_tls: bool = False):
        self.url = url.rstrip("/")
        self.token = token.strip()
        self.node = node
        self.ctx = ssl.create_default_context()
        if not verify_tls:
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE

    def _req(self, method: str, path: str, data: dict = None, timeout: float = 30.0):
        body = urllib.parse.urlencode(data).encode() if data else None
        req = urllib.request.Request(f"{self.url}/api2/json{path}", data=body, method=method)
        req.add_header("Authorization", f"PVEAPIToken={self.token}")
        with urllib.request.urlopen(req, context=self.ctx, timeout=timeout) as r:
            return json.loads(r.read().decode())["data"]

    def status(self, vmid: int) -> dict:
        return self._req("GET", f"/nodes/{self.node}/qemu/{vmid}/status/current")

    def rollback(self, vmid: int, snapshot: str, start: bool = True) -> str:
        return self._req("POST", f"/nodes/{self.node}/qemu/{vmid}/snapshot/{snapshot}/rollback",
                         {"start": 1 if start else 0})

    def task_status(self, upid: str) -> dict:
        return self._req("GET", f"/nodes/{self.node}/tasks/{urllib.parse.quote(upid, safe='')}/status")

    def wait_task(self, upid: str, timeout_s: float = 600.0, poll_s: float = 2.0) -> dict:
        t0 = time.time()
        while True:
            st = self.task_status(upid)
            if st.get("status") == "stopped":
                if st.get("exitstatus") != "OK":
                    raise RuntimeError(f"task {upid} failed: {st.get('exitstatus')}")
                return st
            if time.time() - t0 > timeout_s:
                raise TimeoutError(f"task {upid} still running after {timeout_s}s")
            time.sleep(poll_s)

    def guest_ipv4(self, vmid: int, prefix: str) -> Optional[str]:
        try:
            d = self._req("GET", f"/nodes/{self.node}/qemu/{vmid}/agent/network-get-interfaces", timeout=10)
        except Exception:
            return None
        return pick_ipv4(d.get("result", []) if isinstance(d, dict) else [], prefix)
