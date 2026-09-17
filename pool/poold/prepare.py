import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from gate import GateConfig, GateWindow

STEPS = ("rollback", "resolve_ip", "wait_ssh", "step_clock", "gate", "handover")


@dataclass
class PrepareResult:
    ok: bool
    ip: Optional[str] = None
    attempt: int = 0
    error: Optional[str] = None
    timings: dict = field(default_factory=dict)
    last_gate: str = ""


class Prepare:
    def __init__(self, rollback: Callable[[], None], resolve_ip: Callable[[], Optional[str]],
                 ssh_ok: Callable[[str], bool], step_clock: Callable[[str], None],
                 gate_sample: Callable[[str], str], gate_cfg: GateConfig,
                 clock: Callable[[], float] = time.time, sleep: Callable[[float], None] = time.sleep,
                 retries: int = 1, ip_wait_s: float = 120.0, ssh_wait_s: float = 180.0,
                 on_step: Callable[[str, int], None] = None, log: Callable[[str], None] = None):
        self.rollback, self.resolve_ip, self.ssh_ok = rollback, resolve_ip, ssh_ok
        self.step_clock, self.gate_sample, self.gate_cfg = step_clock, gate_sample, gate_cfg
        self.clock, self.sleep, self.retries = clock, sleep, retries
        self.ip_wait_s, self.ssh_wait_s = ip_wait_s, ssh_wait_s
        self.on_step = on_step or (lambda step, attempt: None)
        self.log = log or (lambda m: None)

    def run(self) -> PrepareResult:
        last_err = None
        for attempt in range(1, self.retries + 2):
            r = self._attempt(attempt)
            if r.ok:
                return r
            last_err = r.error
            self.log(f"prepare attempt {attempt} failed: {r.error}")
        return PrepareResult(ok=False, attempt=self.retries + 1, error=last_err)

    def _attempt(self, attempt: int) -> PrepareResult:
        res = PrepareResult(ok=False, attempt=attempt)
        t0 = self.clock()

        def mark(step):
            self.on_step(step, attempt)
            return self.clock()

        try:
            ts = mark("rollback")
            self.rollback()
            res.timings["rollback"] = self.clock() - ts

            ts = mark("resolve_ip")
            ip = None
            while ip is None:
                ip = self.resolve_ip()
                if ip is None:
                    if self.clock() - ts > self.ip_wait_s:
                        raise TimeoutError("guest agent reported no LAN address")
                    self.sleep(2)
            res.ip = ip
            res.timings["resolve_ip"] = self.clock() - ts

            ts = mark("wait_ssh")
            while not self.ssh_ok(ip):
                if self.clock() - ts > self.ssh_wait_s:
                    raise TimeoutError(f"ssh to {ip} not up")
                self.sleep(2)
            res.timings["wait_ssh"] = self.clock() - ts

            ts = mark("step_clock")
            self.step_clock(ip)
            res.timings["step_clock"] = self.clock() - ts

            ts = mark("gate")
            w = GateWindow(self.gate_cfg, t0)
            while True:
                line = self.gate_sample(ip)
                res.last_gate = line
                v = w.feed(line, self.clock())
                if v == "pass":
                    break
                if v == "fail":
                    raise TimeoutError(f"gate timeout; last={line} streak={w.streak}")
                self.sleep(self.gate_cfg.interval_s if w.first_clean_at is not None else 2)
            res.timings["gate"] = self.clock() - ts
            res.timings["total"] = self.clock() - t0
            mark("handover")
            res.ok = True
            return res
        except Exception as e:
            res.error = f"{type(e).__name__}: {e}"
            return res
