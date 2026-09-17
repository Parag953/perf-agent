from dataclasses import dataclass, field
from typing import Optional

SSH_DOWN = "ssh_down"
INT_FIELDS = ("helm_bad", "deploys", "pods_bad_phase", "pods_not_ready", "ds_bad", "restarts")


@dataclass(frozen=True)
class GateConfig:
    passes: int = 4
    interval_s: float = 10.0
    settle_s: float = 120.0
    timeout_s: float = 600.0
    expect_deploys: int = 13
    expect_ctx: str = "andromeda"


def parse_sample(line: str) -> dict:
    line = line.strip()
    if not line or line == SSH_DOWN:
        return {SSH_DOWN: True}
    out = {}
    for tok in line.split():
        if "=" not in tok:
            continue
        k, v = tok.split("=", 1)
        if k in INT_FIELDS:
            try:
                out[k] = int(v)
            except ValueError:
                pass
        else:
            out[k] = v
    return out


def sample_ok(s: dict, cfg: GateConfig) -> bool:
    if s.get(SSH_DOWN):
        return False
    for k in INT_FIELDS:
        if k not in s:
            return False
    if s.get("ctx") != cfg.expect_ctx:
        return False
    if s["helm_bad"] != 0 or s["pods_bad_phase"] != 0 or s["pods_not_ready"] != 0 or s["ds_bad"] != 0:
        return False
    if s["deploys"] < cfg.expect_deploys:
        return False
    return True


@dataclass
class GateWindow:
    cfg: GateConfig
    t0: float
    streak: int = 0
    restarts_prev: Optional[int] = None
    first_ssh_at: Optional[float] = None
    first_clean_at: Optional[float] = None
    last: dict = field(default_factory=dict)

    def feed(self, line: str, t: float) -> str:
        s = parse_sample(line)
        self.last = s
        if s.get(SSH_DOWN):
            self.streak = 0
            self.restarts_prev = None
        else:
            if self.first_ssh_at is None:
                self.first_ssh_at = t
            ok = sample_ok(s, self.cfg)
            rs = s.get("restarts")
            if ok and self.first_clean_at is None:
                self.first_clean_at = t
            if ok and self.restarts_prev is not None and rs == self.restarts_prev:
                self.streak += 1
            elif ok:
                self.streak = 1
            else:
                self.streak = 0
            self.restarts_prev = rs
        elapsed = t - self.t0
        if self.streak >= self.cfg.passes and elapsed >= self.cfg.settle_s:
            return "pass"
        if elapsed > self.cfg.timeout_s:
            return "fail"
        return "wait"
