import sqlite3
import threading
import time
import uuid
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS boxes(
  name TEXT PRIMARY KEY, vmid INTEGER NOT NULL, ssh_user TEXT NOT NULL, ip TEXT,
  snapshot TEXT NOT NULL, state TEXT NOT NULL, since REAL NOT NULL,
  lease_id TEXT, run_id TEXT, acquired_at REAL, expires_at REAL, last_heartbeat REAL,
  fail_count INTEGER NOT NULL DEFAULT 0, last_error TEXT, heartbeat_required INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS runs(
  run_id TEXT PRIMARY KEY, seq INTEGER NOT NULL, state TEXT NOT NULL, box TEXT,
  task TEXT NOT NULL, owner TEXT, source TEXT, ttl_s INTEGER NOT NULL,
  callback_url TEXT, reset_on_release INTEGER NOT NULL DEFAULT 0,
  created_at REAL NOT NULL, started_at REAL, ended_at REAL, exit_code INTEGER, error TEXT, branch TEXT
);
"""

AVAILABLE = ("free", "dirty")


class Store:
    def __init__(self, path: str):
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self.db.executescript(SCHEMA)
            self._migrate()

    def _migrate(self):
        want = {"boxes": {"heartbeat_required": "INTEGER NOT NULL DEFAULT 1"}}
        for table, cols in want.items():
            have = {r["name"] for r in self.db.execute(f"PRAGMA table_info({table})")}
            for col, decl in cols.items():
                if col not in have:
                    self.db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")

    def _tx(self):
        return _Tx(self)

    def ensure_box(self, name, vmid, ssh_user, ip, snapshot):
        with self._tx():
            if self.db.execute("SELECT 1 FROM boxes WHERE name=?", (name,)).fetchone():
                self.db.execute("UPDATE boxes SET vmid=?, ssh_user=?, snapshot=? WHERE name=?", (vmid, ssh_user, snapshot, name))
            else:
                self.db.execute(
                    "INSERT INTO boxes(name,vmid,ssh_user,ip,snapshot,state,since) VALUES(?,?,?,?,?,'dirty',?)",
                    (name, vmid, ssh_user, ip, snapshot, time.time()))

    def box(self, name) -> Optional[dict]:
        r = self.db.execute("SELECT * FROM boxes WHERE name=?", (name,)).fetchone()
        return dict(r) if r else None

    def boxes(self) -> list:
        return [dict(r) for r in self.db.execute("SELECT * FROM boxes ORDER BY name")]

    def run(self, run_id) -> Optional[dict]:
        r = self.db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return dict(r) if r else None

    def runs(self, limit=50) -> list:
        return [dict(r) for r in self.db.execute("SELECT * FROM runs ORDER BY seq DESC LIMIT ?", (limit,))]

    def queue(self) -> list:
        return [r["run_id"] for r in self.db.execute("SELECT run_id FROM runs WHERE state='queued' ORDER BY seq")]

    def set_ip(self, name, ip):
        with self._tx():
            self.db.execute("UPDATE boxes SET ip=? WHERE name=?", (ip, name))

    def set_state(self, name, state, error=None):
        with self._tx():
            self.db.execute("UPDATE boxes SET state=?, since=?, last_error=COALESCE(?, last_error) WHERE name=?",
                            (state, time.time(), error, name))

    def enqueue(self, task, owner, source, ttl_s, callback_url=None, reset_on_release=False) -> dict:
        run_id = uuid.uuid4().hex[:12]
        with self._tx():
            seq = self.db.execute("SELECT COALESCE(MAX(seq),0)+1 FROM runs").fetchone()[0]
            self.db.execute(
                "INSERT INTO runs(run_id,seq,state,task,owner,source,ttl_s,callback_url,reset_on_release,created_at) "
                "VALUES(?,?,'queued',?,?,?,?,?,?,?)",
                (run_id, seq, task, owner, source, ttl_s, callback_url, int(bool(reset_on_release)), time.time()))
            position = len(self.queue())
        return {"run_id": run_id, "position": position}

    def dispatch(self) -> Optional[dict]:
        with self._tx():
            run = self.db.execute("SELECT * FROM runs WHERE state='queued' ORDER BY seq LIMIT 1").fetchone()
            if not run:
                return None
            box = self.db.execute(
                "SELECT * FROM boxes WHERE state IN ('free','dirty') ORDER BY CASE state WHEN 'free' THEN 0 ELSE 1 END, name LIMIT 1"
            ).fetchone()
            if not box:
                return None
            now = time.time()
            self.db.execute("UPDATE boxes SET state='preparing', since=?, run_id=?, lease_id=NULL WHERE name=?",
                            (now, run["run_id"], box["name"]))
            self.db.execute("UPDATE runs SET state='preparing', box=? WHERE run_id=?", (box["name"], run["run_id"]))
            return {"run_id": run["run_id"], "box": box["name"]}

    def request_reset(self, name) -> dict:
        with self._tx():
            b = self.box(name)
            if not b:
                raise KeyError(name)
            if b["state"] not in AVAILABLE and b["state"] != "quarantined":
                raise ValueError(f"box {name} is {b['state']}")
            self.db.execute("UPDATE boxes SET state='preparing', since=?, run_id=NULL, lease_id=NULL WHERE name=?",
                            (time.time(), name))
            return {"run_id": None, "box": name}

    def mark_leased(self, name, run_id, ttl_s, now=None) -> Optional[str]:
        now = time.time() if now is None else now
        with self._tx():
            if run_id is None:
                self.db.execute("UPDATE boxes SET state='free', since=?, run_id=NULL, lease_id=NULL, fail_count=0 WHERE name=?",
                                (now, name))
                return None
            lease = uuid.uuid4().hex[:12]
            self.db.execute(
                "UPDATE boxes SET state='leased', since=?, lease_id=?, run_id=?, acquired_at=?, expires_at=?, "
                "last_heartbeat=?, fail_count=0, heartbeat_required=1 WHERE name=?",
                (now, lease, run_id, now, now + ttl_s, now, name))
            self.db.execute("UPDATE runs SET state='running', started_at=? WHERE run_id=?", (now, run_id))
            return lease

    def prepare_failed(self, name, error) -> str:
        with self._tx():
            b = self.box(name)
            fails = b["fail_count"] + 1
            state = "quarantined" if fails >= 2 else "dirty"
            self.db.execute("UPDATE boxes SET state=?, since=?, fail_count=?, last_error=?, run_id=NULL WHERE name=?",
                            (state, time.time(), fails, error, name))
            if b["run_id"]:
                self.db.execute("UPDATE runs SET state='queued', box=NULL WHERE run_id=? AND state='preparing'", (b["run_id"],))
            return state

    def end_run(self, run_id, exit_code, error=None, now=None):
        now = time.time() if now is None else now
        with self._tx():
            r = self.run(run_id)
            state = "done" if exit_code == 0 else "failed"
            self.db.execute("UPDATE runs SET state=?, ended_at=?, exit_code=?, error=? WHERE run_id=?",
                            (state, now, exit_code, error, run_id))
            if r and r["box"]:
                self.db.execute("UPDATE boxes SET state='dirty', since=?, lease_id=NULL, run_id=NULL WHERE name=? AND run_id=?",
                                (now, r["box"], run_id))

    def pick_ready(self) -> Optional[str]:
        r = self.db.execute("SELECT name FROM boxes WHERE state='free' ORDER BY name LIMIT 1").fetchone()
        return r["name"] if r else None

    def pick_dirty_idle(self) -> Optional[str]:
        r = self.db.execute("SELECT name FROM boxes WHERE state='dirty' AND run_id IS NULL ORDER BY name LIMIT 1").fetchone()
        return r["name"] if r else None

    def lease_external(self, name, ttl_s, now=None) -> str:
        now = time.time() if now is None else now
        with self._tx():
            b = self.box(name)
            if not b or b["state"] != "free":
                raise ValueError(f"box {name} is not ready")
            lease = uuid.uuid4().hex[:12]
            self.db.execute(
                "UPDATE boxes SET state='leased', since=?, lease_id=?, run_id=NULL, acquired_at=?, expires_at=?, "
                "last_heartbeat=?, heartbeat_required=0 WHERE name=?",
                (now, lease, now, now + ttl_s, now, name))
            return lease

    def release(self, name, now=None) -> bool:
        now = time.time() if now is None else now
        with self._tx():
            cur = self.db.execute(
                "UPDATE boxes SET state='dirty', since=?, lease_id=NULL, run_id=NULL WHERE name=? AND state='leased'",
                (now, name))
            return cur.rowcount == 1

    def unquarantine(self, name):
        with self._tx():
            self.db.execute("UPDATE boxes SET state='dirty', since=?, fail_count=0 WHERE name=? AND state='quarantined'",
                            (time.time(), name))

    def cancel(self, run_id) -> Optional[str]:
        with self._tx():
            r = self.run(run_id)
            if not r:
                raise KeyError(run_id)
            if r["state"] == "queued":
                self.db.execute("UPDATE runs SET state='abandoned', ended_at=? WHERE run_id=?", (time.time(), run_id))
                return None
            if r["state"] == "running":
                self.db.execute("UPDATE runs SET state='abandoned', ended_at=? WHERE run_id=?", (time.time(), run_id))
                self.db.execute("UPDATE boxes SET state='dirty', since=?, lease_id=NULL, run_id=NULL WHERE run_id=?",
                                (time.time(), run_id))
                return r["box"]
            return None

    def heartbeat(self, lease_id, now=None) -> bool:
        now = time.time() if now is None else now
        with self._tx():
            cur = self.db.execute("UPDATE boxes SET last_heartbeat=? WHERE lease_id=? AND state='leased'", (now, lease_id))
            return cur.rowcount == 1

    def sweep(self, now=None, heartbeat_grace_s=90) -> list:
        now = time.time() if now is None else now
        swept = []
        with self._tx():
            rows = self.db.execute(
                "SELECT name, run_id FROM boxes WHERE state='leased' AND "
                "(expires_at < ? OR (heartbeat_required=1 AND last_heartbeat < ?))",
                (now, now - heartbeat_grace_s)).fetchall()
            for r in rows:
                self.db.execute("UPDATE boxes SET state='dirty', since=?, lease_id=NULL, run_id=NULL WHERE name=?", (now, r["name"]))
                if r["run_id"]:
                    self.db.execute("UPDATE runs SET state='abandoned', ended_at=?, error='lease expired' WHERE run_id=? AND state='running'",
                                    (now, r["run_id"]))
                    swept.append(r["run_id"])
        return swept

    def recover_on_startup(self):
        now = time.time()
        with self._tx():
            self.db.execute("UPDATE runs SET state='queued', box=NULL WHERE state='preparing'")
            self.db.execute("UPDATE runs SET state='abandoned', ended_at=?, error='poold restarted' WHERE state='running'", (now,))
            self.db.execute("UPDATE boxes SET state='dirty', since=?, lease_id=NULL, run_id=NULL WHERE state IN ('preparing','leased')", (now,))


class _Tx:
    def __init__(self, s: Store):
        self.s = s

    def __enter__(self):
        self.s.lock.acquire()
        self.s.db.execute("BEGIN IMMEDIATE")
        return self

    def __exit__(self, et, ev, tb):
        try:
            if et is None:
                self.s.db.execute("COMMIT")
            else:
                self.s.db.execute("ROLLBACK")
        finally:
            self.s.lock.release()
        return False
