"""Per-node durable storage engine (SQLite on the pod's EBS PersistentVolume).

* kv     : key -> JSON list of versions (siblings), plus a digest and a key hash
           so Merkle leaves can be computed without decoding every value.
* hints  : writes accepted on behalf of an unreachable replica (hinted handoff).
"""
import json
import sqlite3
import threading
import time

from . import vclock
from .ring import h


class Store:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("""CREATE TABLE IF NOT EXISTS kv (
            key TEXT PRIMARY KEY, versions TEXT NOT NULL, digest TEXT NOT NULL,
            khash INTEGER NOT NULL, updated_at INTEGER NOT NULL)""")
        self.conn.execute("""CREATE TABLE IF NOT EXISTS hints (
            id INTEGER PRIMARY KEY AUTOINCREMENT, target TEXT NOT NULL, key TEXT NOT NULL,
            versions TEXT NOT NULL, created_at INTEGER NOT NULL)""")
        self.conn.execute("CREATE INDEX IF NOT EXISTS hints_target ON hints(target)")
        self.lock = threading.Lock()

    # ------------------------------------------------------------------ kv
    def get(self, key: str) -> list[dict] | None:
        with self.lock:
            row = self.conn.execute("SELECT versions FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def merge_put(self, key: str, incoming: list[dict]) -> tuple[list[dict], bool]:
        """Merge incoming versions with local ones; return (result, changed)."""
        with self.lock:
            row = self.conn.execute("SELECT versions, digest FROM kv WHERE key=?", (key,)).fetchone()
            local = json.loads(row[0]) if row else []
            merged = vclock.reconcile(local + incoming)
            dig = vclock.digest(merged)
            changed = row is None or row[1] != dig
            if changed:
                self.conn.execute(
                    "INSERT INTO kv(key, versions, digest, khash, updated_at) VALUES(?,?,?,?,?) "
                    "ON CONFLICT(key) DO UPDATE SET versions=excluded.versions, digest=excluded.digest, "
                    "updated_at=excluded.updated_at",
                    (key, json.dumps(merged, separators=(",", ":")), dig, h(key) % (2 ** 62), int(time.time() * 1000)))
            return merged, changed

    def digests(self) -> list[tuple[str, str, int]]:
        with self.lock:
            return self.conn.execute("SELECT key, digest, khash FROM kv").fetchall()

    def count(self) -> int:
        with self.lock:
            return self.conn.execute("SELECT count(*) FROM kv").fetchone()[0]

    def recent_keys(self, limit: int = 50) -> list[str]:
        with self.lock:
            return [r[0] for r in self.conn.execute(
                "SELECT key FROM kv ORDER BY updated_at DESC LIMIT ?", (limit,)).fetchall()]

    # ------------------------------------------------------------------ hints
    def add_hint(self, target: str, key: str, versions: list[dict]) -> None:
        with self.lock:
            self.conn.execute("INSERT INTO hints(target, key, versions, created_at) VALUES (?,?,?,?)",
                              (target, key, json.dumps(versions), int(time.time() * 1000)))

    def hints_for(self, target: str, limit: int = 200) -> list[tuple[int, str, list[dict]]]:
        with self.lock:
            rows = self.conn.execute("SELECT id, key, versions FROM hints WHERE target=? ORDER BY id LIMIT ?",
                                     (target, limit)).fetchall()
        return [(i, k, json.loads(v)) for i, k, v in rows]

    def delete_hint(self, hint_id: int) -> None:
        with self.lock:
            self.conn.execute("DELETE FROM hints WHERE id=?", (hint_id,))

    def hint_counts(self) -> dict[str, int]:
        with self.lock:
            return dict(self.conn.execute("SELECT target, count(*) FROM hints GROUP BY target").fetchall())
