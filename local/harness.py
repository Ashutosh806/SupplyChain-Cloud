"""Runs the real service code on your laptop: real FastAPI apps on real ports, real gRPC,
real PostgreSQL, and an in-memory stand-in for Kafka. No AWS account needed.
Used by local/run_local.py (make local / make test).

PostgreSQL is found in this order:
  1. An existing server: TEST_DB_HOST / TEST_DB_PORT / TEST_DB_USER / TEST_DB_PASSWORD
     (e.g. `make db-up`, which starts postgres:16 in Docker). A fresh database is created per run.
  2. Local PostgreSQL binaries (initdb/pg_ctl) on PATH, /usr/lib/postgresql/*/bin or Homebrew.
  3. Docker: starts a throwaway postgres:16 container.
"""
from __future__ import annotations

import atexit
import importlib
import importlib.util
import logging
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVICES = ROOT / "services"
LOCAL = Path(__file__).resolve().parent
_bootstrapped = False


# ------------------------------------------------------------------ basics
def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(fn, timeout: float = 20, interval: float = 0.2):
    """Poll fn() until it returns something truthy; return that value (or None on timeout)."""
    end = time.time() + timeout
    while time.time() < end:
        try:
            value = fn()
            if value:
                return value
        except Exception:
            pass
        time.sleep(interval)
    return None


def bootstrap() -> None:
    """Generate gRPC stubs, install the fake Kafka, tolerate duplicate Prometheus metrics.

    Must run before any service module is imported.
    """
    global _bootstrapped
    if _bootstrapped:
        return
    gen = Path(tempfile.mkdtemp(prefix="sc-gen-"))
    subprocess.run([sys.executable, "-m", "grpc_tools.protoc", f"-I{SERVICES / 'proto'}", f"--python_out={gen}",
                    f"--grpc_python_out={gen}", *map(str, (SERVICES / "proto").glob("*.proto"))], check=True)
    for p in (str(gen), str(SERVICES / "common"), str(LOCAL)):
        if p not in sys.path:
            sys.path.insert(0, p)
    import fake_kafka
    fake_kafka.install()

    import prometheus_client.registry as reg
    original = reg.CollectorRegistry.register

    def register(self, collector):  # several copies of one service module may be loaded
        try:
            original(self, collector)
        except ValueError:
            pass
    reg.CollectorRegistry.register = register
    os.environ.setdefault("POLL_INTERVAL_SECONDS", "0.05")
    os.environ.setdefault("HEARTBEAT_SECONDS", "1")
    os.environ.setdefault("DB_POOL_MAX", "40")
    _bootstrapped = True


def load_service(dirname: str, alias: str, env: dict | None = None):
    """Import services/<dirname>/app/main.py as package `alias` (so several can coexist)."""
    bootstrap()
    os.environ.update(env or {})
    pkg = SERVICES / dirname / "app"
    spec = importlib.util.spec_from_file_location(alias, pkg / "__init__.py", submodule_search_locations=[str(pkg)])
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    spec.loader.exec_module(mod)
    main = importlib.import_module(alias + ".main")
    logging.getLogger().setLevel(logging.WARNING)
    return main


class Server:
    """A uvicorn server in a background thread (runs the app's lifespan: migrations, consumers...)."""

    def __init__(self, app, port: int | None = None):
        import uvicorn
        self.port = port or free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self._server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=self.port,
                                                     log_config=None, lifespan="on"))
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()

    def wait_ready(self, timeout: float = 60) -> bool:
        import httpx
        return bool(wait_until(lambda: httpx.get(self.url + "/readyz", timeout=2).status_code == 200, timeout))

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=10)


# ------------------------------------------------------------------ PostgreSQL
class Postgres:
    """Provides a fresh, empty database and the DB_* env vars the services read."""

    def __init__(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="sc-pg-"))
        self.mode = None
        self.container = None
        self.pg_bin = None
        self.port = None
        self.admin = None   # (host, port, user, password, admin_db)
        self.dbname = f"sc_test_{uuid.uuid4().hex[:8]}"

    # --- discovery -------------------------------------------------------
    @staticmethod
    def _find_bin() -> str | None:
        if os.environ.get("PG_BIN"):
            return os.environ["PG_BIN"]
        on_path = shutil.which("initdb")
        if on_path:
            return str(Path(on_path).parent)
        candidates = []
        for pattern in ("/usr/lib/postgresql/*/bin", "/opt/homebrew/opt/postgresql@*/bin",
                        "/usr/local/opt/postgresql@*/bin", "/usr/pgsql-*/bin",
                        "/Applications/Postgres.app/Contents/Versions/*/bin"):
            candidates += sorted(Path("/").glob(pattern.lstrip("/")), reverse=True)
        return str(candidates[0]) if candidates else None

    def start(self) -> dict:
        atexit.register(self.stop)        # also clean up if a fixture fails before teardown
        if os.environ.get("TEST_DB_HOST"):
            self.mode = "external"
            self.admin = (os.environ["TEST_DB_HOST"], int(os.environ.get("TEST_DB_PORT", 5432)),
                          os.environ.get("TEST_DB_USER", "supplychain"),
                          os.environ.get("TEST_DB_PASSWORD", "supplychain"),
                          os.environ.get("TEST_DB_ADMIN_DB", "postgres"))
        elif self._find_bin():
            self.mode = "local"
            self._start_local(self._find_bin())
        elif shutil.which("docker"):
            self.mode = "docker"
            self._start_docker()
        else:
            raise RuntimeError(
                "No PostgreSQL available. Either install PostgreSQL 16 (brew install postgresql@16 / "
                "apt install postgresql), install Docker, or point TEST_DB_HOST at a server (make db-up).")
        self._create_db()
        host, port, user, password, _ = self.admin
        return {"DB_HOST": host, "DB_PORT": str(port), "DB_USER": user, "DB_PASSWORD": password,
                "DB_NAME": self.dbname, "DB_SSLMODE": "disable"}

    def _run(self, *cmd: str) -> None:
        if os.geteuid() == 0:  # initdb refuses to run as root
            subprocess.run(["su", "postgres", "-c", " ".join(cmd)], check=True, capture_output=True, text=True)
        else:
            subprocess.run(list(cmd), check=True, capture_output=True, text=True)

    def _start_local(self, pg_bin: str) -> None:
        self.pg_bin = pg_bin
        self.port = free_port()
        data = self.tmp / "data"
        data.mkdir()
        if os.geteuid() == 0:
            shutil.chown(self.tmp, "postgres")
            shutil.chown(data, "postgres")
        self._run(f"{pg_bin}/initdb", "-D", str(data), "-U", "postgres", "--auth=trust", "-E", "UTF8")
        opts = f"-p {self.port} -k {self.tmp} -c listen_addresses=127.0.0.1"
        self._run(f"{pg_bin}/pg_ctl", "-D", str(data), "-o", f"'{opts}'" if os.geteuid() == 0 else opts,
                  "-l", str(self.tmp / "pg.log"), "-w", "start")
        self.admin = ("127.0.0.1", self.port, "postgres", "", "postgres")

    def _start_docker(self) -> None:
        name = f"sc-test-pg-{uuid.uuid4().hex[:6]}"
        subprocess.run(["docker", "run", "-d", "--rm", "--name", name, "-e", "POSTGRES_PASSWORD=supplychain",
                        "-e", "POSTGRES_USER=supplychain", "-p", "127.0.0.1::5432", "postgres:16"],
                       check=True, capture_output=True, text=True)
        self.container = name
        out = subprocess.run(["docker", "port", name, "5432/tcp"], check=True, capture_output=True, text=True).stdout
        self.port = int(out.strip().splitlines()[0].rsplit(":", 1)[1])
        self.admin = ("127.0.0.1", self.port, "supplychain", "supplychain", "postgres")

    def _admin_conn(self):
        import psycopg
        host, port, user, password, admin_db = self.admin
        return psycopg.connect(host=host, port=port, user=user, password=password or None, dbname=admin_db,
                               autocommit=True, connect_timeout=3)

    def _create_db(self) -> None:
        conn = wait_until(self._admin_conn, timeout=60, interval=0.5)
        if not conn:
            raise RuntimeError(f"PostgreSQL ({self.mode}) did not become reachable")
        with conn:
            conn.execute(f'CREATE DATABASE "{self.dbname}"')

    def stop(self) -> None:
        if getattr(self, "_stopped", False):
            return
        self._stopped = True
        try:
            from sc_common import db
            if db._pool is not None:
                db._pool.close(timeout=2)
                db._pool = None
        except Exception:
            pass
        if self.mode == "external":
            try:
                with self._admin_conn() as conn:
                    conn.execute(f'DROP DATABASE IF EXISTS "{self.dbname}" WITH (FORCE)')
            except Exception:
                pass
        elif self.mode == "local":
            try:
                self._run(f"{self.pg_bin}/pg_ctl", "-D", str(self.tmp / "data"), "-m", "immediate", "stop")
            except Exception:
                pass
        elif self.mode == "docker":
            subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)
        shutil.rmtree(self.tmp, ignore_errors=True)

