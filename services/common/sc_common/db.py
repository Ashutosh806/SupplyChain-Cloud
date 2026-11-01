"""Postgres (Amazon RDS) access.

Each service owns its own schema in the shared RDS instance (schema-per-service),
and migrates it idempotently at startup under an advisory lock so that many
replicas starting at once do not race on DDL.
"""
import logging
import time

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from . import config

log = logging.getLogger(__name__)
_pool: ConnectionPool | None = None


def conninfo() -> str:
    return (
        f"host={config.DB_HOST} port={config.DB_PORT} dbname={config.DB_NAME} "
        f"user={config.DB_USER} password={config.DB_PASSWORD} sslmode={config.DB_SSLMODE} "
        f"connect_timeout=5 application_name={config.SERVICE_NAME}"
    )


def get_pool(min_size: int = 1, max_size: int | None = None) -> ConnectionPool:
    global _pool
    if _pool is None:
        # Small per-pod pools: with HPAs scaling to ~25 pods the RDS connection limit matters.
        max_size = max_size or config.env_int("DB_POOL_MAX", 5)
        _pool = ConnectionPool(
            conninfo(), min_size=min_size, max_size=max_size,
            kwargs={"row_factory": dict_row, "autocommit": False}, open=False,
        )
        for attempt in range(30):
            try:
                _pool.open(wait=True, timeout=10)
                break
            except Exception as exc:  # DB may still be starting
                log.warning("database not reachable yet", extra={"attempt": attempt, "error": str(exc)})
                time.sleep(2)
        else:
            raise RuntimeError("could not connect to database")
    return _pool


def migrate(schema: str, statements: list[str]) -> None:
    """Run DDL for a service schema exactly once at a time across replicas."""
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(hashtext(%s))", (f"migrate:{schema}",))
            try:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                for stmt in statements:
                    cur.execute(stmt)
                conn.commit()
            finally:
                cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (f"migrate:{schema}",))
                conn.commit()
    log.info("schema migrated", extra={"schema": schema})


def ping() -> bool:
    try:
        with get_pool().connection() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False
