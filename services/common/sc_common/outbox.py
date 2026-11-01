"""Transactional outbox.

A service writes its business row(s) AND an outbox row in the SAME database
transaction. The Event Publisher service relays unpublished outbox rows to
Kafka. This removes the dual-write problem: an event is published if and only
if the business change committed.
"""
import json

from psycopg.types.json import Jsonb

from . import metrics
from .events import envelope


def outbox_ddl(schema: str) -> list[str]:
    return [
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.outbox (
            id            BIGSERIAL PRIMARY KEY,
            event_id      UUID NOT NULL UNIQUE,
            aggregate_type TEXT NOT NULL,
            aggregate_id  TEXT NOT NULL,
            event_type    TEXT NOT NULL,
            envelope      JSONB NOT NULL,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            published_at  TIMESTAMPTZ
        )""",
        f"CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON {schema}.outbox (id) WHERE published_at IS NULL",
    ]


def add_event(cur, schema: str, event_type: str, aggregate_type: str, aggregate_id: str,
              payload: dict, source: str, tracking_id: str | None = None,
              occurred_at: int | None = None) -> dict:
    env = envelope(event_type, aggregate_type, aggregate_id, payload, source,
                   tracking_id=tracking_id, occurred_at=occurred_at)
    cur.execute(
        f"INSERT INTO {schema}.outbox (event_id, aggregate_type, aggregate_id, event_type, envelope) "
        "VALUES (%s, %s, %s, %s, %s)",
        (env["event_id"], aggregate_type, aggregate_id, event_type, Jsonb(env)),
    )
    metrics.DOMAIN_EVENTS.labels(event_type).inc()
    return env


def dumps(obj) -> str:
    return json.dumps(obj, default=str, separators=(",", ":"))
