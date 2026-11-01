"""Event Publisher — reliably relays outbox rows to Kafka (transactional outbox relay).

For every service schema it loops:
    BEGIN;
      SELECT ... FROM <schema>.outbox WHERE published_at IS NULL
      ORDER BY id LIMIT n FOR UPDATE SKIP LOCKED;     -- replicas never double-claim
      produce each envelope to Kafka (idempotent producer, acks=all), flush;
      UPDATE outbox SET published_at = now() WHERE id = ANY(...);
    COMMIT;
If Kafka is down the transaction rolls back and the rows are retried later, so
delivery is at-least-once; consumers de-duplicate on event_id.

It also emits a Heartbeat to every partition so that event-time watermarks in
the Flink job keep advancing when traffic is idle (windows still close).
"""
import json
import logging
import threading
import time
from contextlib import asynccontextmanager

import uvicorn
from prometheus_client import Counter, Gauge, Histogram

from sc_common import config, db, kafka
from sc_common.events import envelope, now_ms
from sc_common.web import create_app

log = logging.getLogger("publisher")
TABLES = [t.strip() for t in config.env(
    "OUTBOX_TABLES", "supplier.outbox,booking.outbox,inventory.outbox,ingestion.outbox").split(",") if t.strip()]
BATCH = config.env_int("PUBLISH_BATCH", 500)
IDLE_SLEEP = config.env_float("POLL_INTERVAL_SECONDS", 0.2)
HEARTBEAT_SECONDS = config.env_float("HEARTBEAT_SECONDS", 10)
RETENTION_HOURS = config.env_int("OUTBOX_RETENTION_HOURS", 24)

PUBLISHED = Counter("outbox_events_published_total", "Events relayed to Kafka", ["table", "event_type"])
FAILURES = Counter("outbox_publish_failures_total", "Failed relay batches", ["table"])
BACKLOG = Gauge("outbox_backlog", "Unpublished outbox rows", ["table"])
RELAY_LAG = Histogram("outbox_relay_lag_seconds", "Time from outbox insert to Kafka ack",
                      buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60))

_stop = threading.Event()
_threads: list[threading.Thread] = []
_ready = threading.Event()
stats = {"last_batch_at": None, "published": 0}


def relay_table(table: str) -> None:
    producer = kafka.make_producer()
    pool = db.get_pool()
    while not _stop.is_set():
        try:
            with pool.connection() as conn, conn.cursor() as cur:
                cur.execute(f"SELECT id, aggregate_id, event_type, envelope FROM {table} "
                            "WHERE published_at IS NULL ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED", (BATCH,))
                rows = cur.fetchall()
                if not rows:
                    conn.rollback()
                    _stop.wait(IDLE_SLEEP)
                    continue
                errors: list[str] = []

                def on_delivery(err, _msg):
                    if err is not None:
                        errors.append(str(err))

                published_at = now_ms()
                for row in rows:
                    env = row["envelope"]
                    env["published_at"] = published_at
                    producer.produce(config.TOPIC_SHIPMENT_EVENTS, key=row["aggregate_id"],
                                     value=json.dumps(env, separators=(",", ":")), on_delivery=on_delivery,
                                     timestamp=published_at)
                    producer.poll(0)
                remaining = producer.flush(15)
                if remaining or errors:
                    raise RuntimeError(f"kafka delivery failed: remaining={remaining} errors={errors[:3]}")
                cur.execute(f"UPDATE {table} SET published_at=now() WHERE id = ANY(%s)",
                            ([r["id"] for r in rows],))
                conn.commit()
                acked = now_ms()
                for row in rows:
                    PUBLISHED.labels(table, row["event_type"]).inc()
                    RELAY_LAG.observe(max(0, acked - row["envelope"]["recorded_at"]) / 1000)
                stats["published"] += len(rows)
                stats["last_batch_at"] = acked
        except Exception as exc:
            FAILURES.labels(table).inc()
            log.error("relay batch failed; will retry", extra={"table": table, "error": str(exc)})
            _stop.wait(2)


def housekeeping() -> None:
    """Backlog gauge + delete old published rows."""
    pool = db.get_pool()
    last_cleanup = 0.0
    while not _stop.wait(5):
        try:
            with pool.connection() as conn:
                for table in TABLES:
                    n = conn.execute(f"SELECT count(*) AS n FROM {table} WHERE published_at IS NULL").fetchone()["n"]
                    BACKLOG.labels(table).set(n)
                if time.time() - last_cleanup > 600:
                    for table in TABLES:
                        conn.execute(f"DELETE FROM {table} WHERE published_at < now() - make_interval(hours => %s)",
                                     (RETENTION_HOURS,))
                    conn.commit()
                    last_cleanup = time.time()
        except Exception as exc:
            log.warning("housekeeping failed", extra={"error": str(exc)})


def heartbeat() -> None:
    producer = kafka.make_producer(**{"enable.idempotence": False, "acks": "1"})
    while not _stop.wait(HEARTBEAT_SECONDS):
        try:
            md = producer.list_topics(config.TOPIC_SHIPMENT_EVENTS, timeout=5)
            partitions = md.topics[config.TOPIC_SHIPMENT_EVENTS].partitions.keys()
            ts = now_ms()
            for p in partitions:
                env = envelope("Heartbeat", "system", "heartbeat", {"partition": p}, "event-publisher")
                env["published_at"] = ts
                producer.produce(config.TOPIC_SHIPMENT_EVENTS, value=json.dumps(env), partition=p, timestamp=ts)
            producer.flush(5)
        except Exception as exc:
            log.warning("heartbeat failed", extra={"error": str(exc)})


def _wait_for_tables() -> None:
    """Outbox tables are created by the owning services; wait until they exist."""
    pool = db.get_pool()
    pending = set(TABLES)
    while pending and not _stop.is_set():
        with pool.connection() as conn:
            for table in list(pending):
                if conn.execute("SELECT to_regclass(%s) AS t", (table,)).fetchone()["t"]:
                    pending.discard(table)
        if pending:
            log.info("waiting for outbox tables", extra={"pending": sorted(pending)})
            _stop.wait(3)


def start_workers() -> None:
    kafka.ensure_topics()
    _wait_for_tables()
    for table in TABLES:
        t = threading.Thread(target=relay_table, args=(table,), daemon=True, name=f"relay-{table}")
        t.start()
        _threads.append(t)
    for fn in (housekeeping, heartbeat):
        t = threading.Thread(target=fn, daemon=True, name=fn.__name__)
        t.start()
        _threads.append(t)
    _ready.set()
    log.info("event publisher running", extra={"tables": TABLES})


@asynccontextmanager
async def lifespan(_app):
    threading.Thread(target=start_workers, daemon=True, name="bootstrap").start()
    yield
    _stop.set()


app = create_app("Event Publisher", lifespan=lifespan, ready_check=lambda: _ready.is_set())


@app.get("/api/publisher/stats")
def publisher_stats():
    with db.get_pool().connection() as conn:
        backlog = {t: conn.execute(f"SELECT count(*) AS n FROM {t} WHERE published_at IS NULL").fetchone()["n"]
                   for t in TABLES}
    return {"pod": config.POD_NAME, "tables": TABLES, "backlog": backlog, **stats}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_config=None)
