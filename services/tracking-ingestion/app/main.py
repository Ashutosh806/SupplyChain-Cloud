"""Tracking Ingestion Service — receives carrier status events.

POST /api/tracking/events accepts a single event or {"events": [...]} from
carrier webhooks / scanners. For each event it:
  * validates the payload and that the tracking id exists (REST -> Booking, cached)
  * de-duplicates on carrier_event_id (unique key) so carrier retries are harmless
  * writes tracking_events + outbox(CarrierStatusReported) in ONE transaction
  * buffers the raw payload and flushes NDJSON.gz batches to S3 (raw data lake)
It is the main high-throughput entry point and is scaled by an HPA.
"""
import gzip
import json
import logging
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import boto3
import httpx
import uvicorn
from cachetools import TTLCache
from fastapi import HTTPException
from prometheus_client import Counter, Gauge
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field, field_validator

from sc_common import config, db, outbox
from sc_common.events import CARRIER_STATUSES
from sc_common.web import create_app

SCHEMA = "ingestion"
SOURCE = "tracking-ingestion"
RAW_BUCKET = config.env("RAW_BUCKET", "")
VALIDATE = config.env_bool("VALIDATE_TRACKING_IDS", True)
log = logging.getLogger("ingestion")

EVENTS = Counter("tracking_events_total", "Carrier events by outcome", ["outcome", "status"])
RAW_BUFFER = Gauge("raw_archive_buffer_size", "Raw events waiting to be flushed to S3")
RAW_FLUSHED = Counter("raw_archive_objects_total", "NDJSON objects written to S3")

DDL = [
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.tracking_events (
        carrier_event_id TEXT PRIMARY KEY,
        tracking_id      TEXT NOT NULL,
        carrier          TEXT NOT NULL,
        status           TEXT NOT NULL,
        location         TEXT,
        event_time       TIMESTAMPTZ NOT NULL,
        received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
        raw              JSONB NOT NULL
    )""",
    f"CREATE INDEX IF NOT EXISTS tracking_events_tid_idx ON {SCHEMA}.tracking_events (tracking_id, event_time)",
    *outbox.outbox_ddl(SCHEMA),
]


class CarrierEvent(BaseModel):
    tracking_id: str = Field(min_length=5, max_length=40)
    carrier: str = Field(min_length=2, max_length=30)
    status: str
    location: str | None = None
    event_time: datetime | int | None = None   # ISO-8601 or epoch ms; defaults to now
    carrier_event_id: str | None = None
    eta: datetime | int | None = None
    note: str | None = Field(default=None, max_length=500)

    @field_validator("status")
    @classmethod
    def _status(cls, v: str) -> str:
        v = v.upper()
        if v not in CARRIER_STATUSES:
            raise ValueError(f"status must be one of {CARRIER_STATUSES}")
        return v


class Batch(BaseModel):
    events: list[CarrierEvent] = Field(min_length=1, max_length=500)


def _ms(v) -> int | None:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return int(v.timestamp() * 1000)


# ------------------------------------------------------------ raw archive -> S3
class RawArchiver:
    def __init__(self, bucket: str, flush_every: float = 10.0, max_batch: int = 2000):
        self.bucket = bucket
        self.flush_every = flush_every
        self.max_batch = max_batch
        self.buf: list[str] = []
        self.lock = threading.Lock()
        self.s3 = boto3.client("s3", region_name=config.AWS_REGION) if bucket else None
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="raw-archiver")

    def add(self, record: dict) -> None:
        if not self.s3:
            return
        with self.lock:
            self.buf.append(json.dumps(record, default=str))
            RAW_BUFFER.set(len(self.buf))
            full = len(self.buf) >= self.max_batch
        if full:
            self.flush()

    def flush(self) -> None:
        with self.lock:
            batch, self.buf = self.buf, []
            RAW_BUFFER.set(0)
        if not batch:
            return
        now = datetime.now(timezone.utc)
        key = (f"raw/carrier-events/dt={now:%Y-%m-%d}/hour={now:%H}/"
               f"{config.POD_NAME}-{int(now.timestamp() * 1000)}.ndjson.gz")
        try:
            self.s3.put_object(Bucket=self.bucket, Key=key, ContentType="application/x-ndjson",
                               ContentEncoding="gzip", Body=gzip.compress("\n".join(batch).encode()))
            RAW_FLUSHED.inc()
            log.info("raw batch archived", extra={"key": key, "events": len(batch)})
        except Exception as exc:
            log.error("raw archive failed; re-queueing", extra={"error": str(exc)})
            with self.lock:
                self.buf = batch + self.buf

    def _loop(self):
        while not self._stop.wait(self.flush_every):
            self.flush()

    def start(self):
        if self.s3:
            self.thread.start()

    def stop(self):
        self._stop.set()
        if self.s3:
            self.flush()


archiver = RawArchiver(RAW_BUCKET)
_known = TTLCache(maxsize=100_000, ttl=600)      # tracking id -> carrier
_unknown = TTLCache(maxsize=10_000, ttl=10)
_http: httpx.Client | None = None


@asynccontextmanager
async def lifespan(_app):
    global _http
    db.migrate(SCHEMA, DDL)
    _http = httpx.Client(timeout=2.0)
    archiver.start()
    yield
    archiver.stop()
    _http.close()


app = create_app("Tracking Ingestion Service", lifespan=lifespan, ready_check=db.ping)


_cache_lock = threading.Lock()


def _tracking_exists(tracking_id: str) -> bool:
    if not VALIDATE:
        return True
    with _cache_lock:
        if tracking_id in _known:
            return True
        if tracking_id in _unknown:
            return False
    try:
        r = _http.get(f"{config.BOOKING_URL}/internal/shipments/{tracking_id}/exists")
    except httpx.HTTPError:
        return True  # fail open: never drop carrier data because booking is briefly down
    with _cache_lock:
        if r.status_code == 200:
            _known[tracking_id] = r.json().get("carrier")
            return True
        _unknown[tracking_id] = True
    return False


def ingest(events: list[CarrierEvent]) -> dict:
    accepted, duplicates, rejected = [], 0, []
    now = int(time.time() * 1000)
    valid = []
    for ev in events:
        if not _tracking_exists(ev.tracking_id):
            rejected.append({"tracking_id": ev.tracking_id, "reason": "unknown tracking id"})
            EVENTS.labels("rejected", ev.status).inc()
            continue
        valid.append(ev)
    if valid:
        with db.get_pool().connection() as conn, conn.cursor() as cur:
            for ev in valid:
                ceid = ev.carrier_event_id or f"{ev.carrier.upper()}-{uuid.uuid4().hex}"
                event_time = _ms(ev.event_time) or now
                raw = ev.model_dump(mode="json") | {"carrier_event_id": ceid, "received_at": now}
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.tracking_events
                        (carrier_event_id, tracking_id, carrier, status, location, event_time, raw)
                        VALUES (%s,%s,%s,%s,%s,to_timestamp(%s/1000.0),%s) ON CONFLICT DO NOTHING""",
                    (ceid, ev.tracking_id, ev.carrier.upper(), ev.status, ev.location, event_time, Jsonb(raw)))
                if cur.rowcount == 0:
                    duplicates += 1
                    EVENTS.labels("duplicate", ev.status).inc()
                    continue
                outbox.add_event(cur, SCHEMA, "CarrierStatusReported", "shipment", ev.tracking_id, {
                    "carrier_event_id": ceid, "carrier": ev.carrier.upper(), "status": ev.status,
                    "location": ev.location, "event_time": event_time, "eta": _ms(ev.eta),
                    "note": ev.note, "received_at": now,
                }, SOURCE, tracking_id=ev.tracking_id, occurred_at=event_time)
                accepted.append(ceid)
                EVENTS.labels("accepted", ev.status).inc()
                archiver.add(raw)
            conn.commit()
    return {"accepted": len(accepted), "duplicates": duplicates, "rejected": rejected,
            "carrier_event_ids": accepted}


@app.post("/api/tracking/events", status_code=202)
def post_events(body: CarrierEvent | Batch):
    events = body.events if isinstance(body, Batch) else [body]
    result = ingest(events)
    if not result["accepted"] and not result["duplicates"] and result["rejected"]:
        raise HTTPException(422, result)
    return result


@app.get("/api/tracking/events/{tracking_id}")
def history(tracking_id: str):
    with db.get_pool().connection() as conn:
        return conn.execute(
            f"SELECT carrier_event_id, tracking_id, carrier, status, location, event_time, received_at "
            f"FROM {SCHEMA}.tracking_events WHERE tracking_id=%s ORDER BY event_time", (tracking_id,)).fetchall()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_config=None)
