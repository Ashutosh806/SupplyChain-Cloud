"""Shipment Analytics Service — query side.

The computation runs as an Apache Flink job on Amazon EMR (analytics/flink-job):
it consumes `shipment-events`, keeps keyed state per shipment and computes
1-minute event-time tumbling windows, then publishes to
`shipment-analytics-results`.

This API consumes those results, persists them in DynamoDB (managed NoSQL,
high-throughput semi-structured data), exposes them over REST and as Prometheus
gauges for Grafana.

Result records (from Flink):
  {"type": "carrier_window",  "carrier", "window_start", "window_end", "events",
   "unique_shipments", "delayed", "exceptions", "delivered", "avg_ingest_lag_ms", "max_ingest_lag_ms"}
  {"type": "shipment_transit", "tracking_id", "carrier", "transit_hours", "delay_minutes",
   "on_time", "picked_up_at", "delivered_at", "promised_delivery"}
  {"type": "delivery_window", "window_start", "window_end", "deliveries", "avg_transit_hours",
   "on_time_ratio", "avg_delay_minutes"}
"""
import logging
import threading
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from decimal import Decimal

import boto3
import uvicorn
from boto3.dynamodb.conditions import Key
from prometheus_client import Counter, Gauge

from sc_common import config
from sc_common.kafka import ConsumerLoop
from sc_common.web import create_app

log = logging.getLogger("analytics")
TABLE = config.env("ANALYTICS_TABLE", "")
ddb = boto3.resource("dynamodb", region_name=config.AWS_REGION).Table(TABLE) if TABLE else None

G_EVENTS = Gauge("analytics_window_events", "Carrier events in the last closed 1-min window", ["carrier"])
G_UNIQUE = Gauge("analytics_window_unique_shipments", "Unique shipments in last window", ["carrier"])
G_DELAYED = Gauge("analytics_window_delayed", "DELAYED/EXCEPTION events in last window", ["carrier"])
G_LAG = Gauge("analytics_window_avg_ingest_lag_ms", "Avg carrier-scan to ingest lag in last window", ["carrier"])
G_TRANSIT = Gauge("analytics_avg_transit_hours", "Average transit time of deliveries in last window")
G_ONTIME = Gauge("analytics_on_time_ratio", "On-time delivery ratio in last window")
RESULTS = Counter("analytics_results_consumed_total", "Flink results consumed", ["type"])

_lock = threading.Lock()
recent = {
    "carrier_window": defaultdict(lambda: deque(maxlen=120)),
    "shipment_transit": deque(maxlen=500),
    "delivery_window": deque(maxlen=120),
}


def _dec(obj):
    if isinstance(obj, float):
        return Decimal(str(round(obj, 4)))
    if isinstance(obj, dict):
        return {k: _dec(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_dec(v) for v in obj]
    return obj


def _undec(obj):
    if isinstance(obj, Decimal):
        return float(obj) if obj % 1 else int(obj)
    if isinstance(obj, dict):
        return {k: _undec(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_undec(v) for v in obj]
    return obj


def handle_result(rec: dict, _msg=None) -> None:
    rtype = rec.get("type")
    if rtype == "carrier_window":
        pk, sk = f"carrier_window#{rec['carrier']}", int(rec["window_start"])
        with _lock:
            recent["carrier_window"][rec["carrier"]].append(rec)
        G_EVENTS.labels(rec["carrier"]).set(rec["events"])
        G_UNIQUE.labels(rec["carrier"]).set(rec["unique_shipments"])
        G_DELAYED.labels(rec["carrier"]).set(rec.get("delayed", 0) + rec.get("exceptions", 0))
        G_LAG.labels(rec["carrier"]).set(rec.get("avg_ingest_lag_ms", 0))
    elif rtype == "shipment_transit":
        pk, sk = "shipment_transit", int(rec["delivered_at"])
        with _lock:
            recent["shipment_transit"].append(rec)
    elif rtype == "delivery_window":
        pk, sk = "delivery_window", int(rec["window_start"])
        with _lock:
            recent["delivery_window"].append(rec)
        G_TRANSIT.set(rec.get("avg_transit_hours") or 0)
        G_ONTIME.set(rec.get("on_time_ratio") or 0)
    else:
        return
    RESULTS.labels(rtype).inc()
    if ddb:
        item = _dec(rec) | {"pk": pk, "sk": sk}
        if rtype == "shipment_transit":
            item["sk"] = sk * 1000 + (hash(rec["tracking_id"]) % 1000)   # keep keys unique per ms
        ddb.put_item(Item=item)


consumer = ConsumerLoop("analytics-api", [config.TOPIC_ANALYTICS_RESULTS], handle_result, offset_reset="earliest")


@asynccontextmanager
async def lifespan(_app):
    consumer.start()
    yield
    consumer.stop()


app = create_app("Shipment Analytics API", lifespan=lifespan)


def _query(pk: str, limit: int) -> list[dict]:
    if not ddb:
        return []
    resp = ddb.query(KeyConditionExpression=Key("pk").eq(pk), ScanIndexForward=False, Limit=limit)
    return [_undec(i) for i in resp.get("Items", [])]


@app.get("/api/analytics/windows")
def windows(carrier: str | None = None, limit: int = 30):
    limit = min(limit, 200)
    with _lock:
        carriers = [carrier] if carrier else list(recent["carrier_window"])
        mem = {c: list(recent["carrier_window"].get(c, []))[-limit:][::-1] for c in carriers}
    if ddb and carrier and not mem.get(carrier):
        mem[carrier] = _query(f"carrier_window#{carrier}", limit)
    return mem


@app.get("/api/analytics/transit")
def transit(limit: int = 50):
    with _lock:
        rows = list(recent["shipment_transit"])[-limit:][::-1]
    return rows or _query("shipment_transit", min(limit, 200))


@app.get("/api/analytics/deliveries")
def deliveries(limit: int = 30):
    with _lock:
        rows = list(recent["delivery_window"])[-limit:][::-1]
    return rows or _query("delivery_window", min(limit, 200))


@app.get("/api/analytics/summary")
def summary():
    with _lock:
        per_carrier = {}
        for carrier, wins in recent["carrier_window"].items():
            last = list(wins)[-15:]
            per_carrier[carrier] = {
                "windows": len(last),
                "events": sum(w["events"] for w in last),
                "unique_shipments_last_window": last[-1]["unique_shipments"] if last else 0,
                "delayed": sum(w.get("delayed", 0) + w.get("exceptions", 0) for w in last),
                "avg_ingest_lag_ms": round(sum(w.get("avg_ingest_lag_ms", 0) for w in last) / len(last), 1) if last else None,
            }
        transits = list(recent["shipment_transit"])
        dw = list(recent["delivery_window"])
    on_time = [t for t in transits if t.get("on_time")]
    return {
        "carriers_last_15_windows": per_carrier,
        "deliveries_observed": len(transits),
        "avg_transit_hours": round(sum(t["transit_hours"] for t in transits) / len(transits), 2) if transits else None,
        "on_time_ratio": round(len(on_time) / len(transits), 3) if transits else None,
        "last_delivery_window": dw[-1] if dw else None,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_config=None)
