"""Delivery Status Service — derives the current state of each shipment.

Runs as a StatefulSet; every pod is also a node of an embedded, leaderless
Dynamo-style replicated store (the course "concept" for this domain):

  Kafka `shipment-events`  ->  quorum read (R)  ->  apply event  ->  quorum write (W)
                                                         |
                                                         +-> SQS notification on important transitions

HTTP (via ALB):
  GET  /api/status/{tracking_id}?r=2              current state + quorum metadata
  GET  /api/status/{tracking_id}/replicas         raw versions + vector clocks on every replica
  POST /api/status/{tracking_id}/pod-upload-url   presigned S3 PUT for proof-of-delivery
  GET  /api/dynamo/ring                           membership, health, ownership, hints
  GET  /api/dynamo/placement/{key}                preference list for a key
  POST /api/dynamo/nodes/{node}/fail?seconds=60   simulate a node crash (demo)
  POST /api/dynamo/nodes/{node}/recover
  POST /api/dynamo/demo/conflict/{tracking_id}    create concurrent siblings (demo)
"""
import hashlib
import json
import logging
import os
import threading
import time
import uuid
from concurrent import futures
from contextlib import asynccontextmanager

import boto3
from botocore.config import Config
import grpc
import httpx
import uvicorn
from fastapi import HTTPException, Query
from prometheus_client import Counter
from pydantic import BaseModel

import dynamo_pb2_grpc as pb_grpc
from sc_common import config
from sc_common.kafka import ConsumerLoop
from sc_common.web import create_app

from . import shipment_state as ss
from .dynamo import DynamoNode, DynamoServicer, QuorumError, Store, vclock

log = logging.getLogger("delivery-status")

NODE_ID = config.env("NODE_ID", config.POD_NAME)
GRPC_PORT = config.env_int("DYNAMO_GRPC_PORT", 50052)
DATA_DIR = config.env("DATA_DIR", "/data")
QUEUE_URL = config.env("NOTIFICATION_QUEUE_URL", "")
RAW_BUCKET = config.env("RAW_BUCKET", "")
TRANSITIONS = Counter("shipment_status_transitions_total", "Derived status changes", ["status"])
NOTIFICATIONS = Counter("notifications_enqueued_total", "Notifications sent to SQS", ["status", "severity"])


def _peers() -> tuple[dict[str, str], dict[str, str]]:
    """Return (grpc_addr, http_base) per node.

    Kubernetes: DYNAMO_REPLICAS=5 + STATEFULSET=delivery-status + HEADLESS=delivery-status-hl
    Local:      DYNAMO_PEERS="n0=localhost:50060@8090,n1=localhost:50061@8091,..."
    """
    explicit = config.env("DYNAMO_PEERS", "")
    grpc_addr, http = {}, {}
    if explicit:
        for item in explicit.split(","):
            name, rest = item.split("=")
            addr, http_port = rest.split("@")
            grpc_addr[name] = addr
            http[name] = f"http://{addr.split(':')[0]}:{http_port}"
        return grpc_addr, http
    sts = config.env("STATEFULSET", "delivery-status")
    headless = config.env("HEADLESS_SERVICE", "delivery-status-hl")
    ns = config.env("POD_NAMESPACE", "supplychain")
    for i in range(config.env_int("DYNAMO_REPLICAS", 5)):
        name = f"{sts}-{i}"
        host = f"{name}.{headless}.{ns}.svc.cluster.local"
        grpc_addr[name] = f"{host}:{GRPC_PORT}"
        http[name] = f"http://{host}:8080"
    return grpc_addr, http


PEERS, PEER_HTTP = _peers()
os.makedirs(DATA_DIR, exist_ok=True)
node = DynamoNode(NODE_ID, PEERS, Store(os.path.join(DATA_DIR, f"{NODE_ID}.sqlite")),
                  n=config.env_int("DYNAMO_N", 3), r=config.env_int("DYNAMO_R", 2), w=config.env_int("DYNAMO_W", 2),
                  vnodes=config.env_int("DYNAMO_VNODES", 128))
sqs = boto3.client("sqs", region_name=config.AWS_REGION) if QUEUE_URL else None
# Regional, virtual-hosted, SigV4 URLs: a browser PUT to the global endpoint can get a 307
# redirect, which fails CORS - so presigned upload URLs always target s3.<region>.amazonaws.com.
s3 = boto3.client("s3", region_name=config.AWS_REGION, endpoint_url=f"https://s3.{config.AWS_REGION}.amazonaws.com",
                  config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"})) if RAW_BUCKET else None


def key_for(tracking_id: str) -> str:
    return f"shipment:{tracking_id}"


# -------------------------------------------------------------------- notifications
def notify(prev: dict | None, new: dict, cause_event_id: str) -> None:
    status = new["status"]
    if prev and prev["status"] == status:
        return
    TRANSITIONS.labels(status).inc()
    if status not in ss.NOTIFY_ON:
        return
    severity = "HIGH" if status in ss.HIGH_SEVERITY else "NORMAL"
    msg = {
        "notification_id": hashlib.sha1(f"{new['tracking_id']}|{status}|{cause_event_id}".encode()).hexdigest(),
        "tracking_id": new["tracking_id"],
        "status": status,
        "previous_status": prev["status"] if prev else None,
        "severity": severity,
        "retailer_id": new.get("retailer_id"),
        "retailer_email": new.get("retailer_email"),
        "supplier_id": new.get("supplier_id"),
        "carrier": new.get("carrier"),
        "location": new.get("last_location"),
        "eta": new.get("eta"),
        "delay_minutes": new.get("delay_minutes"),
        "message": _message(new),
        "occurred_at": int(time.time() * 1000),
        "source": "delivery-status",
    }
    if sqs:
        sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(msg), MessageAttributes={
            "severity": {"DataType": "String", "StringValue": severity},
            "status": {"DataType": "String", "StringValue": status}})
    else:
        log.info("notification (no SQS configured)", extra={"notification": msg})
    NOTIFICATIONS.labels(status, severity).inc()


def _message(s: dict) -> str:
    tid, st = s["tracking_id"], s["status"]
    return {
        "BOOKED": f"Shipment {tid} booked with {s.get('carrier')} ({s.get('origin')} -> {s.get('destination')}).",
        "PICKED_UP": f"Shipment {tid} was picked up by {s.get('carrier')}.",
        "OUT_FOR_DELIVERY": f"Shipment {tid} is out for delivery near {s.get('last_location')}.",
        "DELIVERED": f"Shipment {tid} was delivered at {s.get('last_location')}.",
        "DELAYED": f"Shipment {tid} is delayed ({s.get('delay_reason') or 'past promised date'}).",
        "EXCEPTION": f"Shipment {tid} has a carrier exception at {s.get('last_location')}.",
        "CANCELLED": f"Shipment {tid} was cancelled.",
        "RETURNED": f"Shipment {tid} is being returned to origin.",
    }.get(st, f"Shipment {tid} is now {st}.")


# -------------------------------------------------------------------- event handling
def handle_event(env: dict, _msg=None) -> None:
    if env.get("event_type") not in ss.HANDLED_EVENTS:
        return
    tracking_id = env.get("tracking_id") or env["aggregate_id"]
    key = key_for(tracking_id)
    ctx = node.coordinate_get(key)                         # quorum read (R)
    current = ss.merge_values([v["value"] for v in ctx.versions], tracking_id)
    if ss.has_event(current, env["event_id"]):
        return                                             # duplicate delivery -> idempotent no-op
    prev = ss.derive(current) if current["events"] else None
    updated = ss.apply_event(current, env)
    new = ss.derive(updated)
    notify(prev, new, env["event_id"])                     # SQS (Lambda de-duplicates on notification_id)
    clock = vclock.increment(vclock.merge(*[v["clock"] for v in ctx.versions]), node.node_id)
    node.coordinate_put(key, [{"clock": clock, "value": updated}])   # quorum write (W)


consumer = ConsumerLoop("delivery-status", [config.TOPIC_SHIPMENT_EVENTS], handle_event)
grpc_server: grpc.Server | None = None


@asynccontextmanager
async def lifespan(_app):
    global grpc_server
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    pb_grpc.add_DynamoNodeServicer_to_server(DynamoServicer(node), grpc_server)
    grpc_server.add_insecure_port(f"[::]:{GRPC_PORT}")
    grpc_server.start()
    node.start()
    if config.env_bool("CONSUME_KAFKA", True):
        consumer.start()
    log.info("dynamo node started", extra={"node": NODE_ID, "peers": list(PEERS), "n": node.n,
                                            "r": node.r, "w": node.w})
    yield
    consumer.stop()
    node.stop()
    grpc_server.stop(grace=3)


app = create_app("Delivery Status Service", lifespan=lifespan, ready_check=lambda: not node.is_failed())


# -------------------------------------------------------------------- API
@app.get("/api/status/{tracking_id}")
def get_status(tracking_id: str, r: int = Query(default=None, ge=1, le=5)):
    if node.is_failed():
        raise HTTPException(503, "node is in simulated failure")
    key = key_for(tracking_id)
    try:
        res = node.coordinate_get(key, r)
    except QuorumError as exc:
        raise HTTPException(503, str(exc))
    if not res.versions:
        raise HTTPException(404, "no status yet for this tracking id")
    value = ss.merge_values([v["value"] for v in res.versions], tracking_id)
    resolved = 0
    if res.siblings > 1:
        # Semantic reconciliation (the "client" merge in Dynamo): write back the union.
        clock = vclock.increment(vclock.merge(*[v["clock"] for v in res.versions]), node.node_id)
        node.coordinate_put(key, [{"clock": clock, "value": value}])
        resolved = res.siblings
        final_clock = clock
    else:
        final_clock = res.versions[0]["clock"]
    return {
        "state": ss.derive(value),
        "dynamo": {
            "coordinator": node.node_id,
            "n": node.n, "r": r or node.r, "w": node.w,
            "preference_list": res.preference_list,
            "replied": sorted(res.responses),
            "siblings_found": res.siblings,
            "siblings_reconciled": resolved,
            "read_repaired": res.repaired,
            "vector_clock": final_clock,
        },
    }


@app.get("/api/status/{tracking_id}/replicas")
def get_replicas(tracking_id: str):
    key = key_for(tracking_id)
    out = {}
    for peer in node.ring.preference_list(key, len(PEERS)):
        responded, versions = node.rpc_get(peer, key)
        out[peer] = {
            "in_preference_list": peer in node.ring.preference_list(key, node.n),
            "reachable": responded,
            "versions": [{"clock": v["clock"], "events": len(v["value"]["events"]),
                          "status": ss.derive(v["value"])["status"]} for v in (versions or [])],
        }
    return {"key": key, "replicas": out}


class UploadReq(BaseModel):
    filename: str = "proof-of-delivery.pdf"
    content_type: str = "application/pdf"


@app.post("/api/status/{tracking_id}/pod-upload-url")
def pod_upload_url(tracking_id: str, body: UploadReq):
    if not s3:
        raise HTTPException(501, "RAW_BUCKET not configured")
    safe = "".join(c for c in body.filename if c.isalnum() or c in "._-")[:80] or "pod.bin"
    key = f"pod/{tracking_id}/{uuid.uuid4().hex[:8]}-{safe}"
    url = s3.generate_presigned_url("put_object", Params={
        "Bucket": RAW_BUCKET, "Key": key, "ContentType": body.content_type}, ExpiresIn=600)
    return {"upload_url": url, "key": key, "content_type": body.content_type}


@app.get("/api/dynamo/ring")
def ring_info():
    info = node.describe()
    info["recent_keys"] = node.store.recent_keys(10)
    info["served_by"] = node.node_id
    return info


@app.get("/api/dynamo/placement/{tracking_id}")
def placement(tracking_id: str):
    key = key_for(tracking_id)
    walk = list(node.ring.walk(key))
    return {"key": key, "preference_list": walk[: node.n], "fallbacks": walk[node.n:],
            "healthy": {p: node.is_up(p) for p in walk}}


def _forward(target: str, path: str) -> dict:
    try:
        r = httpx.post(f"{PEER_HTTP[target]}{path}", timeout=3)
        return r.json()
    except httpx.HTTPError as exc:
        raise HTTPException(502, f"could not reach {target}: {exc}")


@app.post("/api/dynamo/nodes/{target}/fail")
def fail_node(target: str, seconds: int = Query(60, ge=5, le=900)):
    if target not in PEERS:
        raise HTTPException(404, "unknown node")
    if target != node.node_id:
        return _forward(target, f"/api/dynamo/nodes/{target}/fail?seconds={seconds}")
    node.simulate_failure(seconds)
    consumer.suspend()

    def _resume():
        time.sleep(seconds)
        if not node.is_failed():
            consumer.resume()
    threading.Thread(target=_resume, daemon=True).start()
    return {"node": node.node_id, "failed_for_seconds": seconds}


@app.post("/api/dynamo/nodes/{target}/recover")
def recover_node(target: str):
    if target not in PEERS:
        raise HTTPException(404, "unknown node")
    if target != node.node_id:
        return _forward(target, f"/api/dynamo/nodes/{target}/recover")
    node.recover()
    consumer.resume()
    return {"node": node.node_id, "recovered": True}


@app.post("/api/dynamo/demo/conflict/{tracking_id}")
def demo_conflict(tracking_id: str):
    """Two coordinators write concurrently from the same context -> siblings on every replica."""
    key = key_for(tracking_id)
    ctx = node.coordinate_get(key)
    base = ss.merge_values([v["value"] for v in ctx.versions], tracking_id)
    base_clock = vclock.merge(*[v["clock"] for v in ctx.versions]) if ctx.versions else {}
    pref = node.ring.preference_list(key, node.n)
    writers = (pref + [node.node_id])[:2]
    written = []
    for i, writer in enumerate(writers):
        env = {"event_id": str(uuid.uuid4()), "event_type": "DemoNote", "occurred_at": int(time.time() * 1000),
               "payload": {"note": f"concurrent update #{i + 1} coordinated by {writer}"}, "source": "demo"}
        value = ss.apply_event(base, env)
        version = {"clock": vclock.increment(base_clock, writer), "value": value}
        for replica in pref:
            node.rpc_put(replica, key, [version])
        written.append({"coordinator": writer, "clock": version["clock"]})
    return {"key": key, "concurrent_writes": written, "replicas": pref,
            "next": f"GET /api/status/{tracking_id}/replicas shows 2 siblings; "
                    f"GET /api/status/{tracking_id} reconciles them"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=config.env_int("HTTP_PORT", 8080), log_config=None)
