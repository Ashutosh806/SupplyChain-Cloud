"""Shipment Booking Service — creates shipments and assigns tracking identifiers.

Flow for POST /api/shipments:
  1. REST  -> Supplier Service: supplier must exist and be ACTIVE
  2. gRPC  -> Inventory Service: ReserveStock (idempotent on tracking id)
  3. one RDS transaction: INSERT shipment + INSERT outbox(ShipmentBooked)
  4. if step 3 fails, compensate with gRPC ReleaseStock (saga-style)
The Event Publisher later relays ShipmentBooked to Kafka.

Also serves the demo "Control Tower" web UI at `/`.
"""
import logging
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import grpc
import httpx
import uvicorn
from fastapi import HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from psycopg.types.json import Jsonb

import inventory_pb2 as pb
import inventory_pb2_grpc as pb_grpc
from sc_common import config, db, outbox
from sc_common.web import create_app

SCHEMA = "booking"
SOURCE = "booking-service"
log = logging.getLogger("booking")
STATIC = Path(__file__).parent / "static"
CARRIER_SLA_DAYS = {"BLUEDART": 2, "DELHIVERY": 3, "DHL": 4, "FEDEX": 3, "ECOMEXPRESS": 5}
_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

DDL = [
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.shipments (
        tracking_id       TEXT PRIMARY KEY,
        supplier_id       TEXT NOT NULL,
        retailer_id       TEXT NOT NULL,
        retailer_email    TEXT,
        origin            TEXT NOT NULL,
        destination       TEXT NOT NULL,
        carrier           TEXT NOT NULL,
        items             JSONB NOT NULL,
        reservation_id    TEXT,
        status            TEXT NOT NULL DEFAULT 'BOOKED',
        promised_delivery TIMESTAMPTZ NOT NULL,
        created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    f"CREATE INDEX IF NOT EXISTS shipments_created_idx ON {SCHEMA}.shipments (created_at DESC)",
    *outbox.outbox_ddl(SCHEMA),
]

_channel: grpc.Channel | None = None
_inventory: pb_grpc.InventoryServiceStub | None = None
_http: httpx.Client | None = None


@asynccontextmanager
async def lifespan(_app):
    global _channel, _inventory, _http
    db.migrate(SCHEMA, DDL)
    _channel = grpc.insecure_channel(config.INVENTORY_GRPC, options=[
        ("grpc.keepalive_time_ms", 30000), ("grpc.lb_policy_name", "round_robin")])
    _inventory = pb_grpc.InventoryServiceStub(_channel)
    _http = httpx.Client(timeout=3.0)
    yield
    _channel.close()
    _http.close()


app = create_app("Shipment Booking Service", lifespan=lifespan, ready_check=db.ping, root_info=False)


class ItemLine(BaseModel):
    sku: str
    quantity: int = Field(gt=0, le=100000)


class ShipmentIn(BaseModel):
    supplier_id: str
    retailer_id: str
    retailer_email: str | None = None
    origin: str = "Mumbai, IN"
    destination: str = "Bengaluru, IN"
    carrier: str = "DELHIVERY"
    items: list[ItemLine] = Field(min_length=1)
    promised_delivery: datetime | None = None


def new_tracking_id() -> str:
    return "SCT" + "".join(secrets.choice(_ALPHABET) for _ in range(10))


def _check_supplier(supplier_id: str) -> dict:
    try:
        r = _http.get(f"{config.SUPPLIER_URL}/api/suppliers/{supplier_id}")  # REST call
    except httpx.HTTPError as exc:
        raise HTTPException(503, f"supplier service unavailable: {exc}")
    if r.status_code == 404:
        raise HTTPException(422, f"unknown supplier {supplier_id}")
    r.raise_for_status()
    supplier = r.json()
    if supplier["status"] != "ACTIVE":
        raise HTTPException(422, f"supplier {supplier_id} is {supplier['status']}")
    return supplier


@app.post("/api/shipments", status_code=201)
def book_shipment(body: ShipmentIn):
    supplier = _check_supplier(body.supplier_id)
    tracking_id = new_tracking_id()
    carrier = body.carrier.upper()
    promised = body.promised_delivery or (
        datetime.now(timezone.utc) + timedelta(days=CARRIER_SLA_DAYS.get(carrier, 3)))

    try:
        res = _inventory.ReserveStock(pb.ReserveRequest(  # gRPC call
            order_ref=tracking_id, supplier_id=body.supplier_id,
            lines=[pb.Line(sku=i.sku, quantity=i.quantity) for i in body.items]), timeout=5)
    except grpc.RpcError as exc:
        raise HTTPException(503, f"inventory service unavailable: {exc.code().name}")
    if not res.ok:
        raise HTTPException(409, {"error": "insufficient_stock",
                                  "shortages": [{"sku": s.sku, "missing": s.quantity} for s in res.shortages]})

    items = [i.model_dump() for i in body.items]
    try:
        with db.get_pool().connection() as conn, conn.cursor() as cur:
            cur.execute(
                f"""INSERT INTO {SCHEMA}.shipments (tracking_id, supplier_id, retailer_id, retailer_email, origin,
                     destination, carrier, items, reservation_id, promised_delivery)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
                (tracking_id, body.supplier_id, body.retailer_id, body.retailer_email, body.origin,
                 body.destination, carrier, Jsonb(items), res.reservation_id, promised))
            row = cur.fetchone()
            outbox.add_event(cur, SCHEMA, "ShipmentBooked", "shipment", tracking_id, {
                "tracking_id": tracking_id, "supplier_id": body.supplier_id, "supplier_name": supplier["name"],
                "retailer_id": body.retailer_id, "retailer_email": body.retailer_email,
                "origin": body.origin, "destination": body.destination, "carrier": carrier,
                "items": items, "reservation_id": res.reservation_id,
                "promised_delivery": int(promised.timestamp() * 1000),
            }, SOURCE, tracking_id=tracking_id)
            conn.commit()
    except Exception:
        log.exception("booking failed, compensating reservation", extra={"tracking_id": tracking_id})
        try:
            _inventory.ReleaseStock(pb.ReleaseRequest(reservation_id=res.reservation_id,
                                                      reason="booking failed"), timeout=5)
        finally:
            raise HTTPException(500, "booking failed; reservation released")
    log.info("shipment booked", extra={"tracking_id": tracking_id, "carrier": carrier})
    return row


@app.get("/api/shipments")
def list_shipments(limit: int = 50):
    with db.get_pool().connection() as conn:
        return conn.execute(f"SELECT * FROM {SCHEMA}.shipments ORDER BY created_at DESC LIMIT %s",
                            (min(limit, 500),)).fetchall()


@app.get("/api/shipments/{tracking_id}")
def get_shipment(tracking_id: str):
    with db.get_pool().connection() as conn:
        row = conn.execute(f"SELECT * FROM {SCHEMA}.shipments WHERE tracking_id=%s", (tracking_id,)).fetchone()
    if not row:
        raise HTTPException(404, "shipment not found")
    return row


@app.post("/api/shipments/{tracking_id}/cancel")
def cancel_shipment(tracking_id: str):
    with db.get_pool().connection() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {SCHEMA}.shipments WHERE tracking_id=%s FOR UPDATE", (tracking_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "shipment not found")
        if row["status"] == "CANCELLED":
            return row
        cur.execute(f"UPDATE {SCHEMA}.shipments SET status='CANCELLED', updated_at=now() "
                    "WHERE tracking_id=%s RETURNING *", (tracking_id,))
        row = cur.fetchone()
        outbox.add_event(cur, SCHEMA, "ShipmentCancelled", "shipment", tracking_id,
                         {"tracking_id": tracking_id, "reason": "cancelled by user"}, SOURCE,
                         tracking_id=tracking_id)
        conn.commit()
    try:
        _inventory.ReleaseStock(pb.ReleaseRequest(order_ref=tracking_id, reason="shipment cancelled"), timeout=5)
    except grpc.RpcError:
        log.warning("release failed; reservation will be reconciled", extra={"tracking_id": tracking_id})
    return row


@app.get("/internal/shipments/{tracking_id}/exists")
def exists(tracking_id: str):
    """Cheap existence check used by the Tracking Ingestion Service (internal REST)."""
    with db.get_pool().connection() as conn:
        row = conn.execute(f"SELECT carrier, status FROM {SCHEMA}.shipments WHERE tracking_id=%s",
                           (tracking_id,)).fetchone()
    if not row:
        raise HTTPException(404, "unknown tracking id")
    return {"tracking_id": tracking_id, **row}


# ------------------------------------------------------------------ demo web UI
@app.get("/", include_in_schema=False)
def ui():
    return FileResponse(STATIC / "index.html")


@app.get("/api/config", include_in_schema=False)
def ui_config():
    return {
        "notification_url": os.environ.get("NOTIFICATION_URL", ""),
        "grafana_path": "/grafana/",
        "region": config.AWS_REGION,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_config=None)
