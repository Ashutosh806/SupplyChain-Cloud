"""Inventory Service — tracks goods available for dispatch.

* REST (via ALB) for stock management.
* gRPC `InventoryService` (port 50051) for synchronous reservations made by the
  Shipment Booking Service.
* Kafka consumer: when a carrier reports PICKED_UP the reserved stock is
  dispatched (on_hand and reserved both decrease).
"""
import logging
import time
import uuid
from concurrent import futures
from contextlib import asynccontextmanager

import grpc
import uvicorn
from fastapi import HTTPException
from pydantic import BaseModel, Field

import inventory_pb2 as pb
import inventory_pb2_grpc as pb_grpc
from sc_common import config, db, metrics, outbox
from sc_common.kafka import ConsumerLoop
from sc_common.web import create_app

SCHEMA = "inventory"
SOURCE = "inventory-service"
GRPC_PORT = int(config.env("GRPC_PORT", "50051"))
log = logging.getLogger("inventory")

DDL = [
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.items (
        sku               TEXT PRIMARY KEY,
        supplier_id       TEXT NOT NULL,
        name              TEXT NOT NULL,
        warehouse         TEXT NOT NULL DEFAULT 'WH-MUMBAI-1',
        quantity_on_hand  INTEGER NOT NULL DEFAULT 0 CHECK (quantity_on_hand >= 0),
        quantity_reserved INTEGER NOT NULL DEFAULT 0 CHECK (quantity_reserved >= 0),
        reorder_level     INTEGER NOT NULL DEFAULT 10,
        updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.reservations (
        reservation_id TEXT PRIMARY KEY,
        order_ref      TEXT NOT NULL UNIQUE,
        supplier_id    TEXT,
        lines          JSONB NOT NULL,
        status         TEXT NOT NULL DEFAULT 'RESERVED',
        created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    *outbox.outbox_ddl(SCHEMA),
]


# --------------------------------------------------------------------------- gRPC
class InventoryServicer(pb_grpc.InventoryServiceServicer):
    def ReserveStock(self, request, context):
        start = time.perf_counter()
        code = "OK"
        try:
            return reserve(request.order_ref, request.supplier_id,
                           [(l.sku, l.quantity) for l in request.lines])
        except Exception as exc:
            code = "INTERNAL"
            log.exception("reserve failed")
            context.abort(grpc.StatusCode.INTERNAL, str(exc))
        finally:
            metrics.GRPC_REQUESTS.labels("ReserveStock", code).inc()
            metrics.GRPC_LATENCY.labels("ReserveStock").observe(time.perf_counter() - start)

    def ReleaseStock(self, request, context):
        start = time.perf_counter()
        try:
            ok, msg = release(request.reservation_id or None, request.order_ref or None, request.reason)
            return pb.ReleaseResponse(ok=ok, message=msg)
        finally:
            metrics.GRPC_REQUESTS.labels("ReleaseStock", "OK").inc()
            metrics.GRPC_LATENCY.labels("ReleaseStock").observe(time.perf_counter() - start)

    def GetAvailability(self, request, context):
        with db.get_pool().connection() as conn:
            row = conn.execute(f"SELECT * FROM {SCHEMA}.items WHERE sku=%s", (request.sku,)).fetchone()
        metrics.GRPC_REQUESTS.labels("GetAvailability", "OK").inc()
        if not row:
            return pb.AvailabilityResponse(sku=request.sku, found=False)
        return pb.AvailabilityResponse(
            sku=row["sku"], on_hand=row["quantity_on_hand"], reserved=row["quantity_reserved"],
            available=row["quantity_on_hand"] - row["quantity_reserved"], found=True)


def reserve(order_ref: str, supplier_id: str, lines: list[tuple[str, int]]) -> pb.ReserveResponse:
    with db.get_pool().connection() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {SCHEMA}.reservations WHERE order_ref=%s", (order_ref,))
        existing = cur.fetchone()
        if existing:  # idempotent retry
            return pb.ReserveResponse(ok=existing["status"] != "RELEASED",
                                      reservation_id=existing["reservation_id"], message="already reserved")
        merged: dict[str, int] = {}
        for sku, qty in lines:
            merged[sku] = merged.get(sku, 0) + qty
        skus = sorted(merged)  # deterministic lock order avoids deadlocks
        cur.execute(f"SELECT * FROM {SCHEMA}.items WHERE sku = ANY(%s) ORDER BY sku FOR UPDATE", (skus,))
        rows = {r["sku"]: r for r in cur.fetchall()}
        shortages = []
        for sku in skus:
            row = rows.get(sku)
            available = (row["quantity_on_hand"] - row["quantity_reserved"]) if row else 0
            if available < merged[sku]:
                shortages.append(pb.Line(sku=sku, quantity=merged[sku] - available))
        if shortages:
            conn.rollback()
            return pb.ReserveResponse(ok=False, message="insufficient stock", shortages=shortages)
        reservation_id = "RSV-" + uuid.uuid4().hex[:12].upper()
        for sku in skus:
            cur.execute(f"UPDATE {SCHEMA}.items SET quantity_reserved = quantity_reserved + %s, updated_at=now() "
                        "WHERE sku=%s RETURNING *", (merged[sku], sku))
            row = cur.fetchone()
            if row["quantity_on_hand"] - row["quantity_reserved"] <= row["reorder_level"]:
                outbox.add_event(cur, SCHEMA, "LowStockDetected", "sku", sku, {
                    "sku": sku, "available": row["quantity_on_hand"] - row["quantity_reserved"],
                    "reorder_level": row["reorder_level"]}, SOURCE)
        lines_json = [{"sku": s, "quantity": merged[s]} for s in skus]
        cur.execute(f"INSERT INTO {SCHEMA}.reservations (reservation_id, order_ref, supplier_id, lines) "
                    "VALUES (%s,%s,%s,%s)", (reservation_id, order_ref, supplier_id, outbox.Jsonb(lines_json)))
        outbox.add_event(cur, SCHEMA, "InventoryReserved", "shipment", order_ref,
                         {"reservation_id": reservation_id, "lines": lines_json}, SOURCE, tracking_id=order_ref)
        conn.commit()
    return pb.ReserveResponse(ok=True, reservation_id=reservation_id, message="reserved")


def _transition(reservation_id: str | None, order_ref: str | None, new_status: str, reason: str) -> tuple[bool, str]:
    """RESERVED -> RELEASED (stock returns) or RESERVED -> DISPATCHED (stock leaves)."""
    with db.get_pool().connection() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {SCHEMA}.reservations WHERE reservation_id=%s OR order_ref=%s FOR UPDATE",
                    (reservation_id, order_ref))
        res = cur.fetchone()
        if not res:
            return False, "reservation not found"
        if res["status"] != "RESERVED":
            return True, f"already {res['status'].lower()}"
        for line in sorted(res["lines"], key=lambda l: l["sku"]):
            if new_status == "RELEASED":
                cur.execute(f"UPDATE {SCHEMA}.items SET quantity_reserved = GREATEST(quantity_reserved - %s, 0), "
                            "updated_at=now() WHERE sku=%s", (line["quantity"], line["sku"]))
            else:
                cur.execute(f"UPDATE {SCHEMA}.items SET quantity_reserved = GREATEST(quantity_reserved - %s, 0), "
                            "quantity_on_hand = GREATEST(quantity_on_hand - %s, 0), updated_at=now() WHERE sku=%s",
                            (line["quantity"], line["quantity"], line["sku"]))
        cur.execute(f"UPDATE {SCHEMA}.reservations SET status=%s, updated_at=now() WHERE reservation_id=%s",
                    (new_status, res["reservation_id"]))
        event_type = "InventoryReleased" if new_status == "RELEASED" else "InventoryDispatched"
        outbox.add_event(cur, SCHEMA, event_type, "shipment", res["order_ref"],
                         {"reservation_id": res["reservation_id"], "lines": res["lines"], "reason": reason},
                         SOURCE, tracking_id=res["order_ref"])
        conn.commit()
    return True, new_status.lower()


def release(reservation_id, order_ref, reason=""):
    return _transition(reservation_id, order_ref, "RELEASED", reason or "released")


def on_event(event: dict, _msg) -> None:
    if event.get("event_type") == "CarrierStatusReported" and event["payload"].get("status") == "PICKED_UP":
        _transition(None, event.get("tracking_id"), "DISPATCHED", "picked up by carrier")


# --------------------------------------------------------------------------- app
grpc_server: grpc.Server | None = None
consumer: ConsumerLoop | None = None


@asynccontextmanager
async def lifespan(_app):
    global grpc_server, consumer
    db.migrate(SCHEMA, DDL)
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb_grpc.add_InventoryServiceServicer_to_server(InventoryServicer(), grpc_server)
    grpc_server.add_insecure_port(f"[::]:{GRPC_PORT}")
    grpc_server.start()
    log.info("gRPC server started", extra={"port": GRPC_PORT})
    consumer = ConsumerLoop("inventory-service", [config.TOPIC_SHIPMENT_EVENTS], on_event)
    consumer.start()
    yield
    consumer.stop()
    grpc_server.stop(grace=5)


app = create_app("Inventory Service", lifespan=lifespan, ready_check=db.ping)


class ItemIn(BaseModel):
    sku: str = Field(min_length=2, max_length=40)
    supplier_id: str
    name: str
    warehouse: str = "WH-MUMBAI-1"
    quantity: int = Field(ge=0)
    reorder_level: int = Field(default=10, ge=0)


class AdjustIn(BaseModel):
    delta: int


def _out(row: dict) -> dict:
    row = dict(row)
    row["available"] = row["quantity_on_hand"] - row["quantity_reserved"]
    return row


@app.post("/api/inventory/items", status_code=201)
def upsert_item(body: ItemIn):
    """Create an item or add stock to an existing one."""
    with db.get_pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {SCHEMA}.items (sku, supplier_id, name, warehouse, quantity_on_hand, reorder_level)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (sku) DO UPDATE SET quantity_on_hand = {SCHEMA}.items.quantity_on_hand + EXCLUDED.quantity_on_hand,
                    name = EXCLUDED.name, warehouse = EXCLUDED.warehouse, reorder_level = EXCLUDED.reorder_level,
                    updated_at = now()
                RETURNING *""",
            (body.sku, body.supplier_id, body.name, body.warehouse, body.quantity, body.reorder_level))
        row = cur.fetchone()
        outbox.add_event(cur, SCHEMA, "InventoryStocked", "sku", body.sku,
                         {"sku": body.sku, "added": body.quantity, "on_hand": row["quantity_on_hand"],
                          "supplier_id": body.supplier_id}, SOURCE)
        conn.commit()
    return _out(row)


@app.get("/api/inventory/items")
def list_items(supplier_id: str | None = None, limit: int = 200):
    q = f"SELECT * FROM {SCHEMA}.items"
    params: list = []
    if supplier_id:
        q += " WHERE supplier_id=%s"
        params.append(supplier_id)
    q += " ORDER BY sku LIMIT %s"
    params.append(min(limit, 1000))
    with db.get_pool().connection() as conn:
        return [_out(r) for r in conn.execute(q, params).fetchall()]


@app.get("/api/inventory/items/{sku}")
def get_item(sku: str):
    with db.get_pool().connection() as conn:
        row = conn.execute(f"SELECT * FROM {SCHEMA}.items WHERE sku=%s", (sku,)).fetchone()
    if not row:
        raise HTTPException(404, "sku not found")
    return _out(row)


@app.post("/api/inventory/items/{sku}/adjust")
def adjust(sku: str, body: AdjustIn):
    with db.get_pool().connection() as conn, conn.cursor() as cur:
        cur.execute(f"UPDATE {SCHEMA}.items SET quantity_on_hand = GREATEST(quantity_on_hand + %s, 0), "
                    "updated_at=now() WHERE sku=%s RETURNING *", (body.delta, sku))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "sku not found")
        outbox.add_event(cur, SCHEMA, "InventoryAdjusted", "sku", sku, {"sku": sku, "delta": body.delta}, SOURCE)
        conn.commit()
    return _out(row)


@app.get("/api/inventory/reservations/{order_ref}")
def get_reservation(order_ref: str):
    with db.get_pool().connection() as conn:
        row = conn.execute(f"SELECT * FROM {SCHEMA}.reservations WHERE order_ref=%s", (order_ref,)).fetchone()
    if not row:
        raise HTTPException(404, "reservation not found")
    return row


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_config=None)
