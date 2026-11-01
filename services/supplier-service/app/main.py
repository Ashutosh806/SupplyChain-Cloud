"""Supplier Service — manages supplier records (REST + RDS Postgres + outbox)."""
import secrets
from contextlib import asynccontextmanager
from typing import Literal

import uvicorn
from fastapi import HTTPException
from pydantic import BaseModel, EmailStr, Field

from sc_common import db, outbox
from sc_common.web import create_app

SCHEMA = "supplier"
SOURCE = "supplier-service"

DDL = [
    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.suppliers (
        supplier_id   TEXT PRIMARY KEY,
        name          TEXT NOT NULL,
        country       TEXT NOT NULL,
        contact_email TEXT NOT NULL,
        rating        NUMERIC(3,2) NOT NULL DEFAULT 4.00,
        status        TEXT NOT NULL DEFAULT 'ACTIVE',
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    )""",
    *outbox.outbox_ddl(SCHEMA),
]


class SupplierIn(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    country: str = Field(min_length=2, max_length=60)
    contact_email: EmailStr
    rating: float = Field(default=4.0, ge=0, le=5)


class StatusIn(BaseModel):
    status: Literal["ACTIVE", "SUSPENDED"]


@asynccontextmanager
async def lifespan(_app):
    db.migrate(SCHEMA, DDL)
    yield


app = create_app("Supplier Service", lifespan=lifespan, ready_check=db.ping)


def _row_out(row: dict) -> dict:
    row = dict(row)
    row["rating"] = float(row["rating"])
    return row


@app.post("/api/suppliers", status_code=201)
def create_supplier(body: SupplierIn):
    supplier_id = "SUP-" + secrets.token_hex(3).upper()
    with db.get_pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {SCHEMA}.suppliers (supplier_id, name, country, contact_email, rating) "
            "VALUES (%s,%s,%s,%s,%s) RETURNING *",
            (supplier_id, body.name, body.country, body.contact_email, body.rating),
        )
        row = cur.fetchone()
        outbox.add_event(cur, SCHEMA, "SupplierRegistered", "supplier", supplier_id,
                         {"supplier_id": supplier_id, "name": body.name, "country": body.country},
                         SOURCE)
        conn.commit()
    return _row_out(row)


@app.get("/api/suppliers")
def list_suppliers(limit: int = 100):
    with db.get_pool().connection() as conn:
        rows = conn.execute(
            f"SELECT * FROM {SCHEMA}.suppliers ORDER BY created_at DESC LIMIT %s", (min(limit, 500),)
        ).fetchall()
    return [_row_out(r) for r in rows]


@app.get("/api/suppliers/{supplier_id}")
def get_supplier(supplier_id: str):
    with db.get_pool().connection() as conn:
        row = conn.execute(f"SELECT * FROM {SCHEMA}.suppliers WHERE supplier_id=%s", (supplier_id,)).fetchone()
    if not row:
        raise HTTPException(404, "supplier not found")
    return _row_out(row)


@app.patch("/api/suppliers/{supplier_id}/status")
def set_status(supplier_id: str, body: StatusIn):
    with db.get_pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE {SCHEMA}.suppliers SET status=%s, updated_at=now() WHERE supplier_id=%s RETURNING *",
            (body.status, supplier_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "supplier not found")
        outbox.add_event(cur, SCHEMA, "SupplierStatusChanged", "supplier", supplier_id,
                         {"supplier_id": supplier_id, "status": body.status}, SOURCE)
        conn.commit()
    return _row_out(row)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_config=None)
