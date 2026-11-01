#!/usr/bin/env python3
"""Run the whole system on your laptop at http://localhost:8000 (no AWS).

    python local/run_local.py            # start; open http://localhost:8000 ; Ctrl+C to stop
    python local/run_local.py --test     # start, run all four member checks, stop

Same code as on AWS. Locally, Kafka is an in-memory stand-in, there is no Lambda/S3/DynamoDB/EMR,
and the checks skip the steps that need them (they run them on AWS).
PostgreSQL: local binaries (brew/apt), Docker, or TEST_DB_HOST=... (see harness.py).
"""
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness  # noqa: E402

ROOT = harness.ROOT
PORTS = {"supplier": 18081, "booking": 18082, "inventory": 18083, "ingestion": 18084,
         "publisher": 18085, "analytics": 18086}
STATUS_PORTS = [18090 + i for i in range(5)]
# Same path routing as the AWS load balancer (deploy/apps/base/ingress.yaml)
ROUTES = [("/api/suppliers", "supplier"), ("/api/shipments", "booking"), ("/api/config", "booking"),
          ("/api/inventory", "inventory"), ("/api/tracking", "ingestion"), ("/api/publisher", "publisher"),
          ("/api/analytics", "analytics"), ("/api/status", "status"), ("/api/dynamo", "status")]

gateway = FastAPI()
client = httpx.AsyncClient(timeout=30)
_rr = [0]


@gateway.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy(path: str, request: Request):
    p = "/" + path
    target = next((t for prefix, t in ROUTES if p == prefix or p.startswith(prefix + "/")), "booking" if p == "/" else None)
    if target is None:
        return Response("no route", status_code=404)
    if target == "status":                       # spread over the 5 store nodes like the ALB does
        _rr[0] += 1
        port = STATUS_PORTS[_rr[0] % 5]
    else:
        port = PORTS[target]
    r = await client.request(request.method, f"http://127.0.0.1:{port}{p}", params=request.query_params,
                             content=await request.body(),
                             headers={"content-type": request.headers.get("content-type", "application/json")})
    return Response(r.content, status_code=r.status_code, media_type=r.headers.get("content-type"),
                    headers={"x-served-by": r.headers.get("x-served-by", "")})


def start():
    pg = harness.Postgres()
    os.environ.update(pg.start())
    os.environ.update({
        "SUPPLIER_URL": f"http://127.0.0.1:{PORTS['supplier']}", "BOOKING_URL": f"http://127.0.0.1:{PORTS['booking']}",
        "INVENTORY_GRPC": "127.0.0.1:50051", "GRPC_PORT": "50051",
    })
    servers = []
    for name, dirname in [("supplier", "supplier-service"), ("inventory", "inventory-service"),
                          ("booking", "booking-service"), ("ingestion", "tracking-ingestion"),
                          ("publisher", "event-publisher"), ("analytics", "analytics-api")]:
        servers.append(harness.Server(harness.load_service(dirname, f"local_{name}").app, PORTS[name]))
    data = tempfile.mkdtemp(prefix="sc-local-")
    peers = ",".join(f"delivery-status-{i}=127.0.0.1:{50060 + i}@{STATUS_PORTS[i]}" for i in range(5))
    for i in range(5):
        m = harness.load_service("delivery-status", f"local_status{i}", {
            "NODE_ID": f"delivery-status-{i}", "DYNAMO_PEERS": peers, "DYNAMO_GRPC_PORT": str(50060 + i),
            "DATA_DIR": os.path.join(data, str(i))})
        servers.append(harness.Server(m.app, STATUS_PORTS[i]))
    servers.append(harness.Server(gateway, 8000))
    if not all(s.wait_ready() for s in servers[:-1]):
        pg.stop()
        sys.exit("some services did not start - see the log lines above")
    harness.wait_until(lambda: httpx.get("http://127.0.0.1:8000/api/config").status_code == 200)
    return pg


def main():
    pg = start()
    print("\n  System running on http://localhost:8000  (Ctrl+C to stop)\n", flush=True)
    try:
        if "--test" in sys.argv:
            env = os.environ | {"BASE": "http://localhost:8000"}
            results = [subprocess.run([sys.executable, str(ROOT / "checks" / f)], env=env).returncode
                       for f in ("m1_supplier_inventory.py", "m2_booking_notification.py",
                                 "m3_ingestion_publisher.py", "m4_status_analytics.py")]
            print("\n  all member checks passed" if not any(results) else "\n  some checks FAILED")
            sys.exit(1 if any(results) else 0)
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        import logging
        logging.disable(logging.CRITICAL)   # background loops complain once the DB is gone; that's expected
        pg.stop()


if __name__ == "__main__":
    import logging
    logging.disable(logging.WARNING)   # keep the terminal readable; services still log errors
    main()
