"""Run the whole system locally (one process) behind an ALB-like path router on :8000.

    sudo python tests/local_stack.py        # then open http://localhost:8000
    k6 run -e BASE_URL=http://localhost:8000 loadtest/k6/smoke.js

Same harness as the e2e test (real Postgres, real HTTP/gRPC, 5 Dynamo nodes, in-memory Kafka).
Handy for developing the UI and the k6 scripts without AWS.
"""
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response

import e2e_local as h

ROUTES = [("/api/suppliers", 18081), ("/api/shipments", 18082), ("/api/config", 18082),
          ("/api/inventory", 18083), ("/api/tracking", 18084), ("/api/publisher", 18085),
          ("/api/analytics", 18086), ("/api/status", 18090), ("/api/dynamo", 18090)]

gateway = FastAPI()
client = httpx.AsyncClient(timeout=30)


@gateway.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy(path: str, request: Request):
    p = "/" + path
    port = next((port for prefix, port in ROUTES if p == prefix or p.startswith(prefix + "/")), None)
    if port is None and p == "/":
        port = 18082
    if port is None:
        return Response("no route", status_code=404)
    r = await client.request(request.method, f"http://127.0.0.1:{port}{p}", params=request.query_params,
                             content=await request.body(), headers={"content-type": request.headers.get("content-type", "application/json")})
    return Response(r.content, status_code=r.status_code, media_type=r.headers.get("content-type"),
                    headers={"x-served-by": r.headers.get("x-served-by", "")})


if __name__ == "__main__":
    h.start_postgres()
    try:
        h.boot()
        h.serve(gateway, 8000)
        print("local stack ready on http://localhost:8000  (Ctrl+C to stop)", flush=True)
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        h.stop_postgres()
