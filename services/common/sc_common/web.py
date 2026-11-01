"""FastAPI application factory with health, readiness, metrics and request logging."""
import logging
import time
import uuid
from typing import Callable

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from . import config
from .logging_setup import setup_logging
from .metrics import PrometheusMiddleware, metrics_response


def create_app(title: str, lifespan=None, ready_check: Callable[[], bool] | None = None,
               root_info: bool = True) -> FastAPI:
    log = setup_logging()
    app = FastAPI(title=title, lifespan=lifespan)
    app.add_middleware(PrometheusMiddleware)
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
    access = logging.getLogger("access")

    @app.middleware("http")
    async def request_log(request: Request, call_next):
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
        start = time.perf_counter()
        response = await call_next(request)
        response.headers["x-request-id"] = rid
        response.headers["x-served-by"] = config.POD_NAME
        if request.url.path not in ("/metrics", "/healthz", "/readyz"):
            access.info("request", extra={
                "method": request.method, "path": request.url.path, "status": response.status_code,
                "duration_ms": round((time.perf_counter() - start) * 1000, 2), "request_id": rid,
            })
        return response

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception):
        log.exception("unhandled error", extra={"path": request.url.path})
        return JSONResponse({"error": "internal_error", "detail": str(exc)}, status_code=500)

    @app.get("/healthz", include_in_schema=False)
    def healthz():
        return {"status": "ok", "service": config.SERVICE_NAME, "pod": config.POD_NAME}

    @app.get("/readyz", include_in_schema=False)
    def readyz():
        ok = ready_check() if ready_check else True
        return JSONResponse({"ready": ok}, status_code=200 if ok else 503)

    @app.get("/metrics", include_in_schema=False)
    def metrics():
        return metrics_response()

    if root_info:
        @app.get("/", include_in_schema=False)
        def root():
            return {"service": config.SERVICE_NAME, "pod": config.POD_NAME, "docs": "/docs"}

    return app
