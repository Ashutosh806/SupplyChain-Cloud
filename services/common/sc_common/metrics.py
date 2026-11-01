"""Prometheus metrics shared by all services.

The `service`, `namespace` and `pod` labels are attached by Prometheus from the
ServiceMonitor target, so application metrics only carry request dimensions.
"""
import time

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

HTTP_REQUESTS = Counter(
    "http_requests_total", "HTTP requests", ["method", "route", "status"]
)
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
GRPC_REQUESTS = Counter("grpc_requests_total", "gRPC requests", ["method", "code"])
GRPC_LATENCY = Histogram(
    "grpc_request_duration_seconds", "gRPC latency", ["method"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)
KAFKA_CONSUMED = Counter("kafka_events_consumed_total", "Kafka events consumed", ["topic", "event_type"])
KAFKA_CONSUME_ERRORS = Counter("kafka_consume_errors_total", "Kafka processing errors", ["topic"])
DOMAIN_EVENTS = Counter("domain_events_written_total", "Events written to the outbox", ["event_type"])
INFLIGHT = Gauge("http_requests_inflight", "In-flight HTTP requests")

_SKIP = {"/metrics", "/healthz", "/readyz"}


class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in _SKIP:
            return await call_next(request)
        start = time.perf_counter()
        INFLIGHT.inc()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            INFLIGHT.dec()
            route = request.scope.get("route")
            label = getattr(route, "path", None) or ("static" if path.startswith("/static") else "unmatched")
            HTTP_REQUESTS.labels(request.method, label, str(status)).inc()
            HTTP_LATENCY.labels(request.method, label).observe(time.perf_counter() - start)


def metrics_response() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
