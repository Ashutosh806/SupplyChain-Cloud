"""Environment-driven configuration.

In Kubernetes, values come from the Terraform-managed ConfigMap `infra-config`
and Secret `db-credentials` (envFrom), plus per-service env in the manifests.
"""
import os
import socket


def env(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable {name}")
    return value


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def env_bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


SERVICE_NAME = env("SERVICE_NAME", "unknown-service")
POD_NAME = env("POD_NAME", socket.gethostname())
AWS_REGION = env("AWS_REGION", "ap-south-1")

KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_SECURITY_PROTOCOL = env("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
TOPIC_SHIPMENT_EVENTS = env("TOPIC_SHIPMENT_EVENTS", "shipment-events")
TOPIC_ANALYTICS_RESULTS = env("TOPIC_ANALYTICS_RESULTS", "shipment-analytics-results")

DB_HOST = env("DB_HOST", "localhost")
DB_PORT = env_int("DB_PORT", 5432)
DB_NAME = env("DB_NAME", "supplychain")
DB_USER = env("DB_USER", "supplychain")
DB_PASSWORD = env("DB_PASSWORD", "supplychain")
DB_SSLMODE = env("DB_SSLMODE", "prefer")

# Internal service URLs (Kubernetes DNS names by default)
SUPPLIER_URL = env("SUPPLIER_URL", "http://supplier-service:8080")
BOOKING_URL = env("BOOKING_URL", "http://booking-service:8080")
INVENTORY_GRPC = env("INVENTORY_GRPC", "inventory-service:50051")
DELIVERY_STATUS_URL = env("DELIVERY_STATUS_URL", "http://delivery-status:8080")
