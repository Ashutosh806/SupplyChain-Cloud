"""Canonical event envelope published on the `shipment-events` Kafka topic.

{
  "event_id": "uuid",                # globally unique, used for idempotency
  "event_type": "CarrierStatusReported",
  "aggregate_type": "shipment",
  "aggregate_id": "SCT9F3K...",      # Kafka partition key
  "tracking_id": "SCT9F3K..." | null,
  "occurred_at": 1730000000000,      # business time (ms)  e.g. carrier scan time
  "recorded_at": 1730000000100,      # written to the outbox (ms)
  "published_at": 1730000000300,     # relayed to Kafka by the Event Publisher (ms)
  "source": "tracking-ingestion",
  "schema_version": 1,
  "payload": {...}
}
"""
import time
import uuid

STATUS_PROGRESS = ["BOOKED", "PICKED_UP", "IN_TRANSIT", "AT_HUB", "OUT_FOR_DELIVERY", "DELIVERED"]
STATUS_SIGNALS = ["DELAYED", "EXCEPTION", "RETURNED"]
CARRIER_STATUSES = STATUS_PROGRESS[1:] + STATUS_SIGNALS


def now_ms() -> int:
    return int(time.time() * 1000)


def envelope(event_type: str, aggregate_type: str, aggregate_id: str, payload: dict,
             source: str, tracking_id: str | None = None, occurred_at: int | None = None,
             event_id: str | None = None) -> dict:
    ts = now_ms()
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "tracking_id": tracking_id,
        "occurred_at": occurred_at or ts,
        "recorded_at": ts,
        "published_at": None,
        "source": source,
        "schema_version": 1,
        "payload": payload,
    }
