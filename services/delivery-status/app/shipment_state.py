"""Shipment state as a conflict-free value + a pure derivation of current status.

The stored value is an *event set* keyed by event_id:
    {"tracking_id": "...", "events": {event_id: {...}, ...}}
Merging two siblings is a set union, which is commutative, associative and
idempotent - so semantic reconciliation of Dynamo siblings can never lose an
update, and Kafka redeliveries are harmless. The current status is *derived* by
replaying the events in event-time order, which also makes the result
independent of arrival order (carriers often deliver scans late/out of order).
"""
import time

PROGRESS = ["BOOKED", "PICKED_UP", "IN_TRANSIT", "AT_HUB", "OUT_FOR_DELIVERY", "DELIVERED"]
RANK = {s: i for i, s in enumerate(PROGRESS)}
NOTIFY_ON = {"BOOKED", "PICKED_UP", "OUT_FOR_DELIVERY", "DELIVERED", "DELAYED", "EXCEPTION",
             "CANCELLED", "RETURNED"}
HIGH_SEVERITY = {"DELAYED", "EXCEPTION", "RETURNED", "CANCELLED"}
HANDLED_EVENTS = {"ShipmentBooked", "ShipmentCancelled", "CarrierStatusReported"}
MAX_EVENTS = 50   # bound the stored value; see compact()


def empty(tracking_id: str) -> dict:
    return {"tracking_id": tracking_id, "events": {}}


def compact(events: dict) -> dict:
    """Bound the event set while keeping merge a well-defined join.

    Keeps every booking/cancel entry, the EARLIEST entry of each status (milestones such as
    PICKED_UP/DELIVERED that the derivation needs) and the most recent remaining entries up to
    MAX_EVENTS. The result depends only on the input set, and an entry dropped from a subset is
    never kept for a superset, so compact(union(...)) stays commutative, associative and idempotent.
    """
    if len(events) <= MAX_EVENTS:
        return events
    order = sorted(events.items(), key=lambda kv: (kv[1].get("event_time") or 0, kv[0]))
    keep, first_of = {}, set()
    for eid, e in order:
        if e["kind"] in ("booking", "cancel") or e["status"] not in first_of:
            keep[eid] = e
            first_of.add(e["status"])
    rest = [(eid, e) for eid, e in order if eid not in keep]
    budget = max(0, MAX_EVENTS - len(keep))
    for eid, e in rest[len(rest) - budget:] if budget else []:
        keep[eid] = e
    return keep


def merge_values(values: list[dict], tracking_id: str) -> dict:
    out = empty(tracking_id)
    for v in values:
        out["events"].update(v.get("events", {}))
    out["events"] = compact(out["events"])
    return out


def has_event(value: dict, event_id: str) -> bool:
    return event_id in value["events"]


def apply_event(value: dict, env: dict) -> dict:
    new = {"tracking_id": value["tracking_id"], "events": dict(value["events"])}
    p = env.get("payload", {})
    base = {"recorded_at": env.get("recorded_at"), "published_at": env.get("published_at"),
            "source": env.get("source")}
    et = env["event_type"]
    if et == "ShipmentBooked":
        entry = base | {"kind": "booking", "status": "BOOKED", "event_time": env["occurred_at"], "booking": p}
    elif et == "ShipmentCancelled":
        entry = base | {"kind": "cancel", "status": "CANCELLED", "event_time": env["occurred_at"],
                        "note": p.get("reason")}
    elif et == "CarrierStatusReported":
        entry = base | {"kind": "carrier", "status": p["status"], "event_time": p.get("event_time") or env["occurred_at"],
                        "location": p.get("location"), "carrier": p.get("carrier"), "eta": p.get("eta"),
                        "note": p.get("note"), "carrier_event_id": p.get("carrier_event_id")}
    else:  # DemoNote and anything else informational
        entry = base | {"kind": "note", "status": "NOTE", "event_time": env.get("occurred_at", 0),
                        "note": p.get("note")}
    new["events"][env["event_id"]] = entry
    new["events"] = compact(new["events"])
    return new


def derive(value: dict, now_ms: int | None = None) -> dict:
    now_ms = now_ms or int(time.time() * 1000)
    events = sorted(value["events"].items(), key=lambda kv: (kv[1].get("event_time") or 0, kv[0]))
    booking = next((e["booking"] for _, e in events if e["kind"] == "booking"), None)
    booked_at = next((e["event_time"] for _, e in events if e["kind"] == "booking"), None)
    cancel = next((e for _, e in events if e["kind"] == "cancel"), None)

    status, rank = ("BOOKED", 0) if booking else ("UNKNOWN", -1)
    last_progress = booked_at or 0
    picked_up = delivered = None
    last_delay = last_exception = returned = None
    location, eta, carrier = None, None, (booking or {}).get("carrier")
    history = []
    for event_id, e in events:
        s = e["status"]
        t = e.get("event_time") or 0
        if e["kind"] == "carrier":
            if s in RANK:
                if RANK[s] > rank:
                    status, rank = s, RANK[s]
                last_progress = max(last_progress, t)
                if s == "PICKED_UP":
                    picked_up = min(picked_up or t, t)
                if s == "DELIVERED":
                    delivered = min(delivered or t, t)
            elif s == "DELAYED":
                last_delay = e
            elif s == "EXCEPTION":
                last_exception = e
            elif s == "RETURNED":
                returned = e
            location = e.get("location") or location
            eta = e.get("eta") or eta
            carrier = e.get("carrier") or carrier
        history.append({"event_id": event_id, "status": s, "event_time": t, "location": e.get("location"),
                        "note": e.get("note"), "kind": e["kind"]})

    promised = (booking or {}).get("promised_delivery")
    delayed = bool(
        (last_delay and last_delay["event_time"] >= last_progress and not delivered)
        or (promised and not delivered and now_ms > promised)
        or (promised and delivered and delivered > promised)
    )
    exception_active = bool(last_exception and last_exception["event_time"] >= last_progress and not delivered)

    if cancel and not delivered:
        display = "CANCELLED"
    elif returned:
        display = "RETURNED"
    elif delivered:
        display = "DELIVERED"
    elif exception_active:
        display = "EXCEPTION"
    elif delayed:
        display = "DELAYED"
    else:
        display = status

    return {
        "tracking_id": value["tracking_id"],
        "status": display,
        "progress_status": status,
        "is_delayed": delayed,
        "exception": exception_active,
        "carrier": carrier,
        "origin": (booking or {}).get("origin"),
        "destination": (booking or {}).get("destination"),
        "supplier_id": (booking or {}).get("supplier_id"),
        "retailer_id": (booking or {}).get("retailer_id"),
        "retailer_email": (booking or {}).get("retailer_email"),
        "last_location": location,
        "eta": eta or promised,
        "promised_delivery": promised,
        "picked_up_at": picked_up,
        "delivered_at": delivered,
        "transit_hours": round((delivered - picked_up) / 3_600_000, 2) if delivered and picked_up else None,
        "delay_minutes": round(max(0, (delivered or now_ms) - promised) / 60000, 1) if promised else None,
        "delay_reason": (last_delay or {}).get("note") if delayed else None,
        "event_count": len(events),
        "history": history,
    }
