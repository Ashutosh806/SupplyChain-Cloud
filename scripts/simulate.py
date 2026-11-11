#!/usr/bin/env python3
"""Realistic background traffic for demos (stdlib only).

Books shipments and walks each through a carrier journey (with occasional DELAYED / EXCEPTION
scans and out-of-order arrivals), so the status store, notifications and Flink windows all have
live data.

    python3 scripts/simulate.py --base http://<alb-dns> [--shipments-per-min 30] [--minutes 30]
"""
import argparse
import json
import random
import threading
import time
import urllib.error
import urllib.request

CARRIERS = ["DELHIVERY", "BLUEDART", "DHL", "FEDEX", "ECOMEXPRESS"]
ROUTES = [("Mumbai, IN", "Bengaluru, IN", ["Mumbai DC", "Pune Hub", "Hubli Hub", "Bengaluru Hub"]),
          ("Delhi, IN", "Kolkata, IN", ["Delhi DC", "Kanpur Hub", "Patna Hub", "Kolkata Hub"]),
          ("Chennai, IN", "Hyderabad, IN", ["Chennai DC", "Nellore Hub", "Hyderabad Hub"]),
          ("Ahmedabad, IN", "Jaipur, IN", ["Ahmedabad DC", "Udaipur Hub", "Jaipur Hub"])]


def call(base, method, path, body=None):
    req = urllib.request.Request(base + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, {}
    except Exception:
        return 0, {}


def journey(base, tid, carrier, hubs, stop):
    """Send the scans of one parcel with compressed timing; event_time spreads over ~1-2 days."""
    now = int(time.time() * 1000)
    h = 3_600_000
    t = now - random.randint(30, 48) * h
    scans = [("PICKED_UP", hubs[0])]
    for hub in hubs[1:]:
        scans += [("IN_TRANSIT", f"Linehaul to {hub}"), ("AT_HUB", hub)]
    if random.random() < 0.15:
        scans.insert(random.randint(1, len(scans)), ("DELAYED", "Weather / congestion"))
    if random.random() < 0.05:
        scans.insert(random.randint(1, len(scans)), ("EXCEPTION", "Address issue"))
    scans += [("OUT_FOR_DELIVERY", hubs[-1]), ("DELIVERED", "Retail store")]
    events = []
    for status, loc in scans:
        t += random.randint(1, 6) * h
        events.append({"tracking_id": tid, "carrier": carrier, "status": status, "location": loc,
                       "event_time": min(t, now), "note": loc if status in ("DELAYED", "EXCEPTION") else None})
    if len(events) > 3 and random.random() < 0.3:      # carriers sometimes deliver scans late
        i = random.randint(1, len(events) - 2)
        events[i], events[i + 1] = events[i + 1], events[i]
    for ev in events:
        if stop.is_set():
            return
        call(base, "POST", "/api/tracking/events", ev)
        time.sleep(random.uniform(1.5, 5))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--shipments-per-min", type=float, default=30)
    ap.add_argument("--minutes", type=float, default=30)
    a = ap.parse_args()
    base = a.base.rstrip("/")
    _, sup = call(base, "POST", "/api/suppliers", {"name": "Simulated Supplier", "country": "India",
                                                    "contact_email": "sim@supplier.example"})
    sku = f"SKU-SIM-{int(time.time())}"
    call(base, "POST", "/api/inventory/items", {"sku": sku, "supplier_id": sup["supplier_id"], "name": "Sim pallet",
                                                "quantity": 1_000_000, "reorder_level": 0})
    print(f"supplier {sup['supplier_id']} sku {sku}; booking {a.shipments_per_min}/min for {a.minutes} min")
    stop = threading.Event()
    end = time.time() + a.minutes * 60
    n = 0
    workers: list[threading.Thread] = []
    try:
        while time.time() < end:
            origin, dest, hubs = random.choice(ROUTES)
            carrier = random.choice(CARRIERS)
            code, ship = call(base, "POST", "/api/shipments", {
                "supplier_id": sup["supplier_id"], "retailer_id": f"RET-{random.randint(1, 40):03d}",
                "retailer_email": "store@retailer.example", "origin": origin, "destination": dest,
                "carrier": carrier, "items": [{"sku": sku, "quantity": random.randint(1, 20)}]})
            if code == 201:
                n += 1
                t = threading.Thread(target=journey, args=(base, ship["tracking_id"], carrier, hubs, stop),
                                     daemon=True)
                t.start()
                workers.append(t)
                print(f"[{n}] booked {ship['tracking_id']} via {carrier}", flush=True)
            time.sleep(60 / a.shipments_per_min)
        print("finishing in-flight journeys (Ctrl+C to abort)...", flush=True)
        for t in workers:
            t.join(timeout=max(0.0, 120 - (time.time() - end)))
    except KeyboardInterrupt:
        pass
    stop.set()
    print(f"done: {n} shipments")


if __name__ == "__main__":
    main()
