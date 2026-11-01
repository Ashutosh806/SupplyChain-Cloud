"""Local end-to-end test: all microservices in one process, real Postgres, real
HTTP + gRPC between services, 5 Dynamo nodes, in-memory Kafka stand-in.

    python tests/e2e_local.py            # needs Postgres 16 binaries (initdb/pg_ctl) on PATH or /usr/lib/postgresql

It exercises: REST + gRPC booking saga, outbox -> Kafka relay, inventory
choreography, carrier ingestion + de-duplication, Dynamo quorum writes/reads,
sloppy quorum + hinted handoff, concurrent siblings + reconciliation,
anti-entropy, notification enqueueing and the analytics query side.
"""
import importlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
SERVICES = ROOT / "services"
TMP = Path(tempfile.mkdtemp(prefix="sc-e2e-"))
PG_BIN = os.environ.get("PG_BIN") or next(
    (str(p) for p in sorted(Path("/usr/lib/postgresql").glob("*/bin"), reverse=True)), "")
PG_PORT = 55432
results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""), flush=True)


# ---------------------------------------------------------------- postgres
def start_postgres():
    data = TMP / "pg"
    data.mkdir()
    shutil.chown(TMP, "postgres")
    shutil.chown(data, "postgres")
    run = lambda *cmd: subprocess.run(["su", "postgres", "-c", " ".join(cmd)], check=True,
                                      capture_output=True, text=True)
    run(f"{PG_BIN}/initdb", "-D", str(data), "-U", "postgres", "--auth=trust", "-E", "UTF8")
    run(f"{PG_BIN}/pg_ctl", "-D", str(data), "-o", f"'-p {PG_PORT} -k /tmp'", "-l", str(TMP / "pg.log"), "-w", "start")
    psql = (f"{PG_BIN}/psql", "-h", "/tmp", "-p", str(PG_PORT), "-U", "postgres", "-c")
    run(*psql, "\"CREATE ROLE supplychain LOGIN PASSWORD 'supplychain'\"")
    run(*psql, "\"CREATE DATABASE supplychain OWNER supplychain\"")


def stop_postgres():
    subprocess.run(["su", "postgres", "-c", f"{PG_BIN}/pg_ctl -D {TMP / 'pg'} -m fast stop"], capture_output=True)


# ---------------------------------------------------------------- harness
def gen_protos():
    gen = TMP / "gen"
    gen.mkdir()
    subprocess.run([sys.executable, "-m", "grpc_tools.protoc", f"-I{SERVICES / 'proto'}", f"--python_out={gen}",
                    f"--grpc_python_out={gen}", *map(str, (SERVICES / "proto").glob("*.proto"))], check=True)
    sys.path[:0] = [str(gen), str(SERVICES / "common")]


def load_service(dirname: str, alias: str, env: dict):
    os.environ.update(env)
    pkg = SERVICES / dirname / "app"
    spec = importlib.util.spec_from_file_location(alias, pkg / "__init__.py", submodule_search_locations=[str(pkg)])
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod
    spec.loader.exec_module(mod)
    return importlib.import_module(alias + ".main")


def serve(app, port):
    import uvicorn
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_config=None, lifespan="on"))
    threading.Thread(target=server.run, daemon=True).start()
    return server


def wait_until(fn, timeout=20, interval=0.25):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            last = fn()
            if last:
                return last
        except Exception as exc:
            last = exc
        time.sleep(interval)
    return None


def main():
    try:
        start_postgres()
        run_test()
    finally:
        stop_postgres()
    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    sys.exit(1 if failed else 0)


def boot():
    """Start every service in-process; returns the 5 delivery-status node modules."""
    sys.path.insert(0, str(Path(__file__).parent))
    import fake_kafka
    fake_kafka.install()
    import prometheus_client.registry as reg
    _orig = reg.CollectorRegistry.register

    def _register(self, collector):  # several copies of a module are loaded in one process
        try:
            _orig(self, collector)
        except ValueError:
            pass
    reg.CollectorRegistry.register = _register
    gen_protos()

    os.environ.update({
        "DB_HOST": "127.0.0.1", "DB_PORT": str(PG_PORT), "DB_SSLMODE": "disable",
        "DB_USER": "supplychain", "DB_PASSWORD": "supplychain", "DB_NAME": "supplychain",
        "SUPPLIER_URL": "http://127.0.0.1:18081", "BOOKING_URL": "http://127.0.0.1:18082",
        "INVENTORY_GRPC": "127.0.0.1:50051", "LOG_LEVEL": "WARNING",
        "POLL_INTERVAL_SECONDS": "0.05", "HEARTBEAT_SECONDS": "1", "DB_POOL_MAX": "40",
    })
    import logging
    logging.getLogger().setLevel(logging.WARNING)

    supplier = load_service("supplier-service", "svc_supplier", {})
    inventory = load_service("inventory-service", "svc_inventory", {"GRPC_PORT": "50051"})
    booking = load_service("booking-service", "svc_booking", {})
    ingestion = load_service("tracking-ingestion", "svc_ingestion", {})
    publisher = load_service("event-publisher", "svc_publisher", {})
    analytics = load_service("analytics-api", "svc_analytics", {})
    peers = ",".join(f"ds-{i}=127.0.0.1:{50060 + i}@{18090 + i}" for i in range(5))
    nodes = []
    for i in range(5):
        nodes.append(load_service("delivery-status", f"svc_ds{i}", {
            "NODE_ID": f"ds-{i}", "DYNAMO_PEERS": peers, "DYNAMO_GRPC_PORT": str(50060 + i),
            "DATA_DIR": str(TMP / f"ds{i}")}))
    logging.getLogger().setLevel(logging.WARNING)

    for app, port in [(supplier.app, 18081), (booking.app, 18082), (inventory.app, 18083),
                      (ingestion.app, 18084), (publisher.app, 18085), (analytics.app, 18086)]:
        serve(app, port)
    for i, n in enumerate(nodes):
        serve(n.app, 18090 + i)
    return nodes


def run_test():
    nodes = boot()
    c = httpx.Client(timeout=10)
    for port in (18081, 18082, 18083, 18084, 18085, 18086, *range(18090, 18095)):
        ok = wait_until(lambda: c.get(f"http://127.0.0.1:{port}/readyz").status_code == 200, 40)
        check(f"service on :{port} ready", ok)

    S, B, I, T, D = ("http://127.0.0.1:18081", "http://127.0.0.1:18082", "http://127.0.0.1:18083",
                     "http://127.0.0.1:18084", "http://127.0.0.1:18090")

    # ---- suppliers + inventory
    sup = c.post(f"{S}/api/suppliers", json={"name": "Acme Components", "country": "India",
                                             "contact_email": "ops@acme.example"}).json()
    check("supplier registered", sup.get("supplier_id", "").startswith("SUP-"), sup)
    item = c.post(f"{I}/api/inventory/items", json={"sku": "SKU-1", "supplier_id": sup["supplier_id"],
                                                    "name": "Pump", "quantity": 100}).json()
    check("stock added", item.get("available") == 100, item)

    # ---- booking: REST supplier check + gRPC reservation + outbox
    r = c.post(f"{B}/api/shipments", json={"supplier_id": sup["supplier_id"], "retailer_id": "RET-1",
                                           "retailer_email": "r@x.com", "carrier": "DHL",
                                           "items": [{"sku": "SKU-1", "quantity": 10}]})
    ship = r.json()
    check("shipment booked", r.status_code == 201 and ship["tracking_id"].startswith("SCT"), ship)
    tid = ship["tracking_id"]
    inv = c.get(f"{I}/api/inventory/items/SKU-1").json()
    check("gRPC reservation applied", inv["quantity_reserved"] == 10 and inv["available"] == 90, inv)
    r = c.post(f"{B}/api/shipments", json={"supplier_id": sup["supplier_id"], "retailer_id": "RET-1",
                                           "items": [{"sku": "SKU-1", "quantity": 1000}]})
    check("insufficient stock rejected (409)", r.status_code == 409, r.text)
    r = c.post(f"{B}/api/shipments", json={"supplier_id": "SUP-NOPE", "retailer_id": "RET-1",
                                           "items": [{"sku": "SKU-1", "quantity": 1}]})
    check("unknown supplier rejected (422)", r.status_code == 422, r.text)

    # ---- outbox -> kafka -> delivery-status (quorum write)
    st = wait_until(lambda: c.get(f"{D}/api/status/{tid}").json().get("state", {}).get("status") == "BOOKED" and
                    c.get(f"{D}/api/status/{tid}").json())
    check("delivery status BOOKED via outbox relay", st, st)

    # ---- carrier events (with out-of-order arrival) + de-duplication
    now = int(time.time() * 1000)
    H = 3_600_000
    journey = [("PICKED_UP", "Mumbai", now - 30 * H), ("AT_HUB", "Pune", now - 12 * H),
               ("IN_TRANSIT", "NH48", now - 20 * H),  # arrives late, older event_time
               ("OUT_FOR_DELIVERY", "Bengaluru", now - 2 * H)]
    for status, loc, t in journey:
        r = c.post(f"{T}/api/tracking/events", json={"tracking_id": tid, "carrier": "DHL", "status": status,
                                                      "location": loc, "event_time": t,
                                                      "carrier_event_id": f"DHL-{status}"})
        check(f"ingest {status}", r.status_code == 202 and r.json()["accepted"] == 1, r.text)
    dup = c.post(f"{T}/api/tracking/events", json={"tracking_id": tid, "carrier": "DHL", "status": "AT_HUB",
                                                    "carrier_event_id": "DHL-AT_HUB"}).json()
    check("duplicate carrier event de-duplicated", dup["duplicates"] == 1 and dup["accepted"] == 0, dup)
    bad = c.post(f"{T}/api/tracking/events", json={"tracking_id": "SCTUNKNOWN99", "carrier": "DHL",
                                                    "status": "PICKED_UP"})
    check("unknown tracking id rejected", bad.status_code == 422, bad.text)
    st = wait_until(lambda: (lambda j: j if j["state"]["status"] == "OUT_FOR_DELIVERY" else None)(
        c.get(f"{D}/api/status/{tid}").json()))
    check("status OUT_FOR_DELIVERY despite out-of-order scan", st, st)
    check("last location from latest event_time", st and st["state"]["last_location"] == "Bengaluru", st)
    inv = wait_until(lambda: (lambda j: j if j["quantity_on_hand"] == 90 else None)(
        c.get(f"{I}/api/inventory/items/SKU-1").json()))
    check("PICKED_UP event dispatched stock (Kafka choreography)", inv, c.get(f"{I}/api/inventory/items/SKU-1").json())

    # ---- every node can coordinate; quorum metadata
    other = c.get(f"http://127.0.0.1:18093/api/status/{tid}").json()
    check("any node coordinates reads (leaderless)", other["dynamo"]["coordinator"] == "ds-3"
          and other["state"]["status"] == "OUT_FOR_DELIVERY", other.get("dynamo"))
    pref = other["dynamo"]["preference_list"]
    check("preference list has N=3 distinct nodes", len(set(pref)) == 3, pref)
    reps = c.get(f"{D}/api/status/{tid}/replicas").json()["replicas"]
    holders = [n for n, v in reps.items() if v["versions"]]
    check("value replicated on its 3 preference-list nodes", sorted(holders) == sorted(pref), reps)

    # ---- failure: crash a replica, keep writing (sloppy quorum + hint), recover (hinted handoff)
    victim = pref[0]
    coord_port = next(18090 + i for i in range(5) if f"ds-{i}" not in pref[:1])
    C = f"http://127.0.0.1:{coord_port}"
    r = c.post(f"{C}/api/dynamo/nodes/{victim}/fail?seconds=30")
    check("simulate crash of a preference-list node", r.status_code == 200, r.text)
    time.sleep(2.5)  # failure detectors notice
    r = c.post(f"{T}/api/tracking/events", json={"tracking_id": tid, "carrier": "DHL", "status": "DELIVERED",
                                                  "location": "Bengaluru Store", "event_time": now})
    st = wait_until(lambda: (lambda j: j if j["state"]["status"] == "DELIVERED" else None)(
        c.get(f"{C}/api/status/{tid}").json()), 20)
    check("write succeeds with a replica down (W=2)", st, st)
    ring = wait_until(lambda: (lambda j: j if sum(j["hints_pending"].values()) >= 0 else None)(
        c.get(f"{C}/api/dynamo/ring").json()))
    hints = {}
    for i in range(5):
        if f"ds-{i}" == victim:
            continue
        hints.update({k: v for k, v in c.get(f"http://127.0.0.1:{18090 + i}/api/dynamo/ring").json()["hints_pending"].items()})
    check("hint stored for the crashed node (sloppy quorum)", hints.get(victim, 0) >= 1, hints)
    reps = c.get(f"{C}/api/status/{tid}/replicas").json()["replicas"]
    check("crashed node unreachable in replica view", reps[victim]["reachable"] is False, reps[victim])
    c.post(f"{C}/api/dynamo/nodes/{victim}/recover")
    victim_port = 18090 + int(victim.split("-")[1])

    def victim_has_delivered():
        node_mod = nodes[int(victim.split("-")[1])]
        vs = node_mod.node.store.get(f"shipment:{tid}")
        return vs and any(node_mod.ss.derive(v["value"])["status"] == "DELIVERED" for v in vs)
    check("hinted handoff delivers missed write after recovery", wait_until(victim_has_delivered, 20))

    # ---- concurrent siblings + semantic reconciliation
    r = c.post(f"{D}/api/dynamo/demo/conflict/{tid}").json()
    reps = c.get(f"{D}/api/status/{tid}/replicas").json()["replicas"]
    sib_counts = [len(v["versions"]) for n, v in reps.items() if v["in_preference_list"]]
    check("concurrent writes create 2 siblings on replicas", all(n == 2 for n in sib_counts), sib_counts)
    st = c.get(f"{D}/api/status/{tid}").json()
    check("read detects siblings and reconciles (union merge)", st["dynamo"]["siblings_found"] == 2
          and st["dynamo"]["siblings_reconciled"] == 2 and st["state"]["status"] == "DELIVERED", st["dynamo"])
    reps = c.get(f"{D}/api/status/{tid}/replicas").json()["replicas"]
    sib_counts = [len(v["versions"]) for n, v in reps.items() if v["in_preference_list"]]
    check("siblings collapsed after reconciliation write-back", all(n == 1 for n in sib_counts), sib_counts)

    # ---- anti-entropy repairs a replica that silently lost data
    import random
    import dynamo_pb2 as dpb
    wiped_id, healthy_id = pref[1], pref[0]
    wiped = nodes[int(wiped_id.split("-")[1])].node
    healthy = nodes[int(healthy_id.split("-")[1])].node
    wiped.store.conn.execute("DELETE FROM kv")                       # simulate silent data loss
    root, _ = healthy.merkle(wiped_id)
    reply = healthy._stub(wiped_id).MerkleLeaves(dpb.MerkleRequest(requester=healthy_id, buckets=healthy.buckets))
    check("merkle roots differ after data loss", reply.root != root)
    orig_choice = random.choice
    random.choice = lambda seq: wiped_id                             # pick the wiped peer this round
    try:
        healthy._anti_entropy()
    finally:
        random.choice = orig_choice
    check("anti-entropy restored the key", wiped.store.get(f"shipment:{tid}") is not None)
    root, _ = healthy.merkle(wiped_id)
    reply = healthy._stub(wiped_id).MerkleLeaves(dpb.MerkleRequest(requester=healthy_id, buckets=healthy.buckets))
    check("merkle roots equal after anti-entropy", reply.root == root)

    # ---- notifications (logged because no SQS in the test) + metrics
    m = c.get(f"{D}/metrics").text + c.get("http://127.0.0.1:18091/metrics").text
    check("dynamo metrics exported", "dynamo_quorum_writes_total" in m and "notifications_enqueued_total" in m)
    pub = c.get("http://127.0.0.1:18085/api/publisher/stats").json()
    check("outbox fully drained", all(v == 0 for v in pub["backlog"].values()), pub)

    # ---- cancel + release over gRPC
    r = c.post(f"{B}/api/shipments", json={"supplier_id": sup["supplier_id"], "retailer_id": "RET-2",
                                           "items": [{"sku": "SKU-1", "quantity": 5}]}).json()
    c.post(f"{B}/api/shipments/{r['tracking_id']}/cancel")
    inv = c.get(f"{I}/api/inventory/items/SKU-1").json()
    check("cancel releases reservation", inv["quantity_reserved"] == 0, inv)
    st = wait_until(lambda: (lambda j: j if j.get("state", {}).get("status") == "CANCELLED" else None)(
        c.get(f"{D}/api/status/{r['tracking_id']}").json()))
    check("cancelled status derived", st, st)

    # ---- analytics query side (results normally produced by Flink)
    import fake_kafka as fk
    p = fk.Producer({})
    p.produce("shipment-analytics-results", json.dumps({
        "type": "carrier_window", "carrier": "DHL", "window_start": now - 60000, "window_end": now,
        "events": 42, "unique_shipments": 17, "delayed": 3, "exceptions": 1, "delivered": 5,
        "avg_ingest_lag_ms": 120.5, "max_ingest_lag_ms": 900}))
    p.produce("shipment-analytics-results", json.dumps({
        "type": "shipment_transit", "tracking_id": tid, "carrier": "DHL", "transit_hours": 30.0,
        "delay_minutes": 0, "on_time": True, "picked_up_at": now - 30 * H, "delivered_at": now,
        "promised_delivery": now + H}))
    summ = wait_until(lambda: (lambda j: j if j["deliveries_observed"] == 1 else None)(
        c.get("http://127.0.0.1:18086/api/analytics/summary").json()))
    check("analytics API consumes Flink results", summ and summ["carriers_last_15_windows"]["DHL"]["events"] == 42, summ)
    m = c.get("http://127.0.0.1:18086/metrics").text
    check("analytics gauges exported", 'analytics_window_events{carrier="DHL"} 42.0' in m)


if __name__ == "__main__":
    main()
