#!/usr/bin/env python3
"""Member 4 self-check - Delivery Status + Shipment Analytics | (f) leaderless Dynamo-style store, (e) Flink on EMR

    make check-m4
"""
import time

import common as d

d.title("Member 4 - Delivery Status (Dynamo-style store) + Analytics | concept + Flink on EMR")
sup, sku = d.supplier_with_stock(50)
_, ship = d.book(sup["supplier_id"], sku)
tid = ship["tracking_id"]
d.scan(tid, "PICKED_UP", "Mumbai DC", hours_ago=30)
d.scan(tid, "IN_TRANSIT", "NH48", hours_ago=20)

d.step("Current shipment status, read through a QUORUM",
       "key -> consistent-hash ring -> preference list of N=3 nodes; read waits for R=2, write for W=2")
body = d.wait_status(tid, "IN_TRANSIT")
d.show("status", {k: body["state"][k] for k in ("tracking_id", "status", "last_location", "carrier")})
d.show("dynamo", body["dynamo"])
d.check("N=3, R=2, W=2 and 3 distinct replicas", body["dynamo"]["n"] == 3 and len(set(body["dynamo"]["preference_list"])) == 3)
pref = body["dynamo"]["preference_list"]

d.step("Leaderless: every node can coordinate a request")
coords = {d.status_of(tid)["dynamo"]["coordinator"] for _ in range(10) if d.status_of(tid)}
d.show("coordinators seen", sorted(coords))
d.check("more than one coordinator served the same key", len(coords) >= 2)

d.step("Each replica's copy and its vector clock")
_, reps = d.get(f"/api/status/{tid}/replicas")
for node, r in reps["replicas"].items():
    print(f"      {node:<20} preference={str(r['in_preference_list']):<5} versions={r['versions']}")
d.check("stored on exactly the 3 preference-list nodes",
        sorted(n for n, r in reps["replicas"].items() if r["versions"]) == sorted(pref))

d.step(f"Crash {pref[0]} (a replica of this key) and keep writing",
       "sloppy quorum: the next healthy node accepts the write with a HINT for the crashed node")
d.post(f"/api/dynamo/nodes/{pref[0]}/fail", seconds=90)
time.sleep(4)
d.scan(tid, "OUT_FOR_DELIVERY", "Bengaluru", hours_ago=1)
d.check("write succeeded with a replica down", d.wait_status(tid, "OUT_FOR_DELIVERY", 40) is not None)
hints = {}
for _ in range(15):
    code, ring = d.get("/api/dynamo/ring")
    if code == 200:
        for k, v in ring["hints_pending"].items():
            hints[k] = max(hints.get(k, 0), v)
d.show("hints held for crashed nodes", hints)
d.check(f"a hint is waiting for {pref[0]}", hints.get(pref[0], 0) >= 1)

d.step(f"Recover {pref[0]} -> hinted handoff brings it up to date")
d.post(f"/api/dynamo/nodes/{pref[0]}/recover")

def crashed_node_caught_up():
    code, reps = d.get(f"/api/status/{tid}/replicas")
    vs = reps["replicas"][pref[0]]["versions"] if code == 200 else []
    return any(v["status"] == "OUT_FOR_DELIVERY" for v in vs)
d.check(f"{pref[0]} has the write it missed", d.wait(crashed_node_caught_up, 30, 2) is not None)

d.step("Two coordinators write concurrently -> siblings -> reconciled on read",
       "vector clocks detect the conflict; the value is a set of events, so merging (union) loses nothing")
d.post(f"/api/dynamo/demo/conflict/{tid}")
_, reps = d.get(f"/api/status/{tid}/replicas")
sib = [len(r["versions"]) for r in reps["replicas"].values() if r["in_preference_list"]]
d.show("versions per replica", sib)
d.check("2 concurrent siblings on the replicas", all(n == 2 for n in sib))
body = d.status_of(tid)
d.show("read result", {k: body["dynamo"][k] for k in ("siblings_found", "siblings_reconciled", "vector_clock")})
d.check("read reconciled them", body["dynamo"]["siblings_reconciled"] == 2 and body["state"]["status"] == "OUT_FOR_DELIVERY")

d.step("(e) Shipment Analytics - Flink on EMR: 1-minute windows per carrier + transit/delay per shipment",
       "Flink reads Kafka 'shipment-events', keeps window + keyed state, writes 'shipment-analytics-results'")
d.scan(tid, "DELIVERED", "Bengaluru store")
if d.LIVE:
    print("   waiting for a 1-minute window to close (up to ~2.5 min)...")
    summary = d.wait(lambda: (lambda s: s if s.get("carriers_last_15_windows") else None)(
        d.get("/api/analytics/summary")[1]), 150, 5)
    d.show("analytics summary", summary)
    d.check("Flink results arriving", bool(summary))
    if d.have("aws", "terraform") and d.tf("emr_cluster_id"):
        d.run(f"aws emr list-steps --region {d.region()} --cluster-id {d.tf('emr_cluster_id')} "
              "--query 'Steps[0].[Name,Status.State]' --output text")
else:
    d.skip("Flink analytics", "runs on AWS (EMR)")

d.done()
