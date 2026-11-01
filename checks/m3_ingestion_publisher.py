#!/usr/bin/env python3
"""Member 3 self-check - Tracking Ingestion + Event Publisher | (c) EKS + HPA + load test, (h) observability

    make check-m3          then, for the scaling demo:  make load   (and watch: kubectl -n supplychain get hpa -w)
"""
import uuid

import common as d

d.title("Member 3 - Tracking Ingestion + Event Publisher | EKS/HPA/load test + observability")
sup, sku = d.supplier_with_stock(20)
_, ship = d.book(sup["supplier_id"], sku)
tid = ship["tracking_id"]

d.step("A carrier webhook posts a scan (Tracking Ingestion Service)",
       "validated, stored, and written to the outbox in one transaction; raw copy archived to S3")
ceid = f"DHL-{uuid.uuid4().hex[:8]}"
code, body = d.scan(tid, "AT_HUB", "Pune Hub", carrier_event_id=ceid)
d.show("response", body)
d.check("accepted (202)", code == 202 and body["accepted"] == 1)

d.step("The carrier retries the same scan -> de-duplicated")
code, body = d.scan(tid, "AT_HUB", "Pune Hub", carrier_event_id=ceid)
d.check("duplicate ignored", body.get("duplicates") == 1 and body.get("accepted") == 0, body)
d.check("unknown tracking id rejected (422)", d.scan("SCTDOESNOTEXIST", "AT_HUB")[0] == 422)

d.step("Event Publisher relays the outbox to Kafka reliably",
       "SELECT ... FOR UPDATE SKIP LOCKED -> produce (acks=all, idempotent) -> mark published")
d.check("Delivery Status saw the scan (AT_HUB)", d.wait_status(tid, "AT_HUB") is not None)
_, stats = d.get("/api/publisher/stats")
d.show("publisher stats", stats)
d.check("outbox backlog drained", d.wait(lambda: sum(d.get('/api/publisher/stats')[1]['backlog'].values()) < 50, 20))

d.step("(c) Managed Kubernetes (EKS Auto Mode) with Horizontal Pod Autoscalers",
       "HPAs on tracking-ingestion, booking-service, inventory-service (CPU/memory). Load: make load")
if d.LIVE and d.have("kubectl"):
    d.run("kubectl get nodes -o wide | cut -c1-90")
    code, out = d.run("kubectl -n supplychain get hpa")
    d.check("at least 2 HPAs", out.count("Deployment/") >= 2)
    d.run("kubectl -n supplychain get pods | head -20")
else:
    d.skip("EKS nodes / HPAs")

d.step("(h) Observability - Prometheus + Grafana dashboard, Loki logs (incl. the Flink job)",
       f"Grafana: {d.BASE}/grafana/  -> dashboard 'Supply Chain - Overview'; Explore -> Loki: "
       '{namespace="supplychain"} | json | level="ERROR"   and   {job="emr-flink"}')
if d.LIVE:
    code, body = d.get("/grafana/api/health")
    d.check("Grafana is up", code == 200, body)
    if d.have("kubectl"):
        d.run("kubectl -n monitoring get pods | head -15")
else:
    d.skip("Grafana / Prometheus / Loki")

d.done()
