#!/usr/bin/env python3
"""Member 2 self-check - Shipment Booking + Notification (Lambda) | (b) serverless + REST/gRPC/queues, (g) storage

    make check-m2
"""
import time
import urllib.request

import common as d

d.title("Member 2 - Booking + Notification (Lambda) | serverless & communication + 3 storage products")
sup, sku = d.supplier_with_stock(50)

d.step("Book a shipment (public REST API)",
       "Booking -> Supplier (REST: is it ACTIVE?) -> Inventory (gRPC: reserve) -> PostgreSQL + outbox")
code, ship = d.book(sup["supplier_id"], sku, qty=5)
d.show("shipment", {k: ship.get(k) for k in ("tracking_id", "status", "carrier", "reservation_id", "promised_delivery")})
d.check("booked with a tracking id", code == 201 and ship["tracking_id"].startswith("SCT"))
tid = ship["tracking_id"]
code, body = d.book(sup["supplier_id"], sku, qty=5000)
d.check("too much quantity -> 409 with the shortage from Inventory", code == 409, body)

d.step("The booking event travels over Kafka (message queue) to Delivery Status")
d.check("status BOOKED", d.wait_status(tid, "BOOKED") is not None)

d.step("Carrier reports a delay -> Delivery Status puts a message on SQS -> Lambda notifies",
       "Lambda (serverless, event-driven): stores the notification in DynamoDB (idempotent) + SNS e-mail")
d.scan(tid, "PICKED_UP", "Mumbai DC", hours_ago=3)
d.scan(tid, "DELAYED", "Pune Hub", note="Heavy rain on NH48")
d.check("status DELAYED", d.wait_status(tid, "DELAYED") is not None)
_, cfg = d.get("/api/config")
feed = cfg.get("notification_url") if isinstance(cfg, dict) else ""
if d.LIVE and feed:
    def notifications():
        code, body = d.call("GET", "", url=f"{feed.rstrip('/')}/?tracking_id={tid}")
        return body.get("items") if code == 200 and isinstance(body, dict) else None
    items = d.wait(lambda: (lambda i: i if i and any(n["status"] == "DELAYED" for n in i) else None)(notifications()), 60, 3)
    d.show("notifications (from the Lambda Function URL)", [{k: n.get(k) for k in ("status", "severity", "message")}
                                                           for n in (items or [])])
    d.check("Lambda created the DELAYED notification", bool(items))
else:
    d.skip("Lambda notification feed")

d.step("Upload a proof-of-delivery file to S3 -> S3 event triggers the same Lambda")
d.scan(tid, "DELIVERED", "Bengaluru store")
if d.LIVE and feed:
    code, up = d.post(f"/api/status/{tid}/pod-upload-url", {"filename": "pod.txt", "content_type": "text/plain"})
    try:
        req = urllib.request.Request(up["upload_url"], data=b"signed by store manager", method="PUT",
                                     headers={"content-type": "text/plain"})
        urllib.request.urlopen(req, timeout=15)
        print(f"   uploaded s3://{d.tf('data_bucket') or '<bucket>'}/{up['key']}")
        uploaded = True
    except Exception as exc:
        uploaded = d.check("upload to S3 (presigned URL)", False, f"{code} {up} {exc}")
    if uploaded:
        got = d.wait(lambda: any(n["status"] == "POD_UPLOADED" for n in (notifications() or [])), 60, 3)
        d.check("Lambda reacted to the S3 upload", bool(got))
else:
    d.skip("S3 upload trigger")

d.step("Cancel a booking -> stock released over gRPC")
_, other = d.book(sup["supplier_id"], sku, qty=3)
d.post(f"/api/shipments/{other['tracking_id']}/cancel")
_, res = d.get(f"/api/inventory/reservations/{other['tracking_id']}")
d.check("reservation RELEASED", res.get("status") == "RELEASED")

d.step("(g) Three storage products in use",
       "RDS PostgreSQL (relational) - DynamoDB (notifications + analytics) - S3 (raw events + uploads)")
if d.LIVE and d.have("aws", "terraform"):
    r = d.region()
    code, out = d.run(f"aws rds describe-db-instances --region {r} --query 'DBInstances[].[DBInstanceIdentifier,Engine,DBInstanceStatus]' --output text")
    d.check("RDS PostgreSQL available", "available" in out)
    code, out = d.run(f"aws dynamodb scan --region {r} --table-name {d.tf('notifications_table')} --select COUNT --query Count")
    d.check("DynamoDB holds notifications", out.isdigit() and int(out) > 0)
    time.sleep(1)
    code, out = d.run(f"aws s3 ls --region {r} s3://{d.tf('data_bucket')}/ --recursive | tail -5")
    d.check("S3 holds raw carrier events / uploads", bool(out))
else:
    d.skip("RDS / DynamoDB / S3 listing")

d.done()
