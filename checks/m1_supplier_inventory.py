#!/usr/bin/env python3
"""Member 1 self-check - Supplier Service + Inventory Service | (a) Infrastructure as Code, (d) GitOps

    make check-m1             (AWS: uses the live load balancer; locally: BASE=http://localhost:8000)
    make gitops-demo         (AWS: change replicas in Git -> Argo CD applies it, no kubectl apply)
"""
import common as d

d.title("Member 1 - Supplier + Inventory services | IaC (Terraform) + GitOps (Argo CD)")

d.step("Register a supplier (Supplier Service, REST + PostgreSQL)",
       "POST /api/suppliers stores the row and an outbox event in ONE database transaction")
code, sup = d.post("/api/suppliers", {"name": "Acme Components", "country": "India", "contact_email": "ops@acme.example"})
d.show("response", sup)
d.check("supplier created", code == 201 and sup["supplier_id"].startswith("SUP-"))
d.check("invalid e-mail rejected (422)", d.post("/api/suppliers", {"name": "Bad", "country": "IN",
                                                                  "contact_email": "nope"})[0] == 422)

d.step("Put goods in stock (Inventory Service, REST)")
sku = "SKU-PUMP-" + sup["supplier_id"][-4:]
code, item = d.post("/api/inventory/items", {"sku": sku, "supplier_id": sup["supplier_id"],
                                             "name": "Industrial pump", "quantity": 100, "reorder_level": 5})
d.show("item", {k: item[k] for k in ("sku", "quantity_on_hand", "quantity_reserved", "available")})
d.check("100 units available", item["available"] == 100)

d.step("Book a shipment -> Booking calls Inventory over gRPC to reserve stock",
       "ReserveStock(order_ref = tracking id) is idempotent and locks rows so stock is never oversold")
code, ship = d.book(sup["supplier_id"], sku, qty=10)
tid = ship["tracking_id"]
_, res = d.get(f"/api/inventory/reservations/{tid}")
d.show("reservation", {k: res.get(k) for k in ("reservation_id", "order_ref", "status", "lines")})
d.check("gRPC reservation RESERVED", res.get("status") == "RESERVED")
d.check("stock reserved (90 available)", d.get(f"/api/inventory/items/{sku}")[1]["available"] == 90)

d.step("Carrier picks the parcel up -> Inventory reacts to the Kafka event and dispatches stock",
       "Tracking Ingestion -> outbox -> Event Publisher -> Kafka -> Inventory consumer")
d.scan(tid, "PICKED_UP", "Mumbai DC")
res = d.wait(lambda: (lambda r: r if r.get("status") == "DISPATCHED" else None)(
    d.get(f"/api/inventory/reservations/{tid}")[1]), 30)
d.check("reservation DISPATCHED via Kafka", res is not None)
d.check("on-hand stock now 90", d.get(f"/api/inventory/items/{sku}")[1]["quantity_on_hand"] == 90)

d.step("Suspend the supplier -> new bookings are refused (Booking checks Supplier over REST)")
d.call("PATCH", f"/api/suppliers/{sup['supplier_id']}/status", {"status": "SUSPENDED"})
d.check("booking refused (422)", d.book(sup["supplier_id"], sku, qty=1)[0] == 422)

d.step("(a) Infrastructure as Code - everything was created by Terraform",
       "infra/terraform/01-platform (cloud) + 02-bootstrap (Argo CD); nothing created by hand")
if d.LIVE and d.have("terraform"):
    _, out = d.run(f"terraform -chdir={d.TF1} state list | sed -E 's/\\[.*//' | sort | uniq -c | sort -rn | head -15")
    _, count = d.run(f"terraform -chdir={d.TF1} state list | wc -l", show_output=False)
    d.check(f"{count.strip()} resources managed by Terraform", int(count.strip() or 0) > 50)
else:
    d.skip("terraform state", "runs on AWS (from the laptop that ran make up)")

d.step("(d) GitOps - Argo CD deploys everything from the Git repository",
       "Apps are Synced to the Git commit; nobody runs kubectl apply. Live change: make gitops-demo")
if d.LIVE and d.have("kubectl"):
    d.run("kubectl -n argocd get applications")
    code, out = d.run("kubectl -n argocd get applications -o jsonpath='{range .items[*]}{.status.sync.status}/{.status.health.status} {end}'",
                      show_output=False)
    d.check("all applications Synced/Healthy", code == 0 and out and all(x == "Synced/Healthy" for x in out.split()))
else:
    d.skip("Argo CD applications")

d.done()
