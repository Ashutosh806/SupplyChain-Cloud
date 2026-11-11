# Supply Chain Shipment Tracking on AWS project

CS/SS G527 Cloud Computing · Domain: shipment tracking from suppliers to retailers ·
Concept: **leaderless replication / Dynamo-like storage**.

8 microservices on AWS. 7 are containers on **EKS** deployed by **Argo CD**; the Notification
Service is a **Lambda**; Shipment Analytics is a **Flink** job on **EMR**. All infrastructure is
**Terraform**, events go over **Kafka (MSK)** and **SQS**, data lives in **RDS PostgreSQL**,
**DynamoDB** and **S3**, and monitoring is **Prometheus/Grafana + Loki**. The public web app is at
`http://<load-balancer>/`.

## Who builds what (2 requirements + 2 services each)

| Member | Services | Requirements | Code | Self-check |
|---|---|---|---|---|
| 1 | Supplier, Inventory | (a) IaC, (d) GitOps | `services/supplier-service`, `services/inventory-service`, `infra/terraform`, `deploy/argocd`, `deploy/platform` | `make check-m1` |
| 2 | Shipment Booking, Notification (Lambda) | (b) serverless + REST/gRPC/queues, (g) storage | `services/booking-service`, `services/notification-lambda`, `infra/terraform/01-platform/{serverless,databases}.tf` | `make check-m2` |
| 3 | Tracking Ingestion, Event Publisher | (c) EKS + HPA + load test, (h) observability | `services/tracking-ingestion`, `services/event-publisher`, `deploy/apps/base/hpa.yaml`, `deploy/monitoring`, `loadtest` | `make check-m3` |
| 4 | Delivery Status, Shipment Analytics | (e) Flink on EMR, (f) Dynamo concept | `services/delivery-status`, `services/analytics-api`, `analytics/flink-job`, `infra/terraform/01-platform/emr.tf` | `make check-m4` |

Shared: `services/common` (logging, metrics, DB, outbox, Kafka helpers) and `services/proto`.

**Self-checks:** each member's `checks/mX_*.py` exercises their part step by step and prints
PASS/FAIL. It runs on a laptop (cloud-only steps are skipped) or against the live system on AWS,
so everyone can confirm their part works before the demo. **The demo itself is one walkthrough
of the whole system**: [docs/DEMO.md](docs/DEMO.md).

## Work on your laptop (no AWS)

```bash
python3 -m venv .venv && source .venv/bin/activate   # Python 3.12, 3.13 or 3.14
pip install -r services/requirements.txt     # + PostgreSQL 16 installed (apt/brew) or Docker
make local                                   # whole system on http://localhost:8000
make check-m2                                # in another terminal: check your part against it
make test                                    # all four member checks (also run by CI on every push)
cd analytics/flink-job && mvn test           # Member 4: Flink job tests
```

## Run it live on AWS

```bash
make configure REPO=https://github.com/<you>/<repo>.git   # once; then: git commit -am configure && git push
make up        # ~75-90 min: Terraform -> images -> Argo CD -> live URL -> EMR + Flink
make urls      # web app, Grafana, Lambda, Argo CD
make check     # every member's check against the live system
make down      # ~30-40 min: deletes everything (~$0 while down)
make status    # confirms nothing is left running
```
~US$1–1.5/hour while up. Details and demo-day plan: [docs/DEPLOY.md](docs/DEPLOY.md) ·
live demo script: [docs/DEMO.md](docs/DEMO.md) · design document: [docs/DESIGN.md](docs/DESIGN.md)

## Layout

```
services/        7 container services + notification-lambda + common lib + protos
analytics/       Flink job (Java)
infra/terraform  01-platform (all AWS infrastructure) · 02-bootstrap (Argo CD)
deploy/          what Argo CD deploys: apps, monitoring, platform
checks/          one self-check script per member
local/           run everything on a laptop
scripts/         configure, up, down, status, urls, load test, gitops demo, simulate
loadtest/k6      HPA load test
docs/            DESIGN.md (deliverable), DEPLOY.md, DEMO.md
task_divison.txt   (spelled exactly as in the assignment brief)
```
