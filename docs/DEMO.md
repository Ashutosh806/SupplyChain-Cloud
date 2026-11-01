# Live demo: the whole system (~15–20 min)

One continuous demo of the complete system. The **Owner** column says which member explains
that part: the brief requires each student to demonstrate their own tasks, so each member speaks
when the story reaches their part.

(The per-member `make check-mX` scripts are for testing before the demo. They are not part of it.)

## Before the demo

* System running: `make up` (day before) → `make check` all green → `make urls`.
* 10–15 min before: start `make simulate` in a terminal and leave it running, so Grafana and the
  Flink windows already show live data.
* Open these tabs:
  1. web app `http://<alb>/`
  2. Grafana `http://<alb>/grafana/` → dashboard *Supply Chain – Overview*
  3. Argo CD (`kubectl -n argocd port-forward svc/argocd-server 8080:80` → http://localhost:8080)
  4. AWS console: EKS, MSK, RDS, DynamoDB, S3, Lambda, EMR
* Open 2 terminals in the repo: one for `make load` / `make gitops-demo`, one for `kubectl -n supplychain get hpa -w`.

## 1. What we built (1 min, any member)

Show the architecture diagram (`docs/DESIGN.md` §2–3), then say:

> Suppliers ship goods to retailers. We track every shipment through **8 microservices** on AWS:
> 7 containers on EKS, 1 Lambda, and a Flink job on EMR. They talk over REST, gRPC, Kafka and SQS,
> and store data in RDS, DynamoDB and S3. Everything is created by Terraform and deployed by Argo CD.
> We'll follow one shipment through all of it, live, on our public URL.

## 2. One shipment through the system (web app tab, ~8 min)

| # | Click in the web app | Owner | Say |
|---|---|---|---|
| 1 | §1 **Register** a supplier, **Add stock** | M1 | Supplier + Inventory services: REST APIs, data in **RDS PostgreSQL** |
| 2 | §2 **Book** | M2 | public **REST** API. Booking checks the supplier (REST) and reserves stock in Inventory over **gRPC**; the toast shows the reservation id. The event is saved in an outbox and published to **Kafka (MSK)** |
| 3 | §3 **Picked up**, **In transit** | M3 | carrier scans arrive at Tracking Ingestion (duplicates ignored, raw copy archived to **S3**). The Event Publisher relays them to Kafka. In §1, stock goes from *reserved* to *shipped*: Inventory reacted to the Kafka event |
| 4 | §4 live status (refreshes itself) | M4 | Delivery Status stores each shipment in our **leaderless, Dynamo-style store** (5 nodes). The key hashes onto a ring and is stored on N=3 nodes; a read waits for R=2, a write for W=2. "Coordinator" changes on every refresh: **no leader**, any node serves any request |
| 5 | §5 **Crash 60s** on the first node of the *preference list*, then §3 **Out for delivery** | M4 | a replica is down, yet the write succeeds (sloppy quorum); the crashed node's card shows *1 hint held for it*. **Recover** → *hints delivered* goes up: it caught up (hinted handoff) |
| 6 | §4 **Create concurrent siblings** | M4 | two nodes wrote at the same time; vector clocks detect the conflict and the next read merges both versions without losing either |
| 7 | §3 **Delayed** (note: "heavy rain"), then type location "Bengaluru store" and click **Delivered** | M2 | Delivery Status → **SQS** → **Lambda** (serverless, event-driven) → DynamoDB + SNS e-mail. §6 shows the notifications, read from the Lambda's own public URL |
| 8 | §3 choose any file → **Upload POD** | M2 | the browser uploads straight to **S3**; the upload event triggers the same Lambda → *POD_UPLOADED* in §6 |
| 9 | §7 analytics | M4 | **Flink on EMR** reads the same Kafka events and computes 1-minute windows per carrier (events, unique shipments, delays) plus transit time and delay per shipment. Results go to a separate Kafka topic, then DynamoDB |

## 3. Scaling, GitOps, monitoring (~6 min)

| # | Do | Owner | Say |
|---|---|---|---|
| 10 | Terminal 1: `make load`. Terminal 2 shows `kubectl -n supplychain get hpa -w` | M3 | k6 sends sustained traffic to the public URL (~250 carrier events/s). The **HPAs** react to CPU; EKS Auto Mode adds nodes if needed. *Leave it running and come back.* |
| 11 | Argo CD tab: root app → child apps, all *Synced/Healthy* | M1 | everything in Kubernetes is deployed by **Argo CD** from our Git repo; nobody runs `kubectl apply` |
| 12 | Terminal 1 (new tab): `make gitops-demo` | M1 | changes the Supplier Service replica count **in Git** and pushes. Argo CD picks up the commit within ~1 min and the pods change (watch the Argo CD tab) |
| 13 | `infra/terraform/` in the editor + AWS console resource list | M1 | **Terraform** created everything: VPC, EKS, RDS, MSK, DynamoDB, S3, SQS, SNS, Lambda, EMR, IAM. `make up` / `make down` builds and deletes the whole environment |
| 14 | Terminal 2: the HPA replicas have gone up | M3 | autoscaling under sustained load |
| 15 | Grafana dashboard | M3 | RPS, error rate, latency (p50/p95/p99) per service; HPA replicas; node CPU/memory, pod restarts; Kafka pipeline; store health; Flink results |
| 16 | Grafana → Explore → Loki: `{namespace="supplychain"} \| json` then `{job="emr-flink"}` | M3 | centralised logs from every microservice **and from the Flink job on EMR** |
| 17 | AWS console: DynamoDB table items, S3 `raw/` + `pod/`, RDS instance | M2 | the **three storage products** and what each stores |
| 18 | AWS console: EMR cluster → Steps / Flink UI | M4 | the Flink job running on the managed cluster, consuming from Kafka and writing results to the separate results topic |

Close: *"That's the whole system: 8 services, every requirement, live. Questions?"*

## If something misbehaves during the demo

* A panel shows an error → refresh the page (the system keeps running).
* §6 stays empty → the Lambda e-mail/feed can lag 10–20 s; continue and come back.
* §7 empty → Flink windows need a minute of traffic (that's why `make simulate` runs beforehand).
* HPA not scaling yet → it reacts within ~1–2 min of load; show Grafana and come back.

## Likely questions

* **Where is the leader election?** There is none, by design: that's what *leaderless* replication means. Raft/Paxos elect a leader that orders all writes and pause when it fails. In our Dynamo-style store every node can coordinate any read or write. Conflicts are handled with vector clocks and merging, missed writes with hinted handoff, read repair and Merkle anti-entropy.
* **R + W > N?** 2 + 2 > 3: any read quorum overlaps any write quorum, so reads see the latest write while 2 replicas are healthy.
* **Why the outbox?** A database commit and a Kafka publish can't be atomic, so the event is written in the same DB transaction and relayed afterwards. Nothing gets lost, and there are no phantom events.
* **Exactly once?** At-least-once delivery + idempotent consumers (event ids, reservation states, conditional writes).
* **Why gRPC for Inventory?** A synchronous, latency-critical call with a typed contract (`inventory.proto`) and deadlines.
* **Why is Delivery Status a StatefulSet?** Each node needs a stable identity on the ring and its own disk; the other services are stateless.
* **What triggers the Lambda?** SQS messages and S3 uploads under `pod/`.
* **How do the Flink windows close when traffic stops?** Watermark idleness + heartbeat events from the Event Publisher.
