# Supply Chain Shipment Tracking Platform on AWS

A cloud-native platform that tracks shipments and performs other logistics operations. The system consists of multiple microservices deployed on Amazon Web Services. All infrastructure is defined in Terraform and every Kubernetes workload is deployed through GitOps.

## Architecture

| # | Service | Responsibility | Runs on |
|---|---|---|---|
| 1 | Supplier Service | Supplier records and status (active / suspended) | Amazon EKS |
| 2 | Shipment Booking Service | Creates and cancels shipments, assigns tracking IDs, serves the web application | Amazon EKS (autoscaled) |
| 3 | Inventory Service | Stock levels, reservations and dispatch | Amazon EKS (autoscaled) |
| 4 | Tracking Ingestion Service | Receives, validates and archives carrier status events | Amazon EKS (autoscaled) |
| 5 | Event Publisher | Publishes database changes to Kafka using the transactional outbox pattern | Amazon EKS |
| 6 | Delivery Status Service | Derives the current state of each shipment; leaderless Dynamo-style store | Amazon EKS (StatefulSet) |
| 7 | Notification Service | Records notifications and sends alerts | AWS Lambda |
| 8 | Shipment Analytics Service | Transit time, delay and event-processing metrics | Apache Flink on Amazon EMR, query API on EKS |


## Technology stack

| Area | Technology |
|---|---|
| Cloud | AWS, region ap-south-1 (Mumbai) |
| Infrastructure as Code | Terraform |
| Containers and orchestration | Docker, Amazon ECR, Amazon EKS (Auto Mode), Horizontal Pod Autoscaler |
| GitOps | Argo CD (app of apps) |
| Services | Python (FastAPI, gRPC), Java (Apache Flink 1.20) |
| Messaging | Amazon MSK (Kafka), Amazon SQS, Amazon SNS |
| Data | Amazon RDS for PostgreSQL, Amazon DynamoDB, Amazon S3 |
| Serverless and analytics | AWS Lambda, Amazon EMR |
| Observability | Prometheus, Grafana, Loki, Fluent Bit |
| Testing | GitHub Actions, k6 |

## Repository structure

```
services/          seven containerised services, the Lambda function, shared library and gRPC contracts
analytics/         Flink job (Java, Maven)
infra/terraform/   01-platform (AWS infrastructure) and 02-bootstrap (Kubernetes namespaces, Argo CD)
infra/emr-bootstrap/  EMR bootstrap action for log shipping
deploy/            manifests deployed by Argo CD: applications, platform settings, monitoring
loadtest/k6/       load test used to demonstrate autoscaling
checks/            end-to-end verification scripts
local/             runs the complete system on a single machine without AWS
scripts/           provisioning, teardown, status, load test, GitOps demonstration, traffic simulator
docs/              deployment guide, technical design notes, task division
```

## Setup

```bash
make configure REPO=<repository>.git REGION=ap-south-1
git add -A && git commit -m "configure" && git push
```

Optional settings in `infra/terraform/01-platform/terraform.tfvars`: `admin_principal_arns` (IAM
users or roles that receive cluster access) and `alert_email` (e-mail subscription for SNS alerts if needed).

## Deployment

`make up` provisions the complete system in five stages. Each stage can be re-run safely; if a
stage fails, running `make up` again continues from where it stopped.

| Stage | Duration | Action |
|---|---|---|
| infra | ~35 min | Terraform creates the VPC, EKS, RDS, MSK, DynamoDB, S3, SQS, SNS, Lambda, ECR and IAM roles |
| images | 5-15 min | Builds the container images, pushes them to ECR and commits the new image tags to Git |
| gitops | ~3 min | Terraform installs Argo CD, which deploys the applications and monitoring from Git |
| wait | 5-15 min | Waits until every Argo CD application is healthy and the public URL responds |
| emr | ~15 min | Creates the EMR cluster and submits the Flink job |

`make down` removes every resource (about 30-40 minutes), including load balancers and volumes
created by Kubernetes. `make status` confirms that nothing is left running. The system costs
approximately US$2-2.5 per hour while it is running.

Terraform state is stored locally, so `make up` and `make down` should be run from the same machine.

## Command reference

| Command | Purpose |
|---|---|
| `make help` | List all commands |
| `make configure REPO=<url> [REGION=<region>]` | One-time configuration of the repository URL and region |
| `make up` | Provision and deploy the complete system on AWS |
| `scripts/up.sh infra\|images\|gitops\|wait\|emr` | Run a single deployment stage |
| `make urls` | Print the web application, Grafana, Lambda and Argo CD URLs and credentials |
| `make status` | Show which AWS resources are running |
| `make down` | Delete all AWS resources |
| `make check` | Run all end-to-end checks against the running system |
| `make check-m1` | Check Supplier and Inventory services, Infrastructure as Code and GitOps |
| `make check-m2` | Check Booking and Notification services, communication and storage |
| `make check-m3` | Check Tracking Ingestion and Event Publisher, autoscaling and observability |
| `make check-m4` | Check Delivery Status (leaderless store) and Shipment Analytics |
| `make simulate` | Generate continuous realistic traffic (bookings and carrier scans) |
| `make load` | Run the k6 load test that drives the Horizontal Pod Autoscalers |
| `make gitops-demo` | Change a replica count in Git and wait for Argo CD to apply it |
| `make flink` | Rebuild and resubmit only the Flink job |
| `make local` | Run the complete system locally at http://localhost:8000 |
| `make test` | Start the system locally and run all checks |

## Local development

The complete system can't be run on one machine without AWS, but there are workarounds. Kafka is replaced by an in-memory
stand-in, and the AWS-only steps (Lambda, S3, DynamoDB, EMR) are skipped by the checks.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r services/requirements.txt    # requires PostgreSQL 16 locally, or Docker
make local                                  # system available at http://localhost:8000
make test                                   # runs all checks against a local instance
cd analytics/flink-job && mvn test          # Flink job unit tests
```

GitHub Actions runs `make test` on Python 3.12 and 3.14 and builds the Flink job on every push
and pull request.

## Demo

Start `make simulate` beforehand so that dashboards and analytics have data.

1. **Web application** (`make urls`): register a supplier, add stock and book a shipment.
2. **Shipment tracking:** follow a shipment's status history and notifications as carrier scans arrive.
3. **Leaderless store:** in the store panel, view the hash ring and a shipment's replicas, take a
   replica node offline while updates continue, bring it back, and create a conflict that is merged on read.
4. **Observability:** open the Grafana dashboard "Supply Chain - Overview", then the Loki logs in
   Explore, including the Flink job's logs (`{job="emr-flink"}`).
5. **Autoscaling:** run `make load` and watch the replica and node counts rise on the dashboard.
6. **GitOps:** run `make gitops-demo` and watch Argo CD apply the change from Git.
7. **Analytics:** show the Flink window results in the web application and on the dashboard.

