# Running it on AWS

## Before the first run (once)

1. **A regular AWS account** with admin rights. AWS Academy Learner Lab won't work: it can't create the IAM roles EKS, EMR and Lambda need.
2. **EC2 vCPU quota ≥ 32** in your region. Check *Service Quotas → EC2 → Running On-Demand Standard instances*. New accounts often have less, so request an increase early: approval can take up to a day.
3. Install the AWS CLI v2 (`aws configure`), Terraform ≥ 1.6, kubectl, Docker Desktop, git and python3.
4. Push the code to a **public** GitHub repo (Argo CD reads it). Then:
   ```bash
   make configure REPO=https://github.com/<you>/<repo>.git REGION=ap-south-1
   git add -A && git commit -m configure && git push
   ```
   Optional, in `infra/terraform/01-platform/terraform.tfvars`: teammates' IAM ARNs
   (`admin_principal_arns`) so they can use kubectl, and `alert_email` for SNS alert e-mails.

## Start: `make up` (~75–90 min)

| Step | Time | What happens |
|---|---|---|
| 1 infra | ~35 min | Terraform creates the VPC, EKS, RDS, MSK, DynamoDB, S3, SQS, SNS, Lambda, ECR and IAM |
| 2 images | 5–15 min | builds the 7 images, pushes them to ECR, commits the image tags to Git (this is the GitOps deployment) |
| 3 gitops | 3 min | Terraform installs Argo CD, which deploys apps and monitoring from Git |
| 4 wait | 5–15 min | waits until every Argo CD app is healthy and the public URL answers |
| 5 emr | ~15 min | EMR cluster + Flink job |

If a step fails, fix the cause and run `make up` again; it continues. To run one step:
`scripts/up.sh infra|images|gitops|wait|emr`. `make urls` prints the links. The load balancer
address is new after every `make up`.

## Stop: `make down` (~30–40 min)

It first deletes the load balancers and disks that Kubernetes created (Terraform doesn't know
about them), then destroys everything with Terraform. Then run `make status`: it must say
*Everything is OFF*. **Run `make down` after every session**; a forgotten system costs ~$30/day.

Data is deleted too. `make simulate` creates fresh, realistic traffic in a few minutes.

## Demo-day plan

| When | What |
|---|---|
| A few days before | Full rehearsal: `make up`, `make check`, run through `docs/DEMO.md` once, `make down` |
| Day before | `make up`, `make check`. Leave it running (~$25–35) or `make down` and start again 2 h before the demo |
| 15 min before | `make status`, `make urls`, start `make simulate` in a terminal, open the tabs (web app, Grafana, Argo CD, AWS console) |
| After | `make down`, `make status` |

Terraform keeps its state on the laptop that ran `make up` (the state files are git-ignored), so
the same person runs `make up` and `make down`. Teammates run their checks from their own laptops
with kubectl access (`admin_principal_arns`).

## If something goes wrong

| Symptom | Fix |
|---|---|
| `not authorized` creating EKS/IAM | Learner Lab or restricted account: use a normal account |
| Pods `Pending` for long | vCPU quota too low (see "Before the first run") |
| Argo app `supplychain` shows `ImagePullBackOff` | image tags not pushed to Git: `scripts/up.sh images` |
| Argo app says "repository not found" | repo is private: make it public, or set `git_username`/`git_token` in `02-bootstrap/terraform.tfvars` |
| S3 "bucket already exists" right after `make down` | wait 10 min, then `make up` again |
| No analytics in the demo | EMR not up (`scripts/up.sh emr`), or no traffic yet (`make simulate`). Flink errors: Grafana → Explore → Loki `{job="emr-flink"}` |
| `make down` stops with a VPC error | run `make down` again |
