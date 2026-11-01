# Supply-chain shipment tracking on AWS - everything you need, nothing more.
# BASE = where the checks point: the live load balancer if the system is up, else your laptop.
BASE ?= $(shell h=$$(kubectl -n supplychain get ingress supplychain -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null); [ -n "$$h" ] && echo http://$$h || echo http://localhost:8000)
CHECK = BASE=$(BASE) python3 checks

.PHONY: help configure up down status urls local test check check-m1 check-m2 check-m3 check-m4 gitops-demo load simulate flink

help:          ## list commands
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-12s %s\n", $$1, $$2}'

# ---- on your laptop (no AWS)
local:         ## run the whole system on http://localhost:8000
	python3 local/run_local.py

test:          ## start the system locally and run all four member checks against it
	python3 local/run_local.py --test

# ---- on AWS
configure:     ## one-time: REPO=https://github.com/you/repo.git [REGION=ap-south-1]
	scripts/configure.sh $(REPO) $(or $(REGION),ap-south-1)

up:            ## start everything on AWS (~75-90 min, safe to re-run)
	scripts/up.sh

down:          ## delete everything from AWS (~30-40 min) - do this after every session
	scripts/down.sh

status:        ## is anything still running / costing money?
	scripts/status.sh

urls:          ## print web app, Grafana, Lambda and Argo CD URLs + passwords
	scripts/urls.sh

# ---- self-checks: each member confirms their own part works (laptop or live)
check:         ## all four member checks
	$(CHECK)/m1_supplier_inventory.py && $(CHECK)/m2_booking_notification.py && $(CHECK)/m3_ingestion_publisher.py && $(CHECK)/m4_status_analytics.py
check-m1:      ## Member 1: Supplier + Inventory | IaC + GitOps
	$(CHECK)/m1_supplier_inventory.py
check-m2:      ## Member 2: Booking + Notification Lambda | serverless/comms + storage
	$(CHECK)/m2_booking_notification.py
check-m3:      ## Member 3: Tracking Ingestion + Event Publisher | EKS/HPA + observability
	$(CHECK)/m3_ingestion_publisher.py
check-m4:      ## Member 4: Delivery Status (Dynamo store) + Analytics | concept + Flink
	$(CHECK)/m4_status_analytics.py

# ---- used during the live whole-system demo (docs/DEMO.md)
gitops-demo:   ## live: change replicas in Git and watch Argo CD apply it (GitOps)
	scripts/gitops-demo.sh
load:          ## live: k6 load test that makes the HPAs scale out
	scripts/load-test.sh
simulate:      ## realistic background traffic (start ~10 min before the demo)
	python3 scripts/simulate.py --base $(BASE)
flink:         ## rebuild + resubmit only the Flink job
	scripts/flink-job.sh
