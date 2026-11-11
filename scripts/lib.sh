#!/usr/bin/env bash
# Small helpers shared by the scripts.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF1="$ROOT/infra/terraform/01-platform"     # AWS infrastructure
TF2="$ROOT/infra/terraform/02-bootstrap"    # Argo CD + environment bindings
SERVICES="supplier-service booking-service inventory-service tracking-ingestion event-publisher delivery-status analytics-api"
CLUSTER=supplychain-eks

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
die() { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }
tf1() { terraform -chdir="$TF1" output -raw "$1" 2>/dev/null; }
region() { tf1 region || sed -nE 's/^region *= *"([^"]+)".*/\1/p' "$TF1/terraform.tfvars" 2>/dev/null || echo ap-south-1; }
alb() { kubectl -n supplychain get ingress supplychain -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true; }
