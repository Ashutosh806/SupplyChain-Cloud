#!/usr/bin/env bash
# Create the EMR (Flink) cluster wired to Loki for log shipping, then build + submit the Flink job.
source "$(dirname "$0")/lib.sh"
need terraform kubectl aws
log "Waiting for the internal Loki NLB (created by Argo CD from deploy/monitoring)"
for i in $(seq 1 60); do
  LOKI_HOST=$(kubectl -n monitoring get svc loki-ingest -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)
  [ -n "$LOKI_HOST" ] && break; sleep 10
done
[ -n "${LOKI_HOST:-}" ] || { echo "loki-ingest NLB not ready; is Argo CD synced?"; exit 1; }
LOKI_URL="http://$LOKI_HOST:3100/loki/api/v1/push"
log "terraform apply 01-platform with enable_emr=true (Loki: $LOKI_URL)"
terraform -chdir="$TF1" apply -input=false -auto-approve -var enable_emr=true -var "loki_push_url=$LOKI_URL"
# remember the flags for later applies
grep -q enable_emr "$TF1/terraform.tfvars" 2>/dev/null || printf 'enable_emr    = true\nloki_push_url = "%s"\n' "$LOKI_URL" >> "$TF1/terraform.tfvars"
"$(dirname "$0")/flink-job.sh"
