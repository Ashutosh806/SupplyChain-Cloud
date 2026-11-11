#!/usr/bin/env bash
# Tear everything down (in the right order so Kubernetes-created ALB/NLB/EBS don't block VPC deletion).
source "$(dirname "$0")/lib.sh"
need terraform kubectl aws
read -r -p "Destroy ALL project resources in AWS? type 'yes': " ok; [ "$ok" = yes ] || exit 1
log "Deleting Argo CD root app (cascades to every app, ingress -> ALB, services -> NLB)"
kubectl -n argocd delete application root --wait=true --timeout=10m 2>/dev/null || true
kubectl -n supplychain delete ingress --all --wait=true 2>/dev/null || true
kubectl -n monitoring delete ingress,svc --all --wait=true 2>/dev/null || true
kubectl -n supplychain delete pvc --all --wait=true 2>/dev/null || true
kubectl -n monitoring delete pvc --all --wait=true 2>/dev/null || true
log "Waiting 90s for AWS load balancers to be released"; sleep 90
terraform -chdir="$TF2" destroy -auto-approve || true
terraform -chdir="$TF1" destroy -auto-approve
log "All resources destroyed."
