#!/usr/bin/env bash
# Member 1 live GitOps demo: change the Supplier Service replica count IN GIT and watch Argo CD apply it.
source "$(dirname "$0")/lib.sh"
F="$ROOT/deploy/apps/base/supplier-service.yaml"
CUR=$(sed -nE 's/^  replicas: ([0-9]+)/\1/p' "$F"); NEW=$([ "$CUR" = 2 ] && echo 3 || echo 2)
log "Changing supplier-service replicas $CUR -> $NEW in Git"
sed -i.bak -E "s/^  replicas: $CUR/  replicas: $NEW/" "$F" && rm -f "$F.bak"
git -C "$ROOT" diff --stat -- "$F"
git -C "$ROOT" commit -q -m "gitops demo: supplier-service replicas $NEW" -- "$F" && git -C "$ROOT" push -q   # commits only this file
log "Pushed. Argo CD polls Git (every ~60 s) and applies the change - no kubectl apply:"
for i in $(seq 1 30); do
  ready=$(kubectl -n supplychain get deploy supplier-service -o jsonpath='{.status.readyReplicas}')
  echo "  supplier-service ready replicas: $ready (want $NEW)"; [ "$ready" = "$NEW" ] && { log "Applied by Argo CD"; exit 0; }
  sleep 10
done
die "Argo CD did not apply the change in 5 min (kubectl -n argocd get application supplychain)"
