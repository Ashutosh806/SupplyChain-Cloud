#!/usr/bin/env bash
# Stage 2: namespaces + environment bindings + Argo CD + root app-of-apps.
source "$(dirname "$0")/lib.sh"
need terraform kubectl aws
log "terraform apply 02-bootstrap"
terraform -chdir="$TF2" init -input=false
terraform -chdir="$TF2" apply -input=false -auto-approve
log "Argo CD is syncing. Watch progress with:  kubectl -n argocd get applications -w"
echo "Argo CD UI:  kubectl -n argocd port-forward svc/argocd-server 8080:80   (user admin)"
echo "password:    $(terraform -chdir="$TF2" output -raw argocd_admin_password_cmd)"
