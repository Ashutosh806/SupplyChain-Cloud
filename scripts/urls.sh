#!/usr/bin/env bash
# Print the demo URLs and passwords.
source "$(dirname "$0")/lib.sh"
HOST=$(alb)
echo
echo "  Web app (public URL) : http://${HOST:-<not ready>}/"
echo "  Grafana              : http://${HOST:-<not ready>}/grafana/   admin / $(terraform -chdir="$TF2" output -raw grafana_admin_password 2>/dev/null)"
echo "  Notification Lambda  : $(tf1 notification_function_url)"
echo "  Argo CD              : kubectl -n argocd port-forward svc/argocd-server 8080:80  -> http://localhost:8080"
echo "                         admin / $(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' 2>/dev/null | base64 -d)"
echo
