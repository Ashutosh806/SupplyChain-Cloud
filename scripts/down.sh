#!/usr/bin/env bash
# Stop the whole system and delete all its AWS resources (~30-40 min). Safe to re-run.
source "$(dirname "$0")/lib.sh"
read -r -p "Delete the whole system from AWS? type 'yes': " ok; [ "$ok" = yes ] || exit 1
REGION=$(region); VPC=$(tf1 vpc_id || true)

# 1. Load balancers and disks are created by Kubernetes (not Terraform): delete them while the cluster runs.
if aws eks update-kubeconfig --name "$CLUSTER" --region "$REGION" >/dev/null 2>&1 && kubectl get ns >/dev/null 2>&1; then
  log "Removing Kubernetes-created load balancers and volumes"
  for app in $(kubectl -n argocd get applications -o name 2>/dev/null); do   # stop Argo CD re-creating them
    kubectl -n argocd patch "$app" --type json -p '[{"op":"remove","path":"/spec/syncPolicy/automated"}]' >/dev/null 2>&1 || true
  done
  kubectl delete ingress -A --all --timeout=5m || true
  kubectl -n supplychain delete statefulset --all --timeout=3m || true
  kubectl -n monitoring delete statefulset --all --timeout=3m || true
  kubectl delete pvc -A --all --timeout=5m || true
  for i in $(seq 1 30); do   # wait until AWS has removed them
    n=$(aws elbv2 describe-load-balancers --region "$REGION" --query "length(LoadBalancers[?VpcId=='$VPC'])" --output text 2>/dev/null || echo 0)
    [ "$n" = 0 ] && break; echo "  waiting for $n load balancer(s)..."; sleep 15
  done
fi

# 2. Argo CD, then all infrastructure.
log "terraform destroy: Argo CD"
terraform -chdir="$TF2" init -input=false >/dev/null 2>&1 || true
terraform -chdir="$TF2" destroy -input=false -auto-approve || rm -f "$TF2/terraform.tfstate"   # cluster already gone
log "terraform destroy: infrastructure"
terraform -chdir="$TF1" init -input=false >/dev/null 2>&1 || true
if ! terraform -chdir="$TF1" destroy -input=false -auto-approve; then
  log "Something was still attached to the VPC - cleaning up and retrying"
  for lb in $(aws elbv2 describe-load-balancers --region "$REGION" --query "LoadBalancers[?VpcId=='$VPC'].LoadBalancerArn" --output text); do
    aws elbv2 delete-load-balancer --region "$REGION" --load-balancer-arn "$lb"; done
  sleep 60
  for sg in $(aws ec2 describe-security-groups --region "$REGION" --filters Name=vpc-id,Values="$VPC" Name=tag:eks:eks-cluster-name,Values="$CLUSTER" --query 'SecurityGroups[].GroupId' --output text); do
    aws ec2 delete-security-group --region "$REGION" --group-id "$sg" || true; done
  terraform -chdir="$TF1" destroy -input=false -auto-approve
fi
"$(dirname "$0")/status.sh" || true
