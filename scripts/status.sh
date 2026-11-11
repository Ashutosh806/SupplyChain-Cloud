#!/usr/bin/env bash
# Is anything of this project still running (= costing money)? Read-only.
source "$(dirname "$0")/lib.sh"
R=$(region)
aws sts get-caller-identity >/dev/null 2>&1 || die "AWS credentials not set - cannot check"
count() { local n; n=$("$@" 2>/dev/null || echo 0); echo "${n:-0}"; }
eks=$(count aws eks describe-cluster --region "$R" --name "$CLUSTER" --query 'length([cluster.name])' --output text)
vpc=$(aws ec2 describe-vpcs --region "$R" --filters Name=tag:Project,Values=supplychain --query 'Vpcs[0].VpcId' --output text 2>/dev/null || true)
ec2=0   # ordinary EC2 servers in our VPC (EMR). EKS Auto Mode nodes are AWS-managed and not listed here
if [ -n "$vpc" ] && [ "$vpc" != None ]; then
  ec2=$(count aws ec2 describe-instances --region "$R" --filters Name=instance-state-name,Values=pending,running Name=vpc-id,Values="$vpc" --query 'length(Reservations[].Instances[])' --output text)
fi
nodes=0  # EKS Auto Mode nodes: ask Kubernetes (they disappear together with the cluster)
if [ "$eks" != 0 ]; then
  nodes=$(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')
fi
msk=$(count aws kafka list-clusters-v2 --region "$R" --cluster-name-filter supplychain --query 'length(ClusterInfoList)' --output text)
rds=$(count aws rds describe-db-instances --region "$R" --query "length(DBInstances[?starts_with(DBInstanceIdentifier,'supplychain')])" --output text)
emr=$(count aws emr list-clusters --region "$R" --active --query "length(Clusters[?starts_with(Name,'supplychain')])" --output text)
nat=$(count aws ec2 describe-nat-gateways --region "$R" --filter Name=state,Values=available Name=tag:Project,Values=supplychain --query 'length(NatGateways)' --output text)
echo; printf '  %-28s %s\n' "EKS cluster" "$eks" "EKS nodes" "$nodes" "EMR servers (EC2)" "$ec2" "MSK (Kafka) clusters" "$msk" "RDS databases" "$rds" "EMR clusters" "$emr" "NAT gateways" "$nat"
if [ $((eks + nodes + ec2 + msk + rds + emr + nat)) = 0 ]; then
  echo; echo "  Everything is OFF (~\$0/hour)."; echo
else
  echo; echo "  System is (partly) RUNNING: ~\$1-1.5/hour. Stop it with: make down"; echo; exit 3
fi