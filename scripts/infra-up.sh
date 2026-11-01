#!/usr/bin/env bash
# Stage 1: VPC, EKS (Auto Mode), RDS, MSK, DynamoDB, S3, SQS/SNS, Lambda, ECR, IAM  (~30-40 min, MSK is slowest)
source "$(dirname "$0")/lib.sh"
need terraform aws
log "terraform apply 01-platform"
terraform -chdir="$TF1" init -input=false
terraform -chdir="$TF1" apply -input=false -auto-approve
aws eks update-kubeconfig --name "$(tf1 cluster_name)" --region "$(region)"
log "Platform ready. Next: scripts/build-and-push.sh  then  scripts/gitops-up.sh"
