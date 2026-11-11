#!/usr/bin/env bash
# Start the whole system on AWS (~75-90 min). Safe to re-run: it continues where it stopped.
#   scripts/up.sh            all steps
#   scripts/up.sh infra|images|gitops|wait|emr    one step
source "$(dirname "$0")/lib.sh"
STEP="${1:-all}"

infra() {        # 1. AWS infrastructure with Terraform
  log "1/5 Terraform: VPC, EKS, RDS, MSK (Kafka), DynamoDB, S3, SQS, SNS, Lambda, ECR, IAM (~35 min)"
  terraform -chdir="$TF1" init -input=false >/dev/null
  # keep EMR as it is if it already exists, otherwise create it later in step 5
  if terraform -chdir="$TF1" state list 2>/dev/null | grep -q aws_emr_cluster.flink; then
    terraform -chdir="$TF1" apply -input=false -auto-approve -var enable_emr=true -var "loki_push_url=$(tf1 loki_push_url)"
  else
    terraform -chdir="$TF1" apply -input=false -auto-approve
  fi
  aws eks update-kubeconfig --name "$CLUSTER" --region "$(region)" >/dev/null
}

images() {       # 2. build the 7 images, push to ECR, point the GitOps overlay at them
  log "2/5 Build + push container images"
  local registry tag overlay="$ROOT/deploy/apps/overlays/aws/kustomization.yaml"
  registry=$(tf1 ecr_registry); [ -n "$registry" ] || die "run step 'infra' first"
  tag=$(git -C "$ROOT" rev-parse --short=12 HEAD:services)    # same code -> same tag
  aws ecr get-login-password --region "$(region)" | docker login -u AWS --password-stdin "$registry" >/dev/null
  for svc in $SERVICES; do
    if aws ecr describe-images --region "$(region)" --repository-name "supplychain/$svc" --image-ids imageTag="$tag" >/dev/null 2>&1; then
      echo "  $svc:$tag already in ECR"
    else
      echo "  building $svc:$tag"
      docker buildx build --platform linux/amd64 --build-arg SERVICE="$svc" -t "$registry/supplychain/$svc:$tag" --push "$ROOT/services"
    fi
  done
  sed -i.bak -E "s#(newTag: ).*#\1$tag#" "$overlay" && rm -f "$overlay.bak"
  if ! git -C "$ROOT" diff --quiet -- "$overlay"; then       # the Git commit IS the deployment (GitOps)
    git -C "$ROOT" add "$overlay" && git -C "$ROOT" commit -q -m "deploy: images $tag"
  fi
  git -C "$ROOT" push -q
}

fetch_chart() {  # download an Argo CD chart package with curl (retries); Helm's own download can time out
  local name=$1 key=$2 v f
  v=$(awk -F'"' -v k="$key" '$1 ~ ("^[ ]+" k "[ ]+=") {print $2; exit}' "$TF2/main.tf")
  [ -n "$v" ] || die "chart version '$key' not found in $TF2/main.tf"
  f="$TF2/.charts/$name-$v.tgz"
  mkdir -p "$TF2/.charts"
  if [ -s "$f" ] && tar -tzf "$f" >/dev/null 2>&1; then return 0; fi
  echo "  downloading chart $name $v"
  curl -fsSL --retry 5 --retry-delay 5 --retry-all-errors --connect-timeout 20 --max-time 300 \
    -o "$f.part" "https://github.com/argoproj/argo-helm/releases/download/$name-$v/$name-$v.tgz" \
    || die "could not download chart $name $v - check the internet connection and run make up again"
  mv "$f.part" "$f"
}

gitops() {       # 3. Argo CD + the root app; Argo CD then deploys everything else from Git
  log "3/5 Terraform: Argo CD (it then deploys apps + monitoring from Git)"
  fetch_chart argo-cd argocd_chart_version
  fetch_chart argocd-apps argocd_apps_chart_version
  terraform -chdir="$TF2" init -input=false >/dev/null
  terraform -chdir="$TF2" apply -input=false -auto-approve
}

wait_ready() {   # 4. wait until Argo CD has deployed everything and the public URL answers
  log "4/5 Waiting for Argo CD to deploy everything and for the public URL (5-15 min)"
  for i in $(seq 1 120); do
    bad=$(kubectl -n argocd get applications -o jsonpath='{range .items[*]}{.metadata.name}={.status.health.status} {end}' 2>/dev/null \
          | tr ' ' '\n' | grep -v '=Healthy$' | grep . || true)
    host=$(alb)
    if [ -z "$bad" ] && [ -n "$host" ] && curl -sf --max-time 5 "http://$host/api/config" >/dev/null; then
      log "LIVE: http://$host/"; return 0
    fi
    [ $((i % 6)) = 0 ] && echo "  still waiting... ${bad:-apps healthy, waiting for the load balancer}"
    sleep 10
  done
  die "not ready after 20 min - check: kubectl -n argocd get applications"
}

emr() {          # 5. EMR cluster + Flink job (logs shipped to Loki through an internal load balancer)
  log "5/5 EMR + Flink job (~15 min)"
  local loki=""
  for i in $(seq 1 60); do
    loki=$(kubectl -n monitoring get svc loki-ingest -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)
    [ -n "$loki" ] && break; sleep 10
  done
  [ -n "$loki" ] || die "Loki load balancer not ready (is step 'wait' done?)"
  terraform -chdir="$TF1" apply -input=false -auto-approve -var enable_emr=true -var "loki_push_url=http://$loki:3100/loki/api/v1/push"
  "$(dirname "$0")/flink-job.sh"
}

case "$STEP" in
  infra) infra ;; images) images ;; gitops) gitops ;; wait) wait_ready ;; emr) emr ;;
  all)
    grep -rqs '__GIT_REPO_URL__' "$ROOT/deploy" && die "run: make configure REPO=https://github.com/<you>/<repo>.git (then commit + push)"
    aws sts get-caller-identity >/dev/null || die "AWS credentials not set (aws configure)"
    START=$(date +%s)
    infra; images; gitops; wait_ready; emr
    "$(dirname "$0")/urls.sh"
    log "System UP in $(( ($(date +%s) - START) / 60 )) min. Stop it with: make down" ;;
  *) die "unknown step $STEP" ;;
esac
