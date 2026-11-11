#!/usr/bin/env bash
# Build the 7 service images (linux/amd64), push to ECR, bump the tags in the GitOps overlay and
# push the commit. Argo CD then deploys the new version - no kubectl apply involved.
#   scripts/build-and-push.sh [service ...]     (default: all)
source "$(dirname "$0")/lib.sh"
need docker aws git
REGISTRY=$(tf1 ecr_registry); REGION=$(region)
TAG="$(git -C "$ROOT" rev-parse --short HEAD)-$(date +%Y%m%d%H%M%S)"
if [ $# -gt 0 ]; then TARGETS=("$@"); else TARGETS=("${SERVICES[@]}"); fi
log "Logging in to $REGISTRY"
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"
for svc in "${TARGETS[@]}"; do
  log "Building $svc:$TAG"
  docker buildx build --platform linux/amd64 --build-arg SERVICE="$svc" \
    -t "$REGISTRY/supplychain/$svc:$TAG" --push "$ROOT/services"
  # bump newTag for this image in the overlay (line after its newName)
  python3 - "$ROOT/deploy/apps/overlays/aws/kustomization.yaml" "$svc" "$TAG" <<'PY'
import re, sys
path, svc, tag = sys.argv[1:]
s = open(path).read()
s = re.sub(rf"(- name: {re.escape(svc)}\n\s+newName: [^\n]+\n\s+newTag: )[^\n]+", rf"\g<1>{tag}", s)
open(path, "w").write(s)
PY
done
log "Committing GitOps change"
git -C "$ROOT" add deploy/apps/overlays/aws/kustomization.yaml
git -C "$ROOT" commit -m "deploy: ${TARGETS[*]} -> $TAG" || true
git -C "$ROOT" push
log "Pushed. Argo CD will roll out $TAG within ~1 minute (watch: kubectl -n argocd get applications)"
