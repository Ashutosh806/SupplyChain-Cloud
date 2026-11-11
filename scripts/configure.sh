#!/usr/bin/env bash
# One-time: put your GitHub repo URL, AWS account and region into the GitOps manifests.
#   scripts/configure.sh https://github.com/<you>/<repo>.git [region]
source "$(dirname "$0")/lib.sh"
REPO="${1:?usage: configure.sh <https github repo url> [region]}"
REGION="${2:-ap-south-1}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text) || die "AWS credentials not set (aws configure)"
REGISTRY="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
grep -rl -e '__GIT_REPO_URL__' -e '__ECR_REGISTRY__' -e '__AWS_REGION__' "$ROOT/deploy" | while read -r f; do
  sed -i.bak -e "s#__GIT_REPO_URL__#$REPO#g" -e "s#__ECR_REGISTRY__#$REGISTRY#g" -e "s#__AWS_REGION__#$REGION#g" "$f"
  rm -f "$f.bak"; echo "  configured ${f#$ROOT/}"
done
printf 'region = "%s"\n# admin_principal_arns = ["arn:aws:iam::%s:user/teammate"]   # teammates get kubectl access\n# alert_email = "you@example.com"\n' "$REGION" "$ACCOUNT" > "$TF1/terraform.tfvars"
printf 'git_repo_url = "%s"\n# private repo? also set git_username and git_token (read-only PAT)\n' "$REPO" > "$TF2/terraform.tfvars"
log "Done. Now: git add -A && git commit -m configure && git push"
