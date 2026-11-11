#!/usr/bin/env bash
# Sustained load against the public URL so the HPAs scale out. Watch: kubectl -n supplychain get hpa -w
source "$(dirname "$0")/lib.sh"
HOST=$(alb); [ -n "$HOST" ] || die "system not up"
if command -v k6 >/dev/null; then k6 run -e BASE_URL="http://$HOST" "$ROOT/loadtest/k6/hpa-load.js"
else docker run --rm -i -v "$ROOT/loadtest/k6":/s grafana/k6 run -e BASE_URL="http://$HOST" /s/hpa-load.js; fi
