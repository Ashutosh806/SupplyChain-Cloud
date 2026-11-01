#!/usr/bin/env bash
# Build the Flink job jar, upload it to S3 and run it on EMR (YARN per-job, detached).
#   scripts/flink-job.sh            build + upload + submit
#   SKIP_BUILD=1 scripts/flink-job.sh
source "$(dirname "$0")/lib.sh"

JOB_DIR="$ROOT/analytics/flink-job"
if [ "${SKIP_BUILD:-0}" != 1 ]; then
  log "Building Flink job"
  if command -v mvn >/dev/null; then (cd "$JOB_DIR" && mvn -q -B -DskipTests package)
  else docker run --rm -v "$JOB_DIR":/src -w /src maven:3.9-eclipse-temurin-17 mvn -q -B -DskipTests package; fi
fi
JAR="$JOB_DIR/target/shipment-analytics.jar"
BUCKET=$(tf1 emr_bucket); EMR_ID=$(tf1 emr_cluster_id); BROKERS=$(tf1 kafka_bootstrap_brokers); REGION=$(region)
[ -n "$EMR_ID" ] || { echo "EMR cluster not created - run: scripts/up.sh emr"; exit 1; }
KEY="jobs/shipment-analytics-$(date +%Y%m%d%H%M%S).jar"
aws s3 cp "$JAR" "s3://$BUCKET/$KEY" --region "$REGION"
log "Waiting for EMR cluster $EMR_ID to be ready"
aws emr wait cluster-running --cluster-id "$EMR_ID" --region "$REGION"
log "Submitting Flink job step"
STEP_JSON=$(mktemp)
python3 - "$BUCKET" "$KEY" "$BROKERS" > "$STEP_JSON" <<'PY'
import json, sys
bucket, key, brokers = sys.argv[1:]
cmd = ("for app in $(yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | awk '/shipment-analytics/{print $1}'); "
       "do yarn application -kill $app; done; "      # replace a previously submitted version
       f"aws s3 cp s3://{bucket}/{key} /home/hadoop/shipment-analytics.jar && "
       f"flink run -m yarn-cluster -d -ynm shipment-analytics /home/hadoop/shipment-analytics.jar "
       f"--bootstrap {brokers} --parallelism 2")
print(json.dumps([{"Type": "CUSTOM_JAR", "Name": "shipment-analytics-flink", "ActionOnFailure": "CONTINUE",
                   "Jar": "command-runner.jar", "Args": ["bash", "-c", cmd]}]))
PY
aws emr add-steps --region "$REGION" --cluster-id "$EMR_ID" --steps "file://$STEP_JSON"
rm -f "$STEP_JSON"
echo "Track it: EMR console -> $EMR_ID -> Steps / Applications (YARN) ; results topic: shipment-analytics-results"
