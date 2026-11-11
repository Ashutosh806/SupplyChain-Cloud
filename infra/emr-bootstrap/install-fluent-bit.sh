#!/bin/bash
# EMR bootstrap action: ship the Flink (YARN container) logs into the same centralized Loki as the
# EKS microservices. Fluent Bit on each EMR node publishes them to a Kafka topic on MSK; Fluent Bit
# in the EKS cluster consumes that topic into Loki (job="emr-flink"). No load balancer is needed.
#   arg1 = MSK bootstrap brokers (host:9092,host:9092)    arg2 = Kafka topic (default: emr-logs)
# Never fails the cluster launch: logging is best-effort.
set -u
BROKERS="${1:-}"
TOPIC="${2:-emr-logs}"
if [ -z "$BROKERS" ]; then
  echo "No Kafka brokers given; skipping Fluent Bit install"
  exit 0
fi
ROLE=$(grep -q '"isMaster": true' /mnt/var/lib/info/instance.json 2>/dev/null && echo primary || echo core)

{
  curl -fsSL https://raw.githubusercontent.com/fluent/fluent-bit/master/install.sh | sudo sh
} || { echo "fluent-bit install failed (continuing without log shipping)"; exit 0; }

sudo mkdir -p /etc/fluent-bit /var/lib/fluent-bit
sudo tee /etc/fluent-bit/fluent-bit.conf >/dev/null <<EOF
[SERVICE]
    Flush        2
    Log_Level    info

[INPUT]
    Name              tail
    Tag               flink.*
    Path              /var/log/hadoop-yarn/containers/application_*/container_*/*.log,/var/log/hadoop-yarn/containers/application_*/container_*/*.out
    Path_Key          file
    DB                /var/lib/fluent-bit/flink.db
    Refresh_Interval  5
    Read_from_Head    true
    Skip_Long_Lines   On

[FILTER]
    Name    record_modifier
    Match   *
    Record  host ${HOSTNAME}
    Record  emr_role ${ROLE}

[OUTPUT]
    Name           kafka
    Match          *
    Brokers        ${BROKERS}
    Topics         ${TOPIC}
    Format         json
    Timestamp_Key  @timestamp
EOF

BIN=/opt/fluent-bit/bin/fluent-bit
[ -x "$BIN" ] || BIN=$(command -v fluent-bit || echo /opt/fluent-bit/bin/fluent-bit)
sudo tee /etc/systemd/system/fluent-bit-emr.service >/dev/null <<EOF
[Unit]
Description=Fluent Bit -> Kafka (EMR Flink logs)
After=network-online.target

[Service]
ExecStart=${BIN} -c /etc/fluent-bit/fluent-bit.conf
Restart=always

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl disable --now fluent-bit 2>/dev/null || true
sudo systemctl enable --now fluent-bit-emr || echo "could not start fluent-bit"
exit 0
