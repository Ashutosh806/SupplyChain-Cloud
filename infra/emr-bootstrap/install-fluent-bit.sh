#!/bin/bash
# EMR bootstrap action: ship Flink (YARN container) logs to Loki so the analytics job's logs
# land in the same centralized logging stack as the EKS microservices.
#   arg1 = Loki push URL, e.g. http://internal-nlb-xxxx.elb.ap-south-1.amazonaws.com:3100/loki/api/v1/push
# Never fails the cluster launch: logging is best-effort.
set -u
LOKI_URL="${1:-}"
if [ -z "$LOKI_URL" ]; then
  echo "No Loki URL given; skipping Fluent Bit install"
  exit 0
fi

HOST=$(echo "$LOKI_URL" | sed -E 's#^https?://([^:/]+).*#\1#')
PORT=$(echo "$LOKI_URL" | sed -nE 's#^https?://[^:/]+:([0-9]+).*#\1#p'); PORT=${PORT:-80}
ROLE=$(grep -q '"isMaster": true' /mnt/var/lib/info/instance.json 2>/dev/null && echo primary || echo core)

{
  curl -fsSL https://raw.githubusercontent.com/fluent/fluent-bit/master/install.sh | sudo sh
} || { echo "fluent-bit install failed (continuing without log shipping)"; exit 0; }

sudo mkdir -p /etc/fluent-bit /var/lib/fluent-bit
sudo tee /etc/fluent-bit/fluent-bit.conf >/dev/null <<EOF
[SERVICE]
    Flush        2
    Log_Level    info
    Parsers_File parsers.conf

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
    Name        loki
    Match       *
    Host        ${HOST}
    Port        ${PORT}
    Labels      job=emr-flink, app=shipment-analytics, namespace=emr, emr_role=${ROLE}
    Line_Format json
EOF

BIN=/opt/fluent-bit/bin/fluent-bit
[ -x "$BIN" ] || BIN=$(command -v fluent-bit || echo /opt/fluent-bit/bin/fluent-bit)
sudo tee /etc/systemd/system/fluent-bit-emr.service >/dev/null <<EOF
[Unit]
Description=Fluent Bit -> Loki (EMR Flink logs)
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
