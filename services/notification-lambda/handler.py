"""Notification Service — AWS Lambda (serverless, asynchronous, event-driven).

Triggers
  1. Amazon SQS `shipment-notifications`  <- Delivery Status Service on important transitions
  2. Amazon S3 ObjectCreated under `pod/`  <- proof-of-delivery uploads (presigned PUT)
  3. Lambda Function URL (HTTP GET)        <- UI reads the notification feed

For each notification it
  * writes an idempotent record to DynamoDB (conditional put on notification_id),
    so SQS at-least-once redelivery never produces duplicate notifications;
  * publishes to SNS (`severity` message attribute; e-mail subscribers can filter
    to HIGH severity only).
Partial batch failures are reported so only failed SQS messages are retried.
"""
import json
import logging
import os
import time
import urllib.parse
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

TABLE = os.environ["NOTIFICATIONS_TABLE"]
TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
table = boto3.resource("dynamodb").Table(TABLE)
sns = boto3.client("sns")


def _log(msg, **kw):
    print(json.dumps({"level": "INFO", "service": "notification-lambda", "msg": msg, **kw}, default=str))


def record(n: dict) -> bool:
    """Store + fan out one notification. Returns False if it was a duplicate."""
    now = int(time.time() * 1000)
    item = {
        "tracking_id": n["tracking_id"],
        "notification_id": n["notification_id"],
        "created_at": now,
        "feed": "ALL",
        "status": n.get("status"),
        "severity": n.get("severity", "NORMAL"),
        "message": n.get("message"),
        "channel": "sns" if TOPIC_ARN else "log",
        "recipient": n.get("retailer_email") or n.get("retailer_id"),
        "source": n.get("source"),
        "expires_at": int(time.time()) + 7 * 24 * 3600,
    }
    try:
        table.put_item(Item={k: v for k, v in item.items() if v is not None},
                       ConditionExpression="attribute_not_exists(notification_id)")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            _log("duplicate notification skipped", notification_id=n["notification_id"])
            return False
        raise
    if TOPIC_ARN:
        sns.publish(
            TopicArn=TOPIC_ARN,
            Subject=f"[{item['severity']}] Shipment {n['tracking_id']}: {n.get('status')}"[:100],
            Message=n.get("message") or json.dumps(n),
            MessageAttributes={"severity": {"DataType": "String", "StringValue": item["severity"]},
                               "status": {"DataType": "String", "StringValue": str(n.get("status"))}},
        )
    _log("notification sent", tracking_id=n["tracking_id"], status=n.get("status"), severity=item["severity"])
    return True


def handle_sqs(event):
    failures = []
    for rec in event["Records"]:
        try:
            record(json.loads(rec["body"]))
        except Exception as exc:
            log.exception("failed to process message")
            _log("notification failed", error=str(exc), message_id=rec["messageId"])
            failures.append({"itemIdentifier": rec["messageId"]})
    return {"batchItemFailures": failures}


def handle_s3(event):
    for rec in event["Records"]:
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
        parts = key.split("/")
        if len(parts) < 3 or parts[0] != "pod":
            continue
        tracking_id = parts[1]
        record({
            "notification_id": f"pod:{key}",
            "tracking_id": tracking_id,
            "status": "POD_UPLOADED",
            "severity": "NORMAL",
            "message": f"Proof of delivery uploaded for shipment {tracking_id} "
                       f"({rec['s3']['object'].get('size', 0)} bytes): s3://{rec['s3']['bucket']['name']}/{key}",
            "source": "s3",
        })
    return {"ok": True}


def _json(obj):
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    raise TypeError


def handle_http(event):
    params = event.get("queryStringParameters") or {}
    limit = min(int(params.get("limit", 50)), 200)
    if params.get("tracking_id"):
        resp = table.query(IndexName="by-time", KeyConditionExpression=Key("tracking_id").eq(params["tracking_id"]),
                           ScanIndexForward=False, Limit=limit)
    else:
        resp = table.query(IndexName="feed-index", KeyConditionExpression=Key("feed").eq("ALL"),
                           ScanIndexForward=False, Limit=limit)
    return {"statusCode": 200, "headers": {"content-type": "application/json"},
            "body": json.dumps({"items": resp.get("Items", [])}, default=_json)}


def lambda_handler(event, context):
    records = event.get("Records") or []
    if records and records[0].get("eventSource") == "aws:sqs":
        return handle_sqs(event)
    if records and records[0].get("eventSource") == "aws:s3":
        return handle_s3(event)
    if "requestContext" in event:
        return handle_http(event)
    _log("unknown event shape", event=event)
    return {"ok": False}
