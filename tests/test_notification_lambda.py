"""Unit test for the Notification Lambda using moto (in-memory DynamoDB + SNS).

    pip install "moto[dynamodb,sns]" pytest && pytest tests/test_notification_lambda.py
"""
import importlib
import json
import os
import sys
from pathlib import Path

import boto3
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "services" / "notification-lambda"))
os.environ.update(AWS_DEFAULT_REGION="ap-south-1", AWS_ACCESS_KEY_ID="x", AWS_SECRET_ACCESS_KEY="x",
                  NOTIFICATIONS_TABLE="notifications")


def _setup():
    ddb = boto3.client("dynamodb")
    ddb.create_table(
        TableName="notifications", BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": n, "AttributeType": t} for n, t in
                              [("tracking_id", "S"), ("notification_id", "S"), ("created_at", "N"), ("feed", "S")]],
        KeySchema=[{"AttributeName": "tracking_id", "KeyType": "HASH"}, {"AttributeName": "notification_id", "KeyType": "RANGE"}],
        LocalSecondaryIndexes=[{"IndexName": "by-time", "KeySchema": [{"AttributeName": "tracking_id", "KeyType": "HASH"},
                               {"AttributeName": "created_at", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}}],
        GlobalSecondaryIndexes=[{"IndexName": "feed-index", "KeySchema": [{"AttributeName": "feed", "KeyType": "HASH"},
                                {"AttributeName": "created_at", "KeyType": "RANGE"}], "Projection": {"ProjectionType": "ALL"}}])
    os.environ["SNS_TOPIC_ARN"] = boto3.client("sns").create_topic(Name="alerts")["TopicArn"]
    import handler
    return importlib.reload(handler)


def _sqs(*bodies):
    return {"Records": [{"eventSource": "aws:sqs", "messageId": f"m{i}", "body": json.dumps(b)} for i, b in enumerate(bodies)]}


@mock_aws
def test_sqs_notifications_are_idempotent_and_queryable():
    h = _setup()
    n = {"notification_id": "abc", "tracking_id": "SCT1", "status": "DELAYED", "severity": "HIGH", "message": "late"}
    assert h.lambda_handler(_sqs(n, n), None) == {"batchItemFailures": []}      # duplicate in same batch
    assert h.lambda_handler(_sqs(n), None) == {"batchItemFailures": []}         # SQS redelivery
    resp = h.lambda_handler({"requestContext": {}, "queryStringParameters": {"tracking_id": "SCT1"}}, None)
    items = json.loads(resp["body"])["items"]
    assert len(items) == 1 and items[0]["status"] == "DELAYED"
    feed = json.loads(h.lambda_handler({"requestContext": {}, "queryStringParameters": None}, None)["body"])["items"]
    assert len(feed) == 1


@mock_aws
def test_bad_message_reported_as_partial_failure():
    h = _setup()
    good = {"notification_id": "g", "tracking_id": "SCT2", "status": "DELIVERED"}
    event = _sqs(good)
    event["Records"].append({"eventSource": "aws:sqs", "messageId": "bad", "body": "{not json"})
    assert h.lambda_handler(event, None) == {"batchItemFailures": [{"itemIdentifier": "bad"}]}


@mock_aws
def test_s3_pod_upload_creates_notification():
    h = _setup()
    h.lambda_handler({"Records": [{"eventSource": "aws:s3", "s3": {"bucket": {"name": "b"},
                      "object": {"key": "pod/SCT3/abc-pod.pdf", "size": 1234}}}]}, None)
    items = json.loads(h.lambda_handler({"requestContext": {}, "queryStringParameters": {"tracking_id": "SCT3"}},
                                        None)["body"])["items"]
    assert items[0]["status"] == "POD_UPLOADED"
