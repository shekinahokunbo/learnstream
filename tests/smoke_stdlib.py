"""Dependency-free smoke test: python3 tests/smoke_stdlib.py

Runs the real handlers against stub AWS clients. Not a replacement for the
pytest + moto suite, but it needs no network and no packages, so it catches
import errors and logic bugs before you spend anything on a deploy.
"""

from __future__ import annotations

import json
import os
import sys
import types
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))


# --------------------------------------------------------------------------
# Minimal boto3 / botocore stubs, installed before the handlers import them.
# --------------------------------------------------------------------------
class ClientError(Exception):
    def __init__(self, response, operation_name="Op"):
        super().__init__(response["Error"]["Code"])
        self.response = response
        self.operation_name = operation_name


class FakeSQS:
    def __init__(self):
        self.sent = []

    def send_message(self, **kwargs):
        self.sent.append(kwargs)
        return {"MessageId": "stub"}


class FakeTable:
    """Implements just enough DynamoDB semantics: conditional put and ADD."""

    def __init__(self):
        self.items: dict[tuple, dict] = {}

    def put_item(self, Item, ConditionExpression=None):
        key = (Item["pk"], Item["sk"])
        if ConditionExpression and key in self.items:
            raise ClientError(
                {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
            )
        self.items[key] = dict(Item)

    def update_item(self, Key, UpdateExpression, ExpressionAttributeNames,
                    ExpressionAttributeValues):
        key = (Key["pk"], Key["sk"])
        item = self.items.setdefault(key, dict(Key))
        for placeholder, attr in ExpressionAttributeNames.items():
            item[attr] = item.get(attr, 0) + ExpressionAttributeValues[":one"]
        item["last_seen_at"] = ExpressionAttributeValues[":ts"]

    def scan(self, **kwargs):
        return {"Items": [v for k, v in self.items.items() if k[1] == "AGG#lifetime"]}

    def get(self, pk, sk):
        return self.items.get((pk, sk))


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[(Bucket, Key)] = Body


FAKE = types.SimpleNamespace(sqs=FakeSQS(), table=FakeTable(), s3=FakeS3())

boto3_stub = types.ModuleType("boto3")
boto3_stub.client = lambda name, **kw: {"sqs": FAKE.sqs, "s3": FAKE.s3}[name]
boto3_stub.resource = lambda name, **kw: types.SimpleNamespace(
    Table=lambda table_name: FAKE.table
)
sys.modules["boto3"] = boto3_stub

botocore = types.ModuleType("botocore")
exceptions = types.ModuleType("botocore.exceptions")
exceptions.ClientError = ClientError
botocore.exceptions = exceptions
sys.modules["botocore"] = botocore
sys.modules["botocore.exceptions"] = exceptions

conditions = types.ModuleType("boto3.dynamodb.conditions")


class _Attr:
    def __init__(self, name):
        self.name = name

    def eq(self, value):
        return ("eq", self.name, value)


conditions.Attr = _Attr
dynamodb_mod = types.ModuleType("boto3.dynamodb")
dynamodb_mod.conditions = conditions
sys.modules["boto3.dynamodb"] = dynamodb_mod
sys.modules["boto3.dynamodb.conditions"] = conditions
boto3_stub.dynamodb = dynamodb_mod

os.environ.setdefault("QUEUE_URL", "https://sqs.test/queue")
os.environ.setdefault("TABLE_NAME", "test-table")
os.environ.setdefault("BUCKET_NAME", "test-bucket")

from common.events import ValidationError, parse_event  # noqa: E402
from ingest import handler as ingest_handler  # noqa: E402
from rollup import handler as rollup_handler  # noqa: E402
from worker import handler as worker_handler  # noqa: E402


def _body(**over):
    payload = {
        "event_id": "evt-1",
        "user_id": "u1",
        "event_type": "lesson_completed",
        "occurred_at": 1789000000,
        "payload": {},
    }
    payload.update(over)
    return json.dumps(payload)


class TestValidation(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(parse_event(_body()).user_id, "u1")

    def test_rejects_bad_type(self):
        with self.assertRaises(ValidationError):
            parse_event(_body(event_type="nope"))

    def test_rejects_bool_timestamp(self):
        with self.assertRaises(ValidationError):
            parse_event(_body(occurred_at=True))

    def test_mints_id(self):
        self.assertTrue(parse_event({"user_id": "u1", "event_type": "session_ended"}).event_id)


class TestIngest(unittest.TestCase):
    def setUp(self):
        FAKE.sqs.sent.clear()

    def test_accepts_and_enqueues(self):
        response = ingest_handler.handler({"body": _body()})
        self.assertEqual(response["statusCode"], 202)
        self.assertEqual(len(FAKE.sqs.sent), 1)

    def test_rejects_without_enqueueing(self):
        response = ingest_handler.handler({"body": _body(event_type="nope")})
        self.assertEqual(response["statusCode"], 400)
        self.assertEqual(FAKE.sqs.sent, [])


class TestWorker(unittest.TestCase):
    def setUp(self):
        FAKE.table.items.clear()

    def _agg(self):
        return FAKE.table.get("USER#u1", "AGG#lifetime")

    def test_counts_new_event(self):
        result = worker_handler.handler({"Records": [{"messageId": "m1", "body": _body()}]})
        self.assertEqual(result, {"batchItemFailures": []})
        self.assertEqual(self._agg()["total_events"], 1)

    def test_duplicate_does_not_double_count(self):
        worker_handler.handler({"Records": [{"messageId": "m1", "body": _body()}]})
        worker_handler.handler({"Records": [{"messageId": "m2", "body": _body()}]})
        self.assertEqual(self._agg()["total_events"], 1)

    def test_replay_without_timestamp_still_dedupes(self):
        worker_handler.handler({"Records": [{"messageId": "m1", "body": _body(occurred_at=1789000000)}]})
        worker_handler.handler({"Records": [{"messageId": "m2", "body": _body(occurred_at=1789000004)}]})
        self.assertEqual(self._agg()["total_events"], 1)

    def test_distinct_events_both_count(self):
        worker_handler.handler({"Records": [
            {"messageId": "m1", "body": _body(event_id="evt-1")},
            {"messageId": "m2", "body": _body(event_id="evt-2")},
        ]})
        self.assertEqual(self._agg()["total_events"], 2)

    def test_malformed_record_dropped_not_retried(self):
        result = worker_handler.handler({"Records": [
            {"messageId": "m1", "body": "{not json"},
            {"messageId": "m2", "body": _body()},
        ]})
        self.assertEqual(result, {"batchItemFailures": []})
        self.assertEqual(self._agg()["total_events"], 1)

    def test_transient_failure_reported(self):
        original = worker_handler._put_event

        def boom(*a, **k):
            raise ClientError({"Error": {"Code": "ProvisionedThroughputExceededException"}})

        worker_handler._put_event = boom
        try:
            result = worker_handler.handler({"Records": [{"messageId": "m1", "body": _body()}]})
        finally:
            worker_handler._put_event = original
        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "m1"}]})


class TestRollup(unittest.TestCase):
    def test_writes_partitioned_object(self):
        FAKE.table.items.clear()
        FAKE.table.items[("USER#u1", "AGG#lifetime")] = {
            "pk": "USER#u1", "sk": "AGG#lifetime", "total_events": 3
        }
        result = rollup_handler.handler({})
        self.assertEqual(result["users"], 1)
        self.assertIn("aggregates/dt=", result["key"])
        self.assertTrue(FAKE.s3.objects)


if __name__ == "__main__":
    unittest.main(verbosity=2)
