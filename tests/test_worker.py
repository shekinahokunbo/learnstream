import importlib
import json

import boto3
import pytest
from moto import mock_aws

from tests.conftest import AWS_REGION

TABLE_NAME = "learnstream-events"


@pytest.fixture
def table(monkeypatch):
    with mock_aws():
        ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
        created = ddb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        monkeypatch.setenv("TABLE_NAME", TABLE_NAME)
        import worker.handler as handler_module

        importlib.reload(handler_module)
        yield created, handler_module


def _record(message_id, **overrides):
    body = {
        "event_id": "evt-1",
        "user_id": "u1",
        "event_type": "lesson_completed",
        "occurred_at": 1789000000,
        "payload": {},
    }
    body.update(overrides)
    return {"messageId": message_id, "body": json.dumps(body)}


def _aggregate(created):
    return created.get_item(Key={"pk": "USER#u1", "sk": "AGG#lifetime"}).get("Item")


def test_processes_a_new_event(table):
    created, handler_module = table
    result = handler_module.handler({"Records": [_record("m1")]})

    assert result == {"batchItemFailures": []}
    assert _aggregate(created)["total_events"] == 1
    assert _aggregate(created)["count_lesson_completed"] == 1


def test_duplicate_delivery_does_not_double_count(table):
    """SQS is at-least-once, so the same event id must be idempotent."""
    created, handler_module = table
    handler_module.handler({"Records": [_record("m1")]})
    handler_module.handler({"Records": [_record("m2")]})  # redelivery

    assert _aggregate(created)["total_events"] == 1


def test_replay_without_a_timestamp_still_dedupes(table):
    """A retry gets a fresh server-side timestamp; that must not create a
    second event. Regression test for a bug found by replaying against the
    deployed stack: 25 deliveries stored 5 events."""
    created, handler_module = table
    handler_module.handler({"Records": [_record("m1", occurred_at=1789000000)]})
    handler_module.handler({"Records": [_record("m2", occurred_at=1789000003)]})
    handler_module.handler({"Records": [_record("m3", occurred_at=1789000009)]})

    assert _aggregate(created)["total_events"] == 1


def test_distinct_events_each_count(table):
    created, handler_module = table
    handler_module.handler(
        {
            "Records": [
                _record("m1", event_id="evt-1"),
                _record("m2", event_id="evt-2", event_type="session_ended"),
            ]
        }
    )

    aggregate = _aggregate(created)
    assert aggregate["total_events"] == 2
    assert aggregate["count_lesson_completed"] == 1
    assert aggregate["count_session_ended"] == 1


def test_malformed_record_is_dropped_not_retried(table):
    """A body that can never parse should not cycle through the DLQ."""
    created, handler_module = table
    result = handler_module.handler(
        {"Records": [{"messageId": "m1", "body": "{not json"}, _record("m2")]}
    )

    assert result == {"batchItemFailures": []}
    assert _aggregate(created)["total_events"] == 1


def test_transient_failure_is_reported_for_redelivery(table, monkeypatch):
    """A throttled write must come back as a batch item failure, not a drop."""
    from botocore.exceptions import ClientError

    created, handler_module = table

    def _throttle(*args, **kwargs):
        raise ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException"}}, "PutItem"
        )

    monkeypatch.setattr(handler_module, "_put_event", _throttle)
    result = handler_module.handler({"Records": [_record("m1")]})

    assert result == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
