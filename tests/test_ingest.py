import importlib
import json

import boto3
import pytest
from moto import mock_aws

from tests.conftest import AWS_REGION


@pytest.fixture
def queue(monkeypatch):
    with mock_aws():
        sqs = boto3.client("sqs", region_name=AWS_REGION)
        url = sqs.create_queue(QueueName="events")["QueueUrl"]
        monkeypatch.setenv("QUEUE_URL", url)
        import ingest.handler as handler_module

        importlib.reload(handler_module)
        yield sqs, url, handler_module


def _request(body):
    return {"body": json.dumps(body)}


def test_accepted_event_is_enqueued(queue):
    sqs, url, handler_module = queue
    response = handler_module.handler(
        _request({"user_id": "u1", "event_type": "lesson_started"})
    )

    assert response["statusCode"] == 202
    assert json.loads(response["body"])["event_id"]

    messages = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)["Messages"]
    assert len(messages) == 1
    assert json.loads(messages[0]["Body"])["user_id"] == "u1"


def test_invalid_event_is_rejected_without_enqueueing(queue):
    sqs, url, handler_module = queue
    response = handler_module.handler(_request({"user_id": "u1", "event_type": "bogus"}))

    assert response["statusCode"] == 400
    assert "event_type" in json.loads(response["body"])["error"]
    assert "Messages" not in sqs.receive_message(QueueUrl=url)
