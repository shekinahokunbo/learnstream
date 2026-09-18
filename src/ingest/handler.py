"""API Gateway -> SQS ingest edge.

Validates at the edge and enqueues. The write path is deliberately thin: the
API returns 202 as soon as the event is durable in SQS, so a slow consumer
never becomes a slow API.
"""

from __future__ import annotations

import json
import logging
import os

import boto3

from common.events import ValidationError, parse_event

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

QUEUE_URL = os.environ.get("QUEUE_URL", "")
_sqs = None


def _client():
    global _sqs
    if _sqs is None:
        _sqs = boto3.client("sqs")
    return _sqs


def _response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body),
    }


def handler(event: dict, context=None) -> dict:
    try:
        learning_event = parse_event(event.get("body"))
    except ValidationError as exc:
        logger.warning("rejected event: %s", exc)
        return _response(400, {"error": str(exc)})

    _client().send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=learning_event.to_json(),
        MessageAttributes={
            "event_type": {
                "DataType": "String",
                "StringValue": learning_event.event_type,
            }
        },
    )

    logger.info(
        json.dumps(
            {
                "msg": "enqueued",
                "event_id": learning_event.event_id,
                "user_id": learning_event.user_id,
                "event_type": learning_event.event_type,
            }
        )
    )
    return _response(202, {"event_id": learning_event.event_id, "status": "accepted"})
