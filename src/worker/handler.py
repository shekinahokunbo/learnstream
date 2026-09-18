"""SQS -> DynamoDB worker.

Two properties this handler is built around:

1. Idempotency. SQS standard queues are at-least-once, so the same event can
   arrive twice. Each event is written with a conditional put; a duplicate
   fails the condition and is acknowledged without double-counting the
   aggregate.
2. Partial batch failure. Returning batchItemFailures means one poison message
   does not force the whole batch to be redelivered.
"""

from __future__ import annotations

import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

from common.events import ValidationError, parse_event

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

TABLE_NAME = os.environ.get("TABLE_NAME", "")
EVENT_TTL_DAYS = int(os.environ.get("EVENT_TTL_DAYS", "90"))

_table = None


def _get_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(TABLE_NAME)
    return _table


def _put_event(table, event) -> bool:
    """Write the event item. Returns False when it is a duplicate.

    The sort key is the event id alone, NOT the timestamp plus the id. A
    producer retrying without its own occurred_at gets a fresh server-side
    timestamp, so keying on time would make every retry look like a new event
    and double-count the aggregate. Caught by a 25x replay against the live
    stack, which stored 5 events instead of 1.
    """
    try:
        table.put_item(
            Item={
                "pk": f"USER#{event.user_id}",
                "sk": f"EVENT#{event.event_id}",
                "event_id": event.event_id,
                "event_type": event.event_type,
                "occurred_at": event.occurred_at,
                "payload": event.payload,
                "expires_at": event.occurred_at + EVENT_TTL_DAYS * 86400,
            },
            ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
        )
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def _bump_aggregate(table, event) -> None:
    """Increment the per-user, per-type counter. Only called for new events."""
    table.update_item(
        Key={"pk": f"USER#{event.user_id}", "sk": "AGG#lifetime"},
        UpdateExpression="ADD #c :one, #t :one SET last_seen_at = :ts",
        ExpressionAttributeNames={"#c": "total_events", "#t": f"count_{event.event_type}"},
        ExpressionAttributeValues={":one": 1, ":ts": event.occurred_at},
    )


def handler(event: dict, context=None) -> dict:
    table = _get_table()
    failures: list[dict] = []
    processed = duplicates = rejected = 0

    for record in event.get("Records", []):
        message_id = record.get("messageId", "")
        try:
            learning_event = parse_event(record.get("body"))
        except ValidationError as exc:
            # Malformed bodies are unprocessable no matter how many times we
            # retry, so they are dropped here rather than cycled to the DLQ.
            rejected += 1
            logger.error(
                json.dumps({"msg": "unprocessable", "message_id": message_id, "error": str(exc)})
            )
            continue

        try:
            if _put_event(table, learning_event):
                _bump_aggregate(table, learning_event)
                processed += 1
            else:
                duplicates += 1
                logger.info(
                    json.dumps({"msg": "duplicate", "event_id": learning_event.event_id})
                )
        except ClientError as exc:
            # Transient: let SQS redeliver this one message.
            failures.append({"itemIdentifier": message_id})
            logger.exception("transient failure on %s: %s", message_id, exc)

    logger.info(
        json.dumps(
            {
                "msg": "batch_complete",
                "processed": processed,
                "duplicates": duplicates,
                "rejected": rejected,
                "retried": len(failures),
            }
        )
    )
    return {"batchItemFailures": failures}
