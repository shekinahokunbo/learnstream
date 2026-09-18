"""Scheduled rollup: DynamoDB aggregates -> S3, partitioned for Athena.

Runs on an EventBridge schedule. Writes newline-delimited JSON under a Hive
style prefix so Athena can read it with partition projection.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os

import boto3
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

TABLE_NAME = os.environ.get("TABLE_NAME", "")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "")

_table = None
_s3 = None


def _get_table():
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(TABLE_NAME)
    return _table


def _get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def _scan_aggregates(table) -> list[dict]:
    """Collect every aggregate item, following pagination."""
    items: list[dict] = []
    kwargs = {"FilterExpression": Attr("sk").eq("AGG#lifetime")}
    while True:
        page = table.scan(**kwargs)
        items.extend(page.get("Items", []))
        token = page.get("LastEvaluatedKey")
        if not token:
            return items
        kwargs["ExclusiveStartKey"] = token


def _serialise(item: dict) -> str:
    # Decimal is not JSON serialisable; DynamoDB returns counters as Decimal.
    return json.dumps(item, default=lambda v: int(v), sort_keys=True)


def handler(event: dict | None = None, context=None) -> dict:
    table = _get_table()
    items = _scan_aggregates(table)

    now = dt.datetime.now(dt.timezone.utc)
    key = (
        f"aggregates/dt={now:%Y-%m-%d}/hour={now:%H}/"
        f"rollup-{now:%Y%m%dT%H%M%S}.jsonl"
    )
    body = "\n".join(_serialise(item) for item in items).encode("utf-8")

    _get_s3().put_object(
        Bucket=BUCKET_NAME, Key=key, Body=body, ContentType="application/x-ndjson"
    )

    logger.info(json.dumps({"msg": "rollup_written", "key": key, "users": len(items)}))
    return {"users": len(items), "key": key}
