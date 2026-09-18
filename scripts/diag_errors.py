#!/usr/bin/env python3
"""Why did requests fail? Reads CloudWatch metrics and recent Lambda logs.

    .venv/bin/python3 scripts/diag_errors.py
"""

from __future__ import annotations

import datetime as dt
import sys

import boto3

REGION = sys.argv[1] if len(sys.argv) > 1 else "us-east-1"
cw = boto3.client("cloudwatch", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)

end = dt.datetime.now(dt.timezone.utc)
start = end - dt.timedelta(minutes=45)


def total(namespace: str, metric: str, dims: list[dict]) -> float:
    res = cw.get_metric_statistics(
        Namespace=namespace, MetricName=metric, Dimensions=dims,
        StartTime=start, EndTime=end, Period=3600, Statistics=["Sum"],
    )
    return sum(p["Sum"] for p in res["Datapoints"])


print(f"window: last 45 minutes, region {REGION}\n")

# Account-level concurrency budget: the usual cause of 500s at low scale.
settings = lam.get_account_settings()["AccountLimit"]
print(f"account concurrent execution limit : {settings['ConcurrentExecutions']}")
print(f"unreserved available               : {settings['UnreservedConcurrentExecutions']}\n")

funcs = [
    f["FunctionName"]
    for page in lam.get_paginator("list_functions").paginate()
    for f in page["Functions"]
    if "LearnStream" in f["FunctionName"]
]

print(f"{'function':<55} {'invocations':>12} {'errors':>8} {'throttles':>10}")
for name in funcs:
    dims = [{"Name": "FunctionName", "Value": name}]
    inv = total("AWS/Lambda", "Invocations", dims)
    err = total("AWS/Lambda", "Errors", dims)
    thr = total("AWS/Lambda", "Throttles", dims)
    print(f"{name[:55]:<55} {inv:>12.0f} {err:>8.0f} {thr:>10.0f}")

print("\nrecent ERROR/Task-timed-out lines from the ingest function:")
ingest = next((f for f in funcs if "Ingest" in f), None)
if ingest:
    group = f"/aws/lambda/{ingest}"
    try:
        res = logs.filter_log_events(
            logGroupName=group,
            startTime=int(start.timestamp() * 1000),
            filterPattern="?ERROR ?Task ?Traceback ?Unable",
            limit=15,
        )
        events = res.get("events", [])
        if not events:
            print("  (none — the function itself did not raise)")
        for e in events:
            print("  " + e["message"].strip()[:220])
    except logs.exceptions.ResourceNotFoundException:
        print("  (no log group yet)")

print("""
How to read this:
  throttles > 0            -> requests were rejected for lack of concurrency,
                              which API Gateway surfaces as HTTP 500. Not a code bug.
  errors > 0, throttles 0  -> the handler raised; the log lines above say why.
""")
