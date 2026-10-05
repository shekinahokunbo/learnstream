#!/usr/bin/env python3
"""Measure throughput, latency, idempotency and fault recovery. Run after the stack is deployed:

    python3 scripts/measure.py --stack LearnStreamStack --region us-east-1

Uses boto3 only (no AWS CLI). Prints a summary block at the end.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
import boto3
import requests


def log(msg: str) -> None:
    print(f"\n==> {msg}", flush=True)


def stack_outputs(stack: str, region: str) -> dict[str, str]:
    cfn = boto3.client("cloudformation", region_name=region)
    stacks = cfn.describe_stacks(StackName=stack)["Stacks"]
    return {o["OutputKey"]: o["OutputValue"] for o in stacks[0].get("Outputs", [])}


_warned = {"tls": False}


def post_event(url: str, body: dict) -> int:
    try:
        resp = requests.post(url, json=body, timeout=15)
        return resp.status_code
    except Exception as exc:  # noqa: BLE001 - a failed probe is data, not a crash
        if not _warned["tls"]:
            print(f"    POST failed: {exc}")
            _warned["tls"] = True
        return 0


def run_load_test(host: str, users: int, runtime: str) -> dict[str, str]:
    log(f"1/4 load test: {users} users for {runtime}")
    # Locust exits non-zero when any request failed. That is information, not
    # a reason to abandon the run, so the exit code is recorded rather than raised.
    proc = subprocess.run(
        [
            sys.executable, "-m", "locust",
            "-f", "loadtest/locustfile.py",
            "--host", host,
            "--users", str(users),
            "--spawn-rate", "10",
            "--run-time", runtime,
            "--headless",
            "--csv", "results/run",
        ],
    )
    if proc.returncode != 0:
        print("    (locust exited non-zero: some requests failed — see the failure count)")
    return read_load_stats()


def read_load_stats() -> dict[str, str]:
    with open("results/run_stats.csv") as fh:
        rows = {r["Name"]: r for r in csv.DictReader(fh)}
    agg = rows.get("Aggregated", {})
    stats = {
        "rps": f"{float(agg.get('Requests/s', 0)):.1f}",
        "p50": agg.get("50%", "?"),
        "p95": agg.get("95%", "?"),
        "p99": agg.get("99%", "?"),
        "failures": agg.get("Failure Count", "?"),
        "requests": agg.get("Request Count", "?"),
    }
    print(f"    {stats['requests']} requests, {stats['rps']}/s, "
          f"p50 {stats['p50']} ms, p95 {stats['p95']} ms, p99 {stats['p99']} ms, "
          f"{stats['failures']} failures")
    return stats


def aggregate_count(ddb, table: str, user: str) -> int:
    item = ddb.get_item(
        TableName=table,
        Key={"pk": {"S": f"USER#{user}"}, "sk": {"S": "AGG#lifetime"}},
    ).get("Item")
    return int(item["total_events"]["N"]) if item else 0


def prove_idempotency(api: str, ddb, table: str, replays: int) -> dict[str, int]:
    log(f"2/4 idempotency: sending one event id {replays} times")
    user = f"dedupe-probe-{int(time.time())}"
    event_id = f"replay-{int(time.time())}"
    for _ in range(replays):
        post_event(api, {"event_id": event_id, "user_id": user,
                         "event_type": "lesson_completed"})
    time.sleep(25)
    stored = aggregate_count(ddb, table, user)
    print(f"    {replays} deliveries -> total_events = {stored} (expected 1)")
    return {"sent": replays, "stored": stored}


def find_worker(lam) -> str:
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page["Functions"]:
            if "WorkerFunction" in fn["FunctionName"]:
                return fn["FunctionName"]
    raise RuntimeError("worker function not found")


def drill_dlq(api: str, ddb, lam, sqs, table: str, dlq_url: str, count: int,
              queue_url: str) -> dict:
    log(f"3/4 fault injection: breaking the worker, sending {count} events, redriving")
    worker = find_worker(lam)
    original = lam.get_function_configuration(FunctionName=worker)["Environment"]["Variables"]

    # Production visibility timeout is 6x the worker timeout, so three failed
    # receives would take ~9 minutes to reach the DLQ. Shorten it for the drill.
    orig_vis = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["VisibilityTimeout"]
    )["Attributes"]["VisibilityTimeout"]
    sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"VisibilityTimeout": "10"})

    broken = dict(original, TABLE_NAME="does-not-exist")
    lam.update_function_configuration(FunctionName=worker, Environment={"Variables": broken})
    lam.get_waiter("function_updated_v2").wait(FunctionName=worker)

    user = f"drill-{int(time.time())}"
    for i in range(count):
        post_event(api, {"event_id": f"{user}-{i}", "user_id": user,
                         "event_type": "question_answered"})

    print("    waiting for retries to exhaust into the DLQ (up to 2 minutes)")
    peak = 0
    try:
        for _ in range(24):
            time.sleep(5)
            depth = int(sqs.get_queue_attributes(
                QueueUrl=dlq_url, AttributeNames=["ApproximateNumberOfMessages"]
            )["Attributes"]["ApproximateNumberOfMessages"])
            peak = max(peak, depth)
            if depth >= count:
                break
        print(f"    DLQ depth at peak: {peak}")
    finally:
        # Always put the worker and the queue back, even on Ctrl-C or an error.
        lam.update_function_configuration(
            FunctionName=worker, Environment={"Variables": original}
        )
        lam.get_waiter("function_updated_v2").wait(FunctionName=worker)
        sqs.set_queue_attributes(
            QueueUrl=queue_url, Attributes={"VisibilityTimeout": orig_vis}
        )
        print("    worker and queue settings restored")

    dlq_arn = sqs.get_queue_attributes(
        QueueUrl=dlq_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    sqs.start_message_move_task(SourceArn=dlq_arn)

    recovered = 0
    for _ in range(12):
        time.sleep(10)
        recovered = aggregate_count(ddb, table, user)
        if recovered >= count:
            break
    print(f"    recovered after redrive: {recovered}/{count}")
    return {"sent": count, "peak_dlq": peak, "recovered": recovered}


def coverage() -> str:
    log("4/4 test coverage")
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "--cov=src", "--cov-report=term", "-q"],
        capture_output=True, text=True,
    )
    print(proc.stdout[-800:])
    for line in proc.stdout.splitlines():
        if line.startswith("TOTAL"):
            return line.split()[-1]
    return "?"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stack", default="LearnStreamStack")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--users", type=int, default=50)
    ap.add_argument("--runtime", default="3m")
    ap.add_argument("--replays", type=int, default=25)
    ap.add_argument("--drill", type=int, default=50)
    ap.add_argument("--skip-drill", action="store_true")
    ap.add_argument("--skip-load", action="store_true",
                    help="reuse results/run_stats.csv from a previous load test")
    ap.add_argument("--restore-only", action="store_true",
                    help="just put the worker's TABLE_NAME back and exit")
    args = ap.parse_args()

    out = stack_outputs(args.stack, args.region)
    api, table, dlq = out["ApiUrl"], out["TableName"], out["DlqUrl"]
    queue = out["QueueUrl"]
    host = api[: -len("events")].rstrip("/")

    ddb = boto3.client("dynamodb", region_name=args.region)
    lam = boto3.client("lambda", region_name=args.region)
    sqs = boto3.client("sqs", region_name=args.region)

    if args.restore_only:
        worker = find_worker(lam)
        env = lam.get_function_configuration(FunctionName=worker)["Environment"]["Variables"]
        env["TABLE_NAME"] = table
        lam.update_function_configuration(FunctionName=worker, Environment={"Variables": env})
        lam.get_waiter("function_updated_v2").wait(FunctionName=worker)
        print(f"worker TABLE_NAME restored to {table}")
        return

    if args.skip_load:
        print("==> 1/4 load test: skipped, reading results/run_stats.csv")
        load = read_load_stats()
    else:
        load = run_load_test(host, args.users, args.runtime)
    dedupe = prove_idempotency(api, ddb, table, args.replays)
    drill = (
        {"sent": 0, "peak_dlq": 0, "recovered": 0}
        if args.skip_drill
        else drill_dlq(api, ddb, lam, sqs, table, dlq, args.drill, queue)
    )
    cov = coverage()

    summary = f"""
------------------------------------------------------------------
LEARNSTREAM MEASURED RESULTS
  throughput      : {load['rps']} events/second ({load['requests']} requests, {load['failures']} failures)
  latency         : p50 {load['p50']} ms | p95 {load['p95']} ms | p99 {load['p99']} ms
  concurrency     : {"(from the saved run)" if args.skip_load else str(args.users) + " concurrent producers"}
  idempotency     : {dedupe['sent']} deliveries of one event id -> {dedupe['stored']} stored
  fault injection : {drill['recovered']}/{drill['sent']} recovered after DLQ redrive (peak DLQ {drill['peak_dlq']})
  test coverage   : {cov}
------------------------------------------------------------------
"""
    print(summary)
    with open("results/summary.txt", "w") as fh:
        fh.write(summary)


if __name__ == "__main__":
    main()
