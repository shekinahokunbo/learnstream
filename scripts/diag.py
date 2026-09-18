#!/usr/bin/env python3
"""Where did the deploy get to? Run:  .venv/bin/python3 scripts/diag.py"""

from __future__ import annotations

import sys

import boto3
from botocore.exceptions import ClientError

REGION = sys.argv[1] if len(sys.argv) > 1 else "us-east-1"
cfn = boto3.client("cloudformation", region_name=REGION)


def stack(name: str):
    try:
        return cfn.describe_stacks(StackName=name)["Stacks"][0]
    except ClientError as exc:
        if "does not exist" in str(exc):
            return None
        raise


print(f"region: {REGION}")
try:
    who = boto3.client("sts", region_name=REGION).get_caller_identity()
    print(f"identity: account {who['Account']}, {who['Arn']}")
except Exception as exc:  # noqa: BLE001
    print(f"CREDENTIALS FAILED: {exc}")
    sys.exit(1)

boot = stack("CDKToolkit")
print(f"\nbootstrap (CDKToolkit): {boot['StackStatus'] if boot else 'NOT CREATED'}")

app = stack("LearnStreamStack")
if app is None:
    print("LearnStreamStack: NOT CREATED — the deploy did not complete.")
    print("\nRe-run just the deploy so the error is visible:")
    print("    cd infra && npx --yes aws-cdk@2 deploy --require-approval never")
    sys.exit(0)

status = app["StackStatus"]
print(f"LearnStreamStack: {status}")

if status.endswith("FAILED") or "ROLLBACK" in status:
    print("\nwhy it failed:")
    events = cfn.describe_stack_events(StackName="LearnStreamStack")["StackEvents"]
    shown = 0
    for e in events:
        if e.get("ResourceStatus", "").endswith("FAILED"):
            print(f"  - {e.get('LogicalResourceId')} ({e.get('ResourceType')})")
            print(f"    {e.get('ResourceStatusReason', '')[:300]}")
            shown += 1
            if shown >= 5:
                break
    print("\nClear the failed stack, then deploy again:")
    print("    cd infra && npx --yes aws-cdk@2 destroy --force && npx --yes aws-cdk@2 deploy --require-approval never")
    sys.exit(0)

outputs = {o["OutputKey"]: o["OutputValue"] for o in app.get("Outputs", [])}
print("\noutputs:")
for k in ("ApiUrl", "TableName", "QueueUrl", "DlqUrl", "BucketName"):
    print(f"  {k}: {outputs.get(k, '(missing)')}")

if status in ("CREATE_COMPLETE", "UPDATE_COMPLETE"):
    print("\nThe stack is live. Measure now:")
    print("    .venv/bin/python3 scripts/measure.py")
