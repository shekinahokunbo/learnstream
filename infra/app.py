#!/usr/bin/env python3
import os

import aws_cdk as cdk

from learnstream_stack import LearnStreamStack

app = cdk.App()
LearnStreamStack(
    app,
    "LearnStreamStack",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
    ),
    description="Event-driven learning analytics pipeline",
)
app.synth()
