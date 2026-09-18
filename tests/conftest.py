import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

AWS_REGION = "us-east-1"


@pytest.fixture(autouse=True)
def aws_credentials(monkeypatch):
    """moto refuses to run against real credentials; force dummies."""
    for key, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": AWS_REGION,
    }.items():
        monkeypatch.setenv(key, value)
