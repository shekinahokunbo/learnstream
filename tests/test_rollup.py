import importlib

import boto3
import pytest
from moto import mock_aws

from tests.conftest import AWS_REGION

TABLE_NAME = "learnstream-events"
BUCKET_NAME = "learnstream-analytics"


@pytest.fixture
def stack(monkeypatch):
    with mock_aws():
        ddb = boto3.resource("dynamodb", region_name=AWS_REGION)
        table = ddb.create_table(
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
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.create_bucket(Bucket=BUCKET_NAME)
        monkeypatch.setenv("TABLE_NAME", TABLE_NAME)
        monkeypatch.setenv("BUCKET_NAME", BUCKET_NAME)
        import rollup.handler as handler_module

        importlib.reload(handler_module)
        yield table, s3, handler_module


def test_writes_aggregates_to_a_partitioned_key(stack):
    table, s3, handler_module = stack
    table.put_item(
        Item={"pk": "USER#u1", "sk": "AGG#lifetime", "total_events": 3}
    )
    table.put_item(Item={"pk": "USER#u1", "sk": "EVENT#1#evt-1"})  # not an aggregate

    result = handler_module.handler({})

    assert result["users"] == 1
    assert result["key"].startswith("aggregates/dt=")
    assert "/hour=" in result["key"]

    body = s3.get_object(Bucket=BUCKET_NAME, Key=result["key"])["Body"].read().decode()
    assert '"total_events":3' in body.replace(" ", "")


def test_handles_an_empty_table(stack):
    _, s3, handler_module = stack
    result = handler_module.handler({})
    assert result["users"] == 0
