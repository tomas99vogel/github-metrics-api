import boto3
import pytest

try:
    from moto import mock_aws
except Exception as e:
    raise ImportError("Install moto with: pip install 'moto[boto3]'") from e

REGION = "eu-west-1"
PROCESSED_TABLE = "sam-app-processed-events"
SUMMARY_TABLE = "sam-app-repo-pr-summary"


@pytest.fixture()
def ddb_env(monkeypatch):
    # Set env before any boto3 clients/resources are created
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("PROCESSED_EVENTS_TABLE", PROCESSED_TABLE)
    monkeypatch.setenv("REPO_PR_SUMMARY_TABLE", SUMMARY_TABLE)
    yield


@pytest.fixture()
def moto_dynamodb(ddb_env):
    # Start moto and create tables before importing the module-under-test
    with mock_aws():
        boto3.setup_default_session(region_name=REGION)
        ddb = boto3.client("dynamodb")

        # Processed events table
        ddb.create_table(
            TableName=PROCESSED_TABLE,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "created_at", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "created_at", "KeyType": "RANGE"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        # Summary table
        ddb.create_table(
            TableName=SUMMARY_TABLE,
            AttributeDefinitions=[{"AttributeName": "repo_name", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "repo_name", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield
