import importlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import boto3

# Make 'api' package importable
sys.path.append(str(Path(__file__).resolve().parents[1]))

PROCESSED_TABLE = "sam-app-processed-events"
REGION = "eu-west-1"


def test_pr_average_computation(moto_dynamodb):
    # Recreate the processed events table with the EventTypeIndex GSI
    ddb = boto3.client("dynamodb", region_name=REGION)
    try:
        ddb.delete_table(TableName=PROCESSED_TABLE)
    except ddb.exceptions.ResourceNotFoundException:
        pass

    ddb.create_table(
        TableName=PROCESSED_TABLE,
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "created_at", "AttributeType": "S"},
            {"AttributeName": "event_type", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "created_at", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "EventTypeIndex",
                "KeySchema": [
                    {"AttributeName": "event_type", "KeyType": "HASH"},
                    {"AttributeName": "created_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )

    # Seed mock PR events for a single repo at 12:00, 12:10, 12:25 UTC
    repo = "octocat/hello-world"
    items = [
        {
            "id": "e1",
            "created_at": "2025-08-23T12:00:00Z",
            "event_type": "PullRequestEvent",
            "repo_name": repo,
            "actor_login": "tester",
            "processed_at": datetime.now(UTC).isoformat(),
            "pr_action": "opened",
        },
        {
            "id": "e2",
            "created_at": "2025-08-23T12:10:00Z",
            "event_type": "PullRequestEvent",
            "repo_name": repo,
            "actor_login": "tester",
            "processed_at": datetime.now(UTC).isoformat(),
            "pr_action": "opened",
        },
        {
            "id": "e3",
            "created_at": "2025-08-23T12:25:00Z",
            "event_type": "PullRequestEvent",
            "repo_name": repo,
            "actor_login": "tester",
            "processed_at": datetime.now(UTC).isoformat(),
            "pr_action": "opened",
        },
        # Noise: other repo and non-PR event
        {
            "id": "e4",
            "created_at": "2025-08-23T12:05:00Z",
            "event_type": "PullRequestEvent",
            "repo_name": "someone/else",
            "actor_login": "tester",
            "processed_at": datetime.now(UTC).isoformat(),
            "pr_action": "opened",
        },
        {
            "id": "e5",
            "created_at": "2025-08-23T12:07:00Z",
            "event_type": "WatchEvent",
            "repo_name": repo,
            "actor_login": "tester",
            "processed_at": datetime.now(UTC).isoformat(),
        },
    ]

    ddb_res = boto3.resource("dynamodb", region_name=REGION)
    table = ddb_res.Table(PROCESSED_TABLE)
    with table.batch_writer() as bw:
        for it in items:
            bw.put_item(
                Item={
                    "pk": f"event#{it['id']}",
                    "created_at": it["created_at"],
                    "event_type": it["event_type"],
                    "repo_name": it["repo_name"],
                    "actor_login": it["actor_login"],
                    "processed_at": it["processed_at"],
                    "id": it["id"],
                    **({"pr_action": it["pr_action"]} if "pr_action" in it else {}),
                }
            )

    # Import after table + GSI exist so module binds to mocked DynamoDB
    from api.StatisticsAPI import app as stats

    importlib.reload(stats)

    # Call the API handler for pr-average with the target repo
    event = {
        "httpMethod": "GET",
        "path": "/metrics/pr-average",
        "queryStringParameters": {"repo": repo},
    }
    resp = stats.lambda_handler(event, context={})
    assert resp["statusCode"] == 200

    body = json.loads(resp["body"])
    # Expected average: diffs = 600s (12:10-12:00), 900s (12:25-12:10) => avg = 750.0
    assert body["repository"] == repo
    assert body["pr_count"] == 3
    assert body["average_time_between_pr"] == 750.0
