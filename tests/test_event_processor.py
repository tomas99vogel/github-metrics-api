import importlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import boto3

sys.path.append(str(Path(__file__).resolve().parents[1]))

PROCESSED_TABLE = "sam-app-processed-events"
SUMMARY_TABLE = "sam-app-repo-pr-summary"
REGION = "eu-west-1"


def make_sqs_event(
    event_id="e-1",
    repo="owner/repo",
    etype="PullRequestEvent",
    action="opened",
    created_at=None,
):
    body = {
        "id": event_id,
        "type": etype,
        "repo": {"name": repo},
        "actor": {"login": "tester"},
        "payload": {"action": action} if action else {},
        "created_at": created_at or datetime.now(UTC).isoformat(),
    }
    return {
        "Records": [
            {
                "messageId": f"m-{event_id}",
                "body": json.dumps(body),
                "attributes": {},
                "messageAttributes": {},
                "md5OfBody": "",
                "eventSource": "aws:sqs",
                "eventSourceARN": f"arn:aws:sqs:{REGION}:123:queue",
                "awsRegion": REGION,
                "receiptHandle": "rh",
            }
        ]
    }


def test_process_various_event_types_and_store(moto_dynamodb):
    from api.EventProcessor import app as ep

    importlib.reload(ep)

    events = [
        make_sqs_event(
            event_id="pr-1",
            repo="octo/repo",
            etype="PullRequestEvent",
            action="opened",
            created_at="2025-08-23T12:00:00Z",
        ),
        make_sqs_event(
            event_id="issue-1",
            repo="octo/repo",
            etype="IssuesEvent",
            action="opened",
            created_at="2025-08-23T12:01:00Z",
        ),
        make_sqs_event(
            event_id="watch-1",
            repo="octo/repo",
            etype="WatchEvent",
            action="started",
            created_at="2025-08-23T12:02:00Z",
        ),
        make_sqs_event(
            event_id="unknown-1",
            repo="octo/repo",
            etype="SomeOtherEvent",
            action="x",
            created_at="2025-08-23T12:03:00Z",
        ),
    ]
    resp = ep.lambda_handler({"Records": [e["Records"][0] for e in events]}, context={})
    assert resp["statusCode"] == 200

    # Verify items stored (table exists in mocked region)
    ddb = boto3.resource("dynamodb", region_name=REGION)
    processed = ddb.Table(PROCESSED_TABLE)
    for e in events:
        body = json.loads(e["Records"][0]["body"])
        pk = f"event#{body['id']}"
        item = processed.get_item(Key={"pk": pk, "created_at": body["created_at"]}).get("Item")
        assert item is not None
        assert item["event_type"] == body["type"]
        assert item["repo_name"] == body["repo"]["name"]


def test_pull_request_opened_updates_summary_once(moto_dynamodb):
    from api.EventProcessor import app as ep

    importlib.reload(ep)

    repo = "octocat/hello-world"
    e1 = make_sqs_event(
        event_id="pr-42",
        repo=repo,
        etype="PullRequestEvent",
        action="opened",
        created_at="2025-08-23T12:00:00Z",
    )
    e1_dup = make_sqs_event(
        event_id="pr-42",
        repo=repo,
        etype="PullRequestEvent",
        action="opened",
        created_at="2025-08-23T12:00:00Z",
    )
    e2 = make_sqs_event(
        event_id="pr-43",
        repo=repo,
        etype="PullRequestEvent",
        action="opened",
        created_at="2025-08-23T12:05:00Z",
    )

    assert ep.lambda_handler(e1, context={})["statusCode"] == 200
    assert ep.lambda_handler(e1_dup, context={})["statusCode"] == 200
    assert ep.lambda_handler(e2, context={})["statusCode"] == 200

    ddb = boto3.resource("dynamodb", region_name=REGION)
    summary = ddb.Table(SUMMARY_TABLE)
    s_item = summary.get_item(Key={"repo_name": repo}).get("Item")
    assert s_item is not None
    assert int(s_item["opened_pr_count"]) == 2
