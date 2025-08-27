# lambda/github_events_poller.py
import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

import boto3
import requests

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

# Configuration
GITHUB_EVENTS_URL = "https://api.github.com/events"
INTERESTED_EVENTS = {"WatchEvent", "PullRequestEvent", "IssuesEvent"}


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Main Lambda handler for GitHub Events polling"""
    logger.info(f"GitHub Events polling started: {datetime.now(UTC).isoformat()}")

    try:
        # Get stored ETag and last seen event IDs
        poll_state = get_poll_state()
        logger.info(f"Current poll state: {poll_state}")

        # Poll GitHub Events API with ETag
        result = poll_github_events(poll_state)

        if result.get("not_modified"):
            logger.info("No new events (304 Not Modified)")
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "No new events", "cached": True}),
            }

        # Process new events
        new_events = filter_new_events(result["events"], poll_state.get("last_seen_ids", []))
        logger.info(f"Found {len(new_events)} new events out of {len(result['events'])} total")

        if new_events:
            send_events_to_sqs(new_events)
            update_poll_state(
                {
                    "etag": result["etag"],
                    "last_seen_ids": [
                        event["id"] for event in result["events"][:100]
                    ],  # Track last IDs
                    "poll_interval": result["poll_interval"],
                    "last_poll": datetime.now(UTC).isoformat(),
                }
            )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": f"Processed {len(new_events)} new events",
                    "total_events": len(result["events"]),
                    "new_events": len(new_events),
                    "next_poll_in": result["poll_interval"],
                }
            ),
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {e}")

        # Check if it's a rate limit error
        if hasattr(e, "response") and e.response is not None:
            if (
                e.response.status_code == 403
                and e.response.headers.get("x-ratelimit-remaining") == "0"
            ):
                reset_time = datetime.fromtimestamp(
                    int(e.response.headers.get("x-ratelimit-reset", 0)), UTC
                )
                logger.warning(f"Rate limited until: {reset_time.isoformat()}")

        raise e
    except Exception as e:
        logger.error(f"GitHub polling failed: {e}")
        raise e


def get_poll_state() -> dict[str, Any]:
    """Retrieve polling state from DynamoDB"""
    try:
        table = dynamodb.Table(os.environ["STATE_TABLE"])
        response = table.get_item(Key={"id": "github_events_poll_state"})

        return response.get(
            "Item",
            {
                "etag": None,
                "last_seen_ids": [],
                "poll_interval": 60,  # Default 60 seconds
                "last_poll": None,
            },
        )
    except Exception as e:
        logger.error(f"Failed to get poll state: {e}")
        return {
            "etag": None,
            "last_seen_ids": [],
            "poll_interval": 60,
            "last_poll": None,
        }


def poll_github_events(poll_state: dict[str, Any]) -> dict[str, Any]:
    """Poll GitHub Events API with ETag support"""
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "MyApp-GitHubEventsPoller/1.0",
    }
    # Optional auth: use PAT if provided to increase rate limits
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    # Add ETag for conditional request
    if poll_state.get("etag"):
        headers["If-None-Match"] = poll_state["etag"]

    try:
        response = requests.get(
            GITHUB_EVENTS_URL,
            headers=headers,
            params={"per_page": 100},  # Maximum allowed
            timeout=25,
        )

        # Handle 304 Not Modified
        if response.status_code == 304:
            logger.info("GitHub returned 304 Not Modified")
            return {"not_modified": True}

        # Raise for other HTTP errors
        response.raise_for_status()

        # Extract GitHub's recommended poll interval
        poll_interval = int(response.headers.get("x-poll-interval", "60"))

        logger.info(
            "GitHub API response: status=%s, event_count=%s, etag=%s, poll_interval=%s, rate_limit_remaining=%s, rate_limit_reset=%s",
            response.status_code,
            len(response.json()),
            response.headers.get("etag"),
            poll_interval,
            response.headers.get("x-ratelimit-remaining"),
            datetime.fromtimestamp(
                int(response.headers.get("x-ratelimit-reset", 0)), UTC
            ).isoformat(),
        )

        return {
            "events": response.json(),
            "etag": response.headers.get("etag"),
            "poll_interval": poll_interval,
            "not_modified": False,
        }

    except requests.exceptions.HTTPError as e:
        # Log rate limit info for debugging
        if e.response is not None and hasattr(e.response, "headers"):
            logger.error(
                "GitHub API error headers: status=%s, remaining=%s, reset=%s, retry_after=%s",
                e.response.status_code,
                e.response.headers.get("x-ratelimit-remaining"),
                e.response.headers.get("x-ratelimit-reset"),
                e.response.headers.get("retry-after"),
            )
        raise e


def filter_new_events(
    events: list[dict[str, Any]], last_seen_ids: list[str]
) -> list[dict[str, Any]]:
    """Filter for interested events and stop when encountering previously seen events"""
    # GitHub events are ordered by recency, so it can stop when it hits a known event
    new_events = []

    for event in events:
        # Stop processing if it encounters a previously seen event
        if event["id"] in last_seen_ids:
            logger.info(f"Found known event {event['id']}, stopping")
            break

        # Only include watched events
        if event["type"] in INTERESTED_EVENTS:
            new_events.append(event)

    logger.info(
        f"Found {len(new_events)} new events of interest: {[e['type'] for e in new_events]}"
    )
    return new_events


def send_events_to_sqs(events: list[dict[str, Any]]) -> None:
    """Send events to SQS in batches"""
    BATCH_SIZE = 10

    for i in range(0, len(events), BATCH_SIZE):
        batch = events[i : i + BATCH_SIZE]

        entries = []
        for idx, event in enumerate(batch):
            entries.append(
                {
                    "Id": f"github_{event['id']}",
                    "MessageBody": json.dumps(
                        {
                            "id": event["id"],
                            "type": event["type"],
                            "actor": event.get("actor"),
                            "repo": event.get("repo"),
                            "payload": event.get("payload"),
                            "public": event.get("public"),
                            "created_at": event.get("created_at"),
                            "org": event.get("org"),
                            # Add metadata
                            "source": "github_events_api",
                            "received_at": datetime.now(UTC).isoformat(),
                        },
                        default=str,
                    ),  # Handle datetime serialization
                    "MessageAttributes": {
                        "eventType": {
                            "DataType": "String",
                            "StringValue": event["type"],
                        },
                        "repoName": {
                            "DataType": "String",
                            "StringValue": event.get("repo", {}).get("name", "unknown"),
                        },
                        "actorLogin": {
                            "DataType": "String",
                            "StringValue": event.get("actor", {}).get("login", "unknown"),
                        },
                    },
                }
            )

        sqs.send_message_batch(QueueUrl=os.environ["EVENTS_QUEUE_URL"], Entries=entries)

        logger.info(f"Sent batch of {len(batch)} events to SQS")


def update_poll_state(new_state: dict[str, Any]) -> None:
    """Update polling state in DynamoDB"""
    table = dynamodb.Table(os.environ["STATE_TABLE"])

    item = {
        "id": "github_events_poll_state",
        "updated_at": datetime.now(UTC).isoformat(),
        **new_state,
    }

    table.put_item(Item=item)
    logger.info(f"Updated poll state: {new_state}")
