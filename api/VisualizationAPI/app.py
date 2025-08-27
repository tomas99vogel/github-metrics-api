import concurrent.futures
import json
import logging
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients with connection pooling
dynamodb = boto3.resource(
    "dynamodb",
    config=boto3.session.Config(
        region_name=os.environ.get("AWS_REGION", "eu-west-1"), max_pool_connections=50
    ),
)

def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Handle visualization API requests"""

    try:
        http_method = event["httpMethod"]
        path = event["path"]
        query_params = event.get("queryStringParameters") or {}

        logger.info(f"Processing {http_method} {path} with params: {query_params}")

        # Handle OPTIONS requests for CORS
        if http_method == "OPTIONS":
            return create_response(200, {"message": "CORS preflight"})

        # Route to appropriate handler
        if path.startswith("/visualization/timeline") and http_method == "GET":
            return handle_timeline_visualization_request(query_params)
        elif path.startswith("/visualisation/timeline") and http_method == "GET":
            # Handle British spelling
            return handle_timeline_visualization_request(query_params)
        else:
            # Provide helpful error message for unknown endpoints
            available_endpoints = [
                "GET /visualization/timeline?hours=24&interval=60",
                "GET /visualisation/timeline?hours=24&interval=60 (alternative spelling)",
            ]
            return create_response(
                404,
                {
                    "error": "Endpoint not found",
                    "message": f"Unknown path: {http_method} {path}",
                    "available_endpoints": available_endpoints,
                    "note": "Both /visualization/ and /visualisation/ spellings are supported",
                },
            )

    except Exception as e:
        logger.error(f"Visualization API request failed: {e}")
        return create_response(500, {"error": "Internal server error"})


def handle_timeline_visualization_request(
    query_params: dict[str, str],
) -> dict[str, Any]:
    """Return timeline data for visualizing event types over time as a line chart"""

    # Parse hours parameter (defaults to 24 hours)
    hours_str = query_params.get("hours", "24")
    try:
        hours = int(hours_str)
        if hours <= 0 or hours > 168:  # Max 1 week
            return create_response(400, {"error": "Hours must be between 1 and 168 (1 week)"})
    except ValueError:
        return create_response(400, {"error": "Hours must be a valid integer"})

    # Parse interval parameter (defaults to 60 minutes)
    interval_str = query_params.get("interval", "60")
    try:
        interval_minutes = int(interval_str)
        if interval_minutes <= 0 or interval_minutes > 1440:  # Max 24 hours
            return create_response(400, {"error": "Interval must be between 1 and 1440 minutes"})
    except ValueError:
        return create_response(400, {"error": "Interval must be a valid integer"})

    try:
        # Get timeline data
        timeline_data = get_event_timeline_data(hours, interval_minutes)

        # Calculate metadata
        total_events = sum(sum(point["counts"].values()) for point in timeline_data)
        time_range = {
            "start_time": (datetime.now(UTC) - timedelta(hours=hours)).isoformat(),
            "end_time": datetime.now(UTC).isoformat(),
            "hours": hours,
            "interval_minutes": interval_minutes,
        }

        return create_response(
            200,
            {
                "visualization_type": "line_chart",
                "title": f"GitHub Events Timeline - Last {hours} Hours",
                "time_range": time_range,
                "data_points": len(timeline_data),
                "total_events": total_events,
                "timeline": timeline_data,
                "chart_config": {
                    "x_axis": {"label": "Time", "type": "datetime"},
                    "y_axis": {"label": "Number of Events", "type": "integer"},
                    "series": [
                        {"name": "WatchEvent", "color": "#2196F3"},
                        {"name": "PullRequestEvent", "color": "#4CAF50"},
                        {"name": "IssuesEvent", "color": "#FF9800"},
                    ],
                },
            },
        )

    except Exception as e:
        logger.error(f"Failed to generate timeline data: {e}")
        return create_response(500, {"error": "Failed to generate timeline visualization"})


def get_event_timeline_data(hours: int, interval_minutes: int) -> list[dict[str, Any]]:
    """Get event timeline data - OPTIMIZED with batch queries"""

    # Calculate number of intervals (limit to prevent timeouts)
    total_minutes = hours * 60
    num_intervals = min(total_minutes // interval_minutes, 100)  # Max 100 data points

    event_types = ["WatchEvent", "PullRequestEvent", "IssuesEvent"]
    timeline_data = []

    # Use concurrent queries for all intervals
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []

        for i in range(num_intervals):
            interval_end = datetime.now(UTC) - timedelta(minutes=i * interval_minutes)
            interval_start = interval_end - timedelta(minutes=interval_minutes)

            future = executor.submit(get_interval_counts, event_types, interval_start, interval_end)
            futures.append((future, interval_start, interval_end))

        # Collect results
        for future, interval_start, interval_end in futures:
            try:
                interval_counts = future.result(timeout=10)  # 10 second timeout per query
                timeline_data.append(
                    {
                        "timestamp": interval_start.isoformat(),
                        "interval_start": interval_start.isoformat(),
                        "interval_end": interval_end.isoformat(),
                        "counts": interval_counts,
                    }
                )
            except Exception as e:
                logger.error(f"Failed to get counts for interval {interval_start}: {e}")
                # Add zero counts for failed intervals
                timeline_data.append(
                    {
                        "timestamp": interval_start.isoformat(),
                        "interval_start": interval_start.isoformat(),
                        "interval_end": interval_end.isoformat(),
                        "counts": {event_type: 0 for event_type in event_types},
                    }
                )

    # Sort chronologically (oldest first)
    timeline_data.sort(key=lambda x: x["timestamp"])

    logger.info(f"Generated timeline data with {len(timeline_data)} intervals")
    return timeline_data


def get_interval_counts(
    event_types: list[str], start_time: datetime, end_time: datetime
) -> dict[str, int]:
    """Get counts for all event types in a specific time interval - OPTIMIZED"""
    table = dynamodb.Table(os.environ.get("PROCESSED_EVENTS_TABLE", "processed-github-events"))

    start_iso = start_time.isoformat()
    end_iso = end_time.isoformat()

    counts = {}

    # Query all event types concurrently for this interval
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        future_to_type = {}

        for event_type in event_types:
            future = executor.submit(query_interval_count, table, event_type, start_iso, end_iso)
            future_to_type[future] = event_type

        for future in concurrent.futures.as_completed(future_to_type):
            event_type = future_to_type[future]
            try:
                count = future.result(timeout=5)  # 5 second timeout
                counts[event_type] = count
            except Exception as e:
                logger.error(f"Failed to count {event_type} in interval: {e}")
                counts[event_type] = 0

    return counts


def query_interval_count(table, event_type: str, start_time: str, end_time: str) -> int:
    """Query count for a specific event type in a time range"""
    try:
        response = table.query(
            IndexName="EventTypeIndex",
            KeyConditionExpression=Key("event_type").eq(event_type)
            & Key("created_at").between(start_time, end_time),
            Select="COUNT",
        )
        return response.get("Count", 0)
    except Exception as e:
        logger.error(f"Query failed for {event_type}: {e}")
        return 0


def convert_decimals(obj):
    """Convert DynamoDB Decimal objects to float for JSON serialization"""
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj


def create_response(status_code: int, body: dict[str, Any]) -> dict[str, Any]:
    """Create standardized API response"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
        },
        "body": json.dumps(body, default=str),
    }
