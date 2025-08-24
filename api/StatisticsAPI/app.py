import json
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

def is_valid_github_repo_name(repo_name: str) -> bool:
    """Validate GitHub repository name format (owner/repo)"""
    if not repo_name or not isinstance(repo_name, str):
        return False
    
    # Check for path traversal attempts
    if '..' in repo_name or repo_name.startswith('/') or repo_name.endswith('/'):
        return False
    
    # Must contain exactly one forward slash
    parts = repo_name.split('/')
    if len(parts) != 2:
        return False
    
    owner, repo = parts
    
    # GitHub username/org and repo name pattern
    # Allows: letters, numbers, hyphens, underscores, dots
    # Cannot start/end with hyphen or dot
    pattern = r'^[a-zA-Z0-9]([a-zA-Z0-9._-])*[a-zA-Z0-9]$|^[a-zA-Z0-9]$'
    
    # Validate owner and repo parts
    if not re.match(pattern, owner) or not re.match(pattern, repo):
        return False
    
    # Length limits (GitHub limits)
    if len(owner) > 39 or len(repo) > 100:
        return False
    
    return True

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle statistics API requests"""
    
    try:
        http_method = event['httpMethod']
        path = event['path']
        query_params = event.get('queryStringParameters') or {}
        
        logger.info(f"Processing {http_method} {path} with params: {query_params}")
        
        # Handle OPTIONS requests for CORS
        if http_method == 'OPTIONS':
            return create_response(200, {'message': 'CORS preflight'})
        
        # Route to appropriate handler
        if path.startswith('/metrics/pr-average') and http_method == 'GET':
            return handle_pr_average_request(query_params)
        elif path.startswith('/metrics/events/count') and http_method == 'GET':
            return handle_event_count_request(query_params)
        else:
            available_endpoints = [
                'GET /metrics/pr-average?repo=owner/repo',
                'GET /metrics/events/count?offset=10',
            ]
            return create_response(404, {
                'error': 'Endpoint not found',
                'available_endpoints': available_endpoints
            })
            
    except Exception as e:
        logger.error(f"API request failed: {e}")
        return create_response(500, {'error': 'Internal server error'})

def handle_pr_average_request(query_params: Dict[str, str]) -> Dict[str, Any]:
    """Calculate average time between opened pull requests for a given repository"""
    repo_name = query_params.get('repo')

    if not repo_name:
        # If repo not provided, return repos that have measurable opened PRs
        try:
            repos = list_repos_with_opened_pr(min_count=2)
            return create_response(200, {'repositories': repos} if repos else None)  # per spec: list or null
        except Exception as e:
            logger.error(f"Failed to list repositories: {e}")
            return create_response(500, {'error': 'Failed to retrieve repositories'})

    if not is_valid_github_repo_name(repo_name):
        return create_response(400, {'error': 'Invalid repository name format. Expected owner/repo'})

    try:
        pr_events = get_pr_events_for_repo(repo_name)
        opened_events = [e for e in pr_events if e.get('pr_action') == 'opened']

        if len(opened_events) < 2:
            return create_response(200, {
                'repository': repo_name,
                'pr_count': len(opened_events),           # opened-only
                'total_pr_events': len(pr_events),        # optional: all PR events
                'average_time_between_pr': None,
                'message': 'Insufficient data - need at least 2 opened PR events'
            })

        avg_seconds = calculate_average_time_between_pr(opened_events)
        # Use opened-only for dates
        first_dt = opened_events[0]['created_at']
        last_dt = opened_events[-1]['created_at']

        return create_response(200, {
            'repository': repo_name,
            'pr_count': len(opened_events),              # opened-only
            'total_pr_events': len(pr_events),           # optional
            'average_time_between_pr': round(avg_seconds, 2),
            'first_pr_date': first_dt,
            'last_pr_date': last_dt
        })

    except ValueError as e:
        return create_response(400, {'error': f'Invalid date format: {e}'})
    except Exception as e:
        logger.error(f"Failed to calculate PR average: {e}")
        return create_response(500, {'error': 'Failed to calculate statistics'})

def handle_event_count_request(query_params: Dict[str, str]) -> Dict[str, Any]:
    """Return total number of events grouped by event type for a given offset"""
    
    # Parse offset parameter (optional, defaults to 10 minutes)
    offset_str = query_params.get('offset', '10')
    
    try:
        offset_minutes = int(offset_str)
        if offset_minutes < 0:
            return create_response(400, {'error': 'Offset must be a positive number'})
    except ValueError:
        return create_response(400, {'error': 'Offset must be a valid integer'})
    
    # Always use all interested event types
    event_types = ['WatchEvent', 'PullRequestEvent', 'IssuesEvent']
    
    try:
        # Get event counts
        event_counts = get_events_by_type_and_time(event_types, offset_minutes)
        
        # Calculate total
        total_events = sum(event_counts.values())
        
        # Calculate time range
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=offset_minutes)
        
        return create_response(200, {
            'time_range': {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'offset_minutes': offset_minutes
            },
            'event_counts': event_counts,
            'total_events': total_events
        })
        
    except Exception as e:
        logger.error(f"Failed to count events: {e}")
        return create_response(500, {'error': 'Failed to retrieve event counts'})

def calculate_average_time_between_pr(pr_events: List[Dict[str, Any]]) -> float:
    """Calculate average time in seconds between PR events"""
    
    # Filter for PR opened events only for accurate timing
    opened_events = [
        event for event in pr_events 
        if event.get('pr_action') == 'opened'
    ]
    
    if len(opened_events) < 2:
        # Fall back to all PR events if not enough opened events
        opened_events = pr_events
    
    if len(opened_events) < 2:
        raise ValueError("Need at least 2 PR events to calculate average")
    
    # Convert ISO timestamps to seconds for calculation
    timestamps = []
    for event in opened_events:
        dt = datetime.fromisoformat(event['created_at'].replace('Z', '+00:00'))
        timestamps.append(dt.timestamp())
    
    timestamps.sort()
    
    time_diffs = []
    for i in range(1, len(timestamps)):
        diff = timestamps[i] - timestamps[i-1]
        time_diffs.append(diff)
    
    # Return average in seconds
    return sum(time_diffs) / len(time_diffs)

def get_pr_events_for_repo(repo_name: str) -> List[Dict[str, Any]]:
    """Get PR events for a repository using GSI by event type and filtering by repo name."""
    table = dynamodb.Table(os.environ.get('PROCESSED_EVENTS_TABLE', 'processed-github-events'))
    items: List[Dict[str, Any]] = []
    try:
        last_evaluated_key = None
        while True:
            kwargs = {
                'IndexName': 'EventTypeIndex',
                'KeyConditionExpression': Key('event_type').eq('PullRequestEvent'),
                'FilterExpression': Attr('repo_name').eq(repo_name),
                'ScanIndexForward': True,
            }
            if last_evaluated_key:
                kwargs['ExclusiveStartKey'] = last_evaluated_key
            resp = table.query(**kwargs)
            for item in resp.get('Items', []):
                items.append(convert_decimals(item))
            last_evaluated_key = resp.get('LastEvaluatedKey')
            if not last_evaluated_key:
                break
        return items
    except Exception as e:
        logger.error(f"Failed to query PR events via GSI: {e}")
        raise e

def get_events_by_type_and_time(event_types: List[str], minutes_offset: int) -> Dict[str, int]:
    """Get event counts by type for the last N minutes"""
    table = dynamodb.Table(os.environ.get('PROCESSED_EVENTS_TABLE', 'processed-github-events'))
    
    # Calculate timestamp threshold as ISO string
    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=minutes_offset)
    # Match GitHub's created_at format: e.g., 2025-08-23T12:34:56Z
    cutoff_iso = cutoff_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    event_counts = {}
    
    for event_type in event_types:
        try:
            response = table.query(
                IndexName='EventTypeIndex',
                KeyConditionExpression=Key('event_type').eq(event_type) & Key('created_at').gte(cutoff_iso),
                Select='COUNT'
            )
            event_counts[event_type] = response.get('Count', 0)
        except Exception as e:
            logger.error(f"Failed to count {event_type} events: {e}")
            event_counts[event_type] = 0
    return event_counts

def list_repos_with_opened_pr(min_count: int = 1) -> List[str]:
    """Scan summary table and return repos where opened_pr_count > min_count."""
    table = dynamodb.Table(os.environ.get('REPO_PR_SUMMARY_TABLE', 'repo-pr-summary'))
    repos: List[str] = []
    last_evaluated_key = None
    while True:
        scan_kwargs = {
            'FilterExpression': Attr('opened_pr_count').gt(min_count)
        }
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
        resp = table.scan(**scan_kwargs)
        repos.extend([item['repo_name'] for item in resp.get('Items', []) if 'repo_name' in item])
        last_evaluated_key = resp.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    return repos

def handle_repos_with_pr_request(query_params: Dict[str, str]) -> Dict[str, Any]:
    """Return repositories with opened_pr_count > min_count from summary table."""
    min_count = 1
    if query_params and 'min_count' in query_params:
        try:
            min_count = int(query_params['min_count'])
        except Exception:
            pass
    try:
        repos = list_repos_with_opened_pr(min_count=min_count)
        return create_response(200, {'repositories': repos, 'min_count': min_count})
    except Exception as e:
        logger.error(f"Failed to scan RepoPRSummary: {e}")
        return create_response(500, {'error': 'Failed to retrieve repo list'})

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

def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Create standardized API response"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'GET, OPTIONS'
        },
        'body': json.dumps(body, default=str)
    }