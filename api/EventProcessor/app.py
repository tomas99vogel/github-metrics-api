# lambda/github_event_processor.py
import json
import os
import boto3
try:
    from botocore.exceptions import ClientError  # type: ignore
except Exception:  
    class ClientError(Exception):  # minimal shim for local linting
        def __init__(self, response=None, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.response = response or {}
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Process GitHub events from SQS"""
    logger.info(f"Processing {len(event.get('Records', []))} SQS records")
    
    processed_events = []
    failed_events = []
    
    for record in event.get('Records', []):
        try:
            # Parse the GitHub event from SQS message
            github_event = json.loads(record['body'])
            logger.info(f"Processing event: {github_event['type']} from {github_event.get('repo', {}).get('name', 'unknown')}")
            
            # Process based on event type
            result = process_github_event(github_event)
            processed_events.append(result)
            
        except Exception as e:
            logger.error(f"Failed to process record: {e}")
            failed_events.append({
                'record_id': record.get('messageId'),
                'error': str(e)
            })
    
    logger.info(f"Successfully processed: {len(processed_events)}, Failed: {len(failed_events)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': len(processed_events),
            'failed': len(failed_events)
        })
    }

def process_github_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single GitHub event based on its type"""
    event_type = event['type']
    repo_info = event.get('repo', {})
    
    # Extract only essential data for metrics calculation
    processed_event = {
        'id': event['id'],
        'type': event_type,
        'repo_name': repo_info.get('name', 'unknown'),
        'repo_id': repo_info.get('id'),
        'actor_login': event.get('actor', {}).get('login', 'unknown'),
        'created_at': event.get('created_at'),
        'processed_at': datetime.now(timezone.utc).isoformat()
    }
    
    # Type-specific processing - only capture data needed for metrics
    if event_type == 'PullRequestEvent':
        processed_event.update(process_pull_request_event(event))
    elif event_type == 'IssuesEvent':
        processed_event.update(process_issues_event(event))
    elif event_type == 'WatchEvent':
        processed_event.update(process_watch_event(event))
    else:
        # This shouldn't happen as it's filtered in the poller
        logger.warning(f"Unexpected event type: {event_type}")
    
    # Store in DynamoDB
    store_processed_event(processed_event)
    
    return processed_event

def process_pull_request_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process PullRequestEvent - capture only data needed for PR metrics"""
    payload = event.get('payload', {})
    
    # Only store action for filtering opened PRs in statistics
    return {
        'pr_action': payload.get('action')
    }

def process_issues_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process IssuesEvent - minimal data for event counting"""
    payload = event.get('payload', {})
    
    # Minimal event data
    return {
        'action': payload.get('action')
    }

def process_watch_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process WatchEvent - minimal data for event counting"""
    payload = event.get('payload', {})
    
    # Minimal event data 
    return {
        'action': payload.get('action') 
    }

def store_processed_event(processed_event: Dict[str, Any]) -> bool:
    # If this is a PullRequestEvent with action 'opened', increment summary table
    """Store processed event in DynamoDB idempotently.
    Returns True if the item was inserted, False if it already existed (duplicate).
    """

    table = dynamodb.Table(os.environ.get('PROCESSED_EVENTS_TABLE', 'processed-github-events'))

    # Use event id as the partition key for uniqueness
    event_id = processed_event['id']
    repo_name = processed_event['repo_name']
    event_type = processed_event['type']
    created_at = processed_event['created_at']

    item = {
        'pk': f"event#{event_id}", 
        'created_at': created_at,
        'event_type': event_type,
        'repo_name': repo_name,
        'actor_login': processed_event['actor_login'],
        'processed_at': processed_event['processed_at'],
        'id': event_id
    }

    if event_type == 'PullRequestEvent':
        item['pr_action'] = processed_event.get('pr_action')
    elif event_type == 'IssuesEvent':
        item['action'] = processed_event.get('action')
    elif event_type == 'WatchEvent':
        item['action'] = processed_event.get('action')

    # Only insert if event id does not exist
    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(pk)",
        )
        logger.info(f"Stored {event_type} event {event_id} for repo {repo_name}")

        # After successful insert, update summary counter for PR opened events
        if event_type == 'PullRequestEvent' and processed_event.get('pr_action') == 'opened':
            try:
                summary_table_name = os.environ.get('REPO_PR_SUMMARY_TABLE', 'repo-pr-summary')
                summary_table = dynamodb.Table(summary_table_name)
                summary_table.update_item(
                    Key={'repo_name': repo_name},
                    UpdateExpression='ADD opened_pr_count :inc',
                    ExpressionAttributeValues={':inc': 1},
                )
            except Exception as e:
                # Don't fail the whole processing on summary update issues
                logger.warning(f"Failed to update RepoPRSummary for {repo_name}: {e}")
        return True
    
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
            logger.info(
                f"Duplicate detected, skipping store for event {event_id} (repo={repo_name}, type={event_type}, created_at={created_at})"
            )
            return False
        raise
