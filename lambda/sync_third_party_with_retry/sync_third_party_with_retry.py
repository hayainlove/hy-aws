import json
import os
import boto3
import requests
from datetime import datetime, timezone
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

THIRD_PARTY_DATA_TABLE = os.environ['THIRD_PARTY_DATA_TABLE']
ALARM_TOPIC_ARN = os.environ.get('ALARM_TOPIC_ARN')

third_party_table = dynamodb.Table(THIRD_PARTY_DATA_TABLE)


class ThirdPartyAPIError(Exception):
    """Custom exception for 3rd party API failures that should trigger retry"""
    pass


def handler(event, context):
    """
    Sync data from 3rd party API with error handling for Step Functions
    
    Input event from Step Functions:
    {
        "resource_type": "posts",
        "limit": 10,
        "attempt": 1
    }
    
    Returns:
    {
        "statusCode": 200,
        "success": true/false,
        "message": "...",
        "synced_count": 10,
        "attempt": 1,
        "should_retry": true/false
    }
    """
    try:
        # Extract parameters
        resource_type = event.get('resource_type', 'posts')
        limit = event.get('limit', 10)
        attempt = event.get('attempt', 1)
        
        print(f"[Attempt {attempt}] Syncing {resource_type} from JSONPlaceholder (limit: {limit})")
        
        # Validate resource type
        valid_resources = ['posts', 'users', 'comments', 'todos', 'albums']
        if resource_type not in valid_resources:
            raise ValueError(f'Invalid resource type: {resource_type}. Must be one of: {", ".join(valid_resources)}')
        
        # Call 3rd party API with timeout
        api_url = f'https://jsonplaceholder.typicode.com/{resource_type}'
        print(f"Calling API: {api_url}")
        
        try:
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise ThirdPartyAPIError(f"API request timed out after 10 seconds")
        except requests.exceptions.ConnectionError:
            raise ThirdPartyAPIError(f"Failed to connect to API")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Rate limit
                raise ThirdPartyAPIError(f"API rate limit exceeded")
            elif e.response.status_code >= 500:  # Server error
                raise ThirdPartyAPIError(f"API server error: {e.response.status_code}")
            else:
                raise ThirdPartyAPIError(f"API request failed: {str(e)}")
        except requests.exceptions.RequestException as e:
            raise ThirdPartyAPIError(f"API request failed: {str(e)}")
        
        items = response.json()
        items_to_sync = items[:limit]
        
        print(f"Received {len(items)} items, syncing {len(items_to_sync)}")
        
        # Save to DynamoDB
        synced_count = 0
        timestamp = datetime.now(timezone.utc).isoformat()
        
        for item in items_to_sync:
            item_id = f"{resource_type}_{item.get('id', synced_count)}"
            
            dynamo_item = {
                'item_id': item_id,
                'source': 'jsonplaceholder',
                'resource_type': resource_type,
                'synced_at': timestamp,
                'data': json.dumps(item),
                'sync_attempt': attempt
            }
            
            # Add searchable fields
            if 'title' in item:
                dynamo_item['title'] = item['title']
            if 'name' in item:
                dynamo_item['name'] = item['name']
            if 'email' in item:
                dynamo_item['email'] = item['email']
            
            third_party_table.put_item(Item=dynamo_item)
            synced_count += 1
        
        print(f"✓ Successfully synced {synced_count} items on attempt {attempt}")
        
        # Return success response for Step Functions
        return {
            'statusCode': 200,
            'success': True,
            'message': f'Successfully synced {synced_count} {resource_type}',
            'resource_type': resource_type,
            'synced_count': synced_count,
            'attempt': attempt,
            'synced_at': timestamp,
            'should_retry': False
        }
        
    except ThirdPartyAPIError as e:
        # API-specific error - should trigger retry
        error_message = str(e)
        print(f"✗ [Attempt {attempt}] Third party API error: {error_message}")
        
        # Return error response for Step Functions to handle retry
        return {
            'statusCode': 500,
            'success': False,
            'error': 'ThirdPartyAPIError',
            'message': error_message,
            'attempt': attempt,
            'resource_type': event.get('resource_type'),
            'should_retry': True  # Tell Step Functions to retry
        }
        
    except ValueError as e:
        # Validation error - should NOT retry
        error_message = str(e)
        print(f"✗ [Attempt {attempt}] Validation error: {error_message}")
        
        return {
            'statusCode': 400,
            'success': False,
            'error': 'ValidationError',
            'message': error_message,
            'attempt': attempt,
            'resource_type': event.get('resource_type'),
            'should_retry': False  # Don't retry validation errors
        }
        
    except Exception as e:
        # Unexpected error - log and notify
        error_message = str(e)
        print(f"✗ [Attempt {attempt}] Unexpected error: {error_message}")
        import traceback
        print(traceback.format_exc())
        
        # Send alert for unexpected errors after final attempt
        if ALARM_TOPIC_ARN and attempt >= 3:
            try:
                sns.publish(
                    TopicArn=ALARM_TOPIC_ARN,
                    Subject=f'⚠️ Third Party Sync Failed After {attempt} Attempts',
                    Message=f"""
Third Party Sync Failed
=======================

Resource Type: {event.get("resource_type", "unknown")}
Attempts: {attempt}
Error: {error_message}

Please check CloudWatch logs for details.

Execution Context:
{json.dumps(event, indent=2, default=str)}
                    """.strip()
                )
                print("✓ Sent SNS alert notification")
            except Exception as sns_error:
                print(f"✗ Failed to send SNS notification: {sns_error}")
        
        return {
            'statusCode': 500,
            'success': False,
            'error': 'UnexpectedError',
            'message': error_message,
            'attempt': attempt,
            'resource_type': event.get('resource_type'),
            'should_retry': False  # Don't retry unexpected errors
        }