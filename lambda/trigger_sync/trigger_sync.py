import json
import os
import boto3
from datetime import datetime

stepfunctions = boto3.client('stepfunctions')

STATE_MACHINE_ARN = os.environ['STATE_MACHINE_ARN']


def handler(event, context):
    """
    Trigger Step Functions state machine for 3rd party sync
    
    Query parameters:
    - resource: Type of resource to sync (posts, users, comments, todos, albums)
    - limit: Number of items to sync (default: 10)
    
    Example:
    POST /integrations/sync-with-retry?resource=posts&limit=20
    """
    try:
        # Get query parameters
        query_params = event.get('queryStringParameters') or {}
        resource_type = query_params.get('resource', 'posts')
        limit = int(query_params.get('limit', 10))
        
        # Validate
        valid_resources = ['posts', 'users', 'comments', 'todos', 'albums']
        if resource_type not in valid_resources:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': f'Invalid resource type. Must be one of: {", ".join(valid_resources)}'
                })
            }
        
        if limit < 1 or limit > 100:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Limit must be between 1 and 100'
                })
            }
        
        # Prepare input for Step Functions
        execution_input = {
            "resource_type": resource_type,
            "limit": limit,
            "attempt": 1  # Initial attempt
        }
        
        # Generate execution name with timestamp
        execution_name = f"sync-{resource_type}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        print(f"Starting Step Functions execution: {execution_name}")
        print(f"Input: {json.dumps(execution_input)}")
        
        # Start Step Functions execution
        response = stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=execution_name,
            input=json.dumps(execution_input)
        )
        
        execution_arn = response['executionArn']
        print(f"✓ Started execution: {execution_arn}")
        
        return {
            'statusCode': 202,  # Accepted
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': 'Third party sync started with retry mechanism',
                'execution_arn': execution_arn,
                'execution_name': execution_name,
                'resource_type': resource_type,
                'limit': limit,
                'status': 'RUNNING',
                'note': 'The sync will retry up to 3 times on failure with exponential backoff'
            })
        }
        
    except ValueError as e:
        print(f"✗ Validation error: {str(e)}")
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Invalid input',
                'details': str(e)
            })
        }
        
    except Exception as e:
        print(f"✗ Error triggering sync: {str(e)}")
        import traceback
        print(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Failed to trigger sync',
                'details': str(e)
            })
        }