import json
import os
import boto3
from datetime import datetime, timezone
import uuid
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

EXPORT_JOBS_TABLE = os.environ['EXPORT_JOBS_TABLE']
EXPORT_JOBS_QUEUE_URL = os.environ['EXPORT_JOBS_QUEUE_URL']

jobs_table = dynamodb.Table(EXPORT_JOBS_TABLE)


def handler(event, context):
    """
    Create an export job and queue it for processing.
    
    Request body:
    {
        "export_type": "users" | "orders",
        "filters": {
            "status": "active",  // optional filters
            "start_date": "2024-01-01",
            "end_date": "2024-12-31"
        },
        "format": "csv" | "json"  // optional, defaults to csv
    }
    """
    try:
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        
        # Extract parameters
        export_type = body.get('export_type')
        filters = body.get('filters', {})
        export_format = body.get('format', 'csv')
        user_id = body.get('user_id')  # Optional: track which user requested
        
        # Validate export_type
        if export_type not in ['users', 'orders']:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': 'Invalid export_type. Must be "users" or "orders"'
                })
            }
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc).isoformat()
        
        # Calculate TTL (30 days from now)
        ttl = int(datetime.now(timezone.utc).timestamp()) + (30 * 24 * 60 * 60)
        
        # Create job record
        job_item = {
            'job_id': job_id,
            'export_type': export_type,
            'format': export_format,
            'filters': filters,
            'status': 'pending',
            'createdAt': timestamp,
            'updatedAt': timestamp,
            'ttl': ttl
        }
        
        if user_id:
            job_item['user_id'] = user_id
        
        # Save to DynamoDB
        jobs_table.put_item(Item=job_item)
        
        # Send message to SQS for processing
        message_body = {
            'job_id': job_id,
            'export_type': export_type,
            'format': export_format,
            'filters': filters
        }
        
        sqs.send_message(
            QueueUrl=EXPORT_JOBS_QUEUE_URL,
            MessageBody=json.dumps(message_body, default=str)
        )
        
        return {
            'statusCode': 202,  # Accepted
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': 'Export job created successfully',
                'job_id': job_id,
                'status': 'pending',
                'export_type': export_type,
                'createdAt': timestamp
            })
        }
        
    except Exception as e:
        print(f"Error creating export job: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Failed to create export job',
                'details': str(e)
            })
        }