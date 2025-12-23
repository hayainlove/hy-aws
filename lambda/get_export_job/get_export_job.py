import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
import traceback

dynamodb = boto3.resource('dynamodb')

EXPORT_JOBS_TABLE = os.environ.get('EXPORT_JOBS_TABLE')

# Add validation
if not EXPORT_JOBS_TABLE:
    raise ValueError("EXPORT_JOBS_TABLE environment variable not set")

jobs_table = dynamodb.Table(EXPORT_JOBS_TABLE)


# Helper function to convert Decimal to float/int for JSON serialization
def decimal_default(obj):
    if isinstance(obj, Decimal):
        # Convert to int if it's a whole number, otherwise float
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    raise TypeError


def handler(event, context):
    """
    Get export job status and download URL if completed.
    """
    try:
        print(f"Event received: {json.dumps(event)}")
        
        # Check if listing jobs for a user
        query_params = event.get('queryStringParameters') or {}
        user_id = query_params.get('user_id')
        
        if user_id:
            return list_user_jobs(user_id)
        
        # Get specific job by ID
        path_params = event.get('pathParameters') or {}
        job_id = path_params.get('jobId')
        
        if not job_id:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Missing jobId parameter'
                })
            }
        
        print(f"Getting job: {job_id}")
        
        response = jobs_table.get_item(Key={'job_id': job_id})
        
        print(f"DynamoDB response: {json.dumps(response, default=str)}")
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Export job not found'
                })
            }
        
        job = response['Item']
        
        # Build response
        result = {
            'job_id': job['job_id'],
            'export_type': job['export_type'],
            'format': job.get('format', 'csv'),
            'status': job['status'],
            'createdAt': job['createdAt'],
            'updatedAt': job['updatedAt']
        }
        
        # Add download URL if completed
        if job['status'] == 'completed':
            result['download_url'] = job.get('download_url')
            result['record_count'] = int(job.get('record_count', 0)) if isinstance(job.get('record_count', 0), Decimal) else job.get('record_count', 0)
            result['s3_key'] = job.get('s3_key')
        
        # Add error message if failed
        if job['status'] == 'failed':
            result['error_message'] = job.get('error_message', 'Unknown error')
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(result, default=decimal_default)
        }
        
    except Exception as e:
        error_msg = f"Error getting export job: {str(e)}"
        print(error_msg)
        print(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Failed to get export job',
                'details': str(e),
                'traceback': traceback.format_exc()
            })
        }


def list_user_jobs(user_id):
    """List all export jobs for a specific user"""
    try:
        response = jobs_table.query(
            IndexName='UserJobsIndex',
            KeyConditionExpression=Key('user_id').eq(user_id),
            ScanIndexForward=False,
            Limit=50
        )
        
        jobs = response.get('Items', [])
        
        simplified_jobs = []
        for job in jobs:
            simplified_job = {
                'job_id': job['job_id'],
                'export_type': job['export_type'],
                'status': job['status'],
                'createdAt': job['createdAt']
            }
            
            if job['status'] == 'completed':
                simplified_job['download_url'] = job.get('download_url')
                simplified_job['record_count'] = int(job.get('record_count', 0)) if isinstance(job.get('record_count', 0), Decimal) else job.get('record_count', 0)
            
            simplified_jobs.append(simplified_job)
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'jobs': simplified_jobs,
                'count': len(simplified_jobs)
            }, default=decimal_default)
        }
        
    except Exception as e:
        print(f"Error listing user jobs: {str(e)}")
        print(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Failed to list export jobs',
                'details': str(e)
            })
        }