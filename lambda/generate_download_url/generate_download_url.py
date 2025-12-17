import json
import os
import boto3
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

BUCKET_NAME = os.environ['USER_FILES_BUCKET']
USERS_TABLE = os.environ['USERS_TABLE']

def handler(event, context):
    """
    Generate a presigned URL for downloading a file from S3
    
    GET /users/{userId}/files/{fileType}
    """
    try:
        # Get parameters
        user_id = event['pathParameters']['userId']
        file_type = event['pathParameters']['fileType']
        
        # Get user and file info from DynamoDB
        table = dynamodb.Table(USERS_TABLE)
        try:
            response = table.get_item(Key={'user_id': user_id})
            if 'Item' not in response:
                return {
                    'statusCode': 404,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({'error': 'User not found'})
                }
            
            user = response['Item']
            
            # Get S3 key from user's files map
            if 'files' not in user or file_type not in user['files']:
                return {
                    'statusCode': 404,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({'error': f'File type "{file_type}" not found for this user'})
                }
            
            s3_key = user['files'][file_type]
            
        except ClientError as e:
            print(f"Error getting user: {e}")
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Error retrieving user data'})
            }
        
        # Check if file exists in S3
        try:
            s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return {
                    'statusCode': 404,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({'error': 'File not found in storage'})
                }
            raise
        
        # Generate presigned URL for download (valid for 1 hour)
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': BUCKET_NAME,
                'Key': s3_key
            },
            ExpiresIn=3600  # 1 hour
        )
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'downloadUrl': presigned_url,
                's3Key': s3_key,
                'fileType': file_type,
                'expiresIn': 3600
            })
        }
        
    except Exception as e:
        print(f"Error generating download URL: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Internal server error'})
        }