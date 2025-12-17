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
    Delete a file from S3 and remove reference from DynamoDB
    
    DELETE /users/{userId}/files/{fileType}
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
        
        # Delete file from S3
        try:
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=s3_key)
            print(f"Deleted file from S3: {s3_key}")
        except ClientError as e:
            print(f"Error deleting from S3: {e}")
            # Continue to remove from DynamoDB anyway
        
        # Remove file reference from DynamoDB
        try:
            table.update_item(
                Key={'user_id': user_id},
                UpdateExpression=f"REMOVE files.#fileType",
                ExpressionAttributeNames={'#fileType': file_type}
            )
        except ClientError as e:
            print(f"Error updating DynamoDB: {e}")
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Error removing file reference'})
            }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': f'File "{file_type}" deleted successfully',
                's3Key': s3_key
            })
        }
        
    except Exception as e:
        print(f"Error deleting file: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Internal server error'})
        }