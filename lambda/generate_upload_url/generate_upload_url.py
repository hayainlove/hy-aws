import json
import os
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

BUCKET_NAME = os.environ['USER_FILES_BUCKET']
USERS_TABLE = os.environ['USERS_TABLE']

def handler(event, context):
    """
    Generate a presigned URL for uploading a file to S3
    
    POST /users/{userId}/files/upload-url
    Body: {
        "fileName": "profile.jpg",
        "fileType": "profilePicture",
        "contentType": "image/jpeg"
    }
    """
    try:
        # Get user ID from path parameters
        user_id = event['pathParameters']['userId']
        
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        file_name = body.get('fileName')
        file_type = body.get('fileType', 'general')
        content_type = body.get('contentType', 'application/octet-stream')
        
        if not file_name:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'fileName is required'})
            }
        
        # Check if user exists
        table = dynamodb.Table(USERS_TABLE)
        try:
            response = table.get_item(Key={'user_id': user_id})
            if 'Item' not in response:
                return {
                    'statusCode': 404,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({'error': 'User not found'})
                }
            user_item = response['Item']
        except ClientError as e:
            print(f"Error checking user: {e}")
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Error checking user'})
            }
        
        # Generate S3 key (path in bucket)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f"users/{user_id}/{file_type}/{timestamp}_{file_name}"
        
        # Generate presigned URL for upload (valid for 15 minutes)
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': BUCKET_NAME,
                'Key': s3_key,
                'ContentType': content_type
            },
            ExpiresIn=900
        )
        
        # Update DynamoDB with file reference
        try:
            # Check if 'files' attribute exists, if not create it first
            if 'files' not in user_item:
                table.update_item(
                    Key={'user_id': user_id},
                    UpdateExpression="SET files = :empty_map",
                    ExpressionAttributeValues={':empty_map': {}}
                )
                print(f"Created 'files' attribute for user: {user_id}")
            
            # Now update with the file reference
            update_expression = "SET files.#fileType = :s3_key, updatedAt = :now"
            expression_attribute_names = {
                '#fileType': file_type
            }
            expression_attribute_values = {
                ':s3_key': s3_key,
                ':now': datetime.now().isoformat()
            }
            
            table.update_item(
                Key={'user_id': user_id},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
            print(f"Successfully updated DynamoDB: {file_type} -> {s3_key}")
            
        except ClientError as e:
            print(f"Error updating DynamoDB: {e}")
            # Continue anyway - file can still be uploaded
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'uploadUrl': presigned_url,
                's3Key': s3_key,
                'fileType': file_type,
                'expiresIn': 900
            })
        }
        
    except Exception as e:
        print(f"Error generating upload URL: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Internal server error'})
        }