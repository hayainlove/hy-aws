import json
import os
import boto3
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')

USERS_TABLE = os.environ['USERS_TABLE']

users_table = dynamodb.Table(USERS_TABLE)


def handler(event, context):
    """
    Cognito Post-Confirmation Trigger
    Automatically creates a DynamoDB user record when user confirms signup
    
    Event structure from Cognito:
    {
        "version": "1",
        "region": "us-east-1",
        "userPoolId": "us-east-1_XXXXX",
        "userName": "user@example.com",
        "request": {
            "userAttributes": {
                "sub": "abc-123-def-456",
                "email": "user@example.com",
                "given_name": "John",
                "family_name": "Doe",
                "phone_number": "+1234567890"
            }
        },
        "response": {}
    }
    """
    try:
        print(f"Post-confirmation event: {json.dumps(event, default=str)}")
        
        # Extract user attributes from Cognito event
        user_attributes = event['request']['userAttributes']
        
        # Cognito user ID (this will be our user_id in DynamoDB)
        cognito_user_id = user_attributes['sub']
        email = user_attributes.get('email', '')
        given_name = user_attributes.get('given_name', '')
        family_name = user_attributes.get('family_name', '')
        phone_number = user_attributes.get('phone_number', '')
        
        # Create username from email (before @)
        username = email.split('@')[0] if email else cognito_user_id
        
        # Full name
        full_name = f"{given_name} {family_name}".strip()
        if not full_name:
            full_name = username
        
        # Current timestamp
        timestamp = datetime.now(timezone.utc).isoformat()
        
        # Create user record in DynamoDB
        user_item = {
            'user_id': cognito_user_id,  # Use Cognito user ID as primary key
            'userName': username,
            'email': email,
            'fullName': full_name,
            'phoneNumber': phone_number,
            'accountStatus': 'active',
            'createdAt': timestamp,
            'updatedAt': timestamp,
            'authProvider': 'cognito',
            'cognitoUsername': event['userName']
        }
        
        print(f"Creating DynamoDB user record: {json.dumps(user_item, default=str)}")
        
        # Save to DynamoDB
        users_table.put_item(Item=user_item)
        
        print(f"✓ Successfully created user record for: {email} (ID: {cognito_user_id})")
        
        # IMPORTANT: Must return the event unchanged for Cognito
        return event
        
    except Exception as e:
        print(f"✗ Error in post-confirmation trigger: {str(e)}")
        import traceback
        print(traceback.format_exc())
        
        # IMPORTANT: Return event even on error to not block user signup
        # The user can still sign in, but won't have a DynamoDB record
        # You can monitor CloudWatch logs for these errors
        return event