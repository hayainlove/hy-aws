import json
import logging
import os
import uuid
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

from mylib import helpers as myhelpers

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
USERS_TABLE = os.environ.get("USERS_TABLE", "users")
USER_NOTIFICATION_TOPIC_ARN = os.environ.get("USER_NOTIFICATION_TOPIC_ARN")

# Initialize SNS client
sns_client = boto3.client('sns')


def _response(status_code: int, body: Dict[str, Any]):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }


def _get_table():
    ddb = boto3.resource("dynamodb")
    return ddb.Table(USERS_TABLE)


def _exists_by_index(table, index_name: str, key_name: str, key_value) -> bool:
    try:
        resp = table.query(IndexName=index_name, KeyConditionExpression=Key(key_name).eq(key_value), Limit=1)
        return resp.get("Count", 0) > 0
    except ClientError:
        logger.exception("Error querying GSI %s", index_name)
        raise


def _send_welcome_email(user: Dict[str, Any]) -> bool:
    """
    Send welcome email to new user via SNS
    Returns True if successful, False otherwise
    """
    if not USER_NOTIFICATION_TOPIC_ARN:
        logger.warning("USER_NOTIFICATION_TOPIC_ARN not configured, skipping email")
        return False
    
    try:
        first_name = user.get('firstName', 'User')
        
        # Create personalized welcome message
        message = f"""Hello {first_name}!

Welcome to MyHayati! 🎉

Your account has been successfully created. Here are your account details:

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Account Information
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Username:     {user.get('userName')}
Email:        {user.get('email')}
User ID:      {user.get('user_id')}
Created:      {user.get('createdAt')}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

You can now start using all MyHayati features:
✓ Browse and purchase products
✓ Track your orders
✓ Manage your profile
✓ Upload documents

If you have any questions, please don't hesitate to contact our support team.

Thank you for choosing MyHayati!

Best regards,
The MyHayati Team

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
This is an automated message. Please do not reply to this email.
"""
        
        # Publish to SNS topic
        response = sns_client.publish(
            TopicArn=USER_NOTIFICATION_TOPIC_ARN,
            Subject=f"Welcome to MyHayati, {first_name}! 🎉",
            Message=message
        )
        
        logger.info(
            "Welcome email sent successfully. MessageId: %s, User: %s",
            response.get('MessageId'),
            user.get('user_id')
        )
        return True
        
    except ClientError as e:
        logger.error(
            "Failed to send welcome email via SNS: %s, User: %s",
            str(e),
            user.get('user_id')
        )
        # Don't fail user creation if email fails
        return False
    except Exception as e:
        logger.error(
            "Unexpected error sending welcome email: %s, User: %s",
            str(e),
            user.get('user_id')
        )
        return False


def handler(event, context):
    logger.info("CreateUserFunction invoked")
    logger.info("event: %s", event)

    # Support API Gateway proxy (event['body']) or direct JSON invocation
    try:
        if isinstance(event, dict) and "body" in event and isinstance(event["body"], str):
            payload = json.loads(event["body"])
            logger.info("Parsed payload from body: %s", payload)
        elif isinstance(event, dict):
            payload = event
            logger.info("Using event directly as payload: %s", payload)
        else:
            payload = json.loads(event)
            logger.info("Parsed event as JSON: %s", payload)
    except Exception as e:
        logger.error("Error parsing payload: %s", str(e))
        return _response(400, {"error": "Invalid JSON payload"})

    email = payload.get("email")
    userName = payload.get("userName")

    if not email or not userName:
        logger.error("Missing required fields. email: %s, userName: %s", email, userName)
        return _response(400, {"error": "Missing required fields: email and userName"})

    logger.info("Validating email: %s", email)
    if not myhelpers.is_valid_email(email):
        logger.error("Invalid email format: %s", email)
        return _response(400, {"error": "Invalid email format"})

    userName = myhelpers.sanitize_username(userName)
    logger.info("Sanitized userName: %s", userName)

    logger.info("Getting DynamoDB table: %s", USERS_TABLE)
    table = _get_table()

    # Check uniqueness using GSIs
    try:
        logger.info("Checking if email exists: %s", email)
        if _exists_by_index(table, "EmailIndex", "email", email):
            logger.info("Email already exists: %s", email)
            return _response(409, {"error": "Email already exists"})
        
        logger.info("Checking if userName exists: %s", userName)
        if _exists_by_index(table, "UserNameIndex", "userName", userName):
            logger.info("UserName already exists: %s", userName)
            return _response(409, {"error": "userName already exists"})
    except ClientError as e:
        logger.error("Error checking uniqueness: %s", str(e))
        return _response(500, {"error": "Error checking uniqueness"})

    user_id = str(uuid.uuid4())
    now = myhelpers.now_iso()

    # Build item with required fields
    item = {
        "user_id": user_id,
        "email": email,
        "userName": userName,
        "createdAt": now,
        "updatedAt": now,
        "status": "ACTIVE",
    }
    
    # Add optional fields if provided
    if "firstName" in payload and payload["firstName"]:
        item["firstName"] = payload["firstName"]
    
    if "lastName" in payload and payload["lastName"]:
        item["lastName"] = payload["lastName"]
    
    if "fullName" in payload and payload["fullName"]:
        item["fullName"] = payload["fullName"]
    
    if "phone" in payload and payload["phone"]:
        item["phone"] = payload["phone"]
    
    if "address" in payload and payload["address"]:
        item["address"] = payload["address"]
    
    if "age" in payload and payload["age"] is not None:
        try:
            item["age"] = int(payload["age"])
        except (ValueError, TypeError):
            logger.warning("Invalid age value: %s", payload["age"])
    
    if "location" in payload and payload["location"]:
        item["location"] = payload["location"]

    logger.info("About to write to DynamoDB table: %s", USERS_TABLE)
    logger.info("Item to write: %s", item)

    try:
        table.put_item(Item=item)
        logger.info("Successfully wrote to DynamoDB")
    except ClientError as e:
        logger.error("DynamoDB write error: %s", str(e))
        logger.exception("Full exception details:")
        return _response(500, {"error": "Failed to create user"})

    logger.info("User created successfully: %s", user_id)
    
    # Send welcome email (non-blocking - won't fail user creation if it fails)
    email_sent = _send_welcome_email(item)
    
    # Add email status to response (optional)
    response_body = {
        "user": item,
        "emailSent": email_sent
    }
    
    return _response(201, response_body)