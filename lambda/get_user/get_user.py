import json
import logging
import os
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

USERS_TABLE = os.environ.get("USERS_TABLE", "users")


# Custom JSON encoder to handle Decimal types
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)


def _response(status_code: int, body: dict):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body, cls=DecimalEncoder)
    }


def _get_table():
    ddb = boto3.resource("dynamodb")
    return ddb.Table(USERS_TABLE)


def handler(event, context):
    logger.info("GetUserFunction invoked")
    logger.info("event: %s", event)

    user_id = event["pathParameters"]["userId"]
    logger.info("Fetching user_id: %s", user_id)

    table = _get_table()

    try:
        response = table.get_item(Key={"user_id": user_id})
    except ClientError as e:
        logger.exception("Error fetching user")
        return _response(500, {"error": "Failed to fetch user"})

    item = response.get("Item")
    if not item:
        logger.info("User not found: %s", user_id)
        return _response(404, {"error": "User not found"})

    logger.info("User found: %s", user_id)
    
    # Wrap the user data in a "user" object for consistency with create_user response
    return _response(200, {"user": item})