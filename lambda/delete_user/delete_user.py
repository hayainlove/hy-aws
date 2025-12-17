import json
import os
import boto3

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["USERS_TABLE"])

def handler(event, context):
    user_id = event["pathParameters"]["userId"]

    # Check if user exists
    resp = table.get_item(Key={"user_id": user_id})
    if "Item" not in resp:
        return {
            "statusCode": 404,
            "body": json.dumps({"error": "User not found"})
        }

    # Delete the user
    table.delete_item(Key={"user_id": user_id})

    # Notify successful delete
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"User {user_id} deleted successfully"
        })
    }
