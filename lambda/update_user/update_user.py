import json
import os
import boto3
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["USERS_TABLE"])


# Custom JSON encoder to handle Decimal types
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)


def handler(event, context):
    """
    Update user information
    Allowed fields: email, userName, firstName, lastName, fullName, phone, address, age, location, status
    """
    try:
        user_id = event["pathParameters"]["userId"]
        body = json.loads(event.get("body") or "{}")
        
        # Define allowed updatable fields
        # Map field names to placeholders (for reserved keywords)
        allowed_fields = {
            "email": {"value_placeholder": ":e", "name_placeholder": None},
            "userName": {"value_placeholder": ":u", "name_placeholder": None},
            "firstName": {"value_placeholder": ":fn", "name_placeholder": None},
            "lastName": {"value_placeholder": ":ln", "name_placeholder": None},
            "fullName": {"value_placeholder": ":full", "name_placeholder": None},
            "phone": {"value_placeholder": ":p", "name_placeholder": None},
            "address": {"value_placeholder": ":addr", "name_placeholder": None},
            "age": {"value_placeholder": ":a", "name_placeholder": None},
            "location": {"value_placeholder": ":l", "name_placeholder": "#loc"},  # Reserved keyword
            "status": {"value_placeholder": ":s", "name_placeholder": "#stat"}  # Reserved keyword
        }
        
        # Build update expression
        update_expr_parts = []
        expr_attr_vals = {}
        expr_attr_names = {}
        
        for field, placeholders in allowed_fields.items():
            if field in body:
                value_placeholder = placeholders["value_placeholder"]
                name_placeholder = placeholders["name_placeholder"]
                
                # Use name placeholder if it's a reserved keyword
                field_name = name_placeholder if name_placeholder else field
                
                # Add to expression attribute names if needed
                if name_placeholder:
                    expr_attr_names[name_placeholder] = field
                
                # Special handling for age - convert to int
                if field == "age":
                    try:
                        value = int(body[field])
                        update_expr_parts.append(f"{field_name} = {value_placeholder}")
                        expr_attr_vals[value_placeholder] = value
                    except (ValueError, TypeError):
                        return {
                            "statusCode": 400,
                            "headers": {
                                "Content-Type": "application/json",
                                "Access-Control-Allow-Origin": "*"
                            },
                            "body": json.dumps({"error": f"Invalid value for {field}"})
                        }
                else:
                    update_expr_parts.append(f"{field_name} = {value_placeholder}")
                    expr_attr_vals[value_placeholder] = body[field]
        
        if not update_expr_parts:
            return {
                "statusCode": 400,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"error": "No updatable fields provided"})
            }
        
        # Add updatedAt timestamp
        update_expr_parts.append("updatedAt = :updated")
        expr_attr_vals[":updated"] = datetime.now().isoformat()
        
        # Build final update expression
        update_expr = "SET " + ", ".join(update_expr_parts)
        
        # Build update parameters
        update_params = {
            "Key": {"user_id": user_id},
            "UpdateExpression": update_expr,
            "ExpressionAttributeValues": expr_attr_vals,
            "ReturnValues": "ALL_NEW"
        }
        
        # Add ExpressionAttributeNames only if needed
        if expr_attr_names:
            update_params["ExpressionAttributeNames"] = expr_attr_names
        
        # Update the item
        resp = table.update_item(**update_params)
        
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"user": resp["Attributes"]}, cls=DecimalEncoder)
        }
        
    except ClientError as e:
        print(f"DynamoDB error: {e}")
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": "Failed to update user"})
        }
    except Exception as e:
        print(f"Error updating user: {e}")
        import traceback
        traceback.print_exc()
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": "Internal server error"})
        }