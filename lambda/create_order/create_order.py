import json
import boto3
import os
from datetime import datetime
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
orders_table = dynamodb.Table(os.environ['ORDERS_TABLE'])
sqs = boto3.client('sqs')
queue_url = os.environ['ORDERS_QUEUE_URL']

def convert_floats_to_decimal(obj):
    """Recursively convert floats to Decimal for DynamoDB"""
    if isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_floats_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))
    else:
        return obj

def handler(event, context):
    try:
        body = json.loads(event['body'])
        
        # Validate required fields
        if 'user_id' not in body:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'user_id is required'})
            }
        
        if 'items' not in body or not body['items']:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'items are required'})
            }
        
        if 'total' not in body:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'total is required'})
            }
        
        # Generate order ID and timestamps
        import uuid
        order_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()
        
        # Convert all floats to Decimal for DynamoDB
        order_data = {
            'order_id': order_id,
            'user_id': body['user_id'],
            'items': convert_floats_to_decimal(body['items']),
            'total': Decimal(str(body['total'])),
            'status': body.get('status', 'PENDING').upper(),
            'createdAt': timestamp,
            'updatedAt': timestamp
        }
        
        # Save to DynamoDB
        orders_table.put_item(Item=order_data)
        print(f"Order created in DynamoDB: {order_id}")
        
        # Send to SQS for processing
        sqs_response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({
                'order_id': order_id,
                'user_id': body['user_id'],
                'status': order_data['status']
            }, default=str)
        )
        print(f"Message sent to SQS: {sqs_response['MessageId']}")
        
        # Convert Decimal back to float for JSON response
        response_data = json.loads(json.dumps(order_data, default=str))
        
        return {
            'statusCode': 201,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': 'Order created successfully',
                'order': response_data
            })
        }
        
    except Exception as e:
        print(f"Error creating order: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': 'Internal server error'})
        }