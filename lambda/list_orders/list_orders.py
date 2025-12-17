import json
import os
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
ORDERS_TABLE = os.environ['ORDERS_TABLE']


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


def handler(event, context):
    """
    List orders for a user or by status
    
    GET /orders?user_id={userId}
    GET /orders?status={status}
    GET /orders (list all orders)
    """
    try:
        query_params = event.get('queryStringParameters') or {}
        user_id = query_params.get('user_id')
        status = query_params.get('status')
        limit = int(query_params.get('limit', 50))
        
        table = dynamodb.Table(ORDERS_TABLE)
        
        if user_id:
            # Query by user using GSI
            response = table.query(
                IndexName='UserOrdersIndex',
                KeyConditionExpression=Key('user_id').eq(user_id),
                ScanIndexForward=False,  # Most recent first
                Limit=limit
            )
        elif status:
            # Query by status using GSI
            response = table.query(
                IndexName='OrderStatusIndex',
                KeyConditionExpression=Key('status').eq(status),
                ScanIndexForward=False,
                Limit=limit
            )
        else:
            # Scan all orders (not recommended for production with large datasets)
            response = table.scan(Limit=limit)
        
        orders = response.get('Items', [])
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'orders': orders,
                'count': len(orders)
            }, cls=DecimalEncoder)
        }
        
    except ClientError as e:
        print(f"DynamoDB error: {e}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Failed to list orders'})
        }
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': 'Internal server error'})
        }