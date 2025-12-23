import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')

THIRD_PARTY_DATA_TABLE = os.environ['THIRD_PARTY_DATA_TABLE']

third_party_table = dynamodb.Table(THIRD_PARTY_DATA_TABLE)


def decimal_default(obj):
    """Convert Decimal to int/float for JSON serialization"""
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    raise TypeError


def handler(event, context):
    """
    Query synced 3rd party data
    
    Query parameters:
    - source: Filter by source (e.g., 'jsonplaceholder')
    - resource_type: Filter by resource type (e.g., 'posts', 'users')
    - limit: Number of items to return (default: 50)
    """
    try:
        # Get query parameters
        query_params = event.get('queryStringParameters') or {}
        source = query_params.get('source')
        resource_type = query_params.get('resource_type')
        limit = int(query_params.get('limit', 50))
        
        print(f"Querying 3rd party data: source={source}, resource_type={resource_type}, limit={limit}")
        
        # Query by source using GSI
        if source:
            response = third_party_table.query(
                IndexName='SourceIndex',
                KeyConditionExpression=Key('source').eq(source),
                ScanIndexForward=False,  # Most recent first
                Limit=limit
            )
        else:
            # Scan if no source specified
            response = third_party_table.scan(Limit=limit)
        
        items = response.get('Items', [])
        
        # Filter by resource_type if specified
        if resource_type:
            items = [item for item in items if item.get('resource_type') == resource_type]
        
        # Parse the JSON data field
        processed_items = []
        for item in items:
            processed_item = {
                'item_id': item['item_id'],
                'source': item['source'],
                'resource_type': item['resource_type'],
                'synced_at': item['synced_at'],
            }
            
            # Parse the data JSON string
            if 'data' in item:
                try:
                    processed_item['data'] = json.loads(item['data'])
                except:
                    processed_item['data'] = item['data']
            
            # Include any additional fields
            if 'title' in item:
                processed_item['title'] = item['title']
            if 'name' in item:
                processed_item['name'] = item['name']
            if 'email' in item:
                processed_item['email'] = item['email']
            
            processed_items.append(processed_item)
        
        print(f"Returning {len(processed_items)} items")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'count': len(processed_items),
                'items': processed_items
            }, default=decimal_default)
        }
        
    except Exception as e:
        print(f"Error querying 3rd party data: {str(e)}")
        import traceback
        print(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Failed to query 3rd party data',
                'details': str(e)
            })
        }
