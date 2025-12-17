import json
import os
import boto3
from datetime import datetime
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection

# Initialize OpenSearch client
opensearch_endpoint = os.environ['OPENSEARCH_ENDPOINT'].replace('https://', '')
region = os.environ.get('AWS_REGION', 'ap-southeast-1')

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    'es',
    session_token=credentials.token
)

opensearch_client = OpenSearch(
    hosts=[{'host': opensearch_endpoint, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=30
)

# Index name for users
USERS_INDEX = 'users'

def ensure_index_exists():
    """Create the users index if it doesn't exist"""
    if not opensearch_client.indices.exists(index=USERS_INDEX):
        index_body = {
            'settings': {
                'number_of_shards': 1,
                'number_of_replicas': 0
            },
            'mappings': {
                'properties': {
                    'user_id': {'type': 'keyword'},
                    'email': {'type': 'keyword'},
                    'userName': {'type': 'text', 'fields': {'keyword': {'type': 'keyword'}}},
                    'firstName': {'type': 'text', 'fields': {'keyword': {'type': 'keyword'}}},
                    'lastName': {'type': 'text', 'fields': {'keyword': {'type': 'keyword'}}},
                    'fullName': {'type': 'text'},
                    'phone': {'type': 'keyword'},
                    'address': {'type': 'text'},
                    'age': {'type': 'integer'},
                    'location': {'type': 'text'},
                    'status': {'type': 'keyword'},
                    'createdAt': {'type': 'date'},
                    'updatedAt': {'type': 'date'}
                }
            }
        }
        opensearch_client.indices.create(index=USERS_INDEX, body=index_body)
        print(f"Created index: {USERS_INDEX}")


def process_insert(record):
    """Process INSERT events - add document to OpenSearch"""
    new_image = record['dynamodb']['NewImage']
    user_data = parse_dynamodb_item(new_image)
    
    user_id = user_data.get('user_id')
    if not user_id:
        print("No user_id found in record")
        return
    
    # Index the document in OpenSearch
    opensearch_client.index(
        index=USERS_INDEX,
        id=user_id,
        body=user_data,
        refresh=True
    )
    print(f"Indexed user: {user_id}")


def process_modify(record):
    """Process MODIFY events - update document in OpenSearch"""
    new_image = record['dynamodb']['NewImage']
    user_data = parse_dynamodb_item(new_image)
    
    user_id = user_data.get('user_id')
    if not user_id:
        print("No user_id found in record")
        return
    
    # Update the document in OpenSearch
    opensearch_client.index(
        index=USERS_INDEX,
        id=user_id,
        body=user_data,
        refresh=True
    )
    print(f"Updated user: {user_id}")


def process_remove(record):
    """Process REMOVE events - delete document from OpenSearch"""
    old_image = record['dynamodb'].get('OldImage')
    if not old_image:
        print("No OldImage found in REMOVE record")
        return
    
    user_data = parse_dynamodb_item(old_image)
    user_id = user_data.get('user_id')
    
    if not user_id:
        print("No user_id found in record")
        return
    
    # Delete the document from OpenSearch
    try:
        opensearch_client.delete(
            index=USERS_INDEX,
            id=user_id,
            refresh=True
        )
        print(f"Deleted user: {user_id}")
    except Exception as e:
        if 'not_found' in str(e).lower():
            print(f"User {user_id} not found in OpenSearch, skipping delete")
        else:
            raise


def parse_dynamodb_item(dynamodb_item):
    """Parse DynamoDB item format to regular Python dict"""
    result = {}
    
    for key, value in dynamodb_item.items():
        if 'S' in value:
            result[key] = value['S']
        elif 'N' in value:
            result[key] = int(value['N']) if '.' not in value['N'] else float(value['N'])
        elif 'BOOL' in value:
            result[key] = value['BOOL']
        elif 'NULL' in value:
            result[key] = None
        elif 'M' in value:
            result[key] = parse_dynamodb_item(value['M'])
        elif 'L' in value:
            result[key] = [parse_dynamodb_item({'item': item})['item'] for item in value['L']]
    
    return result


def handler(event, context):
    """
    Lambda handler for DynamoDB stream events
    Processes INSERT, MODIFY, and REMOVE events and syncs with OpenSearch
    """
    try:
        # Ensure the index exists
        ensure_index_exists()
        
        # Process each record in the batch
        for record in event['Records']:
            event_name = record['eventName']
            
            print(f"Processing {event_name} event")
            
            if event_name == 'INSERT':
                process_insert(record)
            elif event_name == 'MODIFY':
                process_modify(record)
            elif event_name == 'REMOVE':
                process_remove(record)
            else:
                print(f"Unknown event type: {event_name}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully processed {len(event["Records"])} records'
            })
        }
    
    except Exception as e:
        print(f"Error processing stream: {str(e)}")
        raise  # Re-raise to trigger retry mechanism