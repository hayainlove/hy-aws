import json
import os
import boto3
import logging
from boto3.dynamodb.types import TypeDeserializer
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection, helpers
from opensearchpy.exceptions import ConnectionError, TransportError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

# DynamoDB deserializer
deserializer = TypeDeserializer()


def ensure_index_exists():
    """Create the users index if it doesn't exist"""
    try:
        if not opensearch_client.indices.exists(index=USERS_INDEX):
            index_body = {
                'settings': {
                    'number_of_shards': 1,
                    'number_of_replicas': 0,
                    'refresh_interval': '1s'  # Default refresh interval
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
            logger.info(f"✅ Created index: {USERS_INDEX}")
        else:
            logger.info(f"Index {USERS_INDEX} already exists")
    except Exception as e:
        logger.error(f"❌ Error creating index: {str(e)}")
        raise


def delete_index_if_needed():
    """
    Delete the old index if DELETE_OLD_INDEX environment variable is set to 'true'
    This should only be used during initial setup or schema migrations
    """
    delete_flag = os.environ.get('DELETE_OLD_INDEX', 'false').lower() == 'true'
    
    if delete_flag:
        try:
            if opensearch_client.indices.exists(index=USERS_INDEX):
                mapping = opensearch_client.indices.get_mapping(index=USERS_INDEX)
                fields = list(mapping[USERS_INDEX]['mappings']['properties'].keys())
                logger.info(f"Current index fields: {fields}")
                
                opensearch_client.indices.delete(index=USERS_INDEX)
                logger.warning(f"🗑️  DELETED OLD INDEX: {USERS_INDEX}")
        except Exception as e:
            logger.error(f"❌ Error deleting index: {str(e)}")
            raise


def parse_dynamodb_item(dynamodb_item):
    """
    Parse DynamoDB item format to regular Python dict using boto3's TypeDeserializer
    This handles all DynamoDB data types properly
    """
    return {k: deserializer.deserialize(v) for k, v in dynamodb_item.items()}


def enrich_user_data(user_data):
    """
    Enrich user data with computed fields
    """
    # Add fullName if not present but firstName and lastName exist
    if 'fullName' not in user_data:
        first_name = user_data.get('firstName', '').strip()
        last_name = user_data.get('lastName', '').strip()
        if first_name and last_name:
            user_data['fullName'] = f"{first_name} {last_name}"
        elif first_name:
            user_data['fullName'] = first_name
        elif last_name:
            user_data['fullName'] = last_name
    
    return user_data


def prepare_bulk_actions(records):
    """
    Prepare bulk actions for OpenSearch from DynamoDB stream records
    Returns a list of bulk operation dictionaries
    """
    actions = []
    
    for record in records:
        event_name = record['eventName']
        
        try:
            if event_name in ['INSERT', 'MODIFY']:
                new_image = record['dynamodb'].get('NewImage')
                if not new_image:
                    logger.warning(f"No NewImage found in {event_name} record")
                    continue
                
                user_data = parse_dynamodb_item(new_image)
                user_data = enrich_user_data(user_data)
                user_id = user_data.get('user_id')
                
                if not user_id:
                    logger.warning(f"No user_id found in {event_name} record")
                    continue
                
                actions.append({
                    '_op_type': 'index',
                    '_index': USERS_INDEX,
                    '_id': user_id,
                    '_source': user_data
                })
                logger.info(f"📝 Prepared {event_name} for user: {user_id}")
                
            elif event_name == 'REMOVE':
                old_image = record['dynamodb'].get('OldImage')
                if not old_image:
                    logger.warning("No OldImage found in REMOVE record")
                    continue
                
                user_data = parse_dynamodb_item(old_image)
                user_id = user_data.get('user_id')
                
                if not user_id:
                    logger.warning("No user_id found in REMOVE record")
                    continue
                
                actions.append({
                    '_op_type': 'delete',
                    '_index': USERS_INDEX,
                    '_id': user_id
                })
                logger.info(f"🗑️  Prepared DELETE for user: {user_id}")
            
            else:
                logger.warning(f"Unknown event type: {event_name}")
                
        except Exception as e:
            logger.error(f"❌ Error preparing action for record: {str(e)}")
            # Continue processing other records
            continue
    
    return actions


def execute_bulk_operations(actions):
    """
    Execute bulk operations against OpenSearch
    Returns tuple of (success_count, failed_operations)
    """
    if not actions:
        logger.info("No actions to execute")
        return 0, []
    
    try:
        # Execute bulk operation
        # raise_on_error=False allows partial success
        success, failed = helpers.bulk(
            opensearch_client,
            actions,
            raise_on_error=False,
            raise_on_exception=False
        )
        
        logger.info(f"✅ Bulk operation completed - Success: {success}, Failed: {len(failed)}")
        
        if failed:
            for item in failed:
                logger.error(f"❌ Failed operation: {json.dumps(item)}")
        
        return success, failed
        
    except (ConnectionError, TransportError) as e:
        logger.error(f"❌ OpenSearch connection error during bulk operation: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error during bulk operation: {str(e)}")
        raise


def handler(event, context):
    """
    Lambda handler for DynamoDB stream events
    Processes INSERT, MODIFY, and REMOVE events and syncs with OpenSearch
    
    Environment Variables:
    - OPENSEARCH_ENDPOINT: OpenSearch domain endpoint (required)
    - AWS_REGION: AWS region (default: ap-southeast-1)
    - DELETE_OLD_INDEX: Set to 'true' to delete and recreate index (default: false)
    """
    try:
        logger.info(f"📥 Received {len(event['Records'])} DynamoDB stream records")
        
        # Check if we need to delete the old index (controlled by env var)
        delete_index_if_needed()
        
        # Ensure the index exists
        ensure_index_exists()
        
        # Prepare bulk actions from all records
        actions = prepare_bulk_actions(event['Records'])
        
        # Execute bulk operations
        success_count, failed_operations = execute_bulk_operations(actions)
        
        # Prepare response
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Processed {len(event["Records"])} records',
                'successful': success_count,
                'failed': len(failed_operations)
            })
        }
        
        # If there were failures, log them but don't fail the entire batch
        # This prevents infinite retries for permanently failing records
        if failed_operations:
            logger.warning(f"⚠️  {len(failed_operations)} operations failed, but batch completed")
        
        logger.info(f"✅ Stream processing completed successfully")
        return response
    
    except Exception as e:
        logger.error(f"❌ Fatal error processing stream: {str(e)}", exc_info=True)
        # Re-raise to trigger Lambda retry mechanism
        raise


# For local testing
if __name__ == "__main__":
    # Mock event for testing
    test_event = {
        'Records': [
            {
                'eventName': 'INSERT',
                'dynamodb': {
                    'NewImage': {
                        'user_id': {'S': 'test-user-123'},
                        'email': {'S': 'test@example.com'},
                        'userName': {'S': 'testuser'},
                        'firstName': {'S': 'Test'},
                        'lastName': {'S': 'User'},
                        'status': {'S': 'active'}
                    }
                }
            }
        ]
    }