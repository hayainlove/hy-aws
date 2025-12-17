import json
import os
import boto3
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

USERS_INDEX = 'users'


def search_users(query_string, page=1, page_size=10):
    """
    Search users in OpenSearch
    
    Args:
        query_string: Search term (searches in userName, fullName, email, location)
        page: Page number (starts at 1)
        page_size: Number of results per page
    
    Returns:
        Dict with search results and metadata
    """
    from_param = (page - 1) * page_size
    
    if query_string and query_string.strip():
        # Search query - matches across multiple fields
        search_body = {
            "from": from_param,
            "size": page_size,
            "query": {
                "multi_match": {
                    "query": query_string,
                    "fields": ["userName^3", "fullName^2", "email", "location"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            },
            "sort": [
                {"_score": {"order": "desc"}},
                {"createdAt": {"order": "desc"}}
            ]
        }
    else:
        # No search query - list all users
        search_body = {
            "from": from_param,
            "size": page_size,
            "query": {
                "match_all": {}
            },
            "sort": [
                {"createdAt": {"order": "desc"}}
            ]
        }
    
    # Execute search
    response = opensearch_client.search(
        index=USERS_INDEX,
        body=search_body
    )
    
    # Extract results
    hits = response['hits']['hits']
    total = response['hits']['total']['value']
    
    users = [hit['_source'] for hit in hits]
    
    return {
        'users': users,
        'total': total,
        'page': page,
        'page_size': page_size,
        'total_pages': (total + page_size - 1) // page_size
    }


def filter_users(filters, page=1, page_size=10):
    """
    Filter users by specific criteria
    
    Args:
        filters: Dict of field:value pairs to filter by
        page: Page number
        page_size: Number of results per page
    
    Returns:
        Dict with filtered results and metadata
    """
    from_param = (page - 1) * page_size
    
    # Build filter query
    must_clauses = []
    
    for field, value in filters.items():
        if field in ['email', 'userName', 'status']:
            # Exact match for these fields
            must_clauses.append({
                "term": {f"{field}.keyword" if field == 'userName' else field: value}
            })
        elif field in ['fullName', 'location']:
            # Partial match for names and location
            must_clauses.append({
                "match": {field: value}
            })
        elif field == 'age':
            # Numeric match for age
            must_clauses.append({
                "term": {field: int(value)}
            })
    
    search_body = {
        "from": from_param,
        "size": page_size,
        "query": {
            "bool": {
                "must": must_clauses
            }
        },
        "sort": [
            {"createdAt": {"order": "desc"}}
        ]
    }
    
    response = opensearch_client.search(
        index=USERS_INDEX,
        body=search_body
    )
    
    hits = response['hits']['hits']
    total = response['hits']['total']['value']
    
    users = [hit['_source'] for hit in hits]
    
    return {
        'users': users,
        'total': total,
        'page': page,
        'page_size': page_size,
        'total_pages': (total + page_size - 1) // page_size,
        'filters': filters
    }


def handler(event, context):
    """
    Lambda handler for searching/listing users
    
    Query parameters:
        - q: Search query string (optional)
        - page: Page number (default: 1)
        - page_size: Results per page (default: 10, max: 100)
        - status: Filter by status (optional)
        - email: Filter by exact email (optional)
        - userName: Filter by exact userName (optional)
        - fullName: Filter by name (optional)
        - location: Filter by location (optional)
        - age: Filter by age (optional)
    
    Examples:
        GET /users/search?q=john
        GET /users/search?q=john&page=2&page_size=20
        GET /users/search?status=ACTIVE
        GET /users/search?location=Singapore
        GET /users/search (list all users)
    """
    try:
        # Get query parameters
        query_params = event.get('queryStringParameters') or {}
        
        search_query = query_params.get('q', '').strip()
        page = int(query_params.get('page', 1))
        page_size = min(int(query_params.get('page_size', 10)), 100)  # Max 100 per page
        
        # Check if this is a filter request
        filter_fields = {}
        for field in ['status', 'email', 'userName', 'fullName', 'location', 'age']:
            if field in query_params:
                filter_fields[field] = query_params[field]
        
        # Execute search or filter
        if filter_fields:
            # Use filter query
            result = filter_users(filter_fields, page, page_size)
        else:
            # Use search query (or list all if no query)
            result = search_users(search_query, page, page_size)
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(result)
        }
        
    except Exception as e:
        # Check if it's an index not found error
        error_str = str(e).lower()
        if 'index_not_found' in error_str or 'no such index' in error_str:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Index not found',
                    'message': 'Users index does not exist yet. Create some users first.'
                })
            }
        
        # Check for invalid parameters
        if isinstance(e, ValueError):
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Invalid parameters',
                    'message': str(e)
                })
            }
        
        # Generic error
        print(f"Error searching users: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Internal server error',
                'message': 'Failed to search users'
            })
        }