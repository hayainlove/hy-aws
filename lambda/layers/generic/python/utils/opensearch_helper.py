@"
import os
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3

def get_opensearch_client():
    """Get authenticated OpenSearch client"""
    endpoint = os.environ.get('OPENSEARCH_ENDPOINT', '').replace('https://', '')
    
    if not endpoint:
        raise ValueError("OPENSEARCH_ENDPOINT environment variable not set")
    
    # Get AWS credentials
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        os.environ.get('AWS_REGION', 'ap-southeast-1'),
        'es',
        session_token=credentials.token
    )
    
    # Create OpenSearch client
    client = OpenSearch(
        hosts=[{'host': endpoint, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=30
    )
    
    return client

def index_document(client, index_name: str, doc_id: str, document: dict):
    """Index a document in OpenSearch"""
    return client.index(
        index=index_name,
        id=doc_id,
        body=document,
        refresh=True
    )

def delete_document(client, index_name: str, doc_id: str):
    """Delete a document from OpenSearch"""
    return client.delete(
        index=index_name,
        id=doc_id,
        refresh=True,
        ignore=[404]  # Ignore if document doesn't exist
    )

def search_documents(client, index_name: str, query: dict):
    """Search documents in OpenSearch"""
    return client.search(
        index=index_name,
        body=query
    )
"@ | Out-File -FilePath "python/utils/opensearch_helper.py" -Encoding utf8