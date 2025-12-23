import boto3
import os
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection

# OpenSearch endpoint (without https://)
ENDPOINT = 'search-myhayatisearchd-rduxlngerebz-tluhsaq7g2nf5nsn2dk4cpzzri.ap-southeast-1.es.amazonaws.com'
REGION = 'ap-southeast-1'

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    REGION,
    'es',
    session_token=credentials.token
)

client = OpenSearch(
    hosts=[{'host': ENDPOINT, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=30
)

print("Checking if 'users' index exists...")
if client.indices.exists(index='users'):
    print("Index exists. Getting current mapping...")
    mapping = client.indices.get_mapping(index='users')
    print(f"Current fields: {list(mapping['users']['mappings']['properties'].keys())}")
    
    print("\nDeleting index...")
    client.indices.delete(index='users')
    print("✓ Index 'users' deleted successfully!")
else:
    print("Index 'users' does not exist")