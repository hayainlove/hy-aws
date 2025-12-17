@"
import boto3
from typing import Dict, Optional, List
from decimal import Decimal
import json

class DecimalEncoder(json.JSONEncoder):
    """Helper class to convert Decimal to int/float for JSON serialization"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_dynamodb_client():
    """Get DynamoDB client"""
    return boto3.client('dynamodb')

def get_dynamodb_resource():
    """Get DynamoDB resource"""
    return boto3.resource('dynamodb')

def serialize_dynamodb_item(item: Dict) -> Dict:
    """Convert DynamoDB item to JSON-serializable format"""
    return json.loads(json.dumps(item, cls=DecimalEncoder))

def get_table(table_name: str):
    """Get DynamoDB table"""
    dynamodb = get_dynamodb_resource()
    return dynamodb.Table(table_name)
"@ | Out-File -FilePath "python/utils/dynamodb_helper.py" -Encoding utf8