@"
import json
from typing import Any, Dict, Optional

def success_response(data: Any, status_code: int = 200) -> Dict:
    """Return a successful API Gateway response"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS'
        },
        'body': json.dumps(data)
    }

def error_response(message: str, status_code: int = 400) -> Dict:
    """Return an error API Gateway response"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
        },
        'body': json.dumps({'error': message})
    }

def internal_error_response(message: str = "Internal server error") -> Dict:
    """Return a 500 error response"""
    return error_response(message, 500)
"@ | Out-File -FilePath "python/utils/response_helper.py" -Encoding utf8