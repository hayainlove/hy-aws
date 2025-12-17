@"
import re
from typing import Dict, Optional, List

def validate_email(email: str) -> bool:
    """Validate email format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def validate_required_fields(data: Dict, required_fields: List[str]) -> Optional[str]:
    """
    Validate that all required fields are present in data
    Returns None if valid, error message if invalid
    """
    missing_fields = [field for field in required_fields if field not in data or not data[field]]
    
    if missing_fields:
        return f'Missing required fields: {", ".join(missing_fields)}'
    
    return None

def validate_user_data(data: Dict, is_update: bool = False) -> Optional[str]:
    """
    Validate user data
    Returns None if valid, error message if invalid
    """
    if not is_update:
        # For creation, check required fields
        required = ['email', 'userName']
        error = validate_required_fields(data, required)
        if error:
            return error
    
    # Validate email format if present
    if 'email' in data and not validate_email(data['email']):
        return 'Invalid email format'
    
    # Validate age if present
    if 'age' in data:
        try:
            age = int(data['age'])
            if age < 0 or age > 150:
                return 'Age must be between 0 and 150'
        except (ValueError, TypeError):
            return 'Age must be a valid number'
    
    return None
"@ | Out-File -FilePath "python/utils/validators.py" -Encoding utf8