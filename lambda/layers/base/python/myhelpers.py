import re
from datetime import datetime, timezone

EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def is_valid_email(email: str) -> bool:
    if not email:
        return False
    return EMAIL_REGEX.match(email) is not None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_username(name: str) -> str:
    if name is None:
        return ""
    return name.strip()
