"""Small generic utilities packaged into the generic layer."""
import json
from typing import Any


def to_json(obj: Any) -> str:
    return json.dumps(obj, default=str)


def echo(obj: Any) -> Any:
    return obj
