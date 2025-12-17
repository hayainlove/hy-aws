import sys
import os
import json
from types import SimpleNamespace

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LAYER = os.path.join(ROOT, "lambda", "layers", "base", "python")
sys.path.insert(0, LAYER)

import boto3
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(ROOT, "lambda", "create_user"))
import create_user


def make_table_mock():
    table = MagicMock()
    table.query.return_value = {"Count": 0}
    table.put_item.return_value = {}
    return table


@patch("create_user.boto3.resource")
def test_handler_creates_user(mock_resource):
    table = make_table_mock()
    mock_resource.return_value.Table.return_value = table

    event = {"email": "t@example.com", "userName": "tester"}
    resp = create_user.handler(event, SimpleNamespace())
    assert resp["statusCode"] == 201
    body = json.loads(resp["body"])
    assert "user" in body
    assert body["user"]["email"] == "t@example.com"
