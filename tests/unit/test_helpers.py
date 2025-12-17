import sys
import os
import pytest

# Ensure layer package is importable in tests (adds layer path)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LAYER = os.path.join(ROOT, "lambda", "layers", "base", "python")
sys.path.insert(0, LAYER)

from mylib import helpers


def test_is_valid_email():
    assert helpers.is_valid_email("alice@example.com")
    assert not helpers.is_valid_email("")
    assert not helpers.is_valid_email(None)


def test_now_iso():
    s = helpers.now_iso()
    assert isinstance(s, str)


def test_sanitize_username():
    assert helpers.sanitize_username(" alice ") == "alice"
    assert helpers.sanitize_username(None) == ""
