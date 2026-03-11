"""Shared fixtures for the cognitive3dpy test suite."""

import pytest

import cognitive3dpy._client as _client_mod
from cognitive3dpy.auth import _config


@pytest.fixture(autouse=True)
def reset_state():
    """Set credentials and reset the httpx singleton before every test."""
    _config.api_key = "test-api-key"
    _config.project_id = 1234
    _client_mod._client = None
    yield
    _config.api_key = None
    _config.project_id = None
    _client_mod._client = None
