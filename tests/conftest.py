"""Shared fixtures for the cognitive3dpy test suite."""

import pytest
import respx

import cognitive3dpy._client as _client_mod
from cognitive3dpy.auth import _config


@pytest.fixture(autouse=True)
def default_base_url(monkeypatch):
    """Keep a developer's C3D_BASE_URL from redirecting mocked requests."""
    monkeypatch.delenv("C3D_BASE_URL", raising=False)


@pytest.fixture
def edge():
    """A fake Cognitive3D API (tests/lerobot_fakes.py) serving every mocked request."""
    from urllib.parse import urlparse

    from lerobot_fakes import FakeEdge

    fake = FakeEdge()
    with respx.mock(assert_all_called=False) as router:
        host = urlparse(_client_mod.BASE_URL).netloc
        router.route(host=host).mock(side_effect=fake.handle)
        yield fake


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
