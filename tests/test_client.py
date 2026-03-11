"""Tests for _client.py."""

import importlib
from unittest.mock import patch

import httpx
import pytest
import respx

import cognitive3dpy._client as _client_module
from cognitive3dpy._client import (
    BASE_URL,
    C3DAPIError,
    C3DAuthError,
    C3DNotFoundError,
    c3d_get,
    c3d_request,
    c3d_set_timeout,
)


@respx.mock
def test_c3d_get_returns_json():
    respx.get(f"{BASE_URL}/v0/test").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    result = c3d_get("/v0/test")
    assert result == {"ok": True}


@respx.mock
def test_c3d_request_posts_body():
    route = respx.post(f"{BASE_URL}/v0/test").mock(
        return_value=httpx.Response(200, json=[1, 2, 3])
    )
    result = c3d_request("/v0/test", {"key": "value"})
    assert result == [1, 2, 3]
    assert route.called


@respx.mock
def test_c3d_get_sends_auth_header():
    route = respx.get(f"{BASE_URL}/v0/test").mock(
        return_value=httpx.Response(200, json={})
    )
    c3d_get("/v0/test")
    assert route.calls[0].request.headers["Authorization"] == "test-api-key"


@respx.mock
def test_raises_auth_error_on_401():
    respx.get(f"{BASE_URL}/v0/test").mock(
        return_value=httpx.Response(401, text="Unauthorized")
    )
    with pytest.raises(C3DAuthError):
        c3d_get("/v0/test")


@respx.mock
def test_raises_not_found_on_404():
    respx.get(f"{BASE_URL}/v0/test").mock(
        return_value=httpx.Response(404, text="Not Found")
    )
    with pytest.raises(C3DNotFoundError):
        c3d_get("/v0/test")


@respx.mock
def test_raises_api_error_on_500():
    respx.get(f"{BASE_URL}/v0/test").mock(
        return_value=httpx.Response(500, text="Server Error")
    )
    with patch("cognitive3dpy._client.time.sleep"):
        with pytest.raises(C3DAPIError):
            c3d_get("/v0/test")


@respx.mock
def test_retries_on_503_then_succeeds():
    respx.get(f"{BASE_URL}/v0/test").mock(
        side_effect=[
            httpx.Response(503, text="Unavailable"),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    with patch("cognitive3dpy._client.time.sleep"):
        result = c3d_get("/v0/test")
    assert result == {"ok": True}


@respx.mock
def test_retries_on_transport_error_then_succeeds():
    respx.get(f"{BASE_URL}/v0/test").mock(
        side_effect=[
            httpx.TransportError("connection reset"),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    with patch("cognitive3dpy._client.time.sleep"):
        result = c3d_get("/v0/test")
    assert result == {"ok": True}


@respx.mock
def test_raises_after_max_retries():
    respx.get(f"{BASE_URL}/v0/test").mock(
        return_value=httpx.Response(503, text="Unavailable")
    )
    with patch("cognitive3dpy._client.time.sleep"):
        with pytest.raises(C3DAPIError):
            c3d_get("/v0/test")


@respx.mock
def test_no_retry_on_401():
    route = respx.get(f"{BASE_URL}/v0/test").mock(
        return_value=httpx.Response(401, text="Unauthorized")
    )
    with patch("cognitive3dpy._client.time.sleep") as mock_sleep:
        with pytest.raises(C3DAuthError):
            c3d_get("/v0/test")
    assert route.call_count == 1
    mock_sleep.assert_not_called()


def test_c3d_set_timeout_updates_timeout():
    original = _client_module._timeout
    try:
        c3d_set_timeout(60.0)
        assert _client_module._timeout == 60.0
        assert _client_module._client is None
    finally:
        c3d_set_timeout(original)


def test_atexit_not_registered_on_set_timeout():
    # atexit.register should not be called when changing the timeout —
    # it is registered once at module load time via _close_client.
    with patch("cognitive3dpy._client.atexit.register") as mock_register:
        c3d_set_timeout(10.0)
        c3d_set_timeout(20.0)
        c3d_set_timeout(30.0)
    mock_register.assert_not_called()
    c3d_set_timeout(_client_module._DEFAULT_TIMEOUT)


def test_c3d_set_timeout_env_var(monkeypatch):
    monkeypatch.setenv("C3D_TIMEOUT", "45")
    # Reimport to pick up env var (simulate fresh session)
    import cognitive3dpy._client as mod

    importlib.reload(mod)
    assert mod._timeout == 45.0
    importlib.reload(mod)  # restore to default for other tests
