"""Tests for auth.py."""

import pytest

from cognitive3dpy.auth import (
    _config,
    c3d_auth,
    c3d_project,
    get_api_key,
    get_credentials,
    get_project_id,
)


def test_c3d_auth_explicit_key():
    c3d_auth("my-key")
    assert _config.api_key == "my-key"


def test_c3d_auth_env_var(monkeypatch):
    monkeypatch.setenv("C3D_API_KEY", "env-key")
    _config.api_key = None
    c3d_auth()
    assert _config.api_key == "env-key"


def test_c3d_auth_raises_when_no_key(monkeypatch):
    monkeypatch.delenv("C3D_API_KEY", raising=False)
    _config.api_key = None
    with pytest.raises(ValueError, match="No API key"):
        c3d_auth()


def test_c3d_auth_raises_on_empty_string(monkeypatch):
    monkeypatch.setenv("C3D_API_KEY", "env-key")
    with pytest.raises(ValueError, match="empty string"):
        c3d_auth("")


def test_c3d_project_sets_id():
    c3d_project(9999)
    assert _config.project_id == 9999


def test_c3d_project_rejects_string():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_project("9999")


def test_c3d_project_rejects_float():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_project(9999.0)


def test_c3d_project_rejects_bool():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_project(True)


def test_get_api_key_raises_when_unset():
    _config.api_key = None
    with pytest.raises(ValueError, match="API key not set"):
        get_api_key()


def test_get_project_id_raises_when_unset():
    _config.project_id = None
    with pytest.raises(ValueError, match="Project ID not set"):
        get_project_id()


def test_get_credentials_returns_tuple():
    key, pid = get_credentials()
    assert key == "test-api-key"
    assert pid == 1234
