"""Credential storage for the Cognitive3D API."""

from __future__ import annotations

import os


class _Config:
    """Module-level session config. Holds credentials in memory only."""

    def __init__(self) -> None:
        self.api_key: str | None = None
        self.project_id: int | None = None


_config = _Config()


def c3d_auth(api_key: str | None = None) -> None:
    """Set the API key for the current session.

    If *api_key* is not provided, falls back to the ``C3D_API_KEY``
    environment variable.  Raises ``ValueError`` if neither is available.
    """
    if api_key is not None and not api_key:
        raise ValueError("api_key must not be an empty string.")
    key = api_key if api_key is not None else os.environ.get("C3D_API_KEY")
    if not key:
        raise ValueError(
            "No API key provided. Pass api_key= or set the C3D_API_KEY "
            "environment variable."
        )
    _config.api_key = key


def c3d_project(project_id: int) -> None:
    """Set the default project ID for the current session."""
    if isinstance(project_id, bool) or not isinstance(project_id, int):
        raise TypeError(f"project_id must be an int, got {type(project_id).__name__!r}")
    _config.project_id = project_id


def get_credentials() -> tuple[str, int]:
    """Return (api_key, project_id). Raises if either is unset."""
    if _config.api_key is None:
        raise ValueError("API key not set. Call c3d_auth() first.")
    if _config.project_id is None:
        raise ValueError("Project ID not set. Call c3d_project() first.")
    return _config.api_key, _config.project_id


def get_api_key() -> str:
    """Return the API key. Raises if unset."""
    if _config.api_key is None:
        raise ValueError("API key not set. Call c3d_auth() first.")
    return _config.api_key


def get_project_id() -> int:
    """Return the project ID. Raises if unset."""
    if _config.project_id is None:
        raise ValueError("Project ID not set. Call c3d_project() first.")
    return _config.project_id
