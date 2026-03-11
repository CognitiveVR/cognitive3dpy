"""HTTP client wrapper for the Cognitive3D API."""

from __future__ import annotations

import atexit
import logging
import os
import time

import httpx

from cognitive3dpy.auth import get_api_key

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0  # seconds

BASE_URL = "https://api.cognitive3d.com"
_DEFAULT_TIMEOUT = 30.0

_client: httpx.Client | None = None
_timeout: float = float(os.environ.get("C3D_TIMEOUT", _DEFAULT_TIMEOUT))


def _close_client() -> None:
    if _client is not None and not _client.is_closed:
        _client.close()


atexit.register(_close_client)


class C3DError(Exception):
    """Base exception for Cognitive3D API errors."""


class C3DAuthError(C3DError):
    """Raised on 401 Unauthorized."""


class C3DNotFoundError(C3DError):
    """Raised on 404 Not Found."""


class C3DAPIError(C3DError):
    """Raised on any other non-2xx response."""


def _get_client() -> httpx.Client:
    """Return a reusable httpx client, creating one if needed."""
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.Client(base_url=BASE_URL, timeout=_timeout)
    return _client


def c3d_set_timeout(seconds: float) -> None:
    """Set the HTTP request timeout for all API calls.

    Takes effect immediately by recreating the underlying HTTP client.
    Can also be configured via the ``C3D_TIMEOUT`` environment variable
    before the first API call.

    Parameters
    ----------
    seconds : float
        Timeout in seconds per request. Default is 30.0.
    """
    global _timeout, _client
    _timeout = float(seconds)
    if _client is not None and not _client.is_closed:
        _client.close()
    _client = None


def _build_headers() -> dict[str, str]:
    """Build request headers with the stored API key."""
    return {
        "Authorization": get_api_key(),
        "Content-Type": "application/json",
    }


def _raise_for_status(response: httpx.Response) -> None:
    """Raise a typed exception for non-2xx responses."""
    if response.is_success:
        return
    msg = f"{response.status_code}: {response.text}"
    if response.status_code == 401:
        raise C3DAuthError(msg)
    if response.status_code == 404:
        raise C3DNotFoundError(msg)
    raise C3DAPIError(msg)


def _execute_with_retry(fn) -> httpx.Response:
    """Execute a request callable with exponential backoff on transient errors."""
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            response = fn()
            if response.status_code not in _RETRYABLE_STATUS:
                return response
            last_exc = C3DAPIError(f"{response.status_code}: {response.text}")
        except httpx.TransportError as exc:
            last_exc = exc

        wait = _BACKOFF_BASE * (2**attempt)
        logger.warning(
            "Request failed (attempt %d/%d), retrying in %.1fs...",
            attempt + 1,
            _MAX_RETRIES,
            wait,
        )
        time.sleep(wait)

    raise last_exc


def c3d_request(endpoint: str, body: dict) -> dict | list:
    """POST to the API and return the parsed JSON response."""
    client = _get_client()
    response = _execute_with_retry(
        lambda: client.post(endpoint, headers=_build_headers(), json=body)
    )
    _raise_for_status(response)
    return response.json()


def c3d_get(endpoint: str) -> dict | list:
    """GET from the API and return the parsed JSON response."""
    client = _get_client()
    response = _execute_with_retry(
        lambda: client.get(endpoint, headers=_build_headers())
    )
    _raise_for_status(response)
    return response.json()
