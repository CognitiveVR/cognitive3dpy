"""Streaming HTTP client for session data and camera frames.

Separate from ``_client.py`` because downloads need streamed bodies, longer
retries and ``Retry-After`` handling; credentials and the host come from the
same place (``c3d_auth()`` and ``C3D_BASE_URL``).
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from importlib.metadata import PackageNotFoundError, version
from typing import Any
from urllib.parse import quote, urlparse

import httpx

from cognitive3dpy._client import C3DAPIError, C3DAuthError, C3DNotFoundError
from cognitive3dpy._export.errors import C3DRetentionError
from cognitive3dpy._export.progress import progress_bar

logger = logging.getLogger(__name__)

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_KEY_PREFIXES = {
    "orgkey-": "organization",
    "projectkey-": "project",
    "APIKEY:ORGANIZATION ": "organization",
}

try:
    _VERSION = version("cognitive3dpy")
except PackageNotFoundError:  # pragma: no cover - running from a source tree
    _VERSION = "unknown"


def segment(value: Any) -> str:
    """Percent-encode one URL path segment (session and camera ids are free text)."""
    return quote(str(value), safe="")


def key_type(api_key: str) -> str:
    for prefix, kind in _KEY_PREFIXES.items():
        if api_key.startswith(prefix):
            return kind
    return "API"


def redact_key(api_key: str) -> str:
    prefix = next((p for p in _KEY_PREFIXES if api_key.startswith(p)), "")
    return f"{prefix}…{api_key[-4:]}"


def _error(cls: type[Exception], message: str, status: int | None, body: str):
    error = cls(message)
    error.status = status
    error.body = body
    return error


class HttpClient:
    """Authenticated client for the Cognitive3D API, with retries."""

    def __init__(
        self,
        api_key: str,
        base_url: str,
        *,
        timeout: float = 30.0,
        max_retries: int = 5,
        max_auth_retries: int = 8,
        backoff: float = 0.5,
        max_backoff: float = 30.0,
        sleep: Callable[[float], None] | None = None,
        transport: httpx.BaseTransport | None = None,
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.max_auth_retries = max_auth_retries
        self._backoff_base = backoff
        self._max_backoff = max_backoff
        self._sleep = sleep or (lambda seconds: time.sleep(seconds))
        # the API answers 401 when its own auth lookup fails, so a key that already
        # worked in this run gets time to recover before the error is final
        self._key_accepted = False
        self._client = httpx.Client(
            base_url=self.base_url,
            headers={
                "Authorization": api_key,
                "User-Agent": f"cognitive3dpy/{_VERSION}",
            },
            timeout=httpx.Timeout(timeout, read=max(timeout, 120.0)),
            transport=transport,
            follow_redirects=True,
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=32),
        )

    @property
    def host(self) -> str:
        return urlparse(self.base_url).netloc

    @property
    def key_type(self) -> str:
        return key_type(self.api_key)

    @property
    def redacted_key(self) -> str:
        return redact_key(self.api_key)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> HttpClient:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def request_bytes(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json: Any = None,
        context: str,
        progress_desc: str | None = None,
    ) -> bytes:
        """Fetch a whole body, retrying transient failures (also mid-body drops)."""
        attempt = auth_attempt = 0
        while True:
            try:
                with self._client.stream(
                    method, path, params=params, json=json
                ) as response:
                    if response.status_code >= 400:
                        response.read()
                        delay = self._retry_delay(response, attempt, auth_attempt)
                        if delay is None:
                            raise_for_status(response, self, context)
                        if response.status_code in (401, 403):
                            auth_attempt += 1
                        else:
                            attempt += 1
                        self._log_retry(context, response.status_code, delay)
                        self._sleep(delay)
                        continue
                    body = self._read_body(response, progress_desc)
                    self._key_accepted = True
                    return body
            except httpx.TransportError as e:
                if attempt >= self.max_retries:
                    raise C3DAPIError(f"{context}: {type(e).__name__}: {e}") from e
                delay = self._backoff(attempt, None)
                self._log_retry(context, type(e).__name__, delay)
                self._sleep(delay)
                attempt += 1

    @contextmanager
    def open_stream(
        self, method: str, path: str, *, params: dict | None = None, context: str
    ) -> Iterator[httpx.Response]:
        """Open a streamed response. Only opening it is retried; the caller owns
        the body."""
        response = self._open(method, path, params, context)
        try:
            yield response
        finally:
            response.close()

    def _open(
        self, method: str, path: str, params: dict | None, context: str
    ) -> httpx.Response:
        attempt = auth_attempt = 0
        while True:
            try:
                request = self._client.build_request(method, path, params=params)
                response = self._client.send(request, stream=True)
            except httpx.TransportError as e:
                if attempt >= self.max_retries:
                    raise C3DAPIError(f"{context}: {type(e).__name__}: {e}") from e
                delay = self._backoff(attempt, None)
                self._log_retry(context, type(e).__name__, delay)
                self._sleep(delay)
                attempt += 1
                continue
            if response.status_code >= 400:
                response.read()
                response.close()
                delay = self._retry_delay(response, attempt, auth_attempt)
                if delay is None:
                    raise_for_status(response, self, context)
                if response.status_code in (401, 403):
                    auth_attempt += 1
                else:
                    attempt += 1
                self._log_retry(context, response.status_code, delay)
                self._sleep(delay)
                continue
            self._key_accepted = True
            return response

    def _retry_delay(
        self, response: httpx.Response, attempt: int, auth_attempt: int
    ) -> float | None:
        """Seconds to wait before retrying this error response, or None to give up."""
        status = response.status_code
        if status in RETRY_STATUSES and attempt < self.max_retries:
            return self._backoff(attempt, response)
        auth_failed = status in (401, 403)
        if auth_failed and self._key_accepted and auth_attempt < self.max_auth_retries:
            return min(self._max_backoff, 5.0 * 2**auth_attempt)
        return None

    def _backoff(self, attempt: int, response: httpx.Response | None) -> float:
        retry_after = response.headers.get("Retry-After") if response else None
        if retry_after and retry_after.strip().isdigit():
            return float(retry_after)
        delay = min(self._max_backoff, self._backoff_base * 2**attempt)
        return delay * (0.5 + random.random() / 2)

    @staticmethod
    def _log_retry(context: str, reason, delay: float) -> None:
        logger.warning("%s: got %s, retrying in %.1fs...", context, reason, delay)

    def _read_body(self, response: httpx.Response, progress_desc: str | None) -> bytes:
        if progress_desc is None:
            return response.read()
        # Content-Length is the compressed size for encoded bodies
        length = response.headers.get("Content-Length")
        encoded = "Content-Encoding" in response.headers
        total = int(length) if length and not encoded else None
        chunks = []
        with progress_bar(total=total, desc=progress_desc, unit="B") as bar:
            for chunk in response.iter_bytes():
                chunks.append(chunk)
                bar.update(len(chunk))
        return b"".join(chunks)


def raise_for_status(response: httpx.Response, client: HttpClient, context: str):
    status = response.status_code
    try:
        body = response.text.strip()[:300]
    except Exception:
        body = ""
    detail = f": {body}" if body else ""
    if status in (401, 403):
        raise _error(
            C3DAuthError,
            f"{context}: the {client.key_type} key ({client.redacted_key}) was "
            f"rejected (HTTP {status}). Check the key and that it can access "
            "this project.",
            status,
            body,
        )
    if status == 404:
        raise _error(C3DNotFoundError, f"{context}: not found (404){detail}", 404, body)
    if status == 410:
        raise _error(
            C3DRetentionError,
            f"{context}: this session is older than your organization's data "
            "retention (HTTP 410).",
            410,
            body,
        )
    raise _error(C3DAPIError, f"{context}: HTTP {status}{detail}", status, body)
