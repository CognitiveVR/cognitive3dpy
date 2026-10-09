"""Exceptions raised by the session downloader."""

from __future__ import annotations

from cognitive3dpy._client import C3DAPIError, C3DError


class C3DRetentionError(C3DAPIError):
    """Raised on 410: the session is older than the organization's data retention."""


class C3DDatasetError(C3DError):
    """Raised when sessions cannot be written as one LeRobot dataset."""


class C3DIncompleteDownloadError(C3DError):
    """Raised when some camera frames could not be downloaded."""
