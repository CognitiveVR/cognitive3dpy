"""Download camera frames: the streaming tar route, per-frame fallback, disk cache."""

from __future__ import annotations

import io
import json
import os
import re
import tarfile
import tempfile
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from itertools import groupby
from pathlib import Path

import httpx

from cognitive3dpy._client import C3DAPIError, C3DNotFoundError
from cognitive3dpy._lerobot.errors import C3DIncompleteDownloadError
from cognitive3dpy._lerobot.http import HttpClient, segment
from cognitive3dpy._lerobot.models import CameraFrameRef
from cognitive3dpy._lerobot.progress import format_bytes, progress_bar

TRANSPORTS = ("auto", "stream", "per-frame")
_MEMBER_NAME = re.compile(r"(?:^|/)(\d+)_(-?\d+)\.([A-Za-z0-9]+)$")


class StreamUnavailable(Exception):
    """The API does not serve a media stream route (older deployment)."""


def _safe(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", value)


class MediaCache:
    """Blobs (camera frames, lidar scans) are immutable once captured, so a cached
    one never goes stale. ``kind`` names the subfolder: camera or lidar."""

    def __init__(
        self,
        root: Path,
        api_host: str,
        project_id: int,
        session_id: str,
        kind: str = "camera",
    ):
        self.dir = (
            Path(root) / _safe(api_host) / str(project_id) / _safe(session_id) / kind
        )

    def path(self, ref) -> Path:
        name = f"{ref.date:013d}_{ref.seq:010d}.{_safe(ref.extension)}"
        return self.dir / str(ref.version_id) / _safe(ref.stream_id) / name

    def has(self, ref) -> bool:
        path = self.path(ref)
        return path.exists() and path.stat().st_size > 0

    def write(self, ref, data: bytes) -> None:
        path = self.path(ref)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".part-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp, path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def read(self, ref) -> bytes:
        return self.path(ref).read_bytes()


CameraFrameCache = MediaCache


def fetch_each(
    http: HttpClient,
    refs: list,
    path_for: Callable[[object], str],
    params: dict,
    record: Callable[[object, bytes], None],
    *,
    concurrency: int,
    context_for: Callable[[object], str],
) -> None:
    """Fetch one blob per ref with a bounded thread pool, recording each as it lands."""

    def fetch(ref):
        data = http.request_bytes(
            "GET", path_for(ref), params=params, context=context_for(ref)
        )
        return ref, data

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        futures = [pool.submit(fetch, ref) for ref in refs]
        try:
            for future in as_completed(futures):
                ref, data = future.result()
                record(ref, data)
        except BaseException:
            for future in futures:
                future.cancel()
            raise


@dataclass
class CameraDownloadStats:
    frames: int = 0
    from_cache: int = 0
    via_stream: int = 0
    via_per_frame: int = 0
    bytes: int = 0
    transports: set[str] = field(default_factory=set)

    def add(self, other: CameraDownloadStats) -> None:
        self.frames += other.frames
        self.from_cache += other.from_cache
        self.via_stream += other.via_stream
        self.via_per_frame += other.via_per_frame
        self.bytes += other.bytes
        self.transports |= other.transports


class _IterReader(io.RawIOBase):
    def __init__(self, chunks: Iterator[bytes]):
        self._chunks = chunks
        self._buffer = b""

    def readable(self) -> bool:
        return True

    def readinto(self, target) -> int:
        while not self._buffer:
            try:
                self._buffer = next(self._chunks)
            except StopIteration:
                return 0
        n = min(len(target), len(self._buffer))
        target[:n] = self._buffer[:n]
        self._buffer = self._buffer[n:]
        return n


def read_stream_page(
    chunks: Iterator[bytes], on_frame: Callable[[int, int, bytes], None]
) -> dict | None:
    """Parse one tar page of the stream route; its cursor, or None if truncated."""
    cursor = None
    reader = io.BufferedReader(_IterReader(chunks), buffer_size=1 << 16)
    try:
        with tarfile.open(fileobj=reader, mode="r|") as tar:
            for member in tar:
                if not member.isfile():
                    continue
                data = tar.extractfile(member).read()
                if len(data) != member.size:
                    return None
                if member.name == "_cursor.json":
                    cursor = json.loads(data)
                    continue
                match = _MEMBER_NAME.search(member.name)
                if match is None or match.group(3).lower() == "json":
                    continue
                on_frame(int(match.group(1)), int(match.group(2)), data)
    except (tarfile.TarError, EOFError, httpx.TransportError, json.JSONDecodeError):
        return None
    return cursor


def stream_into_cache(
    http: HttpClient,
    path: str,
    refs: list,
    cache: MediaCache,
    record: Callable[[object, bytes], None],
    *,
    project_id: int,
    page_limit: int,
    max_truncated_pages: int,
    context: str,
) -> None:
    """Page through a cursor-paged media stream (camera frames or lidar scans),
    recording every ref not cached yet. A truncated page resumes from the last whole
    entry; past ``max_truncated_pages`` in a row it gives up and leaves the rest to
    the caller. Raises StreamUnavailable when the API has no such route."""
    by_key = {(r.date, r.seq): r for r in refs}
    first_missing = next((i for i, r in enumerate(refs) if not cache.has(r)), None)
    if first_missing is None:
        return
    after = refs[first_missing - 1].cursor if first_missing > 0 else None
    last_received = after
    truncated = 0

    def on_entry(date: int, seq: int, data: bytes) -> None:
        nonlocal last_received
        ref = by_key.get((date, seq))
        if ref is not None and not cache.has(ref):
            record(ref, data)
        last_received = f"{date}:{seq}"

    while True:
        params = {"projectId": int(project_id), "limit": page_limit, "meta": "false"}
        if after:
            params["after"] = after
        try:
            with http.open_stream("GET", path, params=params, context=context) as resp:
                cursor = read_stream_page(resp.iter_bytes(), on_entry)
        except C3DNotFoundError as e:
            raise StreamUnavailable() from e
        except C3DAPIError:
            cursor = None

        if cursor is None:
            truncated += 1
            if truncated > max_truncated_pages:
                return
            after = last_received
            continue
        truncated = 0
        if cursor.get("complete"):
            return
        after = cursor.get("next")
        if not after:
            return


class CameraDownloader:
    def __init__(
        self,
        http: HttpClient,
        *,
        transport: str = "auto",
        concurrency: int = 4,
        page_limit: int = 1000,
        max_truncated_pages: int = 5,
        progress: bool = True,
    ):
        if transport not in TRANSPORTS:
            raise ValueError(f"camera_transport must be one of {TRANSPORTS}")
        self.http = http
        self.transport = transport
        self.concurrency = max(1, concurrency)
        self.page_limit = page_limit
        self.max_truncated_pages = max_truncated_pages
        self.progress = progress
        self._stream_supported: bool | None = None

    def download(
        self,
        cache: CameraFrameCache,
        project_id: int,
        session_id: str,
        cameras: dict[str, list[CameraFrameRef]],
    ) -> CameraDownloadStats:
        stats = CameraDownloadStats()
        for camera_id, refs in cameras.items():
            ordered = sorted(refs, key=lambda r: (r.version_id, r.date, r.seq))
            groups = [list(g) for _, g in groupby(ordered, key=lambda r: r.version_id)]
            for group in groups:
                desc = f"{session_id} {camera_id}"
                if len(groups) > 1:
                    desc += f" (v{group[0].version_id})"
                self._download_group(cache, project_id, session_id, group, desc, stats)
        return stats

    def _download_group(self, cache, project_id, session_id, refs, desc, stats):
        cached = sum(1 for r in refs if cache.has(r))
        stats.frames += len(refs)
        stats.from_cache += cached
        lock = threading.Lock()

        with progress_bar(
            total=len(refs),
            desc=desc,
            unit="frame",
            disable=not self.progress,
            initial=cached,
        ) as bar:
            if cached == len(refs):
                stats.transports.add("cache")
                return

            def record(ref: CameraFrameRef, data: bytes, via: str) -> None:
                cache.write(ref, data)
                with lock:
                    stats.bytes += len(data)
                    if via == "stream":
                        stats.via_stream += 1
                    else:
                        stats.via_per_frame += 1
                    bar.update(1)
                    bar.set_postfix_str(format_bytes(stats.bytes), refresh=False)

            use_stream = self.transport in ("auto", "stream")
            if use_stream and self._stream_supported is not False:
                try:
                    self._stream(cache, project_id, session_id, refs, record)
                    self._stream_supported = True
                    stats.transports.add("stream")
                except StreamUnavailable:
                    if self.transport == "stream":
                        raise
                    self._stream_supported = False

            missing = [r for r in refs if not cache.has(r)]
            if missing and self.transport in ("auto", "per-frame"):
                self._per_frame(project_id, session_id, missing, record)
                stats.transports.add("per-frame")

        still_missing = [r for r in refs if not cache.has(r)]
        if still_missing:
            first = still_missing[0]
            raise C3DIncompleteDownloadError(
                f"{len(still_missing)} of {len(refs)} frames of camera "
                f"{first.camera_id!r} could not be downloaded (first missing: "
                f"date={first.date} seq={first.seq}). Run again to resume."
            )

    def _stream(self, cache, project_id, session_id, refs, record) -> None:
        version_id, camera_id = refs[0].version_id, refs[0].camera_id
        stream_into_cache(
            self.http,
            f"/v0/versions/{int(version_id)}/sessions/{segment(session_id)}"
            f"/cameraFrames/{segment(camera_id)}/stream",
            refs,
            cache,
            lambda ref, data: record(ref, data, "stream"),
            project_id=project_id,
            page_limit=self.page_limit,
            max_truncated_pages=self.max_truncated_pages,
            context=f"camera {camera_id!r} stream",
        )

    def _per_frame(self, project_id, session_id, refs, record) -> None:
        fetch_each(
            self.http,
            refs,
            lambda ref: (
                f"/v0/versions/{int(ref.version_id)}/sessions/{segment(session_id)}"
                f"/cameraFrames/{segment(ref.camera_id)}/{ref.date}/{ref.seq}"
            ),
            {"projectId": int(project_id)},
            lambda ref, data: record(ref, data, "per-frame"),
            concurrency=self.concurrency,
            context_for=lambda ref: f"camera {ref.camera_id!r} frame {ref.cursor}",
        )
