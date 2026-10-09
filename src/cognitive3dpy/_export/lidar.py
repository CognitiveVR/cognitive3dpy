"""Lidar scans: per-scan blob download and decoding.

Blob layouts (the camera/lidar replay contract, as cvr-session-replay decodes them):

- laserscan: ``rangeCount`` little-endian float32 ranges, then as many float32
  intensities only when ``hasIntensities``. The length must be exactly 4N or 8N.
- pointcloud: zstd-compressed when ``zstdLevel > 0``; then planar ``X[N] Y[N] Z[N]``
  (int16 millimetres or float32 metres), ``C[N]`` colour (rgb565, rgb332, rgb888 or
  none) and ``I[N]`` uint8 intensity (or none), all little-endian.

Points are sensor-local and already in the scene axis convention (left-handed,
Y-up); the world point is ``rotate(pose.q, p) + pose.position``.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from itertools import groupby

import numpy as np

from cognitive3dpy._export.camera import (
    MediaCache,
    StreamUnavailable,
    fetch_each,
    stream_into_cache,
)
from cognitive3dpy._export.errors import C3DIncompleteDownloadError
from cognitive3dpy._export.http import HttpClient, segment
from cognitive3dpy._export.models import LidarScanRef
from cognitive3dpy._export.progress import format_bytes, progress_bar

TRANSPORTS = ("auto", "stream", "per-scan")
POSITION_BYTES = {"int16_mm": 2, "float32": 4}
COLOR_BYTES = {"none": 0, "rgb565": 2, "rgb332": 1, "rgb888": 3}
INTENSITY_BYTES = {"none": 0, "uint8": 1}


class LidarDecodeError(ValueError):
    """A scan blob does not match its index row."""


@dataclass
class DecodedCloud:
    positions: np.ndarray  # (N, 3) float32 metres, sensor-local, scene convention
    colors: np.ndarray | None  # (N, 3) uint8
    intensities: np.ndarray | None  # (N,) uint8


def needs_blob(ref: LidarScanRef) -> bool:
    if ref.kind == "laserscan":
        return ref.range_count > 0
    return ref.point_count > 0


def decode_laserscan(blob: bytes, ref: LidarScanRef):
    """Return ``(ranges, intensities or None)`` as float32, values as stored."""
    count = ref.range_count
    expected = count * 4 * (2 if ref.has_intensities else 1)
    if count <= 0 or len(blob) != expected:
        raise LidarDecodeError(
            f"laserscan {ref.sensor_id} {ref.cursor}: {len(blob)} bytes, "
            f"expected {expected}"
        )
    values = np.frombuffer(blob, dtype="<f4").astype(np.float32)
    intensities = values[count:] if ref.has_intensities else None
    return values[:count], intensities


def decode_pointcloud(blob: bytes, ref: LidarScanRef) -> DecodedCloud:
    count = ref.point_count
    position_bytes = POSITION_BYTES.get(ref.position_format)
    color_bytes = COLOR_BYTES.get(ref.color_format or "none")
    intensity_bytes = INTENSITY_BYTES.get(ref.intensity_format or "none")
    if position_bytes is None or color_bytes is None or intensity_bytes is None:
        raise LidarDecodeError(
            f"pointcloud {ref.sensor_id} {ref.cursor}: unsupported format "
            f"{ref.position_format}/{ref.color_format}/{ref.intensity_format}"
        )
    needed = count * (position_bytes * 3 + color_bytes + intensity_bytes)
    block = blob
    if ref.zstd_level > 0:
        import zstandard

        try:
            block = zstandard.ZstdDecompressor().decompress(
                blob, max_output_size=max(needed, 1)
            )
        except zstandard.ZstdError as e:
            raise LidarDecodeError(
                f"pointcloud {ref.sensor_id} {ref.cursor}: zstd: {e}"
            ) from e
    if count <= 0 or len(block) < needed:
        raise LidarDecodeError(
            f"pointcloud {ref.sensor_id} {ref.cursor}: {len(block)} bytes, "
            f"expected {needed}"
        )

    end = count * position_bytes * 3
    if ref.position_format == "int16_mm":
        axes = np.frombuffer(block, dtype="<i2", count=count * 3).astype(np.float32)
        axes /= 1000.0
    else:
        axes = np.frombuffer(block, dtype="<f4", count=count * 3).astype(np.float32)
    positions = np.ascontiguousarray(axes.reshape(3, count).T)

    colors = None
    if color_bytes:
        raw = block[end : end + count * color_bytes]
        colors = _unpack_colors(raw, ref.color_format, count)
        end += count * color_bytes

    intensities = None
    if intensity_bytes:
        intensities = np.frombuffer(block, dtype=np.uint8, count=count, offset=end)
        intensities = intensities.copy()

    return DecodedCloud(positions=positions, colors=colors, intensities=intensities)


def _unpack_colors(raw: bytes, color_format: str, count: int) -> np.ndarray:
    if color_format == "rgb888":
        return np.frombuffer(raw, dtype=np.uint8).reshape(count, 3).copy()
    if color_format == "rgb565":
        v = np.frombuffer(raw, dtype="<u2").astype(np.uint32)
        channels = ((v >> 11) & 0x1F, 31), ((v >> 5) & 0x3F, 63), (v & 0x1F, 31)
    else:  # rgb332
        v = np.frombuffer(raw, dtype=np.uint8).astype(np.uint32)
        channels = ((v >> 5) & 0x07, 7), ((v >> 2) & 0x07, 7), (v & 0x03, 3)
    scaled = [np.rint(c * 255.0 / top) for c, top in channels]
    return np.stack(scaled, axis=1).astype(np.uint8)


@dataclass
class LidarDownloadStats:
    scans: int = 0
    from_cache: int = 0
    downloaded: int = 0
    via_stream: int = 0
    via_per_scan: int = 0
    bytes: int = 0
    sensors: set[str] = field(default_factory=set)
    transports: set[str] = field(default_factory=set)

    def add(self, other: LidarDownloadStats) -> None:
        self.scans += other.scans
        self.from_cache += other.from_cache
        self.downloaded += other.downloaded
        self.via_stream += other.via_stream
        self.via_per_scan += other.via_per_scan
        self.bytes += other.bytes
        self.sensors |= other.sensors
        self.transports |= other.transports


class LidarDownloader:
    """Scans come from the cursor-paged stream route; deployments without it are
    fetched one request per scan, and so is anything the stream left missing."""

    def __init__(
        self,
        http: HttpClient,
        *,
        transport: str = "auto",
        concurrency: int = 4,
        page_limit: int = 5000,
        max_truncated_pages: int = 5,
        progress: bool = True,
    ):
        if transport not in TRANSPORTS:
            raise ValueError(f"lidar_transport must be one of {TRANSPORTS}")
        self.http = http
        self.transport = transport
        self.concurrency = max(1, concurrency)
        self.page_limit = page_limit
        self.max_truncated_pages = max_truncated_pages
        self.progress = progress
        self._stream_supported: bool | None = None

    def download(
        self,
        cache: MediaCache,
        project_id: int,
        session_id: str,
        sensors: dict[str, list[LidarScanRef]],
    ) -> LidarDownloadStats:
        stats = LidarDownloadStats()
        for sensor_id, refs in sensors.items():
            stats.sensors.add(sensor_id)
            wanted = sorted(
                (r for r in refs if needs_blob(r)),
                key=lambda r: (r.version_id, r.date, r.seq),
            )
            missing = [r for r in wanted if not cache.has(r)]
            stats.scans += len(wanted)
            stats.from_cache += len(wanted) - len(missing)
            if wanted and not missing:
                stats.transports.add("cache")
            lock = threading.Lock()
            desc = f"{session_id} lidar {sensor_id}"
            with progress_bar(
                total=len(wanted),
                desc=desc,
                unit="scan",
                disable=not self.progress,
                initial=len(wanted) - len(missing),
            ) as bar:

                def record(ref: LidarScanRef, data: bytes, via: str) -> None:
                    cache.write(ref, data)
                    with lock:
                        stats.downloaded += 1
                        stats.bytes += len(data)
                        if via == "stream":
                            stats.via_stream += 1
                        else:
                            stats.via_per_scan += 1
                        bar.update(1)
                        bar.set_postfix_str(format_bytes(stats.bytes), refresh=False)

                if missing:
                    stats.transports |= self._fetch(
                        project_id, session_id, wanted, cache, record
                    )

            still_missing = [r for r in wanted if not cache.has(r)]
            if still_missing:
                raise C3DIncompleteDownloadError(
                    f"{len(still_missing)} of {len(wanted)} scans of lidar "
                    f"{sensor_id!r} could not be downloaded. Run again to resume."
                )
        return stats

    def _fetch(self, project_id, session_id, wanted, cache, record) -> set[str]:
        """Fetch what is not cached yet; returns the transports that were used."""
        used: set[str] = set()
        use_stream = self.transport in ("auto", "stream")
        if use_stream and self._stream_supported is not False:
            try:
                for _, group in groupby(wanted, key=lambda r: r.version_id):
                    group = list(group)
                    first = group[0]
                    stream_into_cache(
                        self.http,
                        f"/v0/versions/{int(first.version_id)}/sessions/"
                        f"{segment(session_id)}/lidarScans/{segment(first.sensor_id)}"
                        "/stream",
                        group,
                        cache,
                        lambda ref, data: record(ref, data, "stream"),
                        project_id=project_id,
                        page_limit=self.page_limit,
                        max_truncated_pages=self.max_truncated_pages,
                        context=f"lidar {first.sensor_id!r} stream",
                    )
                self._stream_supported = True
                used.add("stream")
            except StreamUnavailable:
                if self.transport == "stream":
                    raise
                self._stream_supported = False

        missing = [r for r in wanted if not cache.has(r)]
        if missing and self.transport in ("auto", "per-scan"):
            fetch_each(
                self.http,
                missing,
                lambda ref: (
                    f"/v0/versions/{int(ref.version_id)}/sessions/"
                    f"{segment(session_id)}/lidarScans/{segment(ref.sensor_id)}"
                    f"/{ref.date}/{ref.seq}"
                ),
                {"projectId": int(project_id)},
                lambda ref, data: record(ref, data, "per-scan"),
                concurrency=self.concurrency,
                context_for=lambda ref: f"lidar {ref.sensor_id!r} scan {ref.cursor}",
            )
            used.add("per-scan")
        return used
