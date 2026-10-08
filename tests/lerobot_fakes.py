"""In-process fake of the Cognitive3D API for the session downloader tests.

Shaped like the real responses: sections keyed by scene version, session-relative
times, camera and lidar index rows with epoch-ms dates, tar pages from the camera
stream route and lidar blobs in the contract's layouts.
"""

import io
import json
import re
import tarfile
from collections import Counter
from dataclasses import dataclass, field
from urllib.parse import unquote

import httpx
import numpy as np
import zstandard
from PIL import Image

API_KEY = "test-api-key"


def jpeg(seed: int, size=(16, 12)) -> bytes:
    rng = np.random.default_rng(seed)
    pixels = rng.integers(0, 255, (size[1], size[0], 3), dtype=np.uint8)
    buffer = io.BytesIO()
    Image.fromarray(pixels).save(buffer, format="JPEG", quality=90)
    return buffer.getvalue()


@dataclass
class FakeSession:
    project_id: int
    session_id: str
    start: float
    docs: dict  # jsonType -> response document
    frames: dict  # (version, camera, date, seq) -> jpeg bytes
    lidar_blobs: dict = field(default_factory=dict)  # (version, sensor, date, seq)


LASER_RANGES = 8
LASER_MIN, LASER_MAX = 0.1, 10.0


def laserscan_ranges(seq: int) -> np.ndarray:
    """Beam 2 never returns (inf), beam 5 reads past range_max."""
    ranges = np.linspace(1.0, 2.0, LASER_RANGES, dtype=np.float32) + 0.2 * seq
    ranges[2] = np.inf
    ranges[5] = 20.0
    return ranges


def cloud_points(seq: int):
    """(positions in metres, int16 millimetres, rgb565 words, expected uint8 rgb)."""
    n = 5 + seq
    mm = (np.arange(n * 3, dtype=np.int16).reshape(n, 3) * 10 + seq).astype(np.int16)
    words = np.array([0xF800, 0x07E0, 0x001F, 0xFFFF, 0x0000] * n, dtype=np.uint16)[:n]
    rgb = np.array([[255, 0, 0], [0, 255, 0], [0, 0, 255], [255, 255, 255], [0, 0, 0]])
    rgb = np.resize(rgb, (n, 3)).astype(np.uint8)
    return mm.astype(np.float32) / 1000.0, mm, words, rgb


def pointcloud_blob(seq: int) -> bytes:
    _, mm, words, _ = cloud_points(seq)
    planar = mm.T.astype("<i2").tobytes() + words.astype("<u2").tobytes()
    return zstandard.ZstdCompressor(level=1).compress(planar)


def make_session(
    session_id: str = "1700000000_test-session",
    *,
    project_id: int = 1234,
    start: float = 1_700_000_000.5,
    gaze_hz: float = 10.0,
    duration: float = 10.0,
    camera_hz: float = 2.0,
    cameras: tuple = ("front",),
    version_id: int = 42,
    sensors: bool = True,
    events: bool = True,
    camera_size=(16, 12),
    lidar: bool = False,
    boundary: bool = False,
) -> FakeSession:
    vkey = str(version_id)
    gaze_rows = []
    for i, t in enumerate(np.round(np.arange(0.05, duration, 1 / gaze_hz), 3)):
        row = {
            "p": [0.1 * i, 1.5, -0.2 * i],
            "versionId": version_id,
            "r": [0.0, 0.38268343, 0.0, 0.92387953],
            "g": [1.0 + i, 1.4, 2.0],
            "time": float(t),
        }
        if i % 10 == 5:
            row["o"] = "obj-7"  # looking at an object: no free-space gaze point
        gaze_rows.append(row)
    header = {
        "duration": duration,
        "formatversion": "artificial 2.0",
        "sessionid": session_id,
        "timestamp": start,
        "metadata": {"hmd": "test-hmd"},
        "properties": {},
    }
    docs = {"GAZE": {**header, "data": {vkey: gaze_rows}}}
    if sensors:
        channels = [
            # unsorted on purpose, like real exports can be
            {
                "versionId": version_id,
                "name": "battery.percent",
                "data": [[5.0, 90.0], [1.0, 99.0], [3.0, 95.0]],
            },
            {
                "versionId": version_id,
                "name": "odom.speed",
                "data": [[0.0, 0.5], [6.0, 1.5]],
            },
        ]
        docs["SENSORS"] = {**header, "data": {vkey: channels}}
    if events:
        rows = [
            {
                "versionId": version_id,
                "name": "c3d.sessionStart",
                "time": 0.0,
                "point": [0, 0, 0],
            },
            {
                "versionId": version_id,
                "name": "pick.start",
                "time": 2.2,
                "point": [1, 0, 0],
                "properties": {"item": "box"},
            },
            {
                "versionId": version_id,
                "name": "pick.done",
                "time": 4.9,
                "point": [2, 0, 0],
            },
        ]
        docs["EVENTS"] = {**header, "data": {vkey: rows}}
    frames = {}
    if cameras:
        rows = []
        for c_index, camera in enumerate(cameras):
            offset = 0.01 * (c_index + 1)
            times = np.round(np.arange(0.0, duration, 1 / camera_hz) + offset, 3)
            for seq, t in enumerate(times):
                date = int(round((start + t) * 1000))
                rows.append(
                    {
                        "date": date,
                        "versionId": version_id,
                        "cameraId": camera,
                        "pose": [0.5 + seq, 0.25, 0.0, -0.5, 0.5, 0.5, 0.5],
                        "frameId": "optical_frame",
                        "format": "jpeg",
                        "width": camera_size[0],
                        "height": camera_size[1],
                        "time": float(t),
                        "seq": seq,
                    }
                )
                seed = seq * 131 + sum(map(ord, camera))
                frames[(version_id, camera, date, seq)] = jpeg(seed, camera_size)
        docs["CAMERA"] = {**header, "cameraFrames": {vkey: rows}}
    lidar_blobs = {}
    if lidar:
        rows = []
        for seq, t in enumerate(np.round(np.arange(0.3, duration, 1 / 3.0), 3)):
            date = int(round((start + t) * 1000))
            rows.append(
                {
                    "date": date,
                    "versionId": version_id,
                    "pose": [0.04 + seq, 0.19, 0.0, 0.5, 0.5, -0.5, 0.5],
                    "frameId": "rplidar",
                    "kind": "laserscan",
                    "time": float(t),
                    "laserscan": {
                        "angleIncrement": 2 * np.pi / LASER_RANGES,
                        "rangeCount": LASER_RANGES,
                        "angleMax": np.pi,
                        "hasIntensities": False,
                        "rangeMax": LASER_MAX,
                        "scanTime": 0.0,
                        "rangeMin": LASER_MIN,
                        "angleMin": -np.pi,
                        "timeIncrement": 0.0,
                    },
                    "seq": seq,
                    "sensorId": "scan",
                }
            )
            key = (version_id, "scan", date, seq)
            lidar_blobs[key] = laserscan_ranges(seq).astype("<f4").tobytes()
        for seq, t in enumerate(np.round(np.arange(1.2, duration, 1.0), 3)):
            date = int(round((start + t) * 1000))
            rows.append(
                {
                    "date": date,
                    "versionId": version_id,
                    "pose": [1.0, 0.25, -2.0 + seq, 0.0, -0.69, 0.0, 0.72],
                    "frameId": "depth_frame",
                    "kind": "pointcloud",
                    "time": float(t),
                    "pointcloud": {
                        "positionFormat": "int16_mm",
                        "zstdLevel": 1,
                        "pointCount": 5 + seq,
                        "intensityFormat": "none",
                        "colorFormat": "rgb565",
                        "voxelLeafMm": 50,
                    },
                    "seq": seq,
                    "sensorId": "depth",
                }
            )
            lidar_blobs[(version_id, "depth", date, seq)] = pointcloud_blob(seq)
        docs["LIDAR"] = {**header, "lidarScans": {vkey: rows}}
    if boundary:
        poses = [
            {
                "p": [0.5 * i, 0.0, -1.0],
                "r": [0.0, 0.38268343, 0.0, 0.92387953],
                "time": t,
            }
            for i, t in enumerate([2.0, 4.0, 6.0])
        ]
        square = [
            [-1.0, 0.0, -1.0],
            [-1.0, 0.0, 1.0],
            [1.0, 0.0, 1.0],
            [1.0, 0.0, -1.0],
        ]
        shapes = [  # out of time order, like real exports
            {"time": 5.0, "points": [[2 * x, y, 2 * z] for x, y, z in square]},
            {"time": 1.5, "points": square},
        ]
        docs["BOUNDARY"] = {**header, "data": {vkey: poses}, "shapes": {vkey: shapes}}
    return FakeSession(project_id, session_id, start, docs, frames, lidar_blobs)


def tar_page(entries: list) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for name, data in entries:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


@dataclass
class FakeEdge:
    key: str = API_KEY
    stream_available: bool = True
    lidar_stream_available: bool = True
    sessions: dict = field(default_factory=dict)
    calls: Counter = field(default_factory=Counter)
    truncate_streams: int = 0  # cut this many stream pages short
    truncate_lidar_streams: int = 0
    fail_first: dict = field(default_factory=dict)  # route kind -> statuses first
    auth_outage: int = 0  # answer 401 to this many valid requests

    def add(self, session: FakeSession) -> FakeSession:
        self.sessions[(session.project_id, session.session_id)] = session
        return session

    def handle(self, request: httpx.Request) -> httpx.Response:
        if request.headers.get("Authorization") != self.key:
            return httpx.Response(401)
        path = request.url.path
        kind = self._kind(path)
        self.calls[kind] += 1
        if self.auth_outage > 0:
            self.auth_outage -= 1
            return httpx.Response(401)
        queued = self.fail_first.get(kind)
        if queued:
            return httpx.Response(queued.pop(0), headers={"Retry-After": "0"})
        handler = {
            "json": self._json,
            "stream": self._stream,
            "frame": self._frame,
            "lidar": self._lidar,
            "lidar_stream": self._lidar_stream,
        }
        if kind in handler:
            return handler[kind](request, path)
        return httpx.Response(404)

    @staticmethod
    def _kind(path: str) -> str:
        if path.endswith("/jsonRequests"):
            return "json"
        if "/lidarScans/" in path and path.endswith("/stream"):
            return "lidar_stream"
        if path.endswith("/stream"):
            return "stream"
        if "/cameraFrames/" in path:
            return "frame"
        if "/lidarScans/" in path:
            return "lidar"
        return "other"

    def _json(self, request, path):
        m = re.match(r"^/v0/projects/(\d+)/sessions/([^/]+)/jsonRequests$", path)
        session = self.sessions.get((int(m.group(1)), unquote(m.group(2))))
        body = json.loads(request.content)
        assert "sessionId" not in body, "the real API rejects sessionId in the body"
        doc = session.docs.get(body["jsonType"]) if session else None
        if doc is None:
            return httpx.Response(404, text="No data found for the requested json file")
        return httpx.Response(200, json=doc)

    def _stream(self, request, path):
        if not self.stream_available:
            return httpx.Response(404)
        pattern = r"^/v0/versions/(\d+)/sessions/([^/]+)/cameraFrames/([^/]+)/stream$"
        m = re.match(pattern, path)
        version = int(m.group(1))
        session_id, camera = unquote(m.group(2)), unquote(m.group(3))
        params = request.url.params
        session = self.sessions.get((int(params["projectId"]), session_id))
        frames = session.frames if session else {}
        keys = sorted(k for k in frames if k[0] == version and k[1] == camera)
        if "after" in params:
            after = tuple(int(x) for x in params["after"].split(":"))
            keys = [k for k in keys if (k[2], k[3]) > after]
        limit = int(params.get("limit", 1000))
        page, rest = keys[:limit], keys[limit:]
        entries = [(f"{camera}/{k[2]:013d}_{k[3]:010d}.jpg", frames[k]) for k in page]
        cursor = {
            "frames": len(page),
            "next": f"{page[-1][2]}:{page[-1][3]}" if rest else None,
            "complete": not rest,
        }
        body = tar_page(entries + [("_cursor.json", json.dumps(cursor).encode())])
        if self.truncate_streams > 0 and len(page) > 1:
            self.truncate_streams -= 1
            # first frame whole, the second cut mid-way
            body = body[: 512 * 2 + len(entries[0][1]) + 512 + 100]
        return httpx.Response(
            200, content=body, headers={"Content-Type": "application/x-tar"}
        )

    def _frame(self, request, path):
        pattern = (
            r"^/v0/versions/(\d+)/sessions/([^/]+)/cameraFrames/([^/]+)/(\d+)/(-?\d+)$"
        )
        m = re.match(pattern, path)
        session_id, camera = unquote(m.group(2)), unquote(m.group(3))
        session = self.sessions.get((int(request.url.params["projectId"]), session_id))
        key = (int(m.group(1)), camera, int(m.group(4)), int(m.group(5)))
        data = session.frames.get(key) if session else None
        if data is None:
            return httpx.Response(404)
        return httpx.Response(200, content=data, headers={"Content-Type": "image/jpeg"})

    def _lidar(self, request, path):
        pattern = (
            r"^/v0/versions/(\d+)/sessions/([^/]+)/lidarScans/([^/]+)/(\d+)/(-?\d+)$"
        )
        m = re.match(pattern, path)
        session_id, sensor = unquote(m.group(2)), unquote(m.group(3))
        session = self.sessions.get((int(request.url.params["projectId"]), session_id))
        key = (int(m.group(1)), sensor, int(m.group(4)), int(m.group(5)))
        data = session.lidar_blobs.get(key) if session else None
        if data is None:
            return httpx.Response(404)
        headers = {"Content-Type": "application/octet-stream"}
        return httpx.Response(200, content=data, headers=headers)

    def _lidar_stream(self, request, path):
        if not self.lidar_stream_available:
            return httpx.Response(404)
        pattern = r"^/v0/versions/(\d+)/sessions/([^/]+)/lidarScans/([^/]+)/stream$"
        m = re.match(pattern, path)
        version = int(m.group(1))
        session_id, sensor = unquote(m.group(2)), unquote(m.group(3))
        params = request.url.params
        session = self.sessions.get((int(params["projectId"]), session_id))
        blobs = session.lidar_blobs if session else {}
        keys = sorted(k for k in blobs if k[0] == version and k[1] == sensor)
        if "after" in params:
            after = tuple(int(x) for x in params["after"].split(":"))
            keys = [k for k in keys if (k[2], k[3]) > after]
        limit = int(params.get("limit", 1000))
        page, rest = keys[:limit], keys[limit:]
        entries = [(f"{sensor}/{k[2]:013d}_{k[3]:010d}.bin", blobs[k]) for k in page]
        cursor = {
            "scans": len(page),
            "next": f"{page[-1][2]}:{page[-1][3]}" if rest else None,
            "complete": not rest,
        }
        body = tar_page(entries + [("_cursor.json", json.dumps(cursor).encode())])
        if self.truncate_lidar_streams > 0 and len(page) > 1:
            self.truncate_lidar_streams -= 1
            # first entry whole, then cut inside the second entry's payload
            first = 512 + -(-len(entries[0][1]) // 512) * 512
            body = body[: first + 512 + max(1, min(10, len(entries[1][1]) - 1))]
        headers = {"Content-Type": "application/x-tar"}
        return httpx.Response(200, content=body, headers=headers)
