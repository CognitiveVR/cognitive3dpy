"""In-memory form of a session's streams."""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np


@dataclass
class GazeSamples:
    """Head pose and gaze, one row per sample; times are session-relative seconds."""

    times: np.ndarray  # (N,)
    positions: np.ndarray  # (N, 3) head position
    rotations: np.ndarray  # (N, 4) head rotation (qx, qy, qz, qw)
    gaze_points: np.ndarray  # (N, 3) NaN where the gaze hit an object or is missing

    def __len__(self) -> int:
        return len(self.times)


@dataclass
class SensorChannel:
    name: str
    times: np.ndarray
    values: np.ndarray


@dataclass
class SessionEvent:
    name: str
    time: float
    point: list[float] | None
    properties: dict
    version_id: int | None


@dataclass(frozen=True)
class CameraFrameRef:
    version_id: int
    camera_id: str
    date: int  # capture time in epoch ms; with seq it addresses the frame on the API
    seq: int
    time: float  # session-relative seconds
    frame_id: str | None
    pose: tuple[float, ...]
    format: str
    width: int | None
    height: int | None

    @property
    def cursor(self) -> str:
        return f"{self.date}:{self.seq}"

    @property
    def stream_id(self) -> str:
        return self.camera_id

    @property
    def extension(self) -> str:
        return "jpg" if self.format.lower() in ("jpeg", "jpg") else self.format.lower()


@dataclass(frozen=True)
class LidarScanRef:
    """One lidar scan's index row; its ranges or points come from the blob route."""

    version_id: int
    sensor_id: str
    kind: str  # "laserscan" | "pointcloud"
    date: int  # capture time in epoch ms; with seq it addresses the scan on the API
    seq: int
    time: float  # session-relative seconds
    frame_id: str | None
    pose: tuple[float, ...]
    range_count: int = 0  # laserscan
    has_intensities: bool = False  # laserscan
    angle_min: float = 0.0  # laserscan, radians
    angle_increment: float = 0.0  # laserscan, radians
    range_min: float = 0.0  # laserscan, metres
    range_max: float = float("inf")  # laserscan, metres
    angle_max: float | None = None  # laserscan, radians
    time_increment: float = 0.0  # laserscan, seconds between beams
    scan_time: float = 0.0  # laserscan, seconds between scans
    point_count: int = 0  # pointcloud
    position_format: str = ""  # pointcloud: int16_mm | float32
    color_format: str = "none"  # pointcloud: none | rgb565 | rgb332 | rgb888
    intensity_format: str = "none"  # pointcloud: none | uint8
    zstd_level: int = 0  # pointcloud

    @property
    def cursor(self) -> str:
        return f"{self.date}:{self.seq}"

    @property
    def stream_id(self) -> str:
        return self.sensor_id

    @property
    def extension(self) -> str:
        return "bin"


@dataclass
class BoundaryData:
    """Play-area boundary: its pose over time and the polygons it took."""

    times: np.ndarray  # (N,) session-relative seconds
    poses: np.ndarray  # (N, 7) px, py, pz, qx, qy, qz, qw
    shape_times: np.ndarray  # (M,)
    shapes: list[np.ndarray]  # M polygons, each (K, 3)
    extras: dict  # roomManifest / roomData as exported, when present


@dataclass
class SessionData:
    project_id: int
    session_id: str
    start_timestamp: float  # epoch seconds
    duration: float
    format_version: str | None
    metadata: dict
    gaze: GazeSamples | None
    sensors: dict[str, SensorChannel] = field(default_factory=dict)
    events: list[SessionEvent] = field(default_factory=list)
    cameras: dict[str, list[CameraFrameRef]] = field(default_factory=dict)
    lidar: dict[str, list[LidarScanRef]] = field(default_factory=dict)
    boundary: BoundaryData | None = None
    properties: dict = field(default_factory=dict)
    raw: dict[str, bytes] = field(default_factory=dict)  # section -> response body

    @property
    def streams(self) -> set[str]:
        present = set()
        if self.gaze is not None and len(self.gaze):
            present |= {"head", "gaze"}
        if self.sensors:
            present.add("sensors")
        if self.events:
            present.add("events")
        if self.cameras:
            present.add("camera")
        if self.lidar:
            present.add("lidar")
        if self.boundary is not None and len(self.boundary.times):
            present.add("boundary")
        return present
