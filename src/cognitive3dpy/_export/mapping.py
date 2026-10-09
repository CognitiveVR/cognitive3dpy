"""Map a session's streams onto LeRobot features and one frame grid."""

from __future__ import annotations

import io
import re
import warnings
from collections.abc import Callable
from dataclasses import dataclass, field

import numpy as np
from PIL import Image

from cognitive3dpy._export.camera import MediaCache
from cognitive3dpy._export.coords import (
    convert_poses,
    convert_positions,
    convert_quaternions,
    convert_scan_poses,
)
from cognitive3dpy._export.errors import C3DDatasetError
from cognitive3dpy._export.lidar import LidarDecodeError, decode_laserscan
from cognitive3dpy._export.models import CameraFrameRef, SessionData
from cognitive3dpy._export.timeline import (
    derive_fps,
    event_index_per_frame,
    hold_indices,
    parse_clock,
    sample_and_hold,
)

STREAMS = ("head", "gaze", "sensors", "events", "camera", "lidar", "boundary")

STATE_KEY = "observation.state"
GAZE_KEY = "observation.gaze"
SENSORS_KEY = "observation.sensors"
EVENT_KEY = "observation.event"
TIME_KEY = "observation.c3d_time_ms"
BOUNDARY_POSE_KEY = "observation.boundary.pose"
BOUNDARY_SHAPE_KEY = "observation.boundary.shape_index"
POSE_NAMES = ["px", "py", "pz", "qx", "qy", "qz", "qw"]


@dataclass
class SessionCaches:
    camera: MediaCache
    lidar: MediaCache


CacheFor = Callable[[SessionData], SessionCaches]


def _feature_name(camera_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "_", camera_id)


def lidar_key(sensor_id: str, field_name: str) -> str:
    return f"observation.lidar.{_feature_name(sensor_id)}.{field_name}"


def camera_feature_key(camera_id: str) -> str:
    return f"observation.images.{_feature_name(camera_id)}"


def camera_pose_key(camera_id: str) -> str:
    return f"observation.camera.{_feature_name(camera_id)}.pose"


def obs_name(feature_key: str) -> str:
    """A feature's name without the LeRobot prefix, for formats with flat
    observation keys: ``observation.images.front`` -> ``front_image``,
    ``observation.lidar.scan.ranges`` -> ``lidar_scan_ranges``."""
    parts = feature_key.split(".")[1:]
    if parts[0] == "images":
        return f"{parts[1]}_image"
    if parts[0] == "camera":
        return f"{parts[1]}_pose"
    return "_".join(parts)


def next_state_actions(states: np.ndarray) -> np.ndarray:
    """Actions for formats that need one: the next step's state (the last step
    keeps its own), i.e. the pose the head moves to."""
    if len(states) == 0:
        return states.copy()
    return np.concatenate([states[1:], states[-1:]]).astype(states.dtype)


@dataclass
class MappingOptions:
    streams: set[str] | None = None
    sensors: list[str] | None = None
    clock: str | None = None
    fps: int | None = None
    coord: str = "rh_yup"
    head_rotation: bool = False


@dataclass
class LidarLayout:
    kind: str  # "laserscan" | "pointcloud"
    range_count: int = 0
    has_intensities: bool = False
    angle_min: float = 0.0
    angle_increment: float = 0.0
    range_min: float = 0.0
    range_max: float = float("inf")


@dataclass
class DatasetLayout:
    streams: list[str]
    fps: int
    clock: tuple[str, str | float | None]
    sensor_names: list[str] = field(default_factory=list)
    event_names: list[str] = field(default_factory=list)
    cameras: dict[str, tuple[int, int]] = field(default_factory=dict)  # id -> (h, w)
    lidar: dict[str, LidarLayout] = field(default_factory=dict)
    boundary: bool = False
    features: dict = field(default_factory=dict)


@dataclass
class EpisodePlan:
    session: SessionData
    times: np.ndarray  # session-relative seconds, one per frame
    numeric: dict[str, np.ndarray]
    images: dict[str, list[CameraFrameRef]]
    skipped_scans: dict[str, int] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.times)


def resolve_streams(sessions: list[SessionData], requested: set[str] | None) -> list:
    for session in sessions:
        if "head" not in session.streams:
            raise C3DDatasetError(
                f"Session {session.session_id} has no head/gaze data, which every "
                "dataset needs."
            )
    if requested is not None:
        unknown = set(requested) - set(STREAMS)
        if unknown:
            raise ValueError(
                f"Unknown stream(s) {sorted(unknown)}; choose from {list(STREAMS)}."
            )
        chosen = set(requested) | {"head"}
        for session in sessions:
            missing = chosen - session.streams
            if missing:
                raise C3DDatasetError(
                    f"Session {session.session_id} has no {sorted(missing)} data; "
                    "leave it out of streams."
                )
        return [s for s in STREAMS if s in chosen]
    common = set.intersection(*(s.streams for s in sessions))
    return [s for s in STREAMS if s in common]


def build_layout(
    sessions: list[SessionData], options: MappingOptions, cache_for: CacheFor
) -> DatasetLayout:
    """Camera frames must already be downloaded: image shapes come from them."""
    streams = resolve_streams(sessions, options.streams)
    cameras: list[str] = []
    if "camera" in streams:
        cameras = sorted(set.intersection(*(set(s.cameras) for s in sessions)))
        if not cameras:
            raise C3DDatasetError(
                "The sessions share no camera id, so they cannot form one dataset; "
                "leave 'camera' out of streams."
            )

    kind, arg = parse_clock(options.clock)
    if kind == "auto":
        kind, arg = ("camera", cameras[0]) if cameras else ("gaze", None)
    if kind == "camera":
        if "camera" not in streams:
            raise C3DDatasetError("clock 'camera' needs the camera stream.")
        arg = arg or cameras[0]
        if arg not in cameras:
            raise C3DDatasetError(
                f"Camera {arg!r} is not in every session; available: {cameras}."
            )

    sensor_names: list[str] = []
    if "sensors" in streams:
        available = sorted(set().union(*(s.sensors.keys() for s in sessions)))
        if options.sensors:
            unknown = [n for n in options.sensors if n not in available]
            if unknown:
                raise C3DDatasetError(
                    f"Unknown sensor channel(s) {unknown}; available: {available}."
                )
            sensor_names = list(dict.fromkeys(options.sensors))
        else:
            sensor_names = available

    event_names: list[str] = []
    if "events" in streams:
        event_names = sorted({e.name for s in sessions for e in s.events})

    camera_shapes: dict[str, tuple[int, int]] = {}
    for camera_id in cameras:
        first = sessions[0].cameras[camera_id][0]
        data = cache_for(sessions[0]).camera.read(first)
        with Image.open(io.BytesIO(data)) as image:
            camera_shapes[camera_id] = (image.height, image.width)

    lidar = _lidar_layout(sessions) if "lidar" in streams else {}

    if options.fps:
        fps = int(options.fps)
    elif kind == "fixed":
        fps = max(1, round(float(arg)))
    else:
        fps = derive_fps(_clock_times(sessions[0], kind, arg, None))

    layout = DatasetLayout(
        streams=streams,
        fps=fps,
        clock=(kind, arg),
        sensor_names=sensor_names,
        event_names=event_names,
        cameras=camera_shapes,
        lidar=lidar,
        boundary="boundary" in streams,
    )
    layout.features = _features(layout, options)
    return layout


def _lidar_layout(sessions: list[SessionData]) -> dict[str, LidarLayout]:
    common = sorted(set.intersection(*(set(s.lidar) for s in sessions)))
    if not common:
        raise C3DDatasetError(
            "The sessions share no lidar sensor id, so they cannot form one dataset; "
            "leave 'lidar' out of streams."
        )
    layouts = {}
    for sensor_id in common:
        refs = [r for s in sessions for r in s.lidar[sensor_id]]
        kinds = {r.kind for r in refs}
        if len(kinds) != 1:
            raise C3DDatasetError(
                f"Lidar {sensor_id!r} is a laserscan in some scans and a pointcloud in "
                "others."
            )
        kind = kinds.pop()
        if kind == "pointcloud":
            layouts[sensor_id] = LidarLayout(kind=kind)
            continue
        first = refs[0]
        layouts[sensor_id] = LidarLayout(
            kind=kind,
            range_count=max(r.range_count for r in refs),
            has_intensities=any(r.has_intensities for r in refs),
            angle_min=first.angle_min,
            angle_increment=first.angle_increment,
            range_min=first.range_min,
            range_max=first.range_max,
        )
    return layouts


def _features(layout: DatasetLayout, options: MappingOptions) -> dict:
    head_names = ["head.x", "head.y", "head.z"]
    if options.head_rotation:
        head_names += ["head.qx", "head.qy", "head.qz", "head.qw"]
    features = {
        STATE_KEY: {
            "dtype": "float32",
            "shape": (len(head_names),),
            "names": head_names,
        }
    }
    if "gaze" in layout.streams:
        features[GAZE_KEY] = {
            "dtype": "float32",
            "shape": (3,),
            "names": ["gaze.x", "gaze.y", "gaze.z"],
        }
    if layout.sensor_names:
        features[SENSORS_KEY] = {
            "dtype": "float32",
            "shape": (len(layout.sensor_names),),
            "names": list(layout.sensor_names),
        }
    if "events" in layout.streams:
        features[EVENT_KEY] = {
            "dtype": "int64",
            "shape": (1,),
            "names": ["event_index"],
        }
    for camera_id, (height, width) in layout.cameras.items():
        features[camera_feature_key(camera_id)] = {
            "dtype": "image",
            "shape": (height, width, 3),
            "names": ["height", "width", "channels"],
        }
        features[camera_pose_key(camera_id)] = {
            "dtype": "float32",
            "shape": (7,),
            "names": list(POSE_NAMES),
        }
    for sensor_id, lidar in layout.lidar.items():
        if lidar.kind == "laserscan":
            beams = [f"beam_{i}" for i in range(lidar.range_count)]
            features[lidar_key(sensor_id, "ranges")] = {
                "dtype": "float32",
                "shape": (lidar.range_count,),
                "names": beams,
            }
            if lidar.has_intensities:
                features[lidar_key(sensor_id, "intensities")] = {
                    "dtype": "float32",
                    "shape": (lidar.range_count,),
                    "names": list(beams),
                }
        else:
            features[lidar_key(sensor_id, "scan_index")] = {
                "dtype": "int64",
                "shape": (1,),
                "names": ["scan_index"],
            }
        features[lidar_key(sensor_id, "pose")] = {
            "dtype": "float32",
            "shape": (7,),
            "names": list(POSE_NAMES),
        }
    if layout.boundary:
        features[BOUNDARY_POSE_KEY] = {
            "dtype": "float32",
            "shape": (7,),
            "names": list(POSE_NAMES),
        }
        features[BOUNDARY_SHAPE_KEY] = {
            "dtype": "int64",
            "shape": (1,),
            "names": ["shape_index"],
        }
    features[TIME_KEY] = {"dtype": "int64", "shape": (1,), "names": ["epoch_ms"]}
    return features


def _clock_times(session: SessionData, kind: str, arg, fps: int | None) -> np.ndarray:
    if kind == "camera":
        if arg not in session.cameras:
            raise C3DDatasetError(
                f"Session {session.session_id} has no camera {arg!r}."
            )
        return np.array([ref.time for ref in session.cameras[arg]], dtype=np.float64)
    if kind == "gaze":
        return session.gaze.times.copy()
    rate = float(fps or arg)
    streams = list(session.cameras.values()) + list(session.lidar.values())
    spans = [session.gaze.times[[0, -1]]] + [
        np.array([refs[0].time, refs[-1].time]) for refs in streams if refs
    ]
    start = min(float(s[0]) for s in spans)
    end = max(float(s[-1]) for s in spans)
    count = int(np.floor((end - start) * rate + 1e-9)) + 1
    return start + np.arange(count, dtype=np.float64) / rate


def build_episode(
    session: SessionData,
    layout: DatasetLayout,
    options: MappingOptions,
    cache_for: CacheFor | None = None,
) -> EpisodePlan:
    """``cache_for`` is needed when the layout has a laserscan (ranges are decoded
    from the downloaded blobs)."""
    kind, arg = layout.clock
    times = _clock_times(session, kind, arg, layout.fps if kind == "fixed" else None)
    coord = options.coord
    numeric: dict[str, np.ndarray] = {}

    gaze = session.gaze
    head = sample_and_hold(gaze.times, gaze.positions, times, before_first="clamp")
    head = convert_positions(head, coord)
    if options.head_rotation:
        rotation = sample_and_hold(
            gaze.times, gaze.rotations, times, before_first="clamp"
        )
        head = np.concatenate([head, convert_quaternions(rotation, coord)], axis=1)
    numeric[STATE_KEY] = head.astype(np.float32)

    if GAZE_KEY in layout.features:
        points = sample_and_hold(gaze.times, gaze.gaze_points, times)
        numeric[GAZE_KEY] = convert_positions(points, coord).astype(np.float32)

    if SENSORS_KEY in layout.features:
        columns = []
        for name in layout.sensor_names:
            channel = session.sensors.get(name)
            if channel is None:
                columns.append(np.full(len(times), np.nan))
            else:
                columns.append(sample_and_hold(channel.times, channel.values, times))
        numeric[SENSORS_KEY] = np.stack(columns, axis=1).astype(np.float32)

    if EVENT_KEY in layout.features:
        codes = {name: i for i, name in enumerate(layout.event_names)}
        event_times = np.array([e.time for e in session.events], dtype=np.float64)
        event_codes = np.array([codes[e.name] for e in session.events], dtype=np.int64)
        per_frame = event_index_per_frame(event_times, event_codes, times)
        numeric[EVENT_KEY] = per_frame.reshape(-1, 1)

    images: dict[str, list[CameraFrameRef]] = {}
    for camera_id in layout.cameras:
        refs = session.cameras[camera_id]
        if kind == "camera" and arg == camera_id:
            frame_refs = list(refs)
        else:
            ref_times = np.array([r.time for r in refs], dtype=np.float64)
            idx = np.clip(hold_indices(ref_times, times), 0, len(refs) - 1)
            frame_refs = [refs[i] for i in idx]
        images[camera_feature_key(camera_id)] = frame_refs
        poses = np.array(
            [r.pose if len(r.pose) == 7 else (np.nan,) * 7 for r in frame_refs],
            dtype=np.float64,
        )
        numeric[camera_pose_key(camera_id)] = convert_poses(poses, coord).astype(
            np.float32
        )

    skipped: dict[str, int] = {}
    for sensor_id, lidar in layout.lidar.items():
        refs = session.lidar[sensor_id]
        scan_times = np.array([r.time for r in refs], dtype=np.float64)
        idx = hold_indices(scan_times, times)
        held = idx >= 0
        poses = np.full((len(times), 7), np.nan)
        poses[held] = [
            refs[i].pose if len(refs[i].pose) == 7 else (np.nan,) * 7 for i in idx[held]
        ]
        convert = convert_scan_poses if lidar.kind == "laserscan" else convert_poses
        numeric[lidar_key(sensor_id, "pose")] = convert(poses, coord).astype(np.float32)
        if lidar.kind == "pointcloud":
            numeric[lidar_key(sensor_id, "scan_index")] = idx.astype(np.int64).reshape(
                -1, 1
            )
            continue
        ranges, intensities, failed = _laserscan_rows(
            refs, idx, lidar, cache_for(session).lidar
        )
        numeric[lidar_key(sensor_id, "ranges")] = ranges
        if lidar.has_intensities:
            numeric[lidar_key(sensor_id, "intensities")] = intensities
        if failed:
            skipped[sensor_id] = failed

    if layout.boundary:
        boundary = session.boundary
        poses = sample_and_hold(boundary.times, boundary.poses, times)
        numeric[BOUNDARY_POSE_KEY] = convert_poses(poses, coord).astype(np.float32)
        shape_idx = hold_indices(boundary.shape_times, times)
        numeric[BOUNDARY_SHAPE_KEY] = shape_idx.astype(np.int64).reshape(-1, 1)

    if kind == "camera":
        epoch_ms = np.array([r.date for r in session.cameras[arg]], dtype=np.int64)
    else:
        epoch_ms = np.rint((session.start_timestamp + times) * 1000.0).astype(np.int64)
    numeric[TIME_KEY] = epoch_ms.reshape(-1, 1)

    return EpisodePlan(
        session=session,
        times=times,
        numeric=numeric,
        images=images,
        skipped_scans=skipped,
    )


def laserscan_values(ref, blob: bytes, width: int):
    """Decoded ranges (no-return and out-of-range beams as NaN) and intensities,
    padded with NaN to ``width`` beams."""
    ranges, intensities = decode_laserscan(blob, ref)
    ranges = ranges.astype(np.float32)
    invalid = ~np.isfinite(ranges) | (ranges < ref.range_min) | (ranges > ref.range_max)
    ranges[invalid] = np.nan
    out_ranges = np.full(width, np.nan, dtype=np.float32)
    out_ranges[: len(ranges)] = ranges[:width]
    out_intensities = np.full(width, np.nan, dtype=np.float32)
    if intensities is not None:
        out_intensities[: len(intensities)] = intensities[:width]
    return out_ranges, out_intensities


def _laserscan_rows(refs, idx, lidar: LidarLayout, cache: MediaCache):
    count = lidar.range_count
    ranges = np.full((len(idx), count), np.nan, dtype=np.float32)
    intensities = np.full((len(idx), count), np.nan, dtype=np.float32)
    decoded: dict[int, tuple | None] = {}
    failed = 0
    for i in np.unique(idx[idx >= 0]):
        ref = refs[i]
        try:
            decoded[i] = laserscan_values(ref, cache.read(ref), count)
        except (LidarDecodeError, OSError) as e:
            warnings.warn(f"Skipping lidar scan: {e}", stacklevel=2)
            decoded[i] = None
            failed += 1
    for row, i in enumerate(idx):
        values = decoded.get(i) if i >= 0 else None
        if values is not None:
            ranges[row], intensities[row] = values
    return ranges, intensities, failed


class FrameLoader:
    """Decodes cached frames to RGB arrays, reusing the last decode while a frame is
    held across steps."""

    def __init__(self, cache_for: CacheFor, layout: DatasetLayout):
        self._cache_for = cache_for
        self._shapes = {
            camera_feature_key(c): shape for c, shape in layout.cameras.items()
        }
        self._last: dict[str, tuple[CameraFrameRef, np.ndarray]] = {}

    def load(self, session: SessionData, feature: str, ref: CameraFrameRef):
        last = self._last.get(feature)
        if last is not None and last[0] == ref:
            return last[1]
        height, width = self._shapes[feature]
        data = self._cache_for(session).camera.read(ref)
        with Image.open(io.BytesIO(data)) as image:
            rgb = image.convert("RGB")
            if rgb.size != (width, height):
                rgb = rgb.resize((width, height))
            array = np.asarray(rgb, dtype=np.uint8)
        self._last[feature] = (ref, array)
        return array
