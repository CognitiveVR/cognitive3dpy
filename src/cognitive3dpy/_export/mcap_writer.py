"""Sessions as MCAP recordings of ROS 2 messages, one rosbag2 bag per session.

Every sample keeps its own timestamp (nothing is resampled) and poses are in
REP-103 axes under a ``map`` frame: the head, each camera and lidar sensor and the
boundary get a frame (``c3d/...``) whose transform is published on ``/tf`` at the
time of each pose. Laserscan frames are the scan plane, so ``sensor_msgs/LaserScan``
reads as recorded; point clouds and boundary polygons are in their sensor's or the
boundary's frame.
"""

from __future__ import annotations

import json
import re
import warnings
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path

import numpy as np

from cognitive3dpy._export.coords import (
    convert_poses,
    convert_positions,
    convert_scan_poses,
)
from cognitive3dpy._export.lidar import (
    LidarDecodeError,
    decode_laserscan,
    decode_pointcloud,
)
from cognitive3dpy._export.mapping import CacheFor
from cognitive3dpy._export.models import SessionData
from cognitive3dpy._export.progress import progress_bar
from cognitive3dpy._export.ros_msgs import definition, schema_name
from cognitive3dpy._export.sidecar import safe_name, staged, write_manifest

WORLD_FRAME = "map"
HEAD_FRAME = "c3d/head"
BOUNDARY_FRAME = "c3d/boundary"
_FLOAT32 = 7  # sensor_msgs/PointField.FLOAT32
_ROS_TOPIC = re.compile(r"^/[A-Za-z_][A-Za-z0-9_]*(/[A-Za-z_][A-Za-z0-9_]*)*$")


@dataclass
class _Message:
    time_ns: int
    order: int  # breaks ties between messages with the same stamp
    topic: str
    msg_type: str
    build: Callable[[], dict | None]  # None skips the message


@dataclass
class BagSummary:
    path: Path
    messages: int = 0
    start_ns: int = 0
    end_ns: int = 0
    topics: dict[str, dict] = field(default_factory=dict)
    skipped_lidar_scans: dict[str, int] = field(default_factory=dict)
    frames: dict[str, str] = field(default_factory=dict)  # frame -> recorded frame id


def ros_name(text: str) -> str:
    """A valid ROS name token: [A-Za-z0-9_], not starting with a digit."""
    token = re.sub(r"[^A-Za-z0-9_]", "_", str(text)) or "_"
    return "_" + token if token[0].isdigit() else token


def _stamp(time_ns: int) -> dict:
    return {"sec": time_ns // 1_000_000_000, "nanosec": time_ns % 1_000_000_000}


def _header(time_ns: int, frame: str) -> dict:
    return {"stamp": _stamp(time_ns), "frame_id": frame}


def _xyz(values) -> dict:
    return {"x": float(values[0]), "y": float(values[1]), "z": float(values[2])}


def _quaternion(values) -> dict:
    x, y, z, w = (float(v) for v in values)
    return {"x": x, "y": y, "z": z, "w": w}


def _tf(time_ns: int, frame: str, pose: np.ndarray) -> dict:
    transform = {"translation": _xyz(pose[:3]), "rotation": _quaternion(pose[3:])}
    return {
        "transforms": [
            {
                "header": _header(time_ns, WORLD_FRAME),
                "child_frame_id": frame,
                "transform": transform,
            }
        ]
    }


def _valid_pose(values) -> bool:
    return len(values) == 7 and bool(np.all(np.isfinite(values)))


def camera_topic(session: SessionData, camera_id: str) -> str:
    """The camera's recorded ROS topic + ``/compressed`` (image_transport), when
    the session carries one."""
    recorded = str(session.properties.get(f"c3d.ros.camera.{camera_id}.topic") or "")
    if _ROS_TOPIC.match(recorded):
        return (
            recorded if recorded.endswith("/compressed") else recorded + "/compressed"
        )
    return f"/c3d/camera/{ros_name(camera_id)}/image/compressed"


def lidar_topic(session: SessionData, sensor_id: str, kind: str) -> str:
    recorded = str(session.properties.get(f"c3d.ros.lidar.{sensor_id}.topic") or "")
    if _ROS_TOPIC.match(recorded):
        return recorded
    leaf = "scan" if kind == "laserscan" else "points"
    return f"/c3d/lidar/{ros_name(sensor_id)}/{leaf}"


def _pointcloud2(header: dict, cloud) -> dict:
    points = convert_positions(cloud.positions, "ros").astype(np.float32)
    layout = [("x", "<f4"), ("y", "<f4"), ("z", "<f4")]
    if cloud.colors is not None:
        layout.append(("rgb", "<u4"))  # packed 0x00RRGGBB, read as FLOAT32 (PCL)
    if cloud.intensities is not None:
        layout.append(("intensity", "<f4"))
    data = np.zeros(len(points), dtype=layout)
    data["x"], data["y"], data["z"] = points[:, 0], points[:, 1], points[:, 2]
    if cloud.colors is not None:
        rgb = cloud.colors.astype(np.uint32)
        data["rgb"] = (rgb[:, 0] << 16) | (rgb[:, 1] << 8) | rgb[:, 2]
    if cloud.intensities is not None:
        data["intensity"] = cloud.intensities
    fields = [
        {
            "name": name,
            "offset": data.dtype.fields[name][1],
            "datatype": _FLOAT32,
            "count": 1,
        }
        for name, _ in layout
    ]
    return {
        "header": header,
        "height": 1,
        "width": len(points),
        "fields": fields,
        "is_bigendian": False,
        "point_step": data.dtype.itemsize,
        "row_step": data.dtype.itemsize * len(points),
        "data": data.tobytes(),
        "is_dense": bool(np.isfinite(points).all()),
    }


def _laserscan(header: dict, ref, blob: bytes) -> dict:
    ranges, intensities = decode_laserscan(blob, ref)
    angle_max = ref.angle_max
    if angle_max is None:
        angle_max = ref.angle_min + (len(ranges) - 1) * ref.angle_increment
    return {
        "header": header,
        "angle_min": ref.angle_min,
        "angle_max": angle_max,
        "angle_increment": ref.angle_increment,
        "time_increment": ref.time_increment,
        "scan_time": ref.scan_time,
        "range_min": ref.range_min,
        "range_max": ref.range_max,
        "ranges": ranges.tolist(),
        "intensities": [] if intensities is None else intensities.tolist(),
    }


_TF, _DATA = 0, 1  # a sensor's transform goes out before its data at the same stamp


def _messages(
    session: SessionData,
    streams: list[str],
    sensor_names: list[str] | None,
    cache_for: CacheFor,
    summary: BagSummary,
) -> list[_Message]:
    start_ns = round(session.start_timestamp * 1e6) * 1000  # exact in float64

    def ns(time: float) -> int:
        return start_ns + round(time * 1e9)

    out: list[_Message] = []

    def add(time: float, order: int, topic: str, msg_type: str, build) -> None:
        out.append(_Message(ns(time), order, topic, msg_type, build))

    def add_tf(time: float, frame: str, pose: np.ndarray) -> None:
        if _valid_pose(pose):
            stamp = ns(time)
            add(time, _TF, "/tf", "tf2_msgs/TFMessage", lambda: _tf(stamp, frame, pose))

    gaze = session.gaze
    first = 0.0 if gaze is None or not len(gaze) else float(gaze.times[0])
    info = json.dumps(
        {
            "project_id": session.project_id,
            "session_id": session.session_id,
            "start_timestamp": session.start_timestamp,
            "duration": session.duration,
            "metadata": session.metadata,
            "properties": session.properties,
            "coordinate_system": "REP-103 (x forward, y left, z up), metres",
        },
        default=str,
    )
    add(first, _DATA, "/c3d/session", "std_msgs/String", lambda: {"data": info})

    if "head" in streams and gaze is not None:
        summary.frames[HEAD_FRAME] = "head"
        poses = convert_poses(np.hstack([gaze.positions, gaze.rotations]), "ros")
        for t, pose in zip(gaze.times, poses):
            if not _valid_pose(pose):
                continue
            add_tf(t, HEAD_FRAME, pose)
            add(
                t,
                _DATA,
                "/c3d/head/pose",
                "geometry_msgs/PoseStamped",
                partial(_pose_stamped, ns(t), pose),
            )

    if "gaze" in streams and gaze is not None:
        points = convert_positions(gaze.gaze_points, "ros")
        for t, point in zip(gaze.times, points):
            if np.all(np.isfinite(point)):
                add(
                    t,
                    _DATA,
                    "/c3d/gaze",
                    "geometry_msgs/PointStamped",
                    partial(_point_stamped, ns(t), point),
                )

    if "sensors" in streams:
        for name in sensor_names or sorted(session.sensors):
            channel = session.sensors.get(name)
            if channel is None:
                continue
            topic = f"/c3d/sensors/{ros_name(name)}"
            for t, value in zip(channel.times, channel.values):
                add(t, _DATA, topic, "std_msgs/Float64", partial(_float64, value))

    if "events" in streams:
        for event in session.events:
            point = event.point
            if point is not None:
                point = convert_positions(np.array(point), "ros").tolist()
            text = json.dumps(
                {
                    "name": event.name,
                    "time": event.time,
                    "point": point,
                    "properties": event.properties,
                },
                default=str,
            )
            add(
                event.time,
                _DATA,
                "/c3d/events",
                "std_msgs/String",
                partial(_string, text),
            )

    caches = cache_for(session)
    if "camera" in streams:
        for camera_id, refs in session.cameras.items():
            frame = f"c3d/camera/{ros_name(camera_id)}"
            summary.frames[frame] = refs[0].frame_id if refs else None
            topic = camera_topic(session, camera_id)
            for ref in refs:
                add_tf(ref.time, frame, convert_poses(_pose(ref.pose), "ros"))
                add(
                    ref.time,
                    _DATA,
                    topic,
                    "sensor_msgs/CompressedImage",
                    partial(_image, ns(ref.time), frame, ref, caches.camera),
                )

    if "lidar" in streams:
        for sensor_id, refs in session.lidar.items():
            if not refs:
                continue
            frame = f"c3d/lidar/{ros_name(sensor_id)}"
            summary.frames[frame] = refs[0].frame_id
            kind = refs[0].kind
            topic = lidar_topic(session, sensor_id, kind)
            if kind == "laserscan":
                convert, msg_type = convert_scan_poses, "sensor_msgs/LaserScan"
            else:
                convert, msg_type = convert_poses, "sensor_msgs/PointCloud2"
            for ref in refs:
                if ref.range_count <= 0 and ref.point_count <= 0:
                    continue
                add_tf(ref.time, frame, convert(_pose(ref.pose), "ros"))
                build = partial(_scan, ns(ref.time), frame, ref, caches.lidar, summary)
                add(ref.time, _DATA, topic, msg_type, build)

    boundary = session.boundary
    if "boundary" in streams and boundary is not None:
        summary.frames[BOUNDARY_FRAME] = "boundary"
        for t, pose in zip(boundary.times, convert_poses(boundary.poses, "ros")):
            add_tf(t, BOUNDARY_FRAME, pose)
        for t, points in zip(boundary.shape_times, boundary.shapes):
            add(
                t,
                _DATA,
                "/c3d/boundary",
                "geometry_msgs/PolygonStamped",
                partial(_polygon, ns(t), convert_positions(points, "ros")),
            )

    out.sort(key=lambda m: (m.time_ns, m.order))
    return out


def _pose(values) -> np.ndarray:
    return np.array(values if len(values) == 7 else (np.nan,) * 7, dtype=np.float64)


def _pose_stamped(stamp: int, pose: np.ndarray) -> dict:
    return {
        "header": _header(stamp, WORLD_FRAME),
        "pose": {"position": _xyz(pose[:3]), "orientation": _quaternion(pose[3:])},
    }


def _point_stamped(stamp: int, point: np.ndarray) -> dict:
    return {"header": _header(stamp, WORLD_FRAME), "point": _xyz(point)}


def _float64(value: float) -> dict:
    return {"data": float(value)}


def _string(text: str) -> dict:
    return {"data": text}


def _image(stamp: int, frame: str, ref, cache) -> dict:
    return {
        "header": _header(stamp, frame),
        "format": "jpeg" if ref.extension == "jpg" else ref.extension,
        "data": cache.read(ref),
    }


def _scan(stamp: int, frame: str, ref, cache, summary: BagSummary) -> dict | None:
    try:
        blob = cache.read(ref)
        if ref.kind == "laserscan":
            return _laserscan(_header(stamp, frame), ref, blob)
        return _pointcloud2(_header(stamp, frame), decode_pointcloud(blob, ref))
    except (LidarDecodeError, OSError) as e:
        warnings.warn(f"Skipping lidar scan: {e}", stacklevel=2)
        skipped = summary.skipped_lidar_scans
        skipped[ref.sensor_id] = skipped.get(ref.sensor_id, 0) + 1
        return None


def _polygon(stamp: int, corners: np.ndarray) -> dict:
    return {
        "header": _header(stamp, BOUNDARY_FRAME),
        "polygon": {"points": [_xyz(c) for c in corners]},
    }


def write_bag(
    bag_dir: Path,
    session: SessionData,
    streams: list[str],
    sensor_names: list[str] | None,
    cache_for: CacheFor,
    *,
    progress: bool = True,
) -> BagSummary:
    """Write ``<bag_dir>/<session>_0.mcap`` and the rosbag2 ``metadata.yaml``."""
    from mcap_ros2.writer import Writer

    bag_dir.mkdir(parents=True, exist_ok=True)
    path = bag_dir / f"{safe_name(session.session_id)}_0.mcap"
    summary = BagSummary(path=path)
    messages = _messages(session, streams, sensor_names, cache_for, summary)
    counts: Counter = Counter()
    types: dict[str, str] = {}
    times: list[int] = []
    desc = f"Writing {session.session_id} MCAP"
    with (
        open(path, "wb") as f,
        progress_bar(
            total=len(messages), desc=desc, unit="msg", disable=not progress
        ) as bar,
    ):
        writer = Writer(f)
        schemas = {}
        for message in messages:
            body = message.build()
            bar.update(1)
            if body is None:
                continue
            if message.msg_type not in schemas:
                schemas[message.msg_type] = writer.register_msgdef(
                    schema_name(message.msg_type), definition(message.msg_type)
                )
            writer.write_message(
                message.topic,
                schemas[message.msg_type],
                body,
                log_time=message.time_ns,
                publish_time=message.time_ns,
                sequence=counts[message.topic],
            )
            counts[message.topic] += 1
            types[message.topic] = schema_name(message.msg_type)
            times.append(message.time_ns)
        writer.finish()

    summary.messages = sum(counts.values())
    summary.start_ns = min(times, default=0)
    summary.end_ns = max(times, default=0)
    summary.topics = {
        topic: {"type": types[topic], "messages": counts[topic]}
        for topic in sorted(counts)
    }
    _write_rosbag2_metadata(bag_dir, session, summary)
    return summary


def _write_rosbag2_metadata(bag_dir: Path, session: SessionData, summary) -> None:
    """rosbag2's ``metadata.yaml`` (version 8), so ``ros2 bag play`` opens the
    folder. JSON strings are valid YAML scalars."""
    q = json.dumps
    duration = summary.end_ns - summary.start_ns
    distro = str(session.properties.get("c3d.ros.distro") or "")
    lines = [
        "rosbag2_bagfile_information:",
        "  version: 8",
        "  storage_identifier: mcap",
        "  duration:",
        f"    nanoseconds: {duration}",
        "  starting_time:",
        f"    nanoseconds_since_epoch: {summary.start_ns}",
        f"  message_count: {summary.messages}",
        "  topics_with_message_count:",
    ]
    for topic, info in summary.topics.items():
        lines += [
            "    - topic_metadata:",
            f"        name: {q(topic)}",
            f"        type: {q(info['type'])}",
            "        serialization_format: cdr",
            '        offered_qos_profiles: ""',
            '        type_description_hash: ""',
            f"      message_count: {info['messages']}",
        ]
    lines += [
        '  compression_format: ""',
        '  compression_mode: ""',
        "  relative_file_paths:",
        f"    - {q(summary.path.name)}",
        "  files:",
        f"    - path: {q(summary.path.name)}",
        "      starting_time:",
        f"        nanoseconds_since_epoch: {summary.start_ns}",
        "      duration:",
        f"        nanoseconds: {duration}",
        f"      message_count: {summary.messages}",
        "  custom_data:",
        f"    cognitive3d.project_id: {q(str(session.project_id))}",
        f"    cognitive3d.session_id: {q(session.session_id)}",
        f"  ros_distro: {q(distro)}",
    ]
    (bag_dir / "metadata.yaml").write_text("\n".join(lines) + "\n")


def write_mcap(
    root: Path,
    sessions: list[SessionData],
    streams: list[str],
    sensor_names: list[str] | None,
    cache_for: CacheFor,
    manifest: dict,
    *,
    progress: bool = True,
) -> list[BagSummary]:
    """One bag per session: ``root`` itself for one session, else
    ``root/<session_id>/``. Writes via ``<root>.partial``."""
    summaries = []
    with staged(root) as partial:
        for session in sessions:
            bag_dir = partial
            if len(sessions) > 1:
                bag_dir = partial / safe_name(session.session_id)
            summary = write_bag(
                bag_dir, session, streams, sensor_names, cache_for, progress=progress
            )
            summaries.append((session, summary, summary.path.relative_to(partial)))
        manifest["frames"] = {
            "world": WORLD_FRAME,
            "axes": "REP-103: x forward, y left, z up; metres",
            "laserscan": "the scan plane: a beam is (r cos θ, r sin θ, 0)",
        }
        manifest["bags"] = [
            {
                "session_id": session.session_id,
                "file": str(relative),
                "messages": summary.messages,
                "start_ns": summary.start_ns,
                "end_ns": summary.end_ns,
                "topics": summary.topics,
                "frames": summary.frames,
                "skipped_lidar_scans": summary.skipped_lidar_scans,
            }
            for session, summary, relative in summaries
        ]
        write_manifest(partial, manifest)
    for _, summary, relative in summaries:
        summary.path = root / relative
    return [summary for _, summary, _ in summaries]
