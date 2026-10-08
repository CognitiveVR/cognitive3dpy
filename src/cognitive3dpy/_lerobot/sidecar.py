"""Per-session files next to the dataset (``c3d/sessions/<session_id>/``).

LeRobot features need a fixed shape, so whatever does not fit one is kept here,
lossless: every lidar scan (frames only hold the latest one at each step), each
point cloud, the boundary polygons and the events with their properties. All of
it is in the dataset's coordinate convention.
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import numpy as np

from cognitive3dpy._lerobot.coords import coord_info, mirror_poses, mirror_positions
from cognitive3dpy._lerobot.lidar import LidarDecodeError, decode_pointcloud
from cognitive3dpy._lerobot.mapping import (
    DatasetLayout,
    EpisodePlan,
    SessionCaches,
    _feature_name,
    laserscan_values,
)
from cognitive3dpy._lerobot.progress import progress_bar


def safe_name(value: str) -> str:
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in value)


def _pose(values, mirror: bool) -> np.ndarray:
    pose = np.array(values if len(values) == 7 else (np.nan,) * 7, dtype=np.float64)
    return (mirror_poses(pose) if mirror else pose).astype(np.float32)


def write_session_sidecar(
    session_dir: Path,
    plan: EpisodePlan,
    layout: DatasetLayout,
    caches: SessionCaches,
    coord: str,
    *,
    progress: bool = True,
) -> dict:
    """Write one session's files; returns what was written, for download.json."""
    session = plan.session
    mirror = coord == "rh_yup"
    system = coord_info(coord)
    session_dir.mkdir(parents=True, exist_ok=True)
    summary: dict = {}

    events = []
    for e in session.events:
        point = e.point
        if point is not None and mirror:
            point = mirror_positions(np.array(point)).tolist()
        events.append(
            {
                "name": e.name,
                "time": e.time,
                "point": point,
                "properties": e.properties,
                "versionId": e.version_id,
            }
        )
    document = {"coordinate_system": system, "events": events}
    (session_dir / "events.json").write_text(json.dumps(document, indent=2))

    for sensor_id, lidar in layout.lidar.items():
        sensor_dir = session_dir / "lidar" / _feature_name(sensor_id)
        sensor_dir.mkdir(parents=True, exist_ok=True)
        refs = session.lidar[sensor_id]
        if lidar.kind == "laserscan":
            summary[sensor_id] = _write_laserscans(
                sensor_dir, refs, lidar, caches, mirror
            )
        else:
            summary[sensor_id] = _write_pointclouds(
                sensor_dir, refs, caches, mirror, system, session.session_id, progress
            )
        summary[sensor_id]["kind"] = lidar.kind

    if layout.boundary and session.boundary is not None:
        boundary = session.boundary
        shapes = [
            {
                "index": i,
                "time": float(t),
                "points": (mirror_positions(points) if mirror else points).tolist(),
            }
            for i, (t, points) in enumerate(zip(boundary.shape_times, boundary.shapes))
        ]
        document = {
            "coordinate_system": system,
            "poses": [
                {"time": float(t), "pose": _pose(p, mirror).tolist()}
                for t, p in zip(boundary.times, boundary.poses)
            ],
            "shapes": shapes,
            **boundary.extras,
        }
        (session_dir / "boundary.json").write_text(json.dumps(document, indent=2))
        summary["boundary"] = {"poses": len(boundary.times), "shapes": len(shapes)}
    return summary


def _write_laserscans(sensor_dir, refs, lidar, caches, mirror) -> dict:
    count = lidar.range_count
    ranges = np.full((len(refs), count), np.nan, dtype=np.float32)
    intensities = np.full((len(refs), count), np.nan, dtype=np.float32)
    skipped = 0
    for i, ref in enumerate(refs):
        if ref.range_count <= 0:
            continue
        try:
            ranges[i], intensities[i] = laserscan_values(
                ref, caches.lidar.read(ref), count
            )
        except (LidarDecodeError, OSError) as e:
            warnings.warn(f"Skipping lidar scan: {e}", stacklevel=2)
            skipped += 1
    arrays = {
        "time": np.array([r.time for r in refs], dtype=np.float64),
        "date": np.array([r.date for r in refs], dtype=np.int64),
        "seq": np.array([r.seq for r in refs], dtype=np.int64),
        "pose": np.stack([_pose(r.pose, mirror) for r in refs]),
        "ranges": ranges,
        "angle_min": np.float64(lidar.angle_min),
        "angle_increment": np.float64(lidar.angle_increment),
        "range_min": np.float64(lidar.range_min),
        "range_max": np.float64(lidar.range_max),
    }
    if lidar.has_intensities:
        arrays["intensities"] = intensities
    np.savez_compressed(sensor_dir / "scans.npz", **arrays)
    return {"scans": len(refs), "skipped": skipped, "file": "scans.npz"}


def _write_pointclouds(sensor_dir, refs, caches, mirror, system, session_id, progress):
    index = []
    skipped = 0
    desc = f"Writing {session_id} lidar {sensor_dir.name} point clouds"
    with progress_bar(
        total=len(refs), desc=desc, unit="scan", disable=not progress
    ) as bar:
        for i, ref in enumerate(refs):
            name = f"{i:06d}.npz"
            entry = {
                "index": i,
                "time": ref.time,
                "date": ref.date,
                "seq": ref.seq,
                "points": 0,
                "file": None,
            }
            try:
                if ref.point_count > 0:
                    cloud = decode_pointcloud(caches.lidar.read(ref), ref)
                    points = cloud.positions
                    if mirror:
                        points = mirror_positions(points).astype(np.float32)
                    arrays = {"points": points, "pose": _pose(ref.pose, mirror)}
                    if cloud.colors is not None:
                        arrays["colors"] = cloud.colors
                    if cloud.intensities is not None:
                        arrays["intensities"] = cloud.intensities
                    np.savez_compressed(sensor_dir / name, **arrays)
                    entry.update(points=len(points), file=name)
            except (LidarDecodeError, OSError) as e:
                warnings.warn(f"Skipping lidar scan: {e}", stacklevel=2)
                skipped += 1
            index.append(entry)
            bar.update(1)
    document = {
        "coordinate_system": system,
        "frame": "sensor-local; world = rotate(pose.q, point) + pose.p",
        "scans": index,
    }
    (sensor_dir / "index.json").write_text(json.dumps(document, indent=2))
    return {"scans": len(refs), "skipped": skipped, "file": "index.json"}
