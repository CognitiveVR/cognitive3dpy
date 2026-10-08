"""Fetch and parse a session's streams (``jsonRequests`` sections)."""

from __future__ import annotations

import json
from collections.abc import Iterable

import numpy as np

from cognitive3dpy._client import C3DNotFoundError
from cognitive3dpy._lerobot.http import HttpClient, segment
from cognitive3dpy._lerobot.models import (
    BoundaryData,
    CameraFrameRef,
    GazeSamples,
    LidarScanRef,
    SensorChannel,
    SessionData,
    SessionEvent,
)

SECTION_TYPES = {
    "gaze": "GAZE",
    "sensors": "SENSORS",
    "events": "EVENTS",
    "camera": "CAMERA",
    "lidar": "LIDAR",
    "boundary": "BOUNDARY",
}
_ROWS_KEY = {
    "gaze": "data",
    "sensors": "data",
    "events": "data",
    "camera": "cameraFrames",
    "lidar": "lidarScans",
    "boundary": "data",
}


def fetch_section(
    http: HttpClient,
    project_id: int,
    session_id: str,
    section: str,
    *,
    progress: bool = True,
) -> dict | None:
    """One section, or None when the session has none of it (the API answers 404)."""
    path = f"/v0/projects/{int(project_id)}/sessions/{segment(session_id)}/jsonRequests"
    try:
        raw = http.request_bytes(
            "POST",
            path,
            json={"jsonType": SECTION_TYPES[section], "addMetadata": True},
            context=f"{section} data for session {session_id}",
            progress_desc=f"{session_id} {section}.json" if progress else None,
        )
    except C3DNotFoundError:
        return None
    return json.loads(raw)


def fetch_session(
    http: HttpClient,
    project_id: int,
    session_id: str,
    sections: Iterable[str] = SECTION_TYPES,
    *,
    progress: bool = True,
) -> SessionData:
    fetched = {
        name: fetch_section(http, project_id, session_id, name, progress=progress)
        for name in sections
    }
    if all(doc is None for doc in fetched.values()):
        error = C3DNotFoundError(
            f"Session {session_id} in project {project_id} was not found or has no "
            "data yet (sessions can take a minute or two to appear after they end)."
        )
        error.status, error.body = 404, ""
        raise error
    return parse_session(project_id, session_id, fetched)


def _rows(doc: dict | None, key: str) -> list[dict]:
    """Rows across scene versions: project scope keys them by version id, scene
    scope returns a plain list."""
    if not doc:
        return []
    container = doc.get(key)
    if container is None:
        return []
    if isinstance(container, list):
        return list(container)
    rows: list[dict] = []
    for version_key, version_rows in container.items():
        for row in version_rows:
            if "versionId" not in row and str(version_key).lstrip("-").isdigit():
                row = {**row, "versionId": int(version_key)}
            rows.append(row)
    return rows


def _vec(value, n: int) -> list[float]:
    if isinstance(value, (list, tuple)) and len(value) == n:
        return [float(v) for v in value]
    return [float("nan")] * n


def parse_gaze(rows: list[dict]) -> GazeSamples | None:
    rows = [r for r in rows if _is_vec3(r.get("p")) and "time" in r]
    if not rows:
        return None
    rows.sort(key=lambda r: float(r["time"]))
    gaze_points = []
    for r in rows:
        # "o" holds the id of a gazed object; only free-space gaze points are kept
        object_hit = r.get("o") not in (None, "")
        gaze_points.append([float("nan")] * 3 if object_hit else _vec(r.get("g"), 3))
    return GazeSamples(
        times=np.array([float(r["time"]) for r in rows], dtype=np.float64),
        positions=np.array([_vec(r["p"], 3) for r in rows], dtype=np.float64),
        rotations=np.array([_vec(r.get("r"), 4) for r in rows], dtype=np.float64),
        gaze_points=np.array(gaze_points, dtype=np.float64),
    )


def _is_vec3(value) -> bool:
    return isinstance(value, list) and len(value) == 3


def parse_sensors(rows: list[dict]) -> dict[str, SensorChannel]:
    samples: dict[str, list[tuple[float, float]]] = {}
    for row in rows:
        name = row.get("name")
        if not name:
            continue
        for point in row.get("data") or []:
            if isinstance(point, (list, tuple)) and len(point) >= 2:
                if point[1] is not None:
                    samples.setdefault(name, []).append(
                        (float(point[0]), float(point[1]))
                    )
    channels = {}
    for name in sorted(samples):
        pairs = sorted(samples[name])
        channels[name] = SensorChannel(
            name=name,
            times=np.array([p[0] for p in pairs], dtype=np.float64),
            values=np.array([p[1] for p in pairs], dtype=np.float64),
        )
    return channels


def parse_events(rows: list[dict]) -> list[SessionEvent]:
    events = [
        SessionEvent(
            name=str(r["name"]),
            time=float(r["time"]),
            point=_vec(r["point"], 3) if isinstance(r.get("point"), list) else None,
            properties=dict(r.get("properties") or {}),
            version_id=r.get("versionId"),
        )
        for r in rows
        if r.get("name") is not None and r.get("time") is not None
    ]
    events.sort(key=lambda e: e.time)
    return events


def parse_camera(rows: list[dict]) -> dict[str, list[CameraFrameRef]]:
    cameras: dict[str, list[CameraFrameRef]] = {}
    for r in rows:
        try:
            ref = CameraFrameRef(
                version_id=int(r["versionId"]),
                camera_id=str(r["cameraId"]),
                date=int(r["date"]),
                seq=int(r["seq"]),
                time=float(r["time"]),
                frame_id=r.get("frameId"),
                pose=tuple(float(v) for v in (r.get("pose") or [])),
                format=str(r.get("format") or "jpeg"),
                width=r.get("width"),
                height=r.get("height"),
            )
        except (KeyError, TypeError, ValueError):
            continue
        cameras.setdefault(ref.camera_id, []).append(ref)
    for refs in cameras.values():
        refs.sort(key=lambda ref: (ref.time, ref.date, ref.seq))
    return dict(sorted(cameras.items()))


def parse_lidar(rows: list[dict]) -> dict[str, list[LidarScanRef]]:
    sensors: dict[str, list[LidarScanRef]] = {}
    for r in rows:
        try:
            kind = str(r["kind"])
            scan = r.get("laserscan") or {}
            cloud = r.get("pointcloud") or {}
            ref = LidarScanRef(
                version_id=int(r["versionId"]),
                sensor_id=str(r["sensorId"]),
                kind=kind,
                date=int(r["date"]),
                seq=int(r["seq"]),
                time=float(r["time"]),
                frame_id=r.get("frameId"),
                pose=tuple(float(v) for v in (r.get("pose") or [])),
                range_count=int(scan.get("rangeCount") or 0),
                has_intensities=bool(scan.get("hasIntensities")),
                angle_min=float(scan.get("angleMin") or 0.0),
                angle_increment=float(scan.get("angleIncrement") or 0.0),
                range_min=float(scan.get("rangeMin") or 0.0),
                range_max=float(scan.get("rangeMax") or float("inf")),
                point_count=int(cloud.get("pointCount") or 0),
                position_format=str(cloud.get("positionFormat") or ""),
                color_format=str(cloud.get("colorFormat") or "none"),
                intensity_format=str(cloud.get("intensityFormat") or "none"),
                zstd_level=int(cloud.get("zstdLevel") or 0),
            )
        except (KeyError, TypeError, ValueError):
            continue
        if kind not in ("laserscan", "pointcloud"):
            continue
        sensors.setdefault(ref.sensor_id, []).append(ref)
    for refs in sensors.values():
        refs.sort(key=lambda ref: (ref.time, ref.date, ref.seq))
    return dict(sorted(sensors.items()))


def parse_boundary(doc: dict | None) -> BoundaryData | None:
    rows = [r for r in _rows(doc, "data") if _is_vec3(r.get("p")) and "time" in r]
    if not rows:
        return None
    rows.sort(key=lambda r: float(r["time"]))
    poses = [_vec(r["p"], 3) + _vec(r.get("r"), 4) for r in rows]
    shapes = sorted(
        (
            (float(s["time"]), np.array([_vec(p, 3) for p in s["points"]]))
            for s in _rows(doc, "shapes")
            if "time" in s and isinstance(s.get("points"), list)
        ),
        key=lambda item: item[0],
    )
    extras = {k: doc[k] for k in ("roomManifest", "roomData") if doc.get(k)}
    return BoundaryData(
        times=np.array([float(r["time"]) for r in rows], dtype=np.float64),
        poses=np.array(poses, dtype=np.float64),
        shape_times=np.array([t for t, _ in shapes], dtype=np.float64),
        shapes=[points.reshape(-1, 3) for _, points in shapes],
        extras=extras,
    )


def parse_session(project_id: int, session_id: str, docs: dict) -> SessionData:
    names = ("gaze", "camera", "sensors", "events", "lidar", "boundary")
    ordered = (docs.get(n) for n in names)
    header = next((doc for doc in ordered if doc), {})
    return SessionData(
        project_id=int(project_id),
        session_id=session_id,
        start_timestamp=float(header.get("timestamp") or 0.0),
        duration=float(header.get("duration") or 0.0),
        format_version=header.get("formatversion"),
        metadata=dict(header.get("metadata") or {}),
        gaze=parse_gaze(_rows(docs.get("gaze"), _ROWS_KEY["gaze"])),
        sensors=parse_sensors(_rows(docs.get("sensors"), _ROWS_KEY["sensors"])),
        events=parse_events(_rows(docs.get("events"), _ROWS_KEY["events"])),
        cameras=parse_camera(_rows(docs.get("camera"), _ROWS_KEY["camera"])),
        lidar=parse_lidar(_rows(docs.get("lidar"), _ROWS_KEY["lidar"])),
        boundary=parse_boundary(docs.get("boundary")),
    )
