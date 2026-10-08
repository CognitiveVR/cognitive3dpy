"""Orchestrate a download: fetch sessions, fetch frames, map, write."""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from cognitive3dpy._lerobot.camera import (
    CameraDownloader,
    CameraDownloadStats,
    MediaCache,
)
from cognitive3dpy._lerobot.coords import coord_info
from cognitive3dpy._lerobot.http import HttpClient
from cognitive3dpy._lerobot.lidar import LidarDownloader, LidarDownloadStats
from cognitive3dpy._lerobot.mapping import (
    STREAMS,
    MappingOptions,
    SessionCaches,
    build_episode,
    build_layout,
    resolve_streams,
)
from cognitive3dpy._lerobot.models import SessionData
from cognitive3dpy._lerobot.sessions import fetch_session

_REPO_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$")


def default_cache_dir() -> Path:
    """``C3D_CACHE_DIR``, else ``$XDG_CACHE_HOME/cognitive3dpy``, else
    ``~/.cache/cognitive3dpy``."""
    if os.environ.get("C3D_CACHE_DIR"):
        return Path(os.environ["C3D_CACHE_DIR"]).expanduser()
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "cognitive3dpy"


@dataclass
class DownloadSpec:
    project_id: int
    session_ids: list[str]
    out: str | Path = "datasets"
    repo_id: str | None = None
    streams: set[str] | list[str] | None = None
    sensors: list[str] | None = None
    clock: str | None = None
    fps: int | None = None
    coord: str = "rh_yup"
    head_rotation: bool = False
    task: str | None = None
    overwrite: bool = False
    camera_transport: str = "auto"
    lidar_transport: str = "auto"
    concurrency: int = 4


@dataclass
class DownloadResult:
    """Where a dataset was written and what went into it."""

    path: Path
    repo_id: str
    episodes: int
    frames: int
    fps: int
    features: list[str]
    sessions: list[str]
    camera: CameraDownloadStats = field(default_factory=CameraDownloadStats)
    lidar: LidarDownloadStats = field(default_factory=LidarDownloadStats)

    def load(self):
        """Open the dataset as a ``lerobot`` ``LeRobotDataset``."""
        from cognitive3dpy._lerobot.writer import quiet_lerobot

        with quiet_lerobot():
            from lerobot.datasets.lerobot_dataset import LeRobotDataset

            return LeRobotDataset(self.repo_id, root=self.path)


def default_repo_id(session_ids: list[str]) -> str:
    if len(session_ids) != 1:
        raise ValueError(
            "repo_id is required when downloading more than one session "
            "(e.g. repo_id='myorg/warehouse_pick')."
        )
    name = re.sub(r"[^A-Za-z0-9._-]", "_", session_ids[0])
    if not name[:1].isalnum():
        name = "s" + name
    return f"cognitive3d/{name}"


_OPTIONAL_SECTIONS = ("sensors", "events", "camera", "lidar", "boundary")


def _sections_for(streams: set[str] | None) -> list[str]:
    if streams is None:
        return ["gaze", *_OPTIONAL_SECTIONS]
    return ["gaze"] + [s for s in _OPTIONAL_SECTIONS if s in streams]


def _lidar_metadata(layout, coord: str) -> dict:
    mirrored = coord == "rh_yup"
    beam = "(-r cos θ, r sin θ, 0)" if mirrored else "(r cos θ, r sin θ, 0)"
    out = {}
    for sensor_id, lidar in layout.lidar.items():
        if lidar.kind == "laserscan":
            out[sensor_id] = {
                "kind": lidar.kind,
                "range_count": lidar.range_count,
                "angle_min": lidar.angle_min,
                "angle_increment": lidar.angle_increment,
                "range_min": lidar.range_min,
                "range_max": lidar.range_max,
                "has_intensities": lidar.has_intensities,
                "point": f"θ = angle_min + i·angle_increment; local = {beam}; "
                "world = rotate(pose.q, local) + pose.p",
            }
        else:
            out[sensor_id] = {
                "kind": lidar.kind,
                "point": "sidecar points are sensor-local; "
                "world = rotate(pose.q, point) + pose.p",
            }
    return out


def _tool_version() -> str:
    try:
        return version("cognitive3dpy")
    except PackageNotFoundError:  # pragma: no cover - running from a source tree
        return "unknown"


def run_download(
    http: HttpClient,
    spec: DownloadSpec,
    cache_root: Path,
    *,
    progress: bool = True,
    echo: Callable[[str], None] = print,
) -> DownloadResult:
    from cognitive3dpy._lerobot.writer import write_dataset

    if not spec.session_ids:
        raise ValueError("No session ids given.")
    session_ids = list(dict.fromkeys(spec.session_ids))
    coord_info(spec.coord)
    streams = set(spec.streams) if spec.streams is not None else None
    if streams is not None and not streams <= set(STREAMS):
        unknown = sorted(streams - set(STREAMS))
        raise ValueError(f"Unknown stream(s) {unknown}; choose from {list(STREAMS)}.")
    repo_id = spec.repo_id or default_repo_id(session_ids)
    if not _REPO_ID.match(repo_id):
        raise ValueError(f"repo_id {repo_id!r} must look like 'namespace/name'.")

    root = Path(spec.out).expanduser().resolve() / repo_id
    if root.exists() and not spec.overwrite:
        raise FileExistsError(
            f"{root} already exists; pass overwrite=True to replace it."
        )

    caches: dict[str, SessionCaches] = {}

    def cache_for(session: SessionData) -> SessionCaches:
        if session.session_id not in caches:
            args = (cache_root, http.host, session.project_id, session.session_id)
            caches[session.session_id] = SessionCaches(
                camera=MediaCache(*args, kind="camera"),
                lidar=MediaCache(*args, kind="lidar"),
            )
        return caches[session.session_id]

    sessions: list[SessionData] = []
    for session_id in session_ids:
        echo(f"Fetching session {session_id} (project {spec.project_id})")
        sessions.append(
            fetch_session(
                http,
                spec.project_id,
                session_id,
                _sections_for(streams),
                progress=progress,
            )
        )

    options = MappingOptions(
        streams=streams,
        sensors=spec.sensors,
        clock=spec.clock,
        fps=spec.fps,
        coord=spec.coord,
        head_rotation=spec.head_rotation,
    )

    chosen = resolve_streams(sessions, streams)
    camera_stats = CameraDownloadStats()
    if "camera" in chosen:
        downloader = CameraDownloader(
            http,
            transport=spec.camera_transport,
            concurrency=spec.concurrency,
            progress=progress,
        )
        for session in sessions:
            camera_stats.add(
                downloader.download(
                    cache_for(session).camera,
                    session.project_id,
                    session.session_id,
                    session.cameras,
                )
            )

    lidar_stats = LidarDownloadStats()
    if "lidar" in chosen:
        common = set.intersection(*(set(s.lidar) for s in sessions))
        lidar_downloader = LidarDownloader(
            http,
            transport=spec.lidar_transport,
            concurrency=spec.concurrency,
            progress=progress,
        )
        for session in sessions:
            sensors = {k: v for k, v in session.lidar.items() if k in common}
            lidar_stats.add(
                lidar_downloader.download(
                    cache_for(session).lidar,
                    session.project_id,
                    session.session_id,
                    sensors,
                )
            )

    layout = build_layout(sessions, options, cache_for)
    plans = [build_episode(s, layout, options, cache_for) for s in sessions]

    sidecar = {
        "tool": "cognitive3dpy",
        "tool_version": _tool_version(),
        "repo_id": repo_id,
        "api_host": http.host,
        "project_id": spec.project_id,
        "downloaded_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "coord": coord_info(spec.coord),
        "clock": {"kind": layout.clock[0], "arg": layout.clock[1]},
        "fps": layout.fps,
        "streams": layout.streams,
        "sensor_names": layout.sensor_names,
        "event_names": layout.event_names,
        "cameras": {
            cid: {"height": h, "width": w} for cid, (h, w) in layout.cameras.items()
        },
        "camera_transports": sorted(camera_stats.transports),
        "lidar_transports": sorted(lidar_stats.transports),
        "lidar": _lidar_metadata(layout, spec.coord),
        "boundary": layout.boundary,
        "episodes": [
            {
                "episode_index": i,
                "session_id": plan.session.session_id,
                "frames": len(plan),
                "session_start": plan.session.start_timestamp,
                "session_duration": plan.session.duration,
                "skipped_lidar_scans": plan.skipped_scans,
            }
            for i, plan in enumerate(plans)
        ],
    }
    task = spec.task or "cognitive3d session"
    write_dataset(
        root,
        repo_id,
        layout,
        plans,
        cache_for,
        task,
        sidecar,
        coord=spec.coord,
        progress=progress,
    )

    return DownloadResult(
        path=root,
        repo_id=repo_id,
        episodes=len(plans),
        frames=sum(len(p) for p in plans),
        fps=layout.fps,
        features=list(layout.features),
        sessions=session_ids,
        camera=camera_stats,
        lidar=lidar_stats,
    )
