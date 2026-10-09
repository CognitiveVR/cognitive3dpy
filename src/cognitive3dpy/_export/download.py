"""Orchestrate a download: fetch sessions, fetch media, convert, write a format."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from cognitive3dpy._export.camera import (
    CameraDownloader,
    CameraDownloadStats,
    MediaCache,
)
from cognitive3dpy._export.coords import LASERSCAN_BEAM, coord_info
from cognitive3dpy._export.http import HttpClient
from cognitive3dpy._export.lidar import LidarDownloader, LidarDownloadStats
from cognitive3dpy._export.mapping import (
    STREAMS,
    MappingOptions,
    SessionCaches,
    build_episode,
    build_layout,
    resolve_streams,
)
from cognitive3dpy._export.models import SessionData
from cognitive3dpy._export.sessions import (
    JSON_SECTIONS,
    fetch_session,
    fetch_session_export,
)
from cognitive3dpy._export.sidecar import SIDECAR_DIR

FORMATS = ("lerobot", "mcap", "hdf5", "rlds", "json")
DEFAULT_COORD = {
    "lerobot": "rh_yup",
    "mcap": "ros",
    "hdf5": "ros",
    "rlds": "ros",
    "json": "native",
}
JSON_STREAMS = JSON_SECTIONS
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
    coord: str | None = None  # None: the format's default (DEFAULT_COORD)
    head_rotation: bool = False
    task: str | None = None
    overwrite: bool = False
    camera_transport: str = "auto"
    lidar_transport: str = "auto"
    concurrency: int = 4
    format: str = "json"
    hdf5_layout: str = "robomimic"


@dataclass
class DownloadResult:
    """Where a dataset was written and what went into it."""

    path: Path
    repo_id: str
    episodes: int
    frames: int
    fps: int | None
    features: list[str]
    sessions: list[str]
    camera: CameraDownloadStats = field(default_factory=CameraDownloadStats)
    lidar: LidarDownloadStats = field(default_factory=LidarDownloadStats)
    format: str = "json"
    coord: str = "native"
    files: list[Path] = field(default_factory=list)
    messages: int = 0  # mcap

    def load(self):
        """Open the dataset: a ``LeRobotDataset`` (lerobot) or a read-only
        ``h5py.File`` of the first file (hdf5)."""
        if self.format == "lerobot":
            from cognitive3dpy._export.writer import quiet_lerobot

            with quiet_lerobot():
                from lerobot.datasets.lerobot_dataset import LeRobotDataset

                return LeRobotDataset(self.repo_id, root=self.path)
        if self.format == "hdf5":
            import h5py

            return h5py.File(self.files[0], "r")
        hints = {
            "rlds": f"tensorflow_datasets.builder_from_directory({str(self.path)!r})",
            "mcap": "Foxglove, `ros2 bag play`, or the mcap / rosbags readers",
            "json": "json.load on the files in the folder",
        }
        raise NotImplementedError(
            f"load() opens lerobot and hdf5 datasets; read {self.format} with "
            f"{hints[self.format]}."
        )


def output_path(base: Path, fmt: str) -> Path:
    """``base`` (``<output>/<repo_id>``) when it does not exist yet or already
    holds this format, else ``<base>_<format>``."""
    if not base.exists() or format_of(base) == fmt:
        return base
    return base.with_name(f"{base.name}_{fmt}")


def format_of(folder: Path) -> str | None:
    """The format of an earlier download in ``folder``, None if it is not one."""
    try:
        manifest = json.loads((folder / SIDECAR_DIR / "download.json").read_text())
    except (OSError, ValueError):
        return None
    # manifests from before there were formats are LeRobot datasets
    return manifest.get("format", "lerobot") if isinstance(manifest, dict) else None


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


def _json_sections(streams: set[str] | None) -> list[str]:
    if streams is None:
        return list(JSON_STREAMS)
    wanted = {"gaze" if s == "head" else s for s in streams}
    return [s for s in JSON_STREAMS if s in wanted]


def _lidar_metadata(layout, coord: str) -> dict:
    beam = LASERSCAN_BEAM[coord]
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


def resolve_coord(fmt: str, coord: str | None) -> str:
    if fmt not in FORMATS:
        raise ValueError(f"format must be one of {FORMATS}, got {fmt!r}")
    chosen = coord or DEFAULT_COORD[fmt]
    coord_info(chosen)
    if fmt == "mcap" and chosen != "ros":
        raise ValueError("MCAP is written in ROS axes (REP-103); leave coord unset.")
    if fmt == "json" and chosen != "native":
        raise ValueError("JSON is exported as recorded; leave coord unset.")
    return chosen


def run_download(
    http: HttpClient,
    spec: DownloadSpec,
    cache_root: Path,
    *,
    progress: bool = True,
    echo: Callable[[str], None] = print,
) -> DownloadResult:
    if not spec.session_ids:
        raise ValueError("No session ids given.")
    fmt = spec.format
    coord = resolve_coord(fmt, spec.coord)
    session_ids = list(dict.fromkeys(spec.session_ids))
    streams = set(spec.streams) if spec.streams is not None else None
    allowed = set(JSON_STREAMS) | {"head"} if fmt == "json" else set(STREAMS)
    if streams is not None and not streams <= allowed:
        unknown = sorted(streams - allowed)
        raise ValueError(f"Unknown stream(s) {unknown}; choose from {sorted(allowed)}.")
    if fmt == "hdf5":
        from cognitive3dpy._export.hdf5_writer import HDF5_LAYOUTS

        if spec.hdf5_layout not in HDF5_LAYOUTS:
            raise ValueError(
                f"hdf5_layout must be one of {HDF5_LAYOUTS}, got {spec.hdf5_layout!r}"
            )
    repo_id = spec.repo_id or default_repo_id(session_ids)
    if not _REPO_ID.match(repo_id):
        raise ValueError(f"repo_id {repo_id!r} must look like 'namespace/name'.")

    root = output_path(Path(spec.out).expanduser().resolve() / repo_id, fmt)
    if root.exists() and not spec.overwrite:
        raise FileExistsError(
            f"{root} already exists; pass overwrite=True to write over it."
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

    sections = _json_sections(streams) if fmt == "json" else _sections_for(streams)
    fetch = fetch_session_export if fmt == "json" else fetch_session
    sessions: list[SessionData] = []
    for session_id in session_ids:
        echo(f"Fetching session {session_id} (project {spec.project_id})")
        sessions.append(
            fetch(http, spec.project_id, session_id, sections, progress=progress)
        )

    if fmt == "json":
        chosen = [s for s in ("camera", "lidar") if s in sections]
    else:
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
        common = (
            None
            if fmt in ("json", "mcap")
            else set.intersection(*(set(s.lidar) for s in sessions))
        )
        lidar_downloader = LidarDownloader(
            http,
            transport=spec.lidar_transport,
            concurrency=spec.concurrency,
            progress=progress,
        )
        for session in sessions:
            sensors = {
                k: v for k, v in session.lidar.items() if common is None or k in common
            }
            lidar_stats.add(
                lidar_downloader.download(
                    cache_for(session).lidar,
                    session.project_id,
                    session.session_id,
                    sensors,
                )
            )

    manifest = {
        "tool": "cognitive3dpy",
        "tool_version": _tool_version(),
        "format": fmt,
        "repo_id": repo_id,
        "api_host": http.host,
        "project_id": spec.project_id,
        "downloaded_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "coord": coord_info(coord),
        "camera_transports": sorted(camera_stats.transports),
        "lidar_transports": sorted(lidar_stats.transports),
    }
    result = DownloadResult(
        path=root,
        repo_id=repo_id,
        episodes=len(sessions),
        frames=0,
        fps=None,
        features=[],
        sessions=session_ids,
        camera=camera_stats,
        lidar=lidar_stats,
        format=fmt,
        coord=coord,
    )

    if fmt == "json":
        from cognitive3dpy._export.json_writer import write_json

        manifest["sessions"] = [
            {"session_id": s.session_id, "sections": sorted(s.raw)} for s in sessions
        ]
        write_json(
            root,
            sessions,
            cache_for,
            manifest,
            streams=set(chosen),
            progress=progress,
        )
        result.files = sorted(
            p
            for p in root.rglob("*.json")
            if SIDECAR_DIR not in p.relative_to(root).parts
        )
        return result

    if fmt == "mcap":
        from cognitive3dpy._export.mcap_writer import write_mcap

        manifest["streams"] = chosen
        bags = write_mcap(
            root, sessions, chosen, spec.sensors, cache_for, manifest, progress=progress
        )
        result.files = [bag.path for bag in bags]
        result.messages = sum(bag.messages for bag in bags)
        result.features = sorted({t for bag in bags for t in bag.topics})
        return result

    options = MappingOptions(
        streams=streams,
        sensors=spec.sensors,
        clock=spec.clock,
        fps=spec.fps,
        coord=coord,
        head_rotation=spec.head_rotation,
    )
    layout = build_layout(sessions, options, cache_for)
    plans = [build_episode(s, layout, options, cache_for) for s in sessions]
    manifest.update(
        {
            "clock": {"kind": layout.clock[0], "arg": layout.clock[1]},
            "fps": layout.fps,
            "streams": layout.streams,
            "sensor_names": layout.sensor_names,
            "event_names": layout.event_names,
            "cameras": {
                cid: {"height": h, "width": w} for cid, (h, w) in layout.cameras.items()
            },
            "lidar": _lidar_metadata(layout, coord),
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
    )
    task = spec.task or "cognitive3d session"
    name = repo_id.split("/")[-1]
    if fmt == "lerobot":
        from cognitive3dpy._export.writer import write_dataset

        write_dataset(
            root,
            repo_id,
            layout,
            plans,
            cache_for,
            task,
            manifest,
            coord=coord,
            progress=progress,
        )
        result.files = [root]
    elif fmt == "hdf5":
        from cognitive3dpy._export.hdf5_writer import write_hdf5

        result.files = write_hdf5(
            root,
            name,
            layout,
            plans,
            cache_for,
            task,
            manifest,
            coord=coord,
            hdf5_layout=spec.hdf5_layout,
            progress=progress,
        )
    else:
        from cognitive3dpy._export.rlds_writer import builder_name, write_rlds

        write_rlds(
            root,
            builder_name(repo_id),
            layout,
            plans,
            cache_for,
            task,
            manifest,
            coord=coord,
            progress=progress,
        )
        result.files = sorted(root.glob("*.tfrecord-*"))

    result.frames = sum(len(p) for p in plans)
    result.fps = layout.fps
    result.features = list(layout.features)
    return result
