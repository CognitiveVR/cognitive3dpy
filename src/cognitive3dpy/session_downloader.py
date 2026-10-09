"""Download sessions as robot-learning datasets: LeRobot, MCAP (ROS 2), HDF5
(robomimic / ALOHA), RLDS (Open X-Embodiment) or the raw JSON.

    import cognitive3dpy as c3d
    from cognitive3dpy import session_downloader

    c3d.c3d_auth()  # reads C3D_API_KEY
    session_downloader.download(output="datasets", project_id=700, session_id="...")

Each format needs its extra: ``pip install "cognitive3dpy[json]"`` for the
default, ``[lerobot]`` (Python 3.12+), ``[mcap]``, ``[hdf5]`` or ``[rlds]``.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from cognitive3dpy import _client
from cognitive3dpy.auth import get_api_key, get_project_id

if TYPE_CHECKING:
    from cognitive3dpy._export.download import DownloadResult
    from cognitive3dpy._export.http import HttpClient
    from cognitive3dpy._export.validate import ValidationReport

_BASE_MODULES = ("numpy", "PIL", "tqdm", "zstandard")
_FORMAT_MODULES = {
    "lerobot": ("lerobot", "datasets"),
    "mcap": ("mcap", "mcap_ros2"),
    "hdf5": ("h5py",),
    "rlds": ("google_crc32c",),
    "json": (),
}


def _install_hint(extra: str) -> str:
    hint = f'needs the {extra} extra: pip install "cognitive3dpy[{extra}]"'
    return hint + " (Python 3.12 or newer)" if extra == "lerobot" else hint


def _require(function: str, *modules: str, extra: str = "lerobot") -> None:
    if extra == "lerobot" and sys.version_info < (3, 12):
        raise ImportError(f"session_downloader.{function}() {_install_hint(extra)}.")
    missing = [m for m in modules if importlib.util.find_spec(m) is None]
    if missing:
        raise ImportError(
            f"session_downloader.{function}() {_install_hint(extra)}; missing: "
            f"{', '.join(missing)}."
        )


def _http() -> HttpClient:
    from cognitive3dpy._export.http import HttpClient

    return HttpClient(get_api_key(), _client.get_base_url(), timeout=_client._timeout)


def _project(project_id: int | None) -> int:
    if project_id is None:
        return get_project_id()
    if isinstance(project_id, bool) or not isinstance(project_id, int):
        raise TypeError(f"project_id must be an int, got {type(project_id).__name__!r}")
    return project_id


def download(
    *,
    session_id: str | list[str],
    output: str | Path = "datasets",
    project_id: int | None = None,
    format: str = "json",
    repo_id: str | None = None,
    streams: set[str] | list[str] | None = None,
    sensors: list[str] | None = None,
    clock: str | None = None,
    fps: int | None = None,
    coord: str | None = None,
    hdf5_layout: str = "robomimic",
    head_rotation: bool = False,
    task: str | None = None,
    overwrite: bool = True,
    camera_transport: str = "auto",
    lidar_transport: str = "auto",
    concurrency: int = 4,
    cache_dir: str | Path | None = None,
    progress: bool = True,
    validate: bool = True,
) -> DownloadResult:
    """Download a session as a dataset in ``format``.

    ==========  ========================================  ======================
    format      what                                      axes (``coord``)
    ==========  ========================================  ======================
    json        the dashboard's JSON export + media       as recorded
    lerobot     LeRobot dataset                           right-handed Y-up
    mcap        rosbag2 bag of ROS 2 messages             ROS (REP-103)
    hdf5        robomimic or ALOHA demonstrations         ROS (REP-103)
    rlds        Open X-Embodiment TFDS folder             ROS (REP-103)
    ==========  ========================================  ======================

    One session is written to ``<output>/cognitive3d/<session_id>/``, whatever
    the format. When that folder already holds another format the download goes
    to ``<session_id>_<format>/`` instead. Downloading into a folder that exists
    replaces the files the new download also has and keeps the rest.

    One session becomes one episode; a list of sessions becomes one dataset with
    an episode per session (``repo_id`` is then required). lerobot, hdf5 and rlds
    align head pose, gaze, sensors, events, camera frames, lidar and the
    play-area boundary onto one clock, and keep lidar scans, point clouds and
    boundary polygons whole in ``c3d/sessions/<session_id>/``. mcap keeps every
    sample at its own timestamp as ROS 2 messages. json is the dashboard's session
    export byte for byte, plus the camera and lidar sections and the frames and
    scans as recorded.

    Parameters
    ----------
    session_id : str or list of str
        Session id(s), as shown in the dashboard or a replay link.
    output : str or Path
        Parent directory. Default ``"datasets"``.
    project_id : int, optional
        Defaults to the project set by ``c3d_project()``.
    format : str
        ``"json"`` (default), ``"lerobot"``, ``"mcap"``, ``"hdf5"`` or ``"rlds"``.
    repo_id : str, optional
        ``"namespace/name"``; names the output folder. Default for one session:
        ``"cognitive3d/<session_id>"``.
    streams : set of str, optional
        Subset of ``head``, ``gaze``, ``sensors``, ``events``, ``camera``,
        ``lidar``, ``boundary``; for json, of the API sections (``gaze``,
        ``fixation``, ``events``, ``dynamics``, ``sensors``, ``boundary``,
        ``space_visits``, ``camera``, ``lidar``). Default: everything there is.
    sensors : list of str, optional
        Only these sensor channels (default: all).
    clock : str, optional
        ``"camera[:<id>]"``, ``"gaze"`` or ``"fixed:<hz>"``. Default: the
        camera if the session has one, otherwise gaze. (lerobot, hdf5, rlds)
    fps : int, optional
        Override the dataset fps. (lerobot, hdf5, rlds)
    coord : str, optional
        ``"rh_yup"``, ``"ros"`` (REP-103: x forward, y left, z up) or
        ``"native"`` (as recorded, left-handed Y-up). Default: per format, as
        above; mcap is always ``"ros"`` and json always ``"native"``.
    hdf5_layout : str
        ``"robomimic"`` (default; one file, ``data/demo_<i>``, as Isaac Lab
        Mimic reads it) or ``"aloha"`` (``episode_<i>.hdf5``, as ACT reads it).
    head_rotation : bool
        Add the head quaternion to ``observation.state``.
    task : str, optional
        Task / language instruction stored with every frame.
    overwrite : bool
        Write over an earlier download of the same format (default ``True``);
        ``False`` raises instead.
    camera_transport : str
        ``"auto"`` (stream, falling back to one request per frame),
        ``"stream"`` or ``"per-frame"``.
    lidar_transport : str
        ``"auto"`` (stream, falling back to one request per scan),
        ``"stream"`` or ``"per-scan"``.
    concurrency : int
        Parallel requests when frames or scans are fetched one at a time.
    cache_dir : str or Path, optional
        Media cache. Default ``C3D_CACHE_DIR`` or ``~/.cache/cognitive3dpy``;
        downloaded frames and scans are reused on re-runs and across formats.
    progress : bool
        Show progress bars (default ``True``).
    validate : bool
        Read the finished dataset back to check it (default ``True``).

    Returns
    -------
    DownloadResult
        ``path``, ``format``, ``files``, ``frames``, ``fps``, ``features``,
        ``camera`` / ``lidar`` download stats, and ``load()`` to open a lerobot
        (``LeRobotDataset``) or hdf5 (``h5py.File``) dataset.
    """
    from cognitive3dpy._export.download import FORMATS

    if format not in FORMATS:
        raise ValueError(f"format must be one of {FORMATS}, got {format!r}")
    _require("download", *_BASE_MODULES, *_FORMAT_MODULES[format], extra=format)
    from cognitive3dpy._export.download import (
        DownloadSpec,
        default_cache_dir,
        run_download,
    )
    from cognitive3dpy._export.errors import C3DDatasetError
    from cognitive3dpy._export.validate import validate_export

    session_ids = [session_id] if isinstance(session_id, str) else list(session_id)
    spec = DownloadSpec(
        project_id=_project(project_id),
        session_ids=session_ids,
        out=output,
        repo_id=repo_id,
        streams=streams,
        sensors=sensors,
        clock=clock,
        fps=fps,
        coord=coord,
        head_rotation=head_rotation,
        task=task,
        format=format,
        hdf5_layout=hdf5_layout,
        overwrite=overwrite,
        camera_transport=camera_transport,
        lidar_transport=lidar_transport,
        concurrency=concurrency,
    )
    cache_root = Path(cache_dir).expanduser() if cache_dir else default_cache_dir()
    echo = print if progress else (lambda _message: None)
    with _http() as http:
        result = run_download(http, spec, cache_root, progress=progress, echo=echo)

    if validate:
        report = validate_export(result.path, result.repo_id)
        if not report.ok:
            raise C3DDatasetError(
                f"{result.path} failed validation: " + "; ".join(report.problems)
            )
    episodes = "episode" if result.episodes == 1 else "episodes"
    if format == "mcap":
        bags = "bag" if len(result.files) == 1 else "bags"
        summary = f"{result.messages} messages on {len(result.features)} topics"
        echo(f"✔ {summary} ({len(result.files)} {bags}) → {result.path}")
    elif format == "json":
        echo(f"✔ {len(result.files)} JSON files + media → {result.path}")
    else:
        echo(
            f"✔ {result.frames} frames ({result.episodes} {episodes}, "
            f"{result.fps} fps) → {result.path}"
        )
    return result


def inspect(session_id: str, *, project_id: int | None = None) -> dict:
    """Summarize what a session contains, without downloading camera frames.

    Returns a dict with ``streams``, ``duration_s``, ``gaze`` sample count and
    rate, ``sensors`` channel names, ``events``, ``cameras`` (frames, size, rate
    per camera), ``lidar`` (kind, scans, rate per sensor) and ``boundary``.
    """
    _require("inspect", "numpy", "tqdm", extra="json")
    from cognitive3dpy._export.sessions import fetch_session

    project = _project(project_id)
    with _http() as http:
        session = fetch_session(http, project, session_id, progress=False)
    return _summarize(session)


def validate(path: str | Path, *, repo_id: str | None = None) -> ValidationReport:
    """Read a downloaded dataset back and check it: frame counts, timestamps and
    images (lerobot, hdf5, rlds), message counts (mcap) or that the JSON parses.
    ``report.ok`` is ``True`` when nothing is wrong."""
    manifest = Path(path) / "c3d" / "download.json"
    fmt = json.loads(manifest.read_text()).get("format") if manifest.exists() else None
    fmt = fmt or "lerobot"
    _require("validate", *_BASE_MODULES, *_FORMAT_MODULES[fmt], extra=fmt)
    from cognitive3dpy._export.validate import validate_export

    return validate_export(path, repo_id)


def _rate(times) -> float | None:
    if len(times) < 2 or times[-1] <= times[0]:
        return None
    return round((len(times) - 1) / float(times[-1] - times[0]), 2)


def _summarize(session) -> dict:
    cameras = {
        camera_id: {
            "frames": len(refs),
            "width": refs[0].width,
            "height": refs[0].height,
            "format": refs[0].format,
            "rate_hz": _rate([r.time for r in refs]),
            "versions": sorted({r.version_id for r in refs}),
        }
        for camera_id, refs in session.cameras.items()
    }
    start = None
    if session.start_timestamp:
        start = datetime.fromtimestamp(session.start_timestamp, tz=UTC)
    gaze = session.gaze
    return {
        "project_id": session.project_id,
        "session_id": session.session_id,
        "start": start.isoformat(timespec="seconds") if start else None,
        "duration_s": session.duration,
        "device": session.metadata.get("hmd"),
        "streams": sorted(session.streams),
        "gaze": (
            {"samples": len(gaze), "rate_hz": _rate(gaze.times)} if gaze else None
        ),
        "sensors": sorted(session.sensors),
        "events": {
            "count": len(session.events),
            "names": sorted({e.name for e in session.events}),
        },
        "cameras": cameras,
        "lidar": {
            sensor_id: _lidar_summary(refs) for sensor_id, refs in session.lidar.items()
        },
        "boundary": (
            {
                "poses": len(session.boundary.times),
                "shapes": len(session.boundary.shapes),
            }
            if session.boundary is not None
            else None
        ),
    }


def _lidar_summary(refs) -> dict:
    summary = {
        "kind": refs[0].kind,
        "scans": len(refs),
        "rate_hz": _rate([r.time for r in refs]),
        "versions": sorted({r.version_id for r in refs}),
    }
    if refs[0].kind == "laserscan":
        summary["range_count"] = max(r.range_count for r in refs)
    else:
        points = [r.point_count for r in refs]
        summary["points_mean"] = round(sum(points) / len(points))
        summary["points_max"] = max(points)
    return summary
