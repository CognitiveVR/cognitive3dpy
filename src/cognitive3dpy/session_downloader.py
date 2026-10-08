"""Download sessions as LeRobot datasets for robot learning.

    import cognitive3dpy as c3d
    from cognitive3dpy import session_downloader

    c3d.c3d_auth()  # reads C3D_API_KEY
    session_downloader.download(output="leRobot", project_id=700, session_id="...")

Needs the ``lerobot`` extra (Python 3.12+): ``pip install "cognitive3dpy[lerobot]"``.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from cognitive3dpy import _client
from cognitive3dpy.auth import get_api_key, get_project_id

if TYPE_CHECKING:
    from cognitive3dpy._lerobot.download import DownloadResult
    from cognitive3dpy._lerobot.http import HttpClient
    from cognitive3dpy._lerobot.validate import ValidationReport

_INSTALL_HINT = (
    'needs the lerobot extra: pip install "cognitive3dpy[lerobot]" '
    "(Python 3.12 or newer)"
)


def _require(function: str, *modules: str, lerobot: bool = False) -> None:
    if lerobot and sys.version_info < (3, 12):
        raise ImportError(f"session_downloader.{function}() {_INSTALL_HINT}.")
    missing = [m for m in modules if importlib.util.find_spec(m) is None]
    if missing:
        raise ImportError(
            f"session_downloader.{function}() {_INSTALL_HINT}; missing: "
            f"{', '.join(missing)}."
        )


def _http() -> HttpClient:
    from cognitive3dpy._lerobot.http import HttpClient

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
    repo_id: str | None = None,
    streams: set[str] | list[str] | None = None,
    sensors: list[str] | None = None,
    clock: str | None = None,
    fps: int | None = None,
    coord: str = "rh_yup",
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
    """Download a session as a LeRobot dataset in ``<output>/<repo_id>``.

    One session becomes one episode; a list of sessions becomes one dataset with
    an episode per session (``repo_id`` is then required). Head pose, gaze,
    sensors, events, camera frames, lidar and the play-area boundary are aligned
    onto one clock and converted from left-handed to right-handed Y-up
    coordinates. Lidar scans, point clouds and boundary polygons are also kept
    whole in ``c3d/sessions/<session_id>/`` next to the dataset.

    Parameters
    ----------
    session_id : str or list of str
        Session id(s), as shown in the dashboard or a replay link.
    output : str or Path
        Parent directory. Default ``"datasets"``, so one session lands in
        ``datasets/cognitive3d/<session_id>``.
    project_id : int, optional
        Defaults to the project set by ``c3d_project()``.
    repo_id : str, optional
        ``"namespace/name"``. Default for one session:
        ``"cognitive3d/<session_id>"``.
    streams : set of str, optional
        Subset of ``head``, ``gaze``, ``sensors``, ``events``, ``camera``,
        ``lidar``, ``boundary``. Default: every stream the sessions have.
    sensors : list of str, optional
        Only these sensor channels (default: all).
    clock : str, optional
        ``"camera[:<id>]"``, ``"gaze"`` or ``"fixed:<hz>"``. Default: the
        camera if the session has one, otherwise gaze.
    fps : int, optional
        Override the dataset fps.
    coord : str
        ``"rh_yup"`` (default) or ``"native"`` (as recorded, left-handed).
    head_rotation : bool
        Add the head quaternion to ``observation.state``.
    task : str, optional
        Task / language instruction stored with every frame.
    overwrite : bool
        Replace an existing dataset at the target path (default ``True``).
    camera_transport : str
        ``"auto"`` (stream, falling back to one request per frame),
        ``"stream"`` or ``"per-frame"``.
    lidar_transport : str
        ``"auto"`` (stream, falling back to one request per scan),
        ``"stream"`` or ``"per-scan"``.
    concurrency : int
        Parallel requests when frames or scans are fetched one at a time.
    cache_dir : str or Path, optional
        Camera frame cache. Default ``C3D_CACHE_DIR`` or
        ``~/.cache/cognitive3dpy``; downloaded frames are reused on re-runs.
    progress : bool
        Show progress bars (default ``True``).
    validate : bool
        Load the finished dataset with lerobot to check it (default ``True``).

    Returns
    -------
    DownloadResult
        ``path``, ``repo_id``, ``frames``, ``fps``, ``features``, ``camera``
        download stats, and ``load()`` to open it as a ``LeRobotDataset``.
    """
    _require(
        "download",
        "numpy",
        "PIL",
        "tqdm",
        "zstandard",
        "lerobot",
        "datasets",
        lerobot=True,
    )
    from cognitive3dpy._lerobot.download import (
        DownloadSpec,
        default_cache_dir,
        run_download,
    )
    from cognitive3dpy._lerobot.errors import C3DDatasetError
    from cognitive3dpy._lerobot.validate import validate_dataset

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
        report = validate_dataset(result.path, result.repo_id)
        if not report.ok:
            raise C3DDatasetError(
                f"{result.path} failed validation: " + "; ".join(report.problems)
            )
    episodes = "episode" if result.episodes == 1 else "episodes"
    echo(
        f"✔ {result.frames} frames ({result.episodes} {episodes}, {result.fps} fps)"
        f" → {result.path}"
    )
    return result


def inspect(session_id: str, *, project_id: int | None = None) -> dict:
    """Summarize what a session contains, without downloading camera frames.

    Returns a dict with ``streams``, ``duration_s``, ``gaze`` sample count and
    rate, ``sensors`` channel names, ``events``, ``cameras`` (frames, size, rate
    per camera), ``lidar`` (kind, scans, rate per sensor) and ``boundary``.
    """
    _require("inspect", "numpy", "tqdm")
    from cognitive3dpy._lerobot.sessions import fetch_session

    project = _project(project_id)
    with _http() as http:
        session = fetch_session(http, project, session_id, progress=False)
    return _summarize(session)


def validate(path: str | Path, *, repo_id: str | None = None) -> ValidationReport:
    """Load a downloaded dataset with lerobot and check frame counts, timestamps
    and images. ``report.ok`` is ``True`` when nothing is wrong."""
    _require("validate", "numpy", "lerobot", "datasets", lerobot=True)
    from cognitive3dpy._lerobot.validate import validate_dataset

    return validate_dataset(path, repo_id)


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
