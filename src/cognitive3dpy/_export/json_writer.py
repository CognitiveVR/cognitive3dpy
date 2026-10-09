"""Sessions as the dashboard exports them: the session JSON files byte for byte
(``<section>_<session_id>.json``), plus the camera and lidar sections, which that
export leaves out, as the replay API serves them, and the camera frames and lidar
scans as their recorded bytes under ``camera/<version>/<camera>/`` and
``lidar/<version>/<sensor>/`` (``<date ms>_<seq>.<ext>``). Coordinates stay
left-handed Y-up."""

from __future__ import annotations

import shutil
from pathlib import Path

from cognitive3dpy._export.mapping import CacheFor
from cognitive3dpy._export.models import SessionData
from cognitive3dpy._export.progress import progress_bar
from cognitive3dpy._export.sidecar import safe_name, staged, write_manifest

CONVENTIONS = {
    "export": "gaze, fixation, events, dynamics, sensors, boundary and space_visits "
    "files are the dashboard's session export: times in epoch seconds, rows in "
    "one list",
    "camera_and_lidar": "camera_ and lidar_ files are the replay API's sections: "
    "time in seconds from the session start, date in epoch milliseconds, rows "
    "keyed by scene version id",
    "coordinates": "as recorded: left-handed, Y-up, metres",
}
BLOB_FORMATS = {
    "camera": "the image as captured (format and size per row of camera_*.json)",
    "laserscan": "N little-endian float32 ranges, then N float32 intensities when "
    "the row's hasIntensities is true; N = rangeCount",
    "pointcloud": "zstd-compressed (zstdLevel > 0) planar X[N] Y[N] Z[N] "
    "(positionFormat int16_mm or float32), then colours C[N] (colorFormat rgb565 / "
    "rgb332 / rgb888 / none), then intensities I[N] (uint8); sensor-local, "
    "world = rotate(pose.q, point) + pose.p",
}


def _media_name(ref) -> str:
    return f"{ref.date:013d}_{ref.seq:010d}.{safe_name(ref.extension)}"


def write_json(
    root: Path,
    sessions: list[SessionData],
    cache_for: CacheFor,
    manifest: dict,
    *,
    streams: set[str],
    progress: bool = True,
) -> dict[str, list[str]]:
    """Writes ``root`` (via ``<root>.partial``); returns each session's files."""
    written: dict[str, list[str]] = {}
    with staged(root) as partial:
        for session in sessions:
            base = partial
            if len(sessions) > 1:
                base = partial / safe_name(session.session_id)
                base.mkdir()
            files = []
            for section, body in session.raw.items():
                name = f"{section}_{safe_name(session.session_id)}.json"
                (base / name).write_bytes(body)
                files.append(name)
            media = []
            if "camera" in streams:
                media += [
                    ("camera", ref, cache_for(session).camera)
                    for refs in session.cameras.values()
                    for ref in refs
                ]
            if "lidar" in streams:
                media += [
                    ("lidar", ref, cache_for(session).lidar)
                    for refs in session.lidar.values()
                    for ref in refs
                    if ref.range_count > 0 or ref.point_count > 0
                ]
            desc = f"Copying {session.session_id} media"
            with progress_bar(
                total=len(media), desc=desc, unit="file", disable=not progress
            ) as bar:
                for kind, ref, cache in media:
                    folder = Path(kind) / str(ref.version_id) / safe_name(ref.stream_id)
                    (base / folder).mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(cache.path(ref), base / folder / _media_name(ref))
                    bar.update(1)
            files += sorted({str(Path(k) / str(r.version_id)) for k, r, _ in media})
            written[session.session_id] = files
        manifest["files"] = written
        manifest["conventions"] = CONVENTIONS
        manifest["blob_formats"] = BLOB_FORMATS
        write_manifest(partial, manifest)
    return written
