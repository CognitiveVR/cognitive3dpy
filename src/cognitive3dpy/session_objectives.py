"""Fetch per-session objective step results from the Cognitive3D API."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import TYPE_CHECKING, Literal

import polars as pl

from cognitive3dpy._filters import build_filters
from cognitive3dpy._lookups import fetch_objectives_metadata, fetch_scenes_metadata
from cognitive3dpy._pagination import paginate_sessions
from cognitive3dpy._transform import to_output, warn_if_empty
from cognitive3dpy.auth import get_project_id

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pandas as pd


def c3d_session_objectives(
    project_id: int | None = None,
    n: int = 500,
    scene_id: str | None = None,
    scene_version_id: int | None = None,
    start_date: str | int | float | date | None = None,
    end_date: str | int | float | date | None = None,
    exclude_test: bool = True,
    exclude_idle: bool = True,
    min_duration: int = 0,
    output: Literal["polars", "pandas"] = "polars",
    warn_empty: bool = True,
) -> pl.DataFrame | pd.DataFrame:
    """Fetch per-session objective step results from the Cognitive3D API.

    Returns one row per step per session per objective, with columns:
    ``project_id``, ``scene_id``, ``scene_name``, ``scene_version_id``,
    ``session_id``, ``participant_id``, ``session_date``,
    ``objective_id``, ``objective_name``,
    ``step_number``, ``step_description``, ``step_timestamp``,
    ``step_duration``, ``step_duration_sec``, ``step_result``.

    Parameters
    ----------
    project_id : int, optional
        Project ID. Defaults to the value set via :func:`c3d_project`.
    n : int
        Maximum number of sessions to fetch per scene version (max 500).
    scene_id : str, optional
        Filter to a specific scene.
    scene_version_id : int, optional
        Filter to a specific scene version.
    start_date, end_date
        Date range. Accepts date/datetime objects, epoch timestamps,
        or flexible string formats. Defaults to last 30 days.
    exclude_test : bool
        Exclude sessions tagged as test.
    exclude_idle : bool
        Exclude sessions tagged as junk/idle.
    min_duration : int
        Minimum session duration in seconds.
    output : str
        ``"polars"`` or ``"pandas"``.
    warn_empty : bool
        If True (default), emit a UserWarning when 0 rows are returned.

    Returns
    -------
    pl.DataFrame or pd.DataFrame
    """
    # Validation.
    if project_id is not None and (
        isinstance(project_id, bool) or not isinstance(project_id, int)
    ):
        raise TypeError(f"project_id must be an int, got {type(project_id).__name__!r}")
    if isinstance(n, bool) or not isinstance(n, int) or n <= 0:
        raise ValueError(f"n must be a positive integer, got {n!r}")
    if scene_id is not None and not isinstance(scene_id, str):
        raise TypeError(f"scene_id must be a str, got {type(scene_id).__name__!r}")
    if scene_version_id is not None and (
        isinstance(scene_version_id, bool) or not isinstance(scene_version_id, int)
    ):
        raise TypeError(
            f"scene_version_id must be an int, got {type(scene_version_id).__name__!r}"
        )

    if project_id is None:
        project_id = get_project_id()

    # Date defaults.
    if start_date is None:
        start_date = date.today() - timedelta(days=30)
    if end_date is None:
        end_date = date.today()

    filters = build_filters(
        start_date=start_date,
        end_date=end_date,
        exclude_test=exclude_test,
        exclude_idle=exclude_idle,
        min_duration=min_duration,
    )

    # Fetch metadata.
    scenes_meta = fetch_scenes_metadata(project_id)
    versions_df = scenes_meta["versions"]
    lookup = scenes_meta["lookup"]

    if versions_df.is_empty():
        if warn_empty:
            warn_if_empty(pl.DataFrame(), "c3d_session_objectives")
        return to_output(pl.DataFrame(), output)

    objectives_meta = fetch_objectives_metadata(project_id)

    if objectives_meta["objectives"].is_empty():
        if warn_empty:
            warn_if_empty(pl.DataFrame(), "c3d_session_objectives")
        return to_output(pl.DataFrame(), output)

    # Determine target scene versions.
    target = versions_df

    if scene_id is not None:
        target = target.filter(pl.col("scene_id") == scene_id)

    if scene_version_id is not None:
        target = target.filter(pl.col("version_id") == scene_version_id)

    # Build objective lookup dicts once, shared across all scene versions.
    objectives_df = objectives_meta["objectives"]
    versions_obj_df = objectives_meta["versions"]
    components_df = objectives_meta["components"]

    obj_name_map: dict[str, str] = {
        str(row["objective_id"]): row["objective_name"]
        for row in objectives_df.iter_rows(named=True)
    }

    # Include ALL versions so older session data still resolves.
    version_lookup: dict[str, dict] = {}
    for row in versions_obj_df.iter_rows(named=True):
        vid_str = str(row["objective_version_id"])
        oid = str(row["objective_id"])
        version_lookup[vid_str] = {
            "objective_id": oid,
            "objective_name": obj_name_map.get(oid, ""),
        }

    step_desc_map: dict[tuple[str, int], str] = {}
    for row in components_df.filter(pl.col("is_step")).iter_rows(named=True):
        key = (str(row["objective_version_id"]), row["step_number"])
        step_desc_map[key] = row.get("step_name") or row.get("step_detail") or ""

    # Fetch sessions with objective data per scene version, in parallel.
    def _fetch_version(row: dict) -> list[dict]:
        vid = row["version_id"]
        sid = row["scene_id"]
        scene_name = lookup.get(str(vid), "")
        sessions = paginate_sessions(
            project_id=project_id,
            session_filters=filters,
            max_sessions=n,
            session_type="scene",
            scene_id=sid,
            version_id=vid,
            include_all_objective_data=True,
        )
        if not sessions:
            return []
        return _unnest_objective_results(
            sessions=sessions,
            project_id=project_id,
            scene_id=sid,
            scene_name=scene_name,
            scene_version_id=vid,
            obj_name_map=obj_name_map,
            version_lookup=version_lookup,
            step_desc_map=step_desc_map,
        )

    all_rows: list[dict] = []
    version_rows = target.iter_rows(named=True)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_fetch_version, row): row for row in version_rows}
        for future in as_completed(futures):
            try:
                all_rows.extend(future.result())
            except Exception as exc:
                row = futures[future]
                logger.error(
                    "Failed to fetch scene version %s: %s",
                    row.get("version_id"),
                    exc,
                )

    if not all_rows:
        if warn_empty:
            warn_if_empty(pl.DataFrame(), "c3d_session_objectives")
        return to_output(pl.DataFrame(), output)

    df = pl.DataFrame(all_rows)

    # Convert step_timestamp to datetime (may be epoch-ms or ISO string)
    if "step_timestamp" in df.columns:
        dtype = df.schema["step_timestamp"]
        if dtype == pl.Utf8:
            df = df.with_columns(
                pl.col("step_timestamp")
                .str.to_datetime(time_zone="UTC", time_unit="us")
                .alias("step_timestamp")
            )
        elif dtype in (pl.Int64, pl.Float64):
            df = df.with_columns(
                pl.col("step_timestamp")
                .cast(pl.Int64)
                .cast(pl.Datetime("ms"))
                .dt.replace_time_zone("UTC")
                .alias("step_timestamp")
            )

    # Convert session_date string to datetime
    if "session_date" in df.columns and df.schema["session_date"] == pl.Utf8:
        df = df.with_columns(
            pl.col("session_date")
            .str.to_datetime(time_zone="UTC", time_unit="us")
            .alias("session_date")
        )

    return to_output(df, output)


def _unnest_objective_results(
    sessions: list[dict],
    project_id: int,
    scene_id: int | str,
    scene_name: str,
    scene_version_id: int,
    obj_name_map: dict[str, str],
    version_lookup: dict[str, dict],
    step_desc_map: dict[tuple[str, int], str],
) -> list[dict]:
    """Flatten objectiveResults from sessions into per-step rows."""
    rows: list[dict] = []
    for session in sessions:
        obj_results = session.get("objectiveResults")
        if not obj_results or not isinstance(obj_results, dict):
            continue

        sess_id = session.get("sessionId", "")
        participant_id = session.get("participantId", "")
        session_date = session.get("date", "")

        for key_str, steps in obj_results.items():
            if not key_str.isdigit():
                continue

            # Try version lookup first, then objective_id fallback
            match = version_lookup.get(key_str)
            if match is not None:
                obj_id = match["objective_id"]
                obj_name = match["objective_name"]
                resolved_ver_id = key_str
            else:
                obj_name = obj_name_map.get(key_str)
                if obj_name is None:
                    continue  # stale/irrelevant version, skip
                obj_id = key_str
                resolved_ver_id = None

            if not isinstance(steps, list):
                continue

            for step in steps:
                step_num = step.get("step")
                duration = step.get("duration")
                timestamp = step.get("timestamp")

                # Lookup step description
                desc_key = (resolved_ver_id, step_num) if resolved_ver_id else None
                step_desc = step_desc_map.get(desc_key, "") if desc_key else ""

                rows.append(
                    {
                        "project_id": project_id,
                        "scene_id": scene_id,
                        "scene_name": scene_name,
                        "scene_version_id": scene_version_id,
                        "session_id": sess_id,
                        "participant_id": participant_id,
                        "session_date": session_date,
                        "objective_id": obj_id,
                        "objective_name": obj_name,
                        "step_number": step_num,
                        "step_description": step_desc,
                        "step_timestamp": timestamp,
                        "step_duration": duration,
                        "step_duration_sec": duration / 1000
                        if duration is not None
                        else None,
                        "step_result": step.get("result", ""),
                    }
                )

    return rows
