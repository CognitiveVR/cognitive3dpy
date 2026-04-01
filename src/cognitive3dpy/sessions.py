"""Fetch session data from the Cognitive3D API."""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING, Literal

import polars as pl

from cognitive3dpy._filters import build_filters
from cognitive3dpy._lookups import fetch_scenes_metadata
from cognitive3dpy._pagination import paginate_sessions
from cognitive3dpy._transform import (
    coerce_types,
    join_scene_names,
    normalize_columns,
    select_compact,
    to_output,
    warn_if_empty,
)
from cognitive3dpy.auth import get_project_id

if TYPE_CHECKING:
    import pandas as pd


def c3d_sessions(
    project_id: int | None = None,
    n: int = 500,
    session_type: Literal["project", "scene"] = "project",
    scene_id: str | None = None,
    scene_version_id: int | None = None,
    start_date: str | int | float | date | None = None,
    end_date: str | int | float | date | None = None,
    exclude_test: bool = True,
    exclude_idle: bool = True,
    min_duration: int = 0,
    compact: bool = True,
    output: Literal["polars", "pandas"] = "polars",
    warn_empty: bool = True,
) -> pl.DataFrame | pd.DataFrame:
    """Fetch session data from the Cognitive3D API.

    Parameters
    ----------
    project_id : int, optional
        Project ID. Defaults to the value set via :func:`c3d_project`.
    n : int
        Maximum number of sessions to fetch (max 500).
    session_type : str
        ``"project"`` (default) or ``"scene"``.
    scene_id : str, optional
        Filter to a specific scene (scene mode only).
    scene_version_id : int, optional
        Filter to a specific scene version (scene mode only).
    start_date, end_date
        Date range. Accepts date/datetime objects, epoch timestamps,
        or flexible string formats. Defaults to last 30 days.
    exclude_test : bool
        Exclude sessions tagged as test.
    exclude_idle : bool
        Exclude sessions tagged as junk/idle.
    min_duration : int
        Minimum session duration in seconds.
    compact : bool
        If True, return only the compact column subset.
    output : str
        ``"polars"`` or ``"pandas"``.
    warn_empty : bool
        If True (default), emit a UserWarning when 0 rows are returned.

    Returns
    -------
    pl.DataFrame or pd.DataFrame
    """
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

    has_scene_args = scene_id is not None or scene_version_id is not None
    if session_type == "project" and has_scene_args:
        raise ValueError(
            "scene_id and scene_version_id are only valid for session_type='scene'."
        )

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

    if session_type == "project":
        results = paginate_sessions(
            project_id=project_id,
            session_filters=filters,
            max_sessions=n,
        )
        if not results:
            if warn_empty:
                warn_if_empty(pl.DataFrame(), "c3d_sessions")
            return to_output(pl.DataFrame(), output)
        lookup: dict[str, str] = {}

    elif session_type == "scene":
        results, lookup = _fetch_scene_sessions(
            project_id,
            filters,
            n,
            scene_id,
            scene_version_id,
        )
        if not results:
            if warn_empty:
                warn_if_empty(pl.DataFrame(), "c3d_sessions")
            return to_output(pl.DataFrame(), output)

    else:
        raise ValueError(
            f"session_type must be 'project' or 'scene', got {session_type!r}"
        )

    # Drop the legacy top-level "hmd" field before DataFrame construction.
    # This field has inconsistent types across projects (null/int/string)
    # which causes Polars schema inference failures. The same data is
    # available reliably via the properties struct as c3d_device_hmd_type.
    for r in results:
        r.pop("hmd", None)

    df = pl.DataFrame(results)
    df = normalize_columns(df)
    df = coerce_types(df)

    # Duplicate c3d_device_hmd_type under the legacy "hmd" name so
    # downstream consumers (Fabric notebook, semantic model) that
    # reference "hmd" continue to work without schema changes.
    if "c3d_device_hmd_type" in df.columns:
        df = df.with_columns(pl.col("c3d_device_hmd_type").alias("hmd"))

    if lookup:
        df = join_scene_names(df, lookup)

    if compact:
        df = select_compact(df)

    return to_output(df, output)


def _fetch_scene_sessions(
    project_id: int,
    filters: list[dict],
    n: int,
    scene_id: str | None,
    scene_version_id: int | None,
) -> tuple[list[dict], dict[str, str]]:
    """Fetch sessions in scene mode.

    Returns
    -------
    tuple[list[dict], dict[str, str]]
        (combined session results, version_id → scene_name lookup)
    """
    scenes_meta = fetch_scenes_metadata(project_id)
    versions_df = scenes_meta["versions"]
    lookup = scenes_meta["lookup"]

    if versions_df.is_empty():
        return [], lookup

    # Determine target scene versions.
    target = versions_df

    if scene_id is not None:
        target = target.filter(pl.col("scene_id") == scene_id)

    if scene_version_id is not None:
        target = target.filter(pl.col("version_id") == scene_version_id)
    else:
        # Default: latest version per scene (max version_number).
        target = target.sort("version_number").group_by("scene_id").last()

    # Fetch sessions for each target version.
    all_results: list[dict] = []
    for row in target.iter_rows(named=True):
        results = paginate_sessions(
            project_id=project_id,
            session_filters=filters,
            max_sessions=n,
            session_type="scene",
            scene_id=row["scene_id"],
            version_id=row["version_id"],
        )
        all_results.extend(results)

    return all_results, lookup
