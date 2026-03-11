"""Fetch event data from the Cognitive3D API."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import TYPE_CHECKING, Literal

import polars as pl

from cognitive3dpy._filters import build_filters
from cognitive3dpy._lookups import fetch_objects_lookup, fetch_scenes_metadata
from cognitive3dpy._pagination import paginate_sessions
from cognitive3dpy._transform import (
    _clean_name,
    join_scene_names,
    to_output,
    warn_if_empty,
)
from cognitive3dpy.auth import get_project_id

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pandas as pd


def c3d_events(
    project_id: int | None = None,
    n: int = 500,
    start_date: str | int | float | date | None = None,
    end_date: str | int | float | date | None = None,
    exclude_test: bool = True,
    exclude_idle: bool = True,
    min_duration: int = 0,
    output: Literal["polars", "pandas"] = "polars",
    warn_empty: bool = True,
) -> pl.DataFrame | pd.DataFrame:
    """Fetch event data from the Cognitive3D API.

    Parameters
    ----------
    project_id : int, optional
        Project ID. Defaults to the value set via :func:`c3d_project`.
    n : int
        Maximum number of sessions to fetch events from (max 500).
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
        One row per event. Returns an empty DataFrame if no events are found.
    """
    if project_id is not None and (
        isinstance(project_id, bool) or not isinstance(project_id, int)
    ):
        raise TypeError(f"project_id must be an int, got {type(project_id).__name__!r}")
    if isinstance(n, bool) or not isinstance(n, int) or n <= 0:
        raise ValueError(f"n must be a positive integer, got {n!r}")
    if project_id is None:
        project_id = get_project_id()

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

    results = paginate_sessions(
        project_id=project_id,
        session_filters=filters,
        max_sessions=n,
        include_events=True,
    )

    if not results:
        if warn_empty:
            warn_if_empty(pl.DataFrame(), "c3d_events")
        return to_output(pl.DataFrame(), output)

    # Warn about truncated sessions.
    truncated = [s["sessionId"] for s in results if s.get("eventsLimited")]
    if truncated:
        logger.warning(
            "%d session(s) had >8000 events and were truncated: %s",
            len(truncated),
            ", ".join(truncated),
        )

    df = _unnest_events(results)

    if df.is_empty():
        if warn_empty:
            warn_if_empty(pl.DataFrame(), "c3d_events")
        return to_output(pl.DataFrame(), output)

    df = _coerce_event_types(df)
    df = _resolve_objects(df, fetch_objects_lookup(project_id))
    df = join_scene_names(df, fetch_scenes_metadata(project_id)["lookup"])

    return to_output(df, output)


def _unnest_events(results: list[dict]) -> pl.DataFrame:
    """Flatten session results into one row per event.

    Session-level fields are propagated to every event row. Event properties
    are unnested into individual columns prefixed with ``prop_``.
    """
    flat: list[dict] = []
    for session in results:
        base = {
            "project_id": session.get("projectId"),
            "session_id": session.get("sessionId"),
            "participant_id": session.get("participantId"),
            "user_key": session.get("userKey"),
            "device_id": session.get("deviceId"),
            "session_date": session.get("date"),
            "duration": session.get("duration"),
        }
        for event in session.get("events", []):
            props = {
                f"prop_{_clean_name(k)}": v
                for k, v in event.get("properties", {}).items()
            }
            flat.append(
                {
                    **base,
                    "event_name": event.get("name"),
                    "event_date": event.get("date"),
                    "position_x": event.get("x"),
                    "position_y": event.get("y"),
                    "position_z": event.get("z"),
                    "object_id": event.get("object"),
                    "scene_version_id": event.get("parentSceneVersionId"),
                    **props,
                }
            )

    if not flat:
        return pl.DataFrame()
    return pl.DataFrame(flat)


def _coerce_event_types(df: pl.DataFrame) -> pl.DataFrame:
    """Parse date strings and convert duration for event DataFrames."""
    cols = set(df.columns)

    for date_col in ("session_date", "event_date"):
        if date_col in cols and df.schema[date_col] == pl.Utf8:
            df = df.with_columns(
                pl.col(date_col)
                .str.to_datetime(time_zone="UTC", time_unit="us")
                .alias(date_col)
            )

    if "duration" in cols:
        df = df.with_columns(
            (pl.col("duration").cast(pl.Float64) / 1000).alias("duration_s")
        ).drop("duration")

    return df


def _resolve_objects(
    df: pl.DataFrame,
    objects_lookup: dict[str, str],
) -> pl.DataFrame:
    """Add an ``object`` column with the friendly name resolved from ``object_id``.

    If ``object_id`` is not found in the lookup the ``object`` value is null.
    """
    if "object_id" not in df.columns or not objects_lookup:
        return df.with_columns(pl.lit(None).cast(pl.Utf8).alias("object"))

    df = df.with_columns(pl.col("object_id").cast(pl.Utf8))
    lookup_df = pl.DataFrame(
        {
            "object_id": list(objects_lookup.keys()),
            "object": list(objects_lookup.values()),
        }
    )
    df = df.join(lookup_df, on="object_id", how="left")
    null_count = df["object"].null_count()
    if null_count:
        logger.warning(
            "%d event(s) had an unrecognised object_id and will have null object name.",
            null_count,
        )
    return df
