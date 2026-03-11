"""Fetch objective results from the Cognitive3D API."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import TYPE_CHECKING, Literal

import polars as pl

from cognitive3dpy._client import c3d_request
from cognitive3dpy._filters import build_filters
from cognitive3dpy._lookups import fetch_objectives_metadata
from cognitive3dpy._transform import to_output, warn_if_empty
from cognitive3dpy.auth import get_project_id

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pandas as pd


def c3d_objective_results(
    project_id: int | None = None,
    objective_id: int | None = None,
    objective_version_id: int | None = None,
    group_by: Literal["version", "steps"] = "version",
    exclude_test: bool = True,
    exclude_idle: bool = True,
    start_date: str | int | float | date | None = None,
    end_date: str | int | float | date | None = None,
    output: Literal["polars", "pandas"] = "polars",
    warn_empty: bool = True,
) -> pl.DataFrame | pd.DataFrame:
    """Fetch objective results from the Cognitive3D API.

    Parameters
    ----------
    project_id : int, optional
        Project ID. Defaults to the value set via :func:`c3d_project`.
    objective_id : int, optional
        Filter to a single objective.
    objective_version_id : int, optional
        Filter to a single objective version.
    group_by : str
        ``"version"`` (default) — one row per objective version.
        ``"steps"`` — one row per step across all versions.
    exclude_test : bool
        Exclude sessions tagged as test.
    exclude_idle : bool
        Exclude sessions tagged as junk/idle.
    start_date, end_date
        Date range. Accepts date/datetime objects, epoch timestamps,
        or flexible string formats. Defaults to last 30 days.
    output : str
        ``"polars"`` or ``"pandas"``.
    warn_empty : bool
        If True (default), emit a UserWarning when 0 rows are returned.

    Returns
    -------
    pl.DataFrame or pd.DataFrame
    """
    if group_by not in ("version", "steps"):
        raise ValueError(f"group_by must be 'version' or 'steps', got {group_by!r}")
    if project_id is not None and (
        isinstance(project_id, bool) or not isinstance(project_id, int)
    ):
        raise TypeError(f"project_id must be an int, got {type(project_id).__name__!r}")
    if objective_id is not None and (
        isinstance(objective_id, bool) or not isinstance(objective_id, int)
    ):
        raise TypeError(
            f"objective_id must be an int, got {type(objective_id).__name__!r}"
        )
    if objective_version_id is not None and (
        isinstance(objective_version_id, bool)
        or not isinstance(objective_version_id, int)
    ):
        raise TypeError(
            "objective_version_id must be an int, "
            f"got {type(objective_version_id).__name__!r}"
        )

    if project_id is None:
        project_id = get_project_id()

    # Fetch objectives metadata.
    metadata = fetch_objectives_metadata(project_id)

    if metadata["objectives"].is_empty():
        if warn_empty:
            warn_if_empty(pl.DataFrame(), "c3d_objective_results")
        return to_output(pl.DataFrame(), output)

    # Build filters.
    if start_date is None:
        start_date = date.today() - timedelta(days=30)
    if end_date is None:
        end_date = date.today()

    filters = build_filters(
        start_date=start_date,
        end_date=end_date,
        exclude_test=exclude_test,
        exclude_idle=exclude_idle,
    )

    # Fetch objective-level results.
    body: dict = {
        "projectId": project_id,
        "sliceByObjectiveVersion": True,
        "sessionFilters": filters,
        "eventFilters": [],
        "separableEventFilters": [],
        "userFilters": [],
    }
    if objective_id is not None:
        body["objectiveId"] = objective_id
    if objective_version_id is not None:
        body["objectiveVersionId"] = objective_version_id

    raw_results = c3d_request("/v0/datasets/objectives/objectiveResultQueries", body)

    if not raw_results:
        if warn_empty:
            warn_if_empty(pl.DataFrame(), "c3d_objective_results")
        return to_output(pl.DataFrame(), output)

    if group_by == "version":
        df = _parse_objective_results(raw_results, metadata)
        if warn_empty:
            warn_if_empty(df, "c3d_objective_results")
        return to_output(df, output)

    # Fetch step results.
    components = metadata["components"]

    # Always use all version IDs from metadata, optionally filtered.
    versions_df = metadata["versions"]
    if objective_id is not None:
        versions_df = versions_df.filter(pl.col("objective_id") == objective_id)
    if objective_version_id is not None:
        versions_df = versions_df.filter(
            pl.col("objective_version_id") == objective_version_id
        )
    version_ids = versions_df["objective_version_id"].to_list()

    steps_df = _fetch_step_results(
        project_id=project_id,
        version_ids=version_ids,
        session_filters=filters,
        versions_meta=metadata["versions"],
        objectives_meta=metadata["objectives"],
        components=components,
    )

    if warn_empty:
        warn_if_empty(steps_df, "c3d_objective_results")
    return to_output(steps_df, output)


def _parse_objective_results(
    raw_results: list[dict],
    metadata: dict,
) -> pl.DataFrame:
    """Parse raw objective version results and join with metadata."""
    rows = [
        {
            "objective_version_id": r.get("objectiveVersionId"),
            "succeeded": r.get("succeeded", 0),
            "failed": r.get("failed", 0),
        }
        for r in raw_results
    ]
    df = pl.DataFrame(rows)
    df = _add_completion_rate(df)
    df = df.join(
        metadata["versions"].select(
            [
                "objective_version_id",
                "objective_id",
                "version_is_active",
                "version_number",
            ]
        ),
        on="objective_version_id",
        how="left",
    )
    df = df.join(
        metadata["objectives"].select(["objective_id", "objective_name"]),
        on="objective_id",
        how="left",
    )
    null_count = df["objective_name"].null_count()
    if null_count:
        logger.warning(
            "%d row(s) had an unrecognised objective_id "
            "and will have null objective_name.",
            null_count,
        )
    return df.select(
        [
            "objective_id",
            "objective_name",
            "objective_version_id",
            "version_number",
            "version_is_active",
            "succeeded",
            "failed",
            "completion_rate",
        ]
    )


def _fetch_step_results(
    project_id: int,
    version_ids: list,
    session_filters: list[dict],
    versions_meta: pl.DataFrame,
    objectives_meta: pl.DataFrame,
    components: pl.DataFrame,
) -> pl.DataFrame:
    """Fetch and assemble step-level results for a list of objective version IDs."""
    if not version_ids:
        logger.info("No objective versions to fetch steps for.")
        return pl.DataFrame()

    body = {
        "projectId": project_id,
        "sliceByObjectiveVersion": True,
        "sessionFilters": session_filters,
        "eventFilters": [],
        "separableEventFilters": [],
        "userFilters": [],
    }
    raw_steps = c3d_request("/v0/datasets/objectives/objectiveStepResultQueries", body)

    if not raw_steps:
        logger.info("No step results found.")
        return pl.DataFrame()

    version_id_strs = {str(v) for v in version_ids}
    all_steps: list[dict] = []
    for ver_id_str, steps in raw_steps.items():
        if ver_id_str not in version_id_strs or not steps:
            continue
        for s in steps:
            ct = s.get("averageStepCompletionTime")
            dur = s.get("averageStepDuration")
            all_steps.append(
                {
                    "objective_version_id": int(ver_id_str),
                    "step_number": s.get("step"),
                    "succeeded": s.get("succeeded", 0),
                    "failed": s.get("failed", 0),
                    "avg_completion_time_s": ct / 1000 if ct is not None else None,
                    "avg_step_duration_s": dur / 1000 if dur is not None else None,
                }
            )

    if not all_steps:
        logger.info("No step results found.")
        return pl.DataFrame()

    df = pl.DataFrame(all_steps)
    df = _add_completion_rate(df, col_name="step_completion_rate")

    # Join version → objective_id, version_number
    df = df.join(
        versions_meta.select(
            [
                "objective_version_id",
                "objective_id",
                "version_number",
            ]
        ),
        on="objective_version_id",
        how="left",
    )

    # Join objective_name
    df = df.join(
        objectives_meta.select(["objective_id", "objective_name"]),
        on="objective_id",
        how="left",
    )
    null_count = df["objective_name"].null_count()
    if null_count:
        logger.warning(
            "%d step row(s) had an unrecognised objective_id "
            "and will have null objective_name.",
            null_count,
        )

    # Join step metadata (only isStep == True components)
    step_comps = components.filter(pl.col("is_step")).select(
        [
            "objective_version_id",
            "step_number",
            "step_type",
            "step_detail",
            "step_name",
        ]
    )
    df = df.join(step_comps, on=["objective_version_id", "step_number"], how="left")
    null_count = df["step_name"].null_count()
    if null_count:
        logger.warning(
            "%d step row(s) had no matching step component "
            "and will have null step_name/step_type/step_detail.",
            null_count,
        )

    return df.select(
        [
            "objective_id",
            "objective_name",
            "objective_version_id",
            "version_number",
            "step_number",
            "step_type",
            "step_detail",
            "step_name",
            "succeeded",
            "failed",
            "step_completion_rate",
            "avg_completion_time_s",
            "avg_step_duration_s",
        ]
    ).sort(["objective_id", "step_number"])


def _add_completion_rate(
    df: pl.DataFrame, col_name: str = "completion_rate"
) -> pl.DataFrame:
    """Add a completion rate column = succeeded / (succeeded + failed), null when 0."""
    total = pl.col("succeeded") + pl.col("failed")
    return df.with_columns(
        pl.when(total == 0)
        .then(None)
        .otherwise(pl.col("succeeded") / total)
        .alias(col_name)
    )
