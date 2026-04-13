"""DataFrame transformations for Cognitive3D API responses.

All data processing uses Polars internally. Pandas conversion is the
final optional step via ``to_output()``.
"""

from __future__ import annotations

import logging
import re
import warnings
from typing import TYPE_CHECKING, Literal

import polars as pl

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pandas as pd


SESSIONS_COMPACT_COLUMNS: list[str] = [
    # Core session fields
    "project_id",
    "session_id",
    "session_date",
    "end_date",
    "duration_s",
    "hmd",
    "user",
    "device_id",
    "participant_id",
    "user_key",
    "friendly_name",
    "tags",
    # Scene fields (populated for session_type="scene")
    "scene_id",
    "scene_version_id",
    "scene_name",
    # Participant info
    "c3d_participant_name",
    "c3d_participant_oculus_id",
    # App & device info
    "c3d_app_name",
    "c3d_app_version",
    "c3d_version",
    "c3d_device_type",
    "c3d_device_hmd_type",
    "c3d_device_os",
    "c3d_device_model",
    # Location & environment
    "c3d_geo_country",
    "c3d_geo_subdivision",
    "c3d_geo_city",
    "c3d_geo_latitude",
    "c3d_geo_longitude",
    "c3d_roomsize_meters",
    # Key metrics (top-level scores)
    "c3d_metrics_fps_score",
    "c3d_metrics_app_performance",
    "c3d_metrics_average_fps",
    "c3d_metrics_presence_score",
    "c3d_metrics_immersion_score",
    "c3d_metrics_orientation_score",
    "c3d_metrics_comfort_score",
    "c3d_metrics_ergonomics_score",
    "c3d_metrics_battery_efficiency",
    "c3d_metrics_boundary_score",
    "c3d_metrics_controller_events_score",
    "c3d_metrics_controller_engagement_score",
    "c3d_metrics_dynamic_engagement_score",
    "c3d_metrics_standing_percentage",
    "c3d_metrics_cyberwellness_score",
    # Metric components (sub-scores)
    "c3d_metric_components_fps_score_degree_app_performance",
    "c3d_metric_components_fps_score_consistency_app_performance",
    "c3d_metric_components_fps_score_fluctuation_app_performance",
    "c3d_metric_components_fps_score_session_percentage",
    "c3d_metric_components_comfort_score_head_orientation_score",
    "c3d_metric_components_comfort_score_head_orientation_score_pitch_score",
    "c3d_metric_components_comfort_score_head_orientation_score_roll_score",
    "c3d_metric_components_comfort_score_controller_ergonomic_score",
    "c3d_metric_components_comfort_score_controller_ergonomic_score_forward_reach_score",
    "c3d_metric_components_comfort_score_controller_ergonomic_score_horizontal_reach_score",
    "c3d_metric_components_comfort_score_controller_ergonomic_score_vertical_reach_score",
    "c3d_metric_components_presence_score_gaze_exploration_score",
    "c3d_metric_components_presence_score_interruption_score",
    "c3d_metric_components_presence_score_controller_movement_score",
    "c3d_metric_components_presence_score_spatial_coverage_score",
    "c3d_metric_components_cyberwellness_visual_continuity",
    "c3d_metric_components_cyberwellness_acceleration_variability",
    "c3d_metric_components_cyberwellness_translational_movement",
    "c3d_metric_components_cyberwellness_translational_speed",
    "c3d_metric_components_cyberwellness_continuous_movement",
    # Controller ergo counts
    "c3d_metric_components_controller_ergo_counts_forwards_near",
    "c3d_metric_components_controller_ergo_counts_forwards_medium",
    "c3d_metric_components_controller_ergo_counts_forwards_far",
    "c3d_metric_components_controller_ergo_counts_forwards_total",
    "c3d_metric_components_controller_ergo_counts_horizontal_near",
    "c3d_metric_components_controller_ergo_counts_horizontal_medium",
    "c3d_metric_components_controller_ergo_counts_horizontal_far",
    "c3d_metric_components_controller_ergo_counts_horizontal_total",
    "c3d_metric_components_controller_ergo_counts_vertical_near",
    "c3d_metric_components_controller_ergo_counts_vertical_medium",
    "c3d_metric_components_controller_ergo_counts_vertical_far",
    "c3d_metric_components_controller_ergo_counts_vertical_total",
    "c3d_metric_components_average_controller_movement_meters_per_second",
    "c3d_metric_components_dynamic_input_controller_percentage",
    "c3d_metric_components_dynamic_input_hand_percentage",
    "c3d_metric_components_dynamic_input_none_percentage",
    # Session flags
    "c3d_session_tag_junk",
    "c3d_session_tag_test",
]


_CAMEL_RE = re.compile(r"([a-z])([A-Z])")
_MULTI_UNDERSCORE_RE = re.compile(r"_+")


def _clean_name(name: str) -> str:
    """Convert a column or property key to a clean snake_case name.

    Transformations applied in order:
    1. Delete apostrophes and quotes.
    2. Replace ``%`` with ``percent`` and ``#`` with ``number``.
    3. Replace dots, spaces, and hyphens with underscores.
    4. camelCase → snake_case.
    5. Lowercase.
    6. Collapse consecutive underscores to one.

    Examples
    --------
    >>> _clean_name("endDate")
    'end_date'
    >>> _clean_name("hasDynamic")
    'has_dynamic'
    >>> _clean_name("c3d.app.name")
    'c3d_app_name'
    >>> _clean_name("c3d.metric_components.comfort_score.head_orientation_score")
    'c3d_metric_components_comfort_score_head_orientation_score'
    >>> _clean_name("Angle from HMD")
    'angle_from_hmd'
    >>> _clean_name("success%")
    'success_percent'
    >>> _clean_name("my-custom-prop")
    'my_custom_prop'
    """
    name = name.replace("'", "").replace('"', "")
    name = name.replace("%", "_percent").replace("#", "_number")
    name = name.replace(".", "_").replace(" ", "_").replace("-", "_")
    name = _CAMEL_RE.sub(r"\1_\2", name).lower()
    return _MULTI_UNDERSCORE_RE.sub("_", name)


# Explicit renames for columns where _clean_name alone isn't enough.
_COLUMN_RENAMES: dict[str, str] = {
    "date": "session_date",  # semantic rename
    "parentSceneVersionId": "scene_version_id",  # drop "parent_" prefix
}


def normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Unnest properties and rename all columns to clean snake_case names.

    1. If a ``properties`` struct column exists, unnest it into top-level columns.
    2. Apply explicit renames (e.g. ``date`` → ``session_date``).
    3. Clean all remaining column names: dots → underscores, camelCase → snake_case.
    """
    # Unnest properties struct if present.
    if "properties" in df.columns:
        dtype = df.schema["properties"]
        if isinstance(dtype, pl.Struct):
            existing_cols = set(df.columns) - {"properties"}
            prop_fields = [f.name for f in dtype.fields]
            dupes = [f for f in prop_fields if f in existing_cols]
            if dupes:
                # Extract only non-duplicate fields, drop duplicates with a warning.
                for d in dupes:
                    logger.warning(
                        "Dropping duplicate property field %r "
                        "(already exists as a top-level column).",
                        d,
                    )
                keep = [f for f in prop_fields if f not in existing_cols]
                if keep:
                    exprs = [
                        pl.col("properties").struct.field(f).alias(f)
                        for f in keep
                    ]
                    df = df.with_columns(exprs)
                df = df.drop("properties")
            else:
                df = df.unnest("properties")

    # Apply explicit renames for special cases.
    existing = set(df.columns)
    explicit = {old: new for old, new in _COLUMN_RENAMES.items() if old in existing}
    if explicit:
        df = df.rename(explicit)

    # Clean all remaining column names, dropping duplicates.
    clean_map: dict[str, str] = {}
    seen_clean: set[str] = set()
    drop_cols: list[str] = []
    for col in df.columns:
        clean = _clean_name(col)
        if clean in seen_clean:
            logger.warning(
                "Dropping column %r (cleans to %r which already exists).",
                col,
                clean,
            )
            drop_cols.append(col)
            continue
        seen_clean.add(clean)
        if clean != col:
            clean_map[col] = clean

    if drop_cols:
        df = df.drop(drop_cols)
    if clean_map:
        df = df.rename(clean_map)

    return df


_DATE_COLUMNS = ("session_date", "end_date", "event_date")
_FLOAT_PREFIXES = ("c3d_metrics_", "c3d_metric_components_")
_NUMERIC_DTYPES = (
    pl.Int8, pl.Int16, pl.Int32, pl.Int64,
    pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
    pl.Float32, pl.Float64,
)
_MIN_YEAR = 2015  # Cognitive3D launch
_MAX_FUTURE_DAYS = 365


def coerce_types(
    df: pl.DataFrame,
    *,
    property_overrides: dict[str, pl.DataType] | None = None,
) -> pl.DataFrame:
    """Coerce column types for any Cognitive3D DataFrame.

    Handles sessions, events, and objectives in a single unified function.
    All steps are condition-guarded — columns must exist and have the
    expected source dtype.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame with already-renamed (snake_case) column names.
    property_overrides : dict, optional
        Mapping of normalized property names to Polars types. Merged from
        the YAML-generated registry and/or the runtime ``propertyNameQueries``
        lookup. Columns present in *df* whose current dtype doesn't match
        the declared type are cast.
    """
    if df.is_empty():
        return df

    cols = set(df.columns)

    # 1. Apply property type overrides (from YAML registry + runtime lookup)
    if property_overrides:
        override_casts = [
            pl.col(col_name).cast(target_dtype)
            for col_name, target_dtype in property_overrides.items()
            if col_name in cols and df.schema[col_name] != target_dtype
        ]
        if override_casts:
            df = df.with_columns(override_casts)

    # 2. Parse ISO 8601 date strings → Datetime(UTC) with validation
    for date_col in _DATE_COLUMNS:
        if date_col in cols and df.schema[date_col] == pl.Utf8:
            df = _parse_date_column(df, date_col)

    # 3. Handle step_timestamp polymorphism (objectives)
    if "step_timestamp" in cols:
        dtype = df.schema["step_timestamp"]
        if dtype == pl.Utf8:
            df = _parse_date_column(df, "step_timestamp")
        elif dtype in (pl.Int64, pl.Float64):
            # Epoch milliseconds → Datetime(us, UTC) for consistency
            df = df.with_columns(
                (pl.col("step_timestamp").cast(pl.Int64) * 1000)
                .cast(pl.Datetime("us"))
                .dt.replace_time_zone("UTC")
                .alias("step_timestamp")
            )

    # 4. Duration ms → seconds, rename to duration_s
    if "duration" in cols:
        df = df.with_columns(
            (pl.col("duration").cast(pl.Float64) / 1000).alias("duration_s")
        ).drop("duration")

    # 5. Tags list → comma-separated string
    if "tags" in cols and df.schema["tags"] == pl.List(pl.Utf8):
        df = df.with_columns(pl.col("tags").list.join(", ").alias("tags"))

    # 6. Cast numeric metric columns to Float64 (safety net for dynamic columns
    #    not in the property registry)
    float_casts = [
        pl.col(col).cast(pl.Float64)
        for col in df.columns
        if (col.startswith(_FLOAT_PREFIXES) or col == "c3d_roomsize_meters")
        and isinstance(df.schema[col], _NUMERIC_DTYPES)
    ]
    if float_casts:
        df = df.with_columns(float_casts)

    # 7. Cast Null-typed columns to String
    null_cols = [name for name, dtype in df.schema.items() if dtype == pl.Null]
    if null_cols:
        df = df.with_columns([pl.col(c).cast(pl.Utf8) for c in null_cols])

    return df


def _parse_date_column(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Parse an ISO 8601 string column to Datetime(UTC) with validation.

    - Malformed strings become null (only in the date column; rest of row intact).
    - Logs warnings for parse failures, missing timezone suffixes, and
      impossible dates (before 2015 or more than a year in the future).
    """
    import datetime as dt

    n_before = df[col].null_count()

    # Check for missing timezone suffix before parsing
    non_null_values = df[col].drop_nulls()
    if len(non_null_values) > 0:
        has_tz = non_null_values.str.contains(r"[Zz]|[+\-]\d{2}:\d{2}$")
        n_no_tz = (~has_tz).sum()
        if n_no_tz > 0:
            logger.warning(
                "%d value(s) in %r lack a timezone suffix — assuming UTC.",
                n_no_tz,
                col,
            )

    # Parse with explicit ISO 8601 format and strict=False so rows that
    # don't match become null instead of raising an exception.
    df = df.with_columns(
        pl.col(col)
        .str.to_datetime(
            format="%Y-%m-%dT%H:%M:%S%.fZ",
            time_zone="UTC",
            time_unit="us",
            strict=False,
        )
        .alias(col)
    )

    # Count parse failures
    n_after = df[col].null_count()
    n_failed = n_after - n_before
    if n_failed > 0:
        logger.warning(
            "%d value(s) in %r could not be parsed as datetime and are now null.",
            n_failed,
            col,
        )

    # Flag impossible dates
    non_null_dates = df[col].drop_nulls()
    if len(non_null_dates) > 0:
        min_date = dt.datetime(_MIN_YEAR, 1, 1, tzinfo=dt.timezone.utc)
        max_date = dt.datetime.now(dt.timezone.utc) + dt.timedelta(
            days=_MAX_FUTURE_DAYS
        )

        n_too_old = (non_null_dates < min_date).sum()
        n_too_new = (non_null_dates > max_date).sum()

        if n_too_old > 0:
            logger.warning(
                "%d value(s) in %r are before %d — possible data issue.",
                n_too_old,
                col,
                _MIN_YEAR,
            )
        if n_too_new > 0:
            logger.warning(
                "%d value(s) in %r are more than %d days in the future — "
                "possible data issue.",
                n_too_new,
                col,
                _MAX_FUTURE_DAYS,
            )

    return df


def select_compact(df: pl.DataFrame) -> pl.DataFrame:
    """Select the compact column subset for sessions."""
    existing = set(df.columns)
    selected = [c for c in SESSIONS_COMPACT_COLUMNS if c in existing]
    return df.select(selected)


def prefix_event_props(
    df: pl.DataFrame,
    standard_columns: set[str],
) -> pl.DataFrame:
    """Prefix non-standard columns with ``prop_`` and replace spaces with underscores.

    Parameters
    ----------
    standard_columns : set[str]
        Column names that should *not* be prefixed.
    """
    rename_map: dict[str, str] = {}
    for col in df.columns:
        if col not in standard_columns:
            clean = col.replace(" ", "_")
            rename_map[col] = f"prop_{clean}"

    if rename_map:
        df = df.rename(rename_map)

    return df


def join_scene_names(
    df: pl.DataFrame,
    lookup: dict[str, str],
) -> pl.DataFrame:
    """Add ``scene_name`` by mapping ``scene_version_id`` through *lookup*."""
    if "scene_version_id" not in df.columns or not lookup:
        return df.with_columns(pl.lit(None).cast(pl.Utf8).alias("scene_name"))

    df = df.with_columns(pl.col("scene_version_id").cast(pl.Utf8))
    lookup_df = pl.DataFrame(
        {"scene_version_id": list(lookup.keys()), "scene_name": list(lookup.values())}
    )
    df = df.join(lookup_df, on="scene_version_id", how="left")
    null_count = df["scene_name"].null_count()
    if null_count:
        logger.warning(
            "%d session(s) had an unrecognised scene_version_id "
            "and will have null scene_name.",
            null_count,
        )
    return df


def warn_if_empty(df: pl.DataFrame, func_name: str) -> None:
    """Emit a UserWarning if the DataFrame has no rows.

    Parameters
    ----------
    df : pl.DataFrame
        The result DataFrame to check.
    func_name : str
        The public function name to include in the warning message.
    """
    if df.is_empty():
        warnings.warn(
            f"{func_name}() returned 0 rows. "
            "Check your date range, project ID, and filters.",
            UserWarning,
            stacklevel=3,
        )


def to_output(
    df: pl.DataFrame,
    output: Literal["polars", "pandas"] = "polars",
) -> pl.DataFrame | pd.DataFrame:
    """Convert the final Polars DataFrame to the requested output format.

    Parameters
    ----------
    output : ``"polars"`` or ``"pandas"``
        Target DataFrame library.

    Raises
    ------
    ValueError
        If *output* is not ``"polars"`` or ``"pandas"``.
    ImportError
        If ``"pandas"`` is requested but pandas is not installed.
    """
    if output == "polars":
        return df
    if output == "pandas":
        try:
            import pandas  # noqa: F401
        except ImportError:
            raise ImportError(
                "pandas is required for output='pandas'. "
                "Install it with: pip install 'cognitive3dpy[pandas]'"
            ) from None
        return df.to_pandas()
    raise ValueError(f"output must be 'polars' or 'pandas', got {output!r}")
