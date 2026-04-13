"""Schema registry for Cognitive3D API type coercion.

Combines auto-generated type mappings from ``_schema_generated.py``
(derived from slicer_fields.yaml) with manual definitions for fields
and domains not covered by the YAML.

Three-tier type resolution for properties:

1. **YAML-generated registry** (static, no network)
2. **``propertyNameQueries`` endpoint** (runtime, cached per project)
3. **Polars inference + Null→Utf8 fallback** (catch-all)
"""

from __future__ import annotations

import logging

import polars as pl

from cognitive3dpy._schema_generated import (
    EVENT_FIELD_TYPES,
    EVENT_PROPERTY_TYPES,
    SESSION_FIELD_TYPES,
    SESSION_PROPERTY_TYPES,
)
from cognitive3dpy._transform import _clean_name

logger = logging.getLogger(__name__)

# ============================================================================
# Layer 1 — Raw overrides
# Applied at pl.DataFrame(data, schema_overrides=...) BEFORE normalize_columns.
# Only top-level fields — properties are nested in a struct at this point.
# ============================================================================

SESSION_RAW_OVERRIDES: dict[str, pl.DataType] = {
    # From YAML-generated session field types
    **SESSION_FIELD_TYPES,
    # Manual supplements for fields not in the YAML
    "endDate": pl.Utf8,
    "hmd": pl.Utf8,  # inconsistent types across projects (null/int/string)
    "user": pl.Utf8,
    "friendlyName": pl.Utf8,
    "tags": pl.List(pl.Utf8),
}

# ============================================================================
# Layer 1b — Property type overrides
# Applied in coerce_types() AFTER normalize_columns() unnests the struct.
# Keys are normalized (snake_case) since they've been through _clean_name().
# ============================================================================

SESSION_PROPERTY_OVERRIDES: dict[str, pl.DataType] = {
    _clean_name(k): v for k, v in SESSION_PROPERTY_TYPES.items()
}

# ============================================================================
# Layer 2 — Post-normalization schemas (for empty_frame)
# Merged field + property types with normalized names.
# ============================================================================

# Build normalized versions of the field types
_session_fields_normalized = {_clean_name(k): v for k, v in SESSION_FIELD_TYPES.items()}
_event_fields_normalized = {_clean_name(k): v for k, v in EVENT_FIELD_TYPES.items()}
_event_props_normalized = {_clean_name(k): v for k, v in EVENT_PROPERTY_TYPES.items()}

SESSION_SCHEMA: dict[str, pl.DataType] = {
    **_session_fields_normalized,
    **SESSION_PROPERTY_OVERRIDES,
    # Manual supplements (normalized names, final types after coerce_types)
    "session_date": pl.Datetime("us", "UTC"),  # renamed from "date", parsed
    "end_date": pl.Datetime("us", "UTC"),  # parsed from ISO string
    "duration_s": pl.Float64,  # converted from ms
    "hmd": pl.Utf8,
    "user": pl.Utf8,
    "friendly_name": pl.Utf8,
    "tags": pl.Utf8,  # joined from list
    "scene_name": pl.Utf8,  # added by join_scene_names()
    "scene_version_id": pl.Utf8,  # cast for joins
}

EVENT_SCHEMA: dict[str, pl.DataType] = {
    # Events are manually flattened with clean names, not raw API names
    "project_id": pl.Int64,
    "session_id": pl.Utf8,
    "participant_id": pl.Utf8,
    "user_key": pl.Utf8,
    "device_id": pl.Utf8,
    "session_date": pl.Datetime("us", "UTC"),
    "duration_s": pl.Float64,
    "event_name": pl.Utf8,
    "event_date": pl.Datetime("us", "UTC"),
    "position_x": pl.Float64,
    "position_y": pl.Float64,
    "position_z": pl.Float64,
    "object_id": pl.Utf8,
    "object": pl.Utf8,
    "scene_version_id": pl.Utf8,
    "scene_name": pl.Utf8,
}

# Fully manual — objectives and exit polls are not in the YAML
SESSION_OBJECTIVE_SCHEMA: dict[str, pl.DataType] = {
    "project_id": pl.Int64,
    "scene_id": pl.Utf8,
    "scene_name": pl.Utf8,
    "scene_version_id": pl.Utf8,
    "session_id": pl.Utf8,
    "participant_id": pl.Utf8,
    "session_date": pl.Datetime("us", "UTC"),
    "objective_id": pl.Int64,
    "objective_name": pl.Utf8,
    "step_number": pl.Int64,
    "step_description": pl.Utf8,
    "step_timestamp": pl.Datetime("us", "UTC"),
    "step_duration": pl.Float64,
    "step_duration_sec": pl.Float64,
    "step_result": pl.Utf8,
}

EXITPOLL_SCHEMA: dict[str, pl.DataType] = {
    "hook": pl.Utf8,
    "version": pl.Int64,
    "question_index": pl.Int64,
    "question_title": pl.Utf8,
    "question_type": pl.Utf8,
    "value": pl.Utf8,
    "value_label": pl.Utf8,
    "count": pl.Int64,
}

# ============================================================================
# Helpers
# ============================================================================

FLOAT64_PREFIXES: tuple[str, ...] = ("c3d_metrics_", "c3d_metric_components_")

_DOMAIN_SCHEMAS: dict[str, dict[str, pl.DataType]] = {
    "session": SESSION_SCHEMA,
    "event": EVENT_SCHEMA,
    "objective": SESSION_OBJECTIVE_SCHEMA,
    "exitpoll": EXITPOLL_SCHEMA,
}


def empty_frame(domain: str) -> pl.DataFrame:
    """Return a zero-row DataFrame with the correct schema for *domain*.

    Parameters
    ----------
    domain : str
        One of ``"session"``, ``"event"``, ``"objective"``, ``"exitpoll"``.
    """
    schema = _DOMAIN_SCHEMAS.get(domain)
    if schema is None:
        raise ValueError(
            f"Unknown domain {domain!r}. "
            f"Expected one of: {', '.join(_DOMAIN_SCHEMAS)}"
        )
    return pl.DataFrame(schema=schema)


# ============================================================================
# Runtime property type lookup (Tier 2)
# ============================================================================

# Response key -> Polars type
_PROPERTY_TYPE_SECTIONS: dict[str, pl.DataType] = {
    "textualSessionProp": pl.Utf8,
    "numericalSessionProp": pl.Float64,
    "booleanSessionProp": pl.Boolean,
}

_property_types_cache: dict[int, dict[str, pl.DataType]] = {}


def fetch_property_types(project_id: int) -> dict[str, pl.DataType]:
    """Fetch property type mappings from the slicer ``propertyNameQueries`` endpoint.

    Returns a dict mapping normalized property names to their Polars types.
    Results are cached per project_id.

    Parameters
    ----------
    project_id : int
        The Cognitive3D project ID.
    """
    if project_id in _property_types_cache:
        return _property_types_cache[project_id]

    from cognitive3dpy._client import c3d_request

    body = {
        "sessionType": "project",
        "entityFilters": {"projectId": project_id},
    }

    try:
        raw = c3d_request("/v0/propertyNameQueries", body)
    except Exception:
        logger.warning(
            "Failed to fetch property types for project %d — "
            "falling back to registry only.",
            project_id,
        )
        _property_types_cache[project_id] = {}
        return {}

    result: dict[str, pl.DataType] = {}
    if isinstance(raw, dict):
        for section_key, polars_type in _PROPERTY_TYPE_SECTIONS.items():
            for prop_name in raw.get(section_key, []):
                result[_clean_name(prop_name)] = polars_type

    _property_types_cache[project_id] = result
    return result
