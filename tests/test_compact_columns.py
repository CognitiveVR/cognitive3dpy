"""Guard SESSIONS_COMPACT_COLUMNS against drift from the cortex registry.

``SESSIONS_COMPACT_COLUMNS`` is a hand-curated list — the registry does not
carry per-consumer presentation intent, so it cannot be generated. What the
registry *does* carry is lifecycle state (``deprecated`` / ``sunsetted``),
which ``sync_schema.py`` emits into ``_schema_generated.py``. These tests use
it to fail CI when the curated list falls out of step with the registry:

- a column resolves to no known field/property at all, or
- a column resolves to a key the registry has retired.

When a schema sync turns one of these red, either drop the column (following
the deprecation convention in CLAUDE.md) or add it to
``_LIFECYCLE_EXCEPTIONS`` with a reason.
"""

from __future__ import annotations

from cognitive3dpy._schema import (
    DEPRECATED_REGISTRY_COLUMNS,
    SESSION_SCHEMA,
    SUNSETTED_REGISTRY_COLUMNS,
)
from cognitive3dpy._transform import SESSIONS_COMPACT_COLUMNS

# Compact columns with no registry key behind them. Each is produced by the
# package or returned by the API under a name the registry does not model,
# so the resolution check can never cover them.
_DERIVED_COMPACT_COLUMNS: dict[str, str] = {
    # API response fields absent from slicer_fields.yaml
    "device_id": "API session field, not modelled in the registry",
    "user_key": "API session field, not modelled in the registry",
    # Data-availability flags. The registry models the ES-side plural names
    # (hasGazes, hasEvents, ...); the list API returns these singular ones,
    # and has_boundary has no registry counterpart at all.
    "has_gaze": "API list-response flag; registry models hasGazes",
    "has_fixation": "API list-response flag; registry models hasFixations",
    "has_event": "API list-response flag; registry models hasEvents",
    "has_dynamic": "API list-response flag; registry models hasDynamics",
    "has_sensor": "API list-response flag; registry models hasSensors",
    "has_boundary": "API list-response flag; no registry counterpart",
}

# Registry-retired columns deliberately kept in the compact output.
# Key: column name. Value: why it stays.
_LIFECYCLE_EXCEPTIONS: dict[str, str] = {}


def test_no_duplicate_compact_columns():
    duplicates = sorted(
        {c for c in SESSIONS_COMPACT_COLUMNS if SESSIONS_COMPACT_COLUMNS.count(c) > 1}
    )
    assert not duplicates, f"Duplicate compact columns: {duplicates}"


def test_every_compact_column_resolves():
    """Each column maps to a schema entry or a documented derived column."""
    unresolved = [
        c
        for c in SESSIONS_COMPACT_COLUMNS
        if c not in SESSION_SCHEMA and c not in _DERIVED_COMPACT_COLUMNS
    ]
    assert not unresolved, (
        f"Compact columns resolve to no known field or property: {unresolved}. "
        "Either the name is stale, or add it to _DERIVED_COMPACT_COLUMNS "
        "with a reason."
    )


def test_no_deprecated_columns_in_compact():
    """Registry-deprecated keys must not sit in the default output."""
    offenders = sorted(
        (set(SESSIONS_COMPACT_COLUMNS) & DEPRECATED_REGISTRY_COLUMNS)
        - set(_LIFECYCLE_EXCEPTIONS)
    )
    assert not offenders, (
        f"Compact columns are deprecated in the registry: {offenders}. "
        "Drop them per the deprecation convention in CLAUDE.md, or add them "
        "to _LIFECYCLE_EXCEPTIONS with a reason."
    )


def test_no_sunsetted_columns_in_compact():
    """Nothing in the default output should be a key no SDK still emits."""
    offenders = sorted(
        (set(SESSIONS_COMPACT_COLUMNS) & SUNSETTED_REGISTRY_COLUMNS)
        - set(_LIFECYCLE_EXCEPTIONS)
    )
    assert not offenders, (
        f"Compact columns are sunsetted in the registry: {offenders}. "
        "Drop them, or add them to _LIFECYCLE_EXCEPTIONS with a reason."
    )


def test_exceptions_are_still_needed():
    """An exception whose column is gone or no longer flagged is stale."""
    flagged = DEPRECATED_REGISTRY_COLUMNS | SUNSETTED_REGISTRY_COLUMNS
    stale = sorted(
        c
        for c in _LIFECYCLE_EXCEPTIONS
        if c not in SESSIONS_COMPACT_COLUMNS or c not in flagged
    )
    assert not stale, f"Stale entries in _LIFECYCLE_EXCEPTIONS: {stale}"


def test_deprecated_columns_have_migration_entries():
    """Columns dropped from compact still warn via handle_deprecated_columns."""
    from cognitive3dpy._schema import DEPRECATED_COLUMNS

    for column in ("c3d_metrics_standing_percentage", "c3d_metrics_battery_efficiency"):
        assert column not in SESSIONS_COMPACT_COLUMNS
        assert column in DEPRECATED_COLUMNS
