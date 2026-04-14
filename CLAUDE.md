# CLAUDE.md — cognitive3dpy contributor guide

## Dev environment

The project uses `uv`. To set up:

```bash
uv sync --all-extras
```

This installs the package, all dev dependencies, and the optional pandas extra.

## Running tests

```bash
uv run pytest tests/ -q                          # all tests
uv run pytest tests/test_transform.py -q         # single file
```

Tests use `respx` to mock HTTP calls — no real API credentials needed.

## Architecture

All data flows through a shared pipeline in `_transform.py`:

```
API JSON → pl.DataFrame → normalize_columns() → coerce_types() → [join_scene_names()] → [select_compact()] → to_output()
```

- `_transform.py` — DataFrame transformation logic (normalization, coercion, column selection)
- `_schema.py` — type registry combining YAML-generated types, manual supplements, and runtime lookups; the first place to look for type/schema issues
- `_schema_generated.py` — auto-generated from `slicer_fields.yaml`; do not edit manually
- `_client.py` — HTTP client, auth headers, error handling
- `_pagination.py` — session list pagination
- `_lookups.py` — metadata resolution (scenes, objects, objectives); results are cached by project_id
- `_filters.py` — builds the session filter payload sent to the API
- Each public function lives in its own module (`sessions.py`, `events.py`, etc.)

## Key conventions

- **Polars internally, pandas only at the boundary** — all processing uses `polars.DataFrame`; `to_output()` is the only place pandas conversion happens
- **Column naming** — `normalize_columns()` converts everything to `snake_case`; `c3d.*` API properties become `c3d_*` (e.g. `c3d.metrics.fps_score` → `c3d_metrics_fps_score`)
- **Property types are schema-driven** — `_schema_generated.py` (from slicer_fields.yaml) and `_SESSION_PROPERTY_SUPPLEMENTS` in `_schema.py` define column types. `coerce_types()` applies these via `property_overrides` (step 1). The runtime `propertyNameQueries` lookup covers project-specific properties not in the YAML.
- **Empty DataFrames have explicit schemas** — functions return typed empty frames (not schema-less) when no data is found
- **Date inputs** — all public functions accept `date`/`datetime` objects, epoch timestamps (int/float seconds), or `"YYYY-MM-DD"` strings
- **Deprecating columns** — when a column/field/property is deprecated: (1) remove it from `SESSIONS_COMPACT_COLUMNS` so it doesn't appear in compact output, (2) emit a `DeprecationWarning` citing the replacement column, (3) keep it in `SESSION_RAW_OVERRIDES` and `SESSION_SCHEMA` so non-compact output and empty frames remain backward-compatible until the column is fully removed

## Release process

Releases are fully automated via `python-semantic-release` on push to `main`. Use conventional commits:

- `fix:` → patch bump
- `feat:` → minor bump
- `feat!:` or `BREAKING CHANGE:` footer → major bump
- `chore:` / `docs:` → no release

The CI workflow (`release.yml`) determines the next version, creates a git tag, builds with `uv`, and publishes to PyPI automatically. Do not manually edit the version in `pyproject.toml`.
