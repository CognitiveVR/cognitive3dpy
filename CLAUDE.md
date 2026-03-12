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

- `_transform.py` — all DataFrame transformation logic; the first place to look for type/schema issues
- `_client.py` — HTTP client, auth headers, error handling
- `_pagination.py` — session list pagination
- `_lookups.py` — metadata resolution (scenes, objects, objectives); results are cached by project_id
- `_filters.py` — builds the session filter payload sent to the API
- Each public function lives in its own module (`sessions.py`, `events.py`, etc.)

## Key conventions

- **Polars internally, pandas only at the boundary** — all processing uses `polars.DataFrame`; `to_output()` is the only place pandas conversion happens
- **Column naming** — `normalize_columns()` converts everything to `snake_case`; `c3d.*` API properties become `c3d_*` (e.g. `c3d.metrics.fps_score` → `c3d_metrics_fps_score`)
- **Numeric metric columns are always Float64** — `coerce_types()` casts all `c3d_metrics_*`, `c3d_metric_components_*`, and `c3d_roomsize_meters` columns to `pl.Float64`, even when the API returns whole numbers as integers
- **Empty DataFrames have explicit schemas** — functions return typed empty frames (not schema-less) when no data is found
- **Date inputs** — all public functions accept `date`/`datetime` objects, epoch timestamps (int/float seconds), or `"YYYY-MM-DD"` strings

## Release process

Releases are fully automated via `python-semantic-release` on push to `main`. Use conventional commits:

- `fix:` → patch bump
- `feat:` → minor bump
- `feat!:` or `BREAKING CHANGE:` footer → major bump
- `chore:` / `docs:` → no release

The CI workflow (`release.yml`) determines the next version, creates a git tag, builds with `uv`, and publishes to PyPI automatically. Do not manually edit the version in `pyproject.toml`.
