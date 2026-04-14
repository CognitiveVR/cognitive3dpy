"""Generate _schema_generated.py from slicer_fields.yaml.

Reads the authoritative field definitions from cvr-slicer (or cvr-cortex)
and produces a Python module with Polars type mappings. The generated file
is committed to the repo so the package works without the YAML present.

Usage:
    uv run python scripts/sync_schema.py                  # auto-detect YAML
    uv run python scripts/sync_schema.py /path/to/yaml    # explicit path
    SLICER_YAML=/path/to/yaml uv run python scripts/sync_schema.py
"""

from __future__ import annotations

import os
import sys
import textwrap
from datetime import UTC, datetime
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# YAML type -> Polars type string (for code generation)
# ---------------------------------------------------------------------------

# Fields have an explicit "type" attribute
FIELD_TYPE_MAP: dict[str, str] = {
    "timestamp": "pl.Utf8",  # API returns ISO strings; coerce_types() parses
    "integral": "pl.Int64",
    "textual": "pl.Utf8",
    "numerical": "pl.Float64",
    "boolean": "pl.Boolean",
}

# Properties are grouped by section name (textual/numerical/boolean)
PROPERTY_SECTION_MAP: dict[str, str] = {
    "textual": "pl.Utf8",
    "numerical": "pl.Float64",
    "boolean": "pl.Boolean",
}

# ---------------------------------------------------------------------------
# Property type overrides
# Corrects known misclassifications in the YAML where a property is listed
# in the wrong section (e.g. numeric data under the textual section).
# ---------------------------------------------------------------------------

PROPERTY_TYPE_OVERRIDES: dict[str, str] = {
    "c3d.participant.height": "pl.Float64",
    "c3d.participant.armlength": "pl.Float64",
    "c3d.participant.Age": "pl.Float64",
    "c3d.multiplayer.port": "pl.Int64",
}

# ---------------------------------------------------------------------------
# YAML location candidates
# ---------------------------------------------------------------------------

YAML_CANDIDATES: list[str] = [
    "../cvr-cortex/doc/slicer_fields.yaml",
    "../cvr-slicer/doc/slicer_fields.yaml",
]

OUTPUT_PATH = (
    Path(__file__).resolve().parent.parent
    / "src"
    / "cognitive3dpy"
    / "_schema_generated.py"
)


def find_yaml(repo_root: Path) -> Path | None:
    """Locate slicer_fields.yaml, checking env var then candidate paths."""
    env_path = os.environ.get("SLICER_YAML")
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return p

    for candidate in YAML_CANDIDATES:
        p = repo_root / candidate
        if p.is_file():
            return p.resolve()

    return None


def parse_fields(fields_dict: dict) -> dict[str, str]:
    """Parse a session_fields or event_fields section.

    Each key is a field name, each value is a dict with a ``type`` attribute.
    Returns ``{field_name: polars_type_string}``.
    """
    result: dict[str, str] = {}
    for field_name, field_meta in fields_dict.items():
        field_type = field_meta.get("type", "textual")
        polars_type = FIELD_TYPE_MAP.get(field_type, "pl.Utf8")
        result[field_name] = polars_type
    return result


def parse_properties(properties_dict: dict) -> dict[str, str]:
    """Parse a session_properties or event_properties section.

    Properties are grouped by type section (textual/numerical/boolean).
    After section-based assignment, ``PROPERTY_TYPE_OVERRIDES`` are applied
    to correct known misclassifications in the YAML.
    Returns ``{property_name: polars_type_string}``.
    """
    result: dict[str, str] = {}
    for section_name, section_entries in properties_dict.items():
        polars_type = PROPERTY_SECTION_MAP.get(section_name)
        if polars_type is None:
            continue
        if not isinstance(section_entries, dict):
            continue
        for prop_name in section_entries:
            result[prop_name] = PROPERTY_TYPE_OVERRIDES.get(prop_name, polars_type)
    return result


def format_dict(
    name: str,
    entries: dict[str, str],
    indent: str = "    ",
    max_line: int = 88,
) -> str:
    """Format a dict as a Python constant declaration."""
    if not entries:
        return f"{name}: dict[str, pl.DataType] = {{}}"

    lines = [f"{name}: dict[str, pl.DataType] = {{"]
    for key, polars_type in entries.items():
        single = f'{indent}"{key}": {polars_type},'
        if len(single) <= max_line:
            lines.append(single)
        else:
            # Break long lines across two lines
            lines.append(f'{indent}"{key}":')
            lines.append(f"{indent}    {polars_type},")
    lines.append("}")
    return "\n".join(lines)


def generate(yaml_path: Path) -> str:
    """Read the YAML and produce the generated module source."""
    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    session_fields = parse_fields(data.get("session_fields", {}))
    session_properties = parse_properties(data.get("session_properties", {}))
    event_fields = parse_fields(data.get("event_fields", {}))
    event_properties = parse_properties(data.get("event_properties", {}))

    yaml_mtime = datetime.fromtimestamp(yaml_path.stat().st_mtime, tz=UTC)
    timestamp = yaml_mtime.strftime("%Y-%m-%dT%H:%M:%SZ")
    yaml_name = yaml_path.name

    header = textwrap.dedent(f"""\
        \"\"\"Auto-generated Polars type mappings from {yaml_name}.

        DO NOT EDIT MANUALLY. Regenerate with:
            uv run python scripts/sync_schema.py

        Generated: {timestamp}
        Source: {yaml_name}
        \"\"\"

        from __future__ import annotations

        import polars as pl
    """)

    sections = [
        header,
        "# " + "=" * 77,
        "# SESSION FIELDS",
        "# Top-level fields on session documents (original API names).",
        "# " + "=" * 77,
        "",
        format_dict("SESSION_FIELD_TYPES", session_fields),
        "",
        "",
        "# " + "=" * 77,
        "# SESSION PROPERTIES",
        '# Nested in "properties" struct; names are dot-case originals.',
        "# " + "=" * 77,
        "",
        format_dict("SESSION_PROPERTY_TYPES", session_properties),
        "",
        "",
        "# " + "=" * 77,
        "# EVENT FIELDS",
        "# Top-level fields on event documents (original API names).",
        "# " + "=" * 77,
        "",
        format_dict("EVENT_FIELD_TYPES", event_fields),
        "",
        "",
        "# " + "=" * 77,
        "# EVENT PROPERTIES",
        "# Nested event properties (original names).",
        "# " + "=" * 77,
        "",
        format_dict("EVENT_PROPERTY_TYPES", event_properties),
        "",
    ]

    return "\n".join(sections)


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent

    if len(sys.argv) > 1:
        yaml_path = Path(sys.argv[1])
        if not yaml_path.is_file():
            print(f"Error: YAML file not found: {yaml_path}", file=sys.stderr)
            sys.exit(1)
    else:
        yaml_path = find_yaml(repo_root)

    if yaml_path is None:
        print(
            "slicer_fields.yaml not found. Checked:\n"
            + "\n".join(f"  - {repo_root / c}" for c in YAML_CANDIDATES)
            + "\n\nSet SLICER_YAML env var or pass the path as an argument.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Reading: {yaml_path}")
    source = generate(yaml_path)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(source)
    print(f"Wrote: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
