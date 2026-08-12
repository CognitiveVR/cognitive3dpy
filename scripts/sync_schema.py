"""Generate _schema_generated.py from slicer_fields.yaml.

Reads the authoritative field definitions from the cvr-cortex registry
and produces a Python module with Polars type mappings. The generated file
is committed to the repo so the package works without the YAML present.

Usage:
    uv run python scripts/sync_schema.py                  # auto-detect YAML
    uv run python scripts/sync_schema.py /path/to/yaml    # explicit path
    SLICER_YAML=/path/to/yaml uv run python scripts/sync_schema.py
"""

from __future__ import annotations

import hashlib
import os
import sys
import textwrap
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
    "c3d.participant.hmdHeight": "pl.Float64",
    "c3d.participant.Age": "pl.Float64",
    "c3d.multiplayer.port": "pl.Int64",
}

# ---------------------------------------------------------------------------
# YAML location candidates
# ---------------------------------------------------------------------------

YAML_CANDIDATES: list[str] = [
    "../cvr-cortex/features/slicer/slicer_fields.yaml",
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


def collect_lifecycle(section: dict, grouped: bool) -> tuple[set[str], set[str]]:
    """Collect keys carrying the registry lifecycle flags.

    The YAML marks two orthogonal facts (see the slicer_fields.yaml header):
    ``deprecated`` is a consumption-side intent (stop reading this key) and
    ``sunsetted`` is a production-side fact (no shipping SDK emits it).

    *grouped* is True for property sections, which nest their entries one
    level deeper under a type section (textual/numerical/boolean).
    Returns ``(deprecated_keys, sunsetted_keys)``.
    """
    deprecated: set[str] = set()
    sunsetted: set[str] = set()

    def scan(entries: dict) -> None:
        for key, meta in entries.items():
            if not isinstance(meta, dict):
                continue
            if meta.get("deprecated"):
                deprecated.add(key)
            if meta.get("sunsetted"):
                sunsetted.add(key)

    if grouped:
        for section_entries in section.values():
            if isinstance(section_entries, dict):
                scan(section_entries)
    else:
        scan(section)

    return deprecated, sunsetted


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


def format_set(name: str, entries: set[str], indent: str = "    ") -> str:
    """Format a set of keys as a frozenset constant declaration."""
    if not entries:
        return f"{name}: frozenset[str] = frozenset()"

    lines = [f"{name}: frozenset[str] = frozenset({{"]
    lines.extend(f'{indent}"{key}",' for key in sorted(entries))
    lines.append("})")
    return "\n".join(lines)


def generate(yaml_path: Path) -> str:
    """Read the YAML and produce the generated module source."""
    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    session_fields = parse_fields(data.get("session_fields", {}))
    session_properties = parse_properties(data.get("session_properties", {}))
    event_fields = parse_fields(data.get("event_fields", {}))
    event_properties = parse_properties(data.get("event_properties", {}))

    # Lifecycle flags across every section — session and event, fields and
    # properties — collapsed into two flat sets of raw registry keys.
    deprecated_keys: set[str] = set()
    sunsetted_keys: set[str] = set()
    for section_name, grouped in (
        ("session_fields", False),
        ("event_fields", False),
        ("session_properties", True),
        ("event_properties", True),
    ):
        dep, sun = collect_lifecycle(data.get(section_name, {}), grouped)
        deprecated_keys |= dep
        sunsetted_keys |= sun

    yaml_name = yaml_path.name
    yaml_hash = hashlib.sha256(yaml_path.read_bytes()).hexdigest()[:12]

    header = textwrap.dedent(f"""\
        \"\"\"Auto-generated Polars type mappings from {yaml_name}.

        DO NOT EDIT MANUALLY. Regenerate with:
            uv run python scripts/sync_schema.py

        Source: {yaml_name} (sha256:{yaml_hash})
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
        "",
        "# " + "=" * 77,
        "# LIFECYCLE FLAGS",
        "# Raw registry keys (session + event, fields + properties) carrying",
        '# "deprecated: true" (consumers should stop reading the key) or',
        '# "sunsetted: true" (no shipping SDK emits it). The two compose freely.',
        "# " + "=" * 77,
        "",
        format_set("DEPRECATED_KEYS", deprecated_keys),
        "",
        "",
        format_set("SUNSETTED_KEYS", sunsetted_keys),
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
