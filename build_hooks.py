"""Hatchling custom build hook for schema generation.

Runs sync_schema.py at build time to regenerate _schema_generated.py
from slicer_fields.yaml if the YAML file is available. If the YAML is
not found (CI, external contributors), the committed generated file is
used as-is and the build proceeds normally.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

logger = logging.getLogger(__name__)

# Candidate locations for slicer_fields.yaml relative to repo root
_YAML_CANDIDATES = [
    "../cvr-cortex/doc/slicer_fields.yaml",
    "../cvr-slicer/doc/slicer_fields.yaml",
]


class CustomBuildHook(BuildHookInterface):
    PLUGIN_NAME = "schema-sync"

    def initialize(self, version, build_data):
        repo_root = Path(self.root)
        yaml_path = self._find_yaml(repo_root)

        if yaml_path is None:
            logger.info(
                "slicer_fields.yaml not found — using committed _schema_generated.py"
            )
            return

        script = repo_root / "scripts" / "sync_schema.py"
        if not script.is_file():
            logger.warning("sync_schema.py not found at %s — skipping", script)
            return

        logger.info("Syncing schema from %s", yaml_path)
        result = subprocess.run(
            [sys.executable, str(script), str(yaml_path)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logger.warning(
                "sync_schema.py failed (exit %d): %s",
                result.returncode,
                result.stderr.strip(),
            )
        else:
            logger.info(result.stdout.strip())

    @staticmethod
    def _find_yaml(repo_root: Path) -> Path | None:
        for candidate in _YAML_CANDIDATES:
            p = repo_root / candidate
            if p.is_file():
                return p.resolve()
        return None
