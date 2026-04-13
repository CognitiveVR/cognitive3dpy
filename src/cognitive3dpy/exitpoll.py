"""Fetch exit poll response counts from the Cognitive3D API."""

from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING, Literal

import polars as pl

from cognitive3dpy._client import c3d_get, c3d_request
from cognitive3dpy._filters import build_filters
from cognitive3dpy._schema import empty_frame
from cognitive3dpy._transform import to_output, warn_if_empty
from cognitive3dpy.auth import get_project_id

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import pandas as pd


def c3d_exitpoll(
    project_id: int | None = None,
    hook: str | None = None,
    version: int | list[int] | None = None,
    exclude_test: bool = True,
    exclude_idle: bool = True,
    start_date: str | int | float | date | None = None,
    end_date: str | int | float | date | None = None,
    output: Literal["polars", "pandas"] = "polars",
    warn_empty: bool = True,
) -> pl.DataFrame | pd.DataFrame:
    """Fetch exit poll response counts from the Cognitive3D API.

    Parameters
    ----------
    project_id : int, optional
        Project ID. Defaults to the value set via :func:`c3d_project`.
    hook : str, optional
        Filter to a specific hook name (e.g. ``"end_questions"``).
        If ``None`` (default), retrieves all hooks.
    version : int or list[int], optional
        Filter to specific version number(s). If ``None`` (default),
        retrieves all versions.
    exclude_test : bool
        Exclude sessions tagged as test.
    exclude_idle : bool
        Exclude sessions tagged as junk/idle.
    start_date, end_date
        Date range. Accepts date/datetime objects, epoch timestamps, or
        flexible string formats. If neither is provided, queries all-time
        (no date filter applied).
    output : str
        ``"polars"`` or ``"pandas"``.
    warn_empty : bool
        If True (default), emit a UserWarning when 0 rows are returned.

    Returns
    -------
    pl.DataFrame or pd.DataFrame
        One row per response option per question per version per hook.
        Columns: ``hook``, ``version``, ``question_index``,
        ``question_title``, ``question_type``, ``value``,
        ``value_label``, ``count``.
    """
    if project_id is not None and (
        isinstance(project_id, bool) or not isinstance(project_id, int)
    ):
        raise TypeError(f"project_id must be an int, got {type(project_id).__name__!r}")
    if project_id is None:
        project_id = get_project_id()

    # Normalise version to a set for filtering (None = all versions)
    version_filter: set[int] | None = None
    if version is not None:
        version_filter = {version} if isinstance(version, int) else set(version)

    # Fetch all metadata in one call.
    # Response is a dict keyed by "hook:version" with full metadata per version.
    raw_all = c3d_get(f"/v0/questionSets?projectIds={project_id}")
    if not raw_all:
        if warn_empty:
            warn_if_empty(empty_frame("exitpoll"), "c3d_exitpoll")
        return to_output(empty_frame("exitpoll"), output)

    versions_meta = _parse_hook_metadata(raw_all)

    # Apply hook and version filters
    if hook is not None:
        versions_meta = [v for v in versions_meta if v["hook"] == hook]
    if version_filter is not None:
        versions_meta = [v for v in versions_meta if v["version"] in version_filter]

    if not versions_meta:
        if warn_empty:
            warn_if_empty(empty_frame("exitpoll"), "c3d_exitpoll")
        return to_output(empty_frame("exitpoll"), output)

    # Build session filters.
    filters = build_filters(
        start_date=start_date,
        end_date=end_date,
        exclude_test=exclude_test,
        exclude_idle=exclude_idle,
    )

    # Fetch response counts for each version.
    all_dfs: list[pl.DataFrame] = []
    for ver_info in versions_meta:
        raw_counts = _fetch_response_counts(
            project_id, ver_info["hook"], ver_info["version"], filters
        )
        if not raw_counts:
            continue

        df = _parse_responses(
            raw_counts, ver_info["version"], ver_info["hook"], ver_info["questions"]
        )
        if not df.is_empty():
            all_dfs.append(df)

    if not all_dfs:
        if warn_empty:
            warn_if_empty(empty_frame("exitpoll"), "c3d_exitpoll")
        return to_output(empty_frame("exitpoll"), output)

    return to_output(pl.concat(all_dfs, how="diagonal"), output)


def _parse_hook_metadata(raw: dict) -> list[dict]:
    """Parse the full questionSets dict response into a list of version dicts.

    The API returns a dict keyed by ``"hook:version"`` where each value contains
    full metadata including version number and questions. Each returned dict
    includes an embedded ``"questions"`` list so callers need only one list.
    """
    versions: list[dict] = []

    for ver_obj in raw.values():
        hook_name = ver_obj.get("name") or ver_obj.get("id", "").split(":")[0]
        ver_num = ver_obj.get("version")
        if ver_num is None or not hook_name:
            continue
        versions.append(
            {
                "hook": hook_name,
                "version": int(ver_num),
                "questions": [
                    {
                        "question_index": qi,
                        "question_title": (q.get("title") or "").strip(),
                        "question_type": (q.get("type") or "").lower(),
                        "answers": q.get("answers", []),
                    }
                    for qi, q in enumerate(ver_obj.get("questions", []), start=1)
                ],
            }
        )

    return versions


def _fetch_response_counts(
    project_id: int,
    hook: str,
    version: int,
    filters: list[dict],
) -> list | None:
    """POST response count query for a single version."""
    endpoint = (
        f"/v0/projects/{project_id}/questionSets/{hook}/{version}/responseCountQueries"
    )
    body = {
        "sessionFilters": [{"op": "and", "children": filters}] if filters else [],
    }
    return c3d_request(endpoint, body)


def _parse_responses(
    raw_counts: list,
    version_num: int,
    hook: str,
    questions_meta: list[dict],
) -> pl.DataFrame:
    """Parse a response count array into a tidy DataFrame."""
    n_process = min(len(raw_counts), len(questions_meta))
    if n_process == 0:
        return pl.DataFrame()

    if len(raw_counts) != len(questions_meta):
        logger.warning(
            "Response count length (%d) does not match question count (%d) "
            "for %s v%d. Processing %d questions.",
            len(raw_counts),
            len(questions_meta),
            hook,
            version_num,
            n_process,
        )

    rows: list[dict] = []
    for qi in range(n_process):
        q = questions_meta[qi]
        for resp in raw_counts[qi]:
            is_skipped = resp.get("skipped") or resp.get("value") is None
            if is_skipped:
                value_str = "skipped"
                label = "skipped"
            else:
                value_str = str(resp["value"])
                label = _map_value_label(value_str, q["question_type"], q["answers"])

            rows.append(
                {
                    "hook": hook,
                    "version": version_num,
                    "question_index": q["question_index"],
                    "question_title": q["question_title"],
                    "question_type": q["question_type"],
                    "value": value_str,
                    "value_label": label,
                    "count": int(resp.get("count") or 0),
                }
            )

    return pl.DataFrame(rows) if rows else pl.DataFrame()


_KNOWN_QUESTION_TYPES = {"boolean", "scale", "happysad", "multiple", "thumbs", "voice"}
_WARNED_QUESTION_TYPES: set[str] = set()


def _map_value_label(value: str, question_type: str, answers: list[dict]) -> str:
    """Map a raw response value to a human-readable label."""
    if question_type == "boolean":
        return {"0": "False", "1": "True"}.get(value, value)
    if question_type == "scale":
        return value
    if question_type == "happysad":
        return {"0": "Sad", "1": "Happy"}.get(value, value)
    if question_type == "multiple":
        try:
            idx = int(value)
            return str(answers[idx].get("answer", value))
        except (ValueError, IndexError):
            return value
    if question_type == "thumbs":
        return {"0": "Down", "1": "Up"}.get(value, value)
    if question_type == "voice":
        return {"0": "Responded"}.get(value, value)
    if (
        question_type not in _KNOWN_QUESTION_TYPES
        and question_type not in _WARNED_QUESTION_TYPES
    ):
        _WARNED_QUESTION_TYPES.add(question_type)
        logger.warning(
            "Unrecognised question type %r — value_label will be the raw value.",
            question_type,
        )
    return value
